#!/bin/sh
# gc dolt health — Lightweight Dolt data-plane health report.
#
# Checks server status and latency through connect-fresh representative
# probes (majority verdict), connection-pool saturation, per-database
# commit counts and open beads, backup freshness, orphan databases, and
# zombie Dolt processes.
#
# Environment: GC_CITY_PATH, GC_DOLT_PORT, GC_DOLT_HOST, GC_DOLT_USER,
#              GC_DOLT_PASSWORD, GC_HEALTH_PROBE_ATTEMPTS
set -e

: "${GC_DOLT_USER:=root}"
PACK_DIR="${GC_PACK_DIR:-$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)}"
. "$PACK_DIR/assets/scripts/runtime.sh"

metadata_files() {
  printf '%s\n' "$GC_CITY_PATH/.beads/metadata.json"

  if command -v gc >/dev/null 2>&1; then
    # Bound the gc rig list call: if gc is itself in a bad state (the
    # failure mode this patrol is meant to detect) we must not block
    # here. Degrade to the fallback rig scan below.
    rig_paths=$(run_bounded 5 gc rig list --json 2>/dev/null \
      | if command -v jq >/dev/null 2>&1; then
          jq -r '.rigs[].path' 2>/dev/null
        else
          grep '"path"' | sed 's/.*"path": *"//;s/".*//'
        fi) || true
    if [ -n "$rig_paths" ]; then
      printf '%s\n' "$rig_paths" | while IFS= read -r p; do
        [ -n "$p" ] && printf '%s\n' "$p/.beads/metadata.json"
      done
      return
    fi
  fi

  # Fallback: scan local rigs/ directory only. Cannot discover external rigs
  # when gc is unavailable — acceptable degradation.
  find "$GC_CITY_PATH/rigs" -path '*/.beads/metadata.json' 2>/dev/null || true
}

metadata_db() {
  meta="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -r '.dolt_database // empty' "$meta" 2>/dev/null || true
    return
  fi
  grep -o '"dolt_database"[[:space:]]*:[[:space:]]*"[^"]*"' "$meta" 2>/dev/null | sed 's/.*: *"//;s/"$//' || true
}

json_output=false
data_dir="$DOLT_DATA_DIR"

while [ $# -gt 0 ]; do
  case "$1" in
    --json) json_output=true; shift ;;
    -h|--help)
      echo "Usage: gc dolt health [--json]"
      echo ""
      echo "Lightweight Dolt data-plane health report for patrol cycles."
      echo ""
      echo "Flags:"
      echo "  --json    Output as JSON (consumed by deacon patrol)"
      exit 0
      ;;
    *) echo "gc dolt health: unknown flag: $1" >&2; exit 1 ;;
  esac
done

# Note: run_bounded / TIMEOUT_BIN are provided by assets/scripts/runtime.sh.

# Determine host for probing.
host="${GC_DOLT_HOST:-127.0.0.1}"

# Check if server is running.
server_running=false
server_pid=0
server_latency=0
server_reachable=false
server_degraded=false

# Verdict probe counters. The probe makes several connect-fresh attempts
# of a representative query (see below) and requires a strict majority of
# successes for reachable=true. One attempt is not evidence in either
# direction: during the 2026-06 pool wedge a single trivial probe
# occasionally squeezed through the saturated wait queue and flipped the
# verdict to healthy while every real bd client was being rejected with
# "max waiting connections reached". Conversely, a single transient blip
# must not declare the data plane down. Majority-of-N gives sustained
# evidence both ways within one bounded invocation.
probe_attempts_target="${GC_HEALTH_PROBE_ATTEMPTS:-3}"
case "$probe_attempts_target" in
  ''|*[!0-9]*) probe_attempts_target=3 ;;
esac
[ "$probe_attempts_target" -lt 1 ] && probe_attempts_target=1
probe_attempts=0
probe_successes=0
probe_rejected=0
probe_timeouts=0
probe_errors=0
probe_last_error=""
probe_db=""

# Pool stats. active/max come from one bounded PROCESSLIST query;
# saturated is set by observed wait-queue rejections OR active>=90% of
# max. Pool saturation is the first-class signal this report previously
# lacked: a wedged pool can keep answering an occasional trivial probe
# while rejecting the steady-state client load.
pool_active=0
pool_max=0
pool_saturated=false
pool_probe_ok=false

# Per-database inventory probes that fail (timeout, rejection) are
# counted here and surface as probe_ok=false per entry — previously they
# were masked as a healthy-looking "0 commits".
db_probe_failures=0

# json_escape STR — sanitize STR for embedding in a JSON string value:
# strip control characters, escape backslashes and quotes, truncate.
json_escape() {
  printf '%s' "$1" | tr -d '\000-\037' | sed 's/\\/\\\\/g; s/"/\\"/g' | cut -c1-200
}

# Portable millisecond timestamp. BSD date(1) on macOS treats %N as a
# literal 'N' (exits 0, output like "1776740122N"), so the GNU-only
# || fallback never triggers. Feature-test the output instead.
now_ms() {
  _raw=$(date +%s%N 2>/dev/null)
  case "$_raw" in
    ''|*[!0-9]*) printf '%s000' "$(date +%s 2>/dev/null)" ;;
    *)        printf '%s' "$_raw" | cut -c1-13 ;;
  esac
}

# db_name_is_safe NAME — accept only identifiers safe to interpolate
# into SQL: first byte alnum/underscore, rest [A-Za-z0-9_-]. Shared by
# the verdict probe and the inventory loop below.
db_name_is_safe() {
  case "$1" in
    [A-Za-z0-9_]*)
      case "$1" in *[!A-Za-z0-9_-]*) return 1 ;; esac
      return 0
      ;;
  esac
  return 1
}

# db_name_is_system NAME — true for schemas the report must skip.
db_name_is_system() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    information_schema|mysql|dolt_cluster|performance_schema|sys|__gc_probe) return 0 ;;
  esac
  return 1
}

# Find dolt PID by port.
pid=$(managed_runtime_listener_pid "$GC_DOLT_PORT" || true)
if [ -n "$pid" ] || managed_runtime_tcp_reachable "$GC_DOLT_PORT"; then
  server_running=true
  [ -n "$pid" ] && server_pid="$pid"
  conn_args="--host $host --port $GC_DOLT_PORT --user $GC_DOLT_USER --no-tls"
  # Always export DOLT_CLI_PASSWORD (even empty) so the client does not
  # prompt for a password on stdin. Without this, the probe silently
  # fails with "Failed to parse credentials: operation not supported by
  # device" on sessions without a controlling TTY — which then left the
  # health report claiming "server: running" but never reporting
  # per-database detail.
  export DOLT_CLI_PASSWORD="${GC_DOLT_PASSWORD:-}"

  # Pick a representative database: the first user database on disk
  # (deterministic glob order). The verdict probe must run the same
  # shape of work real bd clients run — a fresh client connection plus
  # a query that touches the database's storage layer (dolt_log reads
  # the commit graph). A bare SELECT 1 proves only that the SQL engine
  # can echo a constant: during the 2026-06 wedge it completed
  # instantly whenever it squeezed through the saturated pool, while
  # every table-touching client query ran 30+ minutes.
  if [ -d "$data_dir" ]; then
    for d in "$data_dir"/*/; do
      [ ! -d "$d/.dolt" ] && continue
      name="$(basename "$d")"
      db_name_is_system "$name" && continue
      db_name_is_safe "$name" || continue
      probe_db="$name"
      break
    done
  fi
  if [ -n "$probe_db" ]; then
    probe_query="USE \`$probe_db\`; SELECT COUNT(*) FROM dolt_log;"
  else
    # Fresh city with no databases yet: connectivity is all there is
    # to measure.
    probe_query="SELECT 1"
  fi

  # Bound each attempt. A TCP-reachable but unresponsive server (stuck
  # goroutine, saturated pool, migration lock) would otherwise hang.
  # Each `dolt sql` invocation is a fresh client process and therefore
  # a fresh connection — the same connect path that surfaced
  # "max waiting connections reached" to real clients.
  _probe_err=$(mktemp)
  while [ "$probe_attempts" -lt "$probe_attempts_target" ]; do
    probe_attempts=$((probe_attempts + 1))
    attempt_start=$(now_ms)
    if run_bounded 5 dolt $conn_args sql -q "$probe_query" >/dev/null 2>"$_probe_err"; then
      attempt_end=$(now_ms)
      attempt_ms=$((attempt_end - attempt_start))
      [ "$attempt_ms" -lt 0 ] && attempt_ms=0
      probe_successes=$((probe_successes + 1))
      # Report the slowest successful attempt: the pessimistic bound on
      # what a fresh client experiences right now.
      [ "$attempt_ms" -gt "$server_latency" ] && server_latency="$attempt_ms"
    else
      probe_rc=$?
      if [ "$probe_rc" -eq 124 ]; then
        probe_timeouts=$((probe_timeouts + 1))
        probe_last_error="probe timed out after 5s"
      elif grep -qiE 'max waiting connections|too many connections' "$_probe_err" 2>/dev/null; then
        probe_rejected=$((probe_rejected + 1))
        probe_last_error=$(head -1 "$_probe_err" 2>/dev/null || true)
      else
        probe_errors=$((probe_errors + 1))
        probe_last_error=$(head -1 "$_probe_err" 2>/dev/null || true)
      fi
    fi
  done
  rm -f "$_probe_err"

  # Strict majority of attempts must succeed. successes*2 > attempts is
  # exact integer math for >50%.
  if [ $((probe_successes * 2)) -gt "$probe_attempts" ]; then
    server_reachable=true
  fi

  # Observed wait-queue rejections are direct evidence of saturation —
  # they are what real clients hit during the wedge.
  [ "$probe_rejected" -gt 0 ] && pool_saturated=true

  # Pool stats through one more bounded fresh connection. Run even when
  # the verdict is unreachable: if this squeezes through it captures the
  # smoking gun (active≈max) in the same report the escalation cites.
  pool_csv=$(run_bounded 5 dolt $conn_args sql --result-format csv \
    -q "SELECT COUNT(*) AS active, @@GLOBAL.max_connections AS max_conn FROM information_schema.PROCESSLIST" 2>/dev/null || true)
  pool_line=$(printf '%s\n' "$pool_csv" | grep -E '^[0-9]+,[0-9]+$' | head -1)
  if [ -n "$pool_line" ]; then
    pool_probe_ok=true
    pool_active="${pool_line%%,*}"
    pool_max="${pool_line##*,}"
    if [ "$pool_max" -gt 0 ] && [ $((pool_active * 100)) -ge $((pool_max * 90)) ]; then
      pool_saturated=true
    fi
  fi
fi

# Cache metadata file paths once (avoids repeated gc calls and word-splitting).
_meta_cache=$(mktemp)
metadata_files > "$_meta_cache"
trap 'rm -f "$_meta_cache"' EXIT

# Collect database info.
#
# NOTE: we must NOT invoke `dolt log` against the on-disk database
# directory while the sql-server holds it open. Historically this was
# done with `cd "$d" && dolt log --oneline | wc -l`; on an active DB
# the client contends with the server for Dolt's file locks and the
# client process blocks indefinitely, orphaning zombie `dolt log`
# processes and wedging the health CLI. Query the running server via
# SQL instead — it's the authoritative source, never deadlocks with
# itself, and is cheap (dolt_log is indexed by commit hash).
db_info=""
if [ -d "$data_dir" ] && [ "$server_reachable" = true ]; then
  for d in "$data_dir"/*/; do
    [ ! -d "$d/.dolt" ] && continue
    name="$(basename "$d")"
    db_name_is_system "$name" && continue
    # Reject names with anything outside [A-Za-z0-9_-] before interpolating
    # into the SQL identifier. The first byte must still be alnum/underscore
    # to avoid option-shaped names. Dolt permits directory names that shell
    # basename happily returns (e.g. backticks, semicolons) but which
    # would break out of the identifier and execute attacker-chosen SQL
    # as the patrol user. Not an external-attack surface today — data
    # directories are server-controlled — but fragile enough under
    # config drift that it's worth skipping rather than probing.
    db_name_is_safe "$name" || continue
    # Count commits via SQL (bounded). Extract the first fully-numeric
    # line rather than `sed -n '2p'`: future dolt builds may emit a
    # status row for `USE` or a warning banner, in which case positional
    # parsing silently collapses the count to 0 and the "empty repo"
    # fallback masks the parse miss. Numeric-line grep gives a
    # deterministic result or clearly-failed parse.
    commits_csv=$(run_bounded 5 dolt $conn_args sql --result-format csv \
      -q "USE \`$name\`; SELECT COUNT(*) FROM dolt_log;" 2>/dev/null || true)
    commits=$(printf '%s\n' "$commits_csv" | grep -E '^[0-9]+$' | head -1)
    # JSON consumers (deacon patrol) require a number, so failures still
    # emit commits=0 — but they are flagged probe_ok=false and counted,
    # never silently conflated with an empty repo. During the 2026-06
    # wedge every per-database count timed out and the report showed a
    # plausible-looking inventory of zeros.
    db_probe_ok=true
    case "$commits" in
      ''|*[!0-9]*)
        commits=0
        db_probe_ok=false
        db_probe_failures=$((db_probe_failures + 1))
        ;;
    esac
    # Count open beads (best-effort).
    open_beads=0
    while IFS= read -r meta; do
      [ -f "$meta" ] || continue
      db=$(metadata_db "$meta")
      if [ "$db" = "$name" ]; then
        beads_dir="$(dirname "$meta")"
        if [ -f "$beads_dir/beads.jsonl" ]; then
          open_beads=$(grep -c '"status":"open"' "$beads_dir/beads.jsonl" 2>/dev/null || echo 0)
        fi
        break
      fi
    done < "$_meta_cache"
    db_info="$db_info$name|$commits|$open_beads|$db_probe_ok
"
  done
fi

# Degraded: the server answered (reachable) but fresh clients are not
# getting clean service — failed verdict attempts, a saturated pool, a
# failed pool-stats probe, or failing per-database probes. Reachable
# consumers treat this as a warning, not an outage.
if [ "$server_reachable" = true ]; then
  if [ "$probe_successes" -lt "$probe_attempts" ] \
    || [ "$pool_saturated" = true ] \
    || [ "$pool_probe_ok" != true ] \
    || [ "$db_probe_failures" -gt 0 ]; then
    server_degraded=true
  fi
fi

# Check backup freshness.
backup_freshness=""
backup_stale=false
backup_age_sec=0
newest_backup=$(ls -1d "$GC_CITY_PATH"/migration-backup-* 2>/dev/null | sort -r | head -1 || true)
if [ -n "$newest_backup" ]; then
  backup_mtime=$(stat -c %Y "$newest_backup" 2>/dev/null || stat -f %m "$newest_backup" 2>/dev/null || echo 0)
  now=$(date +%s)
  backup_age_sec=$((now - backup_mtime))
  if [ "$backup_age_sec" -ge 3600 ]; then
    backup_freshness="$((backup_age_sec / 3600))h$((backup_age_sec % 3600 / 60))m"
  elif [ "$backup_age_sec" -ge 60 ]; then
    backup_freshness="$((backup_age_sec / 60))m$((backup_age_sec % 60))s"
  else
    backup_freshness="${backup_age_sec}s"
  fi
  [ "$backup_age_sec" -gt 1800 ] && backup_stale=true
fi

# Find orphan databases.
orphan_list=""
orphan_count=0
if [ -d "$data_dir" ]; then
  referenced=""
  while IFS= read -r meta; do
    [ -f "$meta" ] || continue
    db=$(metadata_db "$meta")
    [ -n "$db" ] && referenced="$referenced $db "
  done < "$_meta_cache"
  for d in "$data_dir"/*/; do
    [ ! -d "$d/.dolt" ] && continue
    name="$(basename "$d")"
    case "$(printf '%s' "$name" | tr '[:upper:]' '[:lower:]')" in information_schema|mysql|dolt_cluster|performance_schema|sys|__gc_probe) continue ;; esac
    case "$referenced" in *" $name "*) continue ;; esac
    size_kb=$(du -sk "$d" 2>/dev/null | cut -f1)
    size_bytes=$(( ${size_kb:-0} * 1024 ))
    if [ "$size_bytes" -ge 1048576 ]; then
      size=$(awk "BEGIN {printf \"%.1f MB\", $size_bytes/1048576}")
    elif [ "$size_bytes" -ge 1024 ]; then
      size=$(awk "BEGIN {printf \"%.1f KB\", $size_bytes/1024}")
    else
      size="${size_bytes} B"
    fi
    orphan_list="$orphan_list$name|$size
"
    orphan_count=$((orphan_count + 1))
  done
fi

# Check for zombie dolt processes.
# Use pgrep -x to match only processes named "dolt", then verify
# each is actually running sql-server via ps. This avoids false
# positives from processes that merely mention "dolt" in their args
# (e.g., Claude sessions whose prompt text contains "dolt sql-server").
#
# Rig-local Dolt servers (configured via dolt.port in config.yaml)
# are legitimate — exclude any PID listening on a known rig port.
#
# GC_HEALTH_SKIP_ZOMBIE_SCAN is a test-only escape hatch. Zombie
# enumeration spawns one `ps` per matching process, which on shared
# dev machines with many accumulated dolt processes dominates the
# runtime of the hang-mode test below. Setting it to "1" skips the
# scan so tests exercise just the bounded-probe behavior they care
# about without being hostage to ambient process state.
zombie_count=0
zombie_pids=""
if [ "${GC_HEALTH_SKIP_ZOMBIE_SCAN:-0}" != "1" ]; then
  # Collect PIDs of legitimate rig-local Dolt servers.
  rig_dolt_pids=""
  while IFS= read -r meta; do
    [ -f "$meta" ] || continue
    config_file="$(dirname "$meta")/config.yaml"
    [ -f "$config_file" ] || continue
    rig_port=$(grep '^dolt\.port:' "$config_file" 2>/dev/null | sed "s/^dolt\\.port:[[:space:]]*//; s/[[:space:]]*#.*$//; s/['\\\"]//g; s/[[:space:]]*$//" | head -1)
    case "$rig_port" in ''|*[!0-9]*) continue ;; esac
    [ "$rig_port" = "$GC_DOLT_PORT" ] && continue
    rig_pid=$(managed_runtime_listener_pid "$rig_port" || true)
    [ -n "$rig_pid" ] && rig_dolt_pids="$rig_dolt_pids $rig_pid "
  done < "$_meta_cache"

  # Enumerate the process table ONCE, not one `ps -p <pid> -o args=` fork per
  # `pgrep -x dolt` match. pgrep matches every dolt-named process including
  # Z-state zombies, so under a non-reaping PID 1 the old per-PID fork became
  # an O(zombies) `ps` storm re-paid on every 30s health tick (#2482). Collect
  # the candidate PIDs from pgrep, then classify them in a single `ps`+`awk`
  # pass: keep candidates that are dolt sql-server processes, skip Z-state
  # zombies (a defunct dolt never carries sql-server args anyway), and exclude
  # the managed city server and rig-local dolts.
  candidate_pids=" $(pgrep -x dolt 2>/dev/null | tr '\n' ' ' || true)"
  matched_pids=$(ps -eo pid=,stat=,args= 2>/dev/null | awk \
    -v server="$server_pid" -v rigs="$rig_dolt_pids" -v cands="$candidate_pids" '
    BEGIN {
      # Build an O(1) lookup set from the pgrep candidates once. The
      # per-row membership test below was an index() substring scan
      # re-paid for every process-table row, i.e. O(rows x candidate
      # string length); the reported incident had ~41k candidate PIDs
      # (#2618). Splitting into an associative set makes each lookup O(1).
      n = split(cands, a, " ")
      for (i = 1; i <= n; i++) if (a[i] != "") cand[a[i]] = 1
    }
    {
      pid = $1
      if (!(pid in cand)) next                   # not a pgrep -x dolt match
      if (pid == server) next                     # the managed city server
      if (index(rigs, " " pid " ") != 0) next     # a configured rig-local dolt
      if ($2 ~ /Z/) next                          # Z-state zombie: never a server
      if (index($0, "sql-server") == 0) next      # not a dolt sql-server
      print pid
    }' || true)
  for p in $matched_pids; do
    zombie_count=$((zombie_count + 1))
    zombie_pids="$zombie_pids $p"
  done
fi

# Output.
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

if [ "$json_output" = true ]; then
  # Build JSON output. `server.reachable` reports whether a strict
  # majority of connect-fresh representative-query probes succeeded.
  # Consumers (deacon patrol) should key health off `server.reachable`,
  # not `server.running`, because a process can hold the port while its
  # goroutines are wedged — and should treat `server.degraded` /
  # `server.pool.saturated` as early warning that real clients are
  # already failing even though the verdict is still reachable.
  #
  # Scalar server fields stay ahead of the nested probe/pool objects:
  # the jq-less fallback parser in `gc dolt health-check` extracts them
  # from the range between `"server":` and the first `}`.
  cat <<JSONEOF
{
  "timestamp": "$timestamp",
  "server": {
    "running": $server_running,
    "reachable": $server_reachable,
    "degraded": $server_degraded,
    "pid": $server_pid,
    "port": $GC_DOLT_PORT,
    "latency_ms": $server_latency,
    "probe": {
      "attempts": $probe_attempts,
      "successes": $probe_successes,
      "rejected": $probe_rejected,
      "timeouts": $probe_timeouts,
      "errors": $probe_errors,
      "database": "$(json_escape "$probe_db")",
      "last_error": "$(json_escape "$probe_last_error")"
    },
    "pool": {
      "active_connections": $pool_active,
      "max_connections": $pool_max,
      "saturated": $pool_saturated,
      "probe_ok": $pool_probe_ok
    }
  },
  "databases": [
JSONEOF
  first=true
  echo "$db_info" | while IFS='|' read -r name commits open_beads db_ok; do
    [ -z "$name" ] && continue
    if [ "$first" = true ]; then first=false; else echo ","; fi
    printf '    {"name": "%s", "commits": %s, "open_beads": %s, "probe_ok": %s}' "$name" "$commits" "$open_beads" "${db_ok:-true}"
  done
  cat <<JSONEOF

  ],
  "backups": {
    "dolt_freshness": "$backup_freshness",
    "dolt_age_sec": $backup_age_sec,
    "dolt_stale": $backup_stale
  },
  "orphans": [
JSONEOF
  first=true
  echo "$orphan_list" | while IFS='|' read -r name size; do
    [ -z "$name" ] && continue
    if [ "$first" = true ]; then first=false; else echo ","; fi
    printf '    {"name": "%s", "size": "%s"}' "$name" "$size"
  done
  cat <<JSONEOF

  ],
  "processes": {
    "zombie_count": $zombie_count,
    "zombie_pids": [$(echo "$zombie_pids" | tr -s ' ' ',' | sed 's/^,//;s/,$//')]
  }
}
JSONEOF
  # JSON mode always exits 0 when the payload is well-formed. Health
  # state is signalled in-band via `server.reachable` (and the rest of
  # the document). Automation that parses the JSON — notably the deacon
  # patrol formula — must not fail before stdout is parsed just because
  # the server is down; that's exactly the condition the patrol is
  # supposed to detect and react to. Callers that want exit-code
  # signalling should use the human-readable form.
  exit 0
fi

# Human-readable output.
if [ "$server_running" = true ]; then
  echo "Server: running (PID $server_pid, port $GC_DOLT_PORT, latency ${server_latency}ms)"
else
  echo "Server: not running"
fi

if [ "$probe_attempts" -gt 0 ]; then
  probe_target_label="${probe_db:-SELECT 1}"
  probe_detail=""
  if [ "$probe_successes" -lt "$probe_attempts" ]; then
    probe_detail=" [rejected=$probe_rejected timeouts=$probe_timeouts errors=$probe_errors]"
    [ -n "$probe_last_error" ] && probe_detail="$probe_detail $probe_last_error"
  fi
  echo "Probe: $probe_successes/$probe_attempts attempts ok ($probe_target_label)$probe_detail"
fi

if [ "$pool_probe_ok" = true ]; then
  pool_flag=""
  [ "$pool_saturated" = true ] && pool_flag=" [SATURATED]"
  echo "Pool: $pool_active/$pool_max active connections$pool_flag"
elif [ "$pool_saturated" = true ]; then
  echo "Pool: stats unavailable [SATURATED: wait-queue rejections observed]"
fi

if [ -n "$db_info" ]; then
  echo ""
  echo "Databases:"
  echo "$db_info" | while IFS='|' read -r name commits open_beads db_ok; do
    [ -z "$name" ] && continue
    if [ "$db_ok" = "false" ]; then
      echo "  $name: probe failed"
    else
      echo "  $name: $commits commits, $open_beads open beads"
    fi
  done
fi

if [ -n "$backup_freshness" ]; then
  stale=""
  [ "$backup_stale" = true ] && stale=" [STALE]"
  echo ""
  echo "Backups: ${backup_freshness} ago${stale}"
else
  echo ""
  echo "Backups: none found"
fi

if [ "$orphan_count" -gt 0 ]; then
  echo ""
  echo "Orphans: $orphan_count"
  echo "$orphan_list" | while IFS='|' read -r name size; do
    [ -z "$name" ] && continue
    echo "  $name ($size)"
  done
fi

if [ "$zombie_count" -gt 0 ]; then
  echo ""
  echo "Zombie processes: $zombie_count (PIDs:$zombie_pids)"
fi

# Exit status (human mode only): 0 when the data plane is healthy
# (server running AND a majority of representative probes answered).
# Non-zero signals a CLI caller that something is wrong — server not
# running, port in use by a process that isn't speaking MySQL, or a
# pool rejecting fresh clients. Degraded-but-reachable, stale backups,
# orphans, and zombies are informational and do not fail the exit code.
#
# JSON mode is unconditionally exit 0 (see above) — programmatic
# consumers read `server.reachable` from the payload instead.
if [ "$server_reachable" = true ]; then
  exit 0
fi
exit 1
