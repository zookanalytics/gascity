#!/usr/bin/env bash
# mol-dog-doctor — probe Dolt server health and report findings.
#
# Converted from the former mol-dog-doctor formula. All checks are read-only: SQL probe,
# PROCESSLIST count, disk usage, orphan DB detection, backup artifact freshness.
# No LLM judgment needed — runs inline in the controller.
#
# Runs as an exec order (no LLM, no agent, no wisp).
#
# RPO note: BACKUP_STALE_S (default 43200 = 12h = 2x backup interval) is the
# threshold at which backup artifact age triggers a [WARN: backup stale] advisory.
# With 6h backup syncs and fail-closed journal corruption recovery, maximum data
# loss on journal corruption without manual intervention is one 6h backup interval.
# If BACKUP_STALE_S exceeds 2x the backup interval, a single missed backup cycle
# is undetected. Keep BACKUP_STALE_S <= 2x the configured backup order
# interval (`interval` in orders/mol-dog-backup.toml, currently 6h).
set -euo pipefail

PACK_DIR="${GC_PACK_DIR:-$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
. "$PACK_DIR/assets/scripts/runtime.sh"
. "$PACK_DIR/assets/scripts/latency.sh"
. "$PACK_DIR/assets/scripts/advisory_state.sh"
. "$PACK_DIR/assets/scripts/_notify.sh"

PORT="$GC_DOLT_PORT"
HOST="${GC_DOLT_HOST:-127.0.0.1}"
USER="${GC_DOLT_USER:-root}"
# Latency warn threshold in milliseconds. GC_DOCTOR_LATENCY_WARN_MS takes
# precedence; otherwise derive from the legacy seconds knob (default 1s ->
# 1000ms) for backward compatibility.
LATENCY_WARN_MS="${GC_DOCTOR_LATENCY_WARN_MS:-$(( ${GC_DOCTOR_LATENCY_WARN_S:-1} * 1000 ))}"
CONN_WARN_PCT="${GC_DOCTOR_CONN_WARN_PCT:-80}"
BACKUP_STALE_S="${GC_DOCTOR_BACKUP_STALE_S:-43200}"  # 2x 6h backup interval
BACKUP_ARTIFACT_DIR="${GC_BACKUP_ARTIFACT_DIR:-$GC_CITY_PATH/.dolt-backup}"
# Advisory dedup state (#3409): records the signature of the last-sent [MEDIUM]
# advisory so a persistent condition collapses into one rolling alert instead of
# a fresh bead every 5-min tick. DOLT_STATE_DIR is set by runtime.sh.
ADVISORY_STATE_FILE="${GC_DOCTOR_ADVISORY_STATE_FILE:-$DOLT_STATE_DIR/doctor-advisory-state}"
# Advisory compaction (gc-00rcf): the dedup state above stops NEW duplicates;
# compaction archives EXISTING open advisory wisps that the state file cannot
# reach — the pre-dedup pile and advisories left open by a now-superseded
# condition set. Recipient mirrors escalate.sh's default so compaction targets
# exactly what the advisories were addressed to; the prefix matches the [MEDIUM]
# advisory subject without matching the CRITICAL "server unreachable" escalation.
ADVISORY_RECIPIENT="${GC_ESCALATION_RECIPIENT:-human}"
ADVISORY_SUBJECT_PREFIX="Dolt health advisory"

dolt_sql() {
    DOLT_CLI_PASSWORD="${GC_DOLT_PASSWORD:-}" \
        run_bounded 10 \
        dolt --host "$HOST" --port "$PORT" --user "$USER" --no-tls sql "$@"
}

# CONN_MAX: explicit override > server @@GLOBAL.max_connections > fallback.
if [ -n "${GC_DOCTOR_CONN_MAX:-}" ]; then
    CONN_MAX="$GC_DOCTOR_CONN_MAX"
else
    _server_max=$(dolt_sql -r csv -q "SELECT @@GLOBAL.max_connections" 2>/dev/null | tail -1 || true)
    case "${_server_max:-}" in
        ''|*[!0-9]*) CONN_MAX=256 ;;
        *) CONN_MAX="$_server_max" ;;
    esac
    unset _server_max
fi

file_mtime() {
    file_path="$1"
    file_mtime_value=$(stat -c %Y "$file_path" 2>/dev/null \
        || stat -f %m "$file_path" 2>/dev/null || echo "0")
    case "$file_mtime_value" in
        ''|*[!0-9]*) file_mtime_value=0 ;;
    esac
    printf '%s\n' "$file_mtime_value"
}

backup_path_matches_db() {
    db_name="$1"
    backup_rel_path="$2"
    case "$backup_rel_path" in
        "$db_name"|"$db_name"/*|"$db_name".*|"$db_name"-*|*"/$db_name"|*"/$db_name"/*|*"/$db_name".*|*"/$db_name"-*)
            return 0
            ;;
    esac
    return 1
}

newest_backup_mtime_for_db() {
    db_name="$1"
    newest_mtime=0
    while IFS= read -r -d '' backup_path; do
        backup_rel_path="${backup_path#$BACKUP_ARTIFACT_DIR/}"
        if backup_path_matches_db "$db_name" "$backup_rel_path"; then
            backup_mtime=$(file_mtime "$backup_path")
            if [ "$backup_mtime" -gt "$newest_mtime" ]; then
                newest_mtime="$backup_mtime"
            fi
        fi
    done < <(find -L "$BACKUP_ARTIFACT_DIR" -type f -print0 2>/dev/null)
    printf '%s\n' "$newest_mtime"
}

# newest_backup_mtime_in_dir TARGET_DIR — return the newest mtime of any
# file under TARGET_DIR (recursively), or 0 if the directory is missing
# or empty. Used for Path A freshness checks where the backup URL points
# at a directory dedicated to a single database, so no name prefixing is
# required.
newest_backup_mtime_in_dir() {
    target_dir="$1"
    if [ ! -d "$target_dir" ]; then
        printf '0\n'
        return 0
    fi
    newest_mtime=0
    while IFS= read -r -d '' backup_path; do
        backup_mtime=$(file_mtime "$backup_path")
        if [ "$backup_mtime" -gt "$newest_mtime" ]; then
            newest_mtime="$backup_mtime"
        fi
    done < <(find -L "$target_dir" -type f -print0 2>/dev/null)
    printf '%s\n' "$newest_mtime"
}

# named_backup_url_for_db DB — print the URL of the <db>-backup named
# Dolt backup, or nothing if no such backup is configured. Parses the
# verbose `dolt backup -v` output (format: "<name> <url> {<params>}").
# Mirrors the auto-discovery used by mol-dog-backup.sh so the doctor's
# source of truth matches the backup script's.
named_backup_url_for_db() {
    db_name="$1"
    db_dir="$DOLT_DATA_DIR/$db_name"
    if [ ! -d "$db_dir/.dolt" ]; then
        return 0
    fi
    (cd "$db_dir" && run_bounded 10 dolt backup -v 2>/dev/null) \
        | awk -v want="${db_name}-backup" '$1 == want {print $2; exit}' \
        || true
}

append_backup_stale() {
    backup_stale_item="$1"
    if [ -n "$BACKUP_STALE_ITEMS" ]; then
        BACKUP_STALE_ITEMS="$BACKUP_STALE_ITEMS, $backup_stale_item"
    else
        BACKUP_STALE_ITEMS="$backup_stale_item"
    fi
}

send_escalation() {
    local subject="$1"
    local message="$2"
    local err
    if ! err=$(dolt_escalate "$subject" "$message" 2>&1 >/dev/null); then
        if [ -n "$err" ]; then
            echo "doctor: escalation failed: $err" >&2
        else
            echo "doctor: escalation failed" >&2
        fi
        return 1
    fi
}

# --- Step 1: Probe connectivity and measure latency ---

PROBE_START_MS=$(now_ms)
if ! dolt_sql -q "SELECT active_branch()" >/dev/null 2>&1; then
    if send_escalation \
        "ESCALATION: Dolt server unreachable on port $PORT [CRITICAL]" \
        "Doctor probe failed: server did not respond to active_branch() query."; then
        dolt_notify_done "doctor — server: UNREACHABLE (escalated)"
        echo "doctor: server unreachable on port $PORT (escalated)"
    else
        dolt_notify_done "doctor — server: UNREACHABLE (escalation failed)"
        echo "doctor: server unreachable on port $PORT (escalation failed)"
    fi
    exit 0
fi
PROBE_END_MS=$(now_ms)
# latency_delta (latency.sh) guards the subtraction so a zero, stale, or
# wrong-precision now_ms reading cannot surface an impossible epoch-scale
# latency in the advisory; an untrustworthy probe reports 0ms this tick.
LATENCY_MS=$(latency_delta "$PROBE_START_MS" "$PROBE_END_MS")
LATENCY_WARN=""
if latency_should_warn "$LATENCY_MS" "$LATENCY_WARN_MS"; then
    LATENCY_WARN=" [WARN: latency ${LATENCY_MS}ms >= threshold ${LATENCY_WARN_MS}ms]"
fi

# --- Step 2: Check resource conditions ---

CONN_COUNT=$(dolt_sql -r csv -q "SELECT COUNT(*) FROM information_schema.PROCESSLIST" 2>/dev/null \
    | tail -1 || echo "0")
CONN_WARN=""
CONN_WARN_AT=$(( (CONN_MAX * CONN_WARN_PCT) / 100 ))
if [ "${CONN_COUNT:-0}" -ge "$CONN_WARN_AT" ]; then
    CONN_WARN=" [WARN: ${CONN_COUNT} connections >= ${CONN_WARN_PCT}% of max ${CONN_MAX}]"
fi

# Disk usage of Dolt data directory.
DISK_USAGE=$(du -sh "$DOLT_DATA_DIR" 2>/dev/null | cut -f1 || echo "unknown")

# Orphan database detection.
ALL_DBS=$(dolt_sql -r csv -q "SHOW DATABASES" 2>/dev/null | tail -n +2 || true)
ORPHAN_PATTERNS="^(testdb_|beads_t|beads_pt|beads_vr|doctest_|doctortest_)"
SYSTEM_DBS="^(information_schema|mysql|dolt_cluster|__gc_probe|performance_schema|sys)$"
USER_DBS=$(printf '%s\n' "$ALL_DBS" | grep -viE "$SYSTEM_DBS" || true)
ORPHANS=$(printf '%s\n' "$USER_DBS" | grep -iE "$ORPHAN_PATTERNS" || true)
ORPHAN_COUNT=$(printf '%s\n' "$ORPHANS" | awk 'NF {count++} END {print count + 0}')
ORPHAN_WARN=""
if [ "${ORPHAN_COUNT:-0}" -gt 0 ]; then
    ORPHAN_WARN=" [WARN: $ORPHAN_COUNT orphan DBs detected — run gc dolt cleanup]"
fi

# Backup freshness: every user database with a .dolt dir is in scope.
# DBs without a configured <db>-backup remote are reported as a coverage
# gap rather than silently excluded — the exclusion is how unconfigured
# production DBs went unbacked-up until journal corruption made them
# unrecoverable (#3176). mol-dog-backup.sh auto-configures the remote on
# its next run, so this warning self-heals unless the backup dog itself
# is failing. For each eligible DB, freshness is checked at the per-DB
# named backup URL discovered via `dolt backup -v` (Path A); fall back to
# the legacy local artifact dir (Path B) when no URL is discovered, for
# back-compat with Path B-only configs.
BACKUP_ELIGIBLE_DBS=""
BACKUP_STALE_ITEMS=""
for db in $USER_DBS; do
    db_dir="$DOLT_DATA_DIR/$db"
    if [ -d "$db_dir/.dolt" ]; then
        if (cd "$db_dir" && run_bounded 30 dolt backup 2>/dev/null | awk '{print $1}' | grep -qx "${db}-backup"); then
            BACKUP_ELIGIBLE_DBS="$BACKUP_ELIGIBLE_DBS $db"
        else
            append_backup_stale "$db backup remote missing"
        fi
    fi
done
BACKUP_ELIGIBLE_DBS=$(printf '%s\n' "$BACKUP_ELIGIBLE_DBS" | tr ' ' '\n' | grep -v '^$' || true)

BACKUP_STALE=""
if [ -n "$BACKUP_ELIGIBLE_DBS" ]; then
    NOW_S=$(date +%s)
    LEGACY_DIR_EXISTS=0
    if [ -d "$BACKUP_ARTIFACT_DIR" ]; then
        LEGACY_DIR_EXISTS=1
    fi
    for db in $BACKUP_ELIGIBLE_DBS; do
        backup_url=$(named_backup_url_for_db "$db")
        case "$backup_url" in
            file://*)
                # Path A: freshness lives at the named-backup URL.
                NEWEST_BACKUP_MTIME=$(newest_backup_mtime_in_dir "${backup_url#file://}")
                ;;
            '')
                # No URL surfaced via `dolt backup -v` (shouldn't
                # happen since the DB is eligible, but defensive).
                # Fall back to Path B if available, else skip silently.
                if [ "$LEGACY_DIR_EXISTS" -eq 1 ]; then
                    NEWEST_BACKUP_MTIME=$(newest_backup_mtime_for_db "$db")
                else
                    continue
                fi
                ;;
            *)
                # Remote URL (s3://, http://, gs://, etc.). Remote
                # freshness is the remote's problem; the doctor must
                # not reach external services from a 5-minute probe.
                continue
                ;;
        esac
        if [ "$NEWEST_BACKUP_MTIME" -le 0 ]; then
            append_backup_stale "$db backup missing"
            continue
        fi
        BACKUP_AGE=$((NOW_S - NEWEST_BACKUP_MTIME))
        if [ "$BACKUP_AGE" -gt "$BACKUP_STALE_S" ]; then
            append_backup_stale "$db backup is $((BACKUP_AGE / 3600))h old"
        fi
    done
fi
if [ -n "$BACKUP_STALE_ITEMS" ]; then
    BACKUP_STALE="$BACKUP_STALE [WARN: backup freshness: $BACKUP_STALE_ITEMS]"
fi

# --- Step 3: Compose report and escalate if critical ---

WARNINGS="${LATENCY_WARN}${CONN_WARN}${ORPHAN_WARN}${BACKUP_STALE}"
if [ -n "$WARNINGS" ]; then
    # Dedup (#3409): key on which conditions are active — not their tick-volatile
    # values (exact latency ms, connection count, backup age) — and re-send only
    # when that set changes. Record after a successful send so a failed
    # escalation retries next tick. The CRITICAL "server unreachable" path above
    # is never deduped, so a true outage always alerts.
    ADVISORY_SIG=""
    if [ -n "$LATENCY_WARN" ]; then ADVISORY_SIG="${ADVISORY_SIG}latency "; fi
    if [ -n "$CONN_WARN" ]; then ADVISORY_SIG="${ADVISORY_SIG}conn "; fi
    if [ -n "$ORPHAN_WARN" ]; then ADVISORY_SIG="${ADVISORY_SIG}orphan "; fi
    if [ -n "$BACKUP_STALE" ]; then ADVISORY_SIG="${ADVISORY_SIG}backup "; fi
    if advisory_changed "$ADVISORY_SIG" "$ADVISORY_STATE_FILE"; then
        # Supersede: archive any prior open advisories before sending the fresh
        # one, so a changed condition set leaves exactly one current advisory
        # open instead of stacking a new wisp on top of the superseded ones.
        advisory_compact "$ADVISORY_RECIPIENT" "$ADVISORY_SUBJECT_PREFIX"
        if send_escalation \
            "Dolt health advisory [MEDIUM]" \
            "Latency: ${LATENCY_MS}ms${LATENCY_WARN}
Connections: ${CONN_COUNT}/${CONN_MAX}${CONN_WARN}
Disk: ${DISK_USAGE}
Orphan DBs: ${ORPHAN_COUNT}${ORPHAN_WARN}${BACKUP_STALE}"; then
            advisory_record "$ADVISORY_SIG" "$ADVISORY_STATE_FILE"
        fi
    else
        # Steady warning (unchanged signature) — the common state once a
        # persistent condition has its signature on file. The send-time dedup
        # correctly suppresses a re-send, but neither the supersede above nor the
        # healthy drain below runs here, so a pre-dedup pile (or advisories left
        # by a prior run before this signature was recorded) would stay open for
        # the life of a condition that never clears, e.g. orphan DBs. Drain the
        # duplicates while keeping the single current advisory (--keep-newest 1)
        # so the operator still sees one standing alert. Idempotent: once
        # converged to one, this archives nothing.
        advisory_compact "$ADVISORY_RECIPIENT" "$ADVISORY_SUBJECT_PREFIX" "" 1
    fi
else
    # Healthy: forget the last advisory so a future condition re-alerts, then
    # archive any open advisories — the condition cleared, so none should remain
    # open. Drains the pile too whenever the server is healthy on this tick.
    advisory_clear "$ADVISORY_STATE_FILE"
    advisory_compact "$ADVISORY_RECIPIENT" "$ADVISORY_SUBJECT_PREFIX"
fi

SUMMARY="doctor — server: ok, latency: ${LATENCY_MS}ms, conns: ${CONN_COUNT}/${CONN_MAX}, disk: ${DISK_USAGE}, orphans: ${ORPHAN_COUNT}"
dolt_notify_done "$SUMMARY"
echo "doctor: $SUMMARY"
