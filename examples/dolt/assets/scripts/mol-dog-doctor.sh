#!/usr/bin/env bash
# mol-dog-doctor — probe Dolt server health and report findings.
#
# Replaces mol-dog-doctor formula. All checks are read-only: SQL probe,
# PROCESSLIST count, disk usage, orphan DB detection, backup artifact freshness.
# No LLM judgment needed — runs inline in the controller.
#
# Runs as an exec order (no LLM, no agent, no wisp).
set -euo pipefail

PACK_DIR="${GC_PACK_DIR:-$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
. "$PACK_DIR/assets/scripts/runtime.sh"
. "$PACK_DIR/assets/scripts/latency.sh"

PORT="$GC_DOLT_PORT"
HOST="${GC_DOLT_HOST:-127.0.0.1}"
USER="${GC_DOLT_USER:-root}"
# Latency warn threshold in milliseconds. GC_DOCTOR_LATENCY_WARN_MS takes
# precedence; otherwise derive from the legacy seconds knob (default 1s ->
# 1000ms) for backward compatibility.
LATENCY_WARN_MS="${GC_DOCTOR_LATENCY_WARN_MS:-$(( ${GC_DOCTOR_LATENCY_WARN_S:-1} * 1000 ))}"
CONN_MAX="${GC_DOCTOR_CONN_MAX:-50}"
CONN_WARN_PCT="${GC_DOCTOR_CONN_WARN_PCT:-80}"
BACKUP_STALE_S="${GC_DOCTOR_BACKUP_STALE_S:-43200}"  # 2x 6h backup interval
BACKUP_ARTIFACT_DIR="${GC_BACKUP_ARTIFACT_DIR:-$GC_CITY_PATH/.dolt-backup}"

# Advisory coalescing. The doctor runs once per health-check tick (default
# every 5m). Emitting the "Dolt health advisory" mail on every tick while a
# borderline condition persists turns the monitor into a self-DoS: each mail
# is a Dolt write, so a sustained latency event piles write load onto the data
# plane precisely when it is already latency-stressed. Coalesce instead: alert
# on the rising edge (a new/changed warning set), re-alert at most once per
# cooldown window while the condition is unchanged, and announce recovery once.
#
# State lives in a small local JSON file, NOT the data plane: the whole point
# is to avoid touching Dolt to decide whether to notify. This is notification
# cooldown state, not a process-liveness status file — if it is lost the script
# fails open (sends), which is safe and self-correcting.
ADVISORY_COOLDOWN_S="${GC_DOCTOR_ADVISORY_COOLDOWN_S:-3600}"
ADVISORY_STATE_FILE="${GC_DOCTOR_ADVISORY_STATE_FILE:-$DOLT_STATE_DIR/doctor-advisory-state.json}"

dolt_sql() {
    DOLT_CLI_PASSWORD="${GC_DOLT_PASSWORD:-}" \
        run_bounded 10 \
        dolt --host "$HOST" --port "$PORT" --user "$USER" --no-tls sql "$@"
}

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

send_mayor_mail() {
    local mail_err
    if ! mail_err=$(gc mail send mayor/ --from controller "$@" 2>&1 >/dev/null); then
        if [ -n "$mail_err" ]; then
            echo "doctor: mail send failed: $mail_err" >&2
        else
            echo "doctor: mail send failed" >&2
        fi
        return 1
    fi
}

# write_advisory_state SIGNATURE EPOCH_S — persist the last-sent advisory
# signature and timestamp for cross-tick coalescing. Atomic (temp + rename)
# and fail-soft: a write failure must never abort the read-only doctor probe.
# SIGNATURE is built only from fixed [a-z,] category tokens, so no JSON
# escaping is required.
write_advisory_state() {
    _adv_sig="$1"
    _adv_ts="$2"
    _adv_dir=$(dirname "$ADVISORY_STATE_FILE")
    mkdir -p "$_adv_dir" 2>/dev/null || return 0
    _adv_tmp="$ADVISORY_STATE_FILE.tmp.$$"
    if printf '{"signature":"%s","last_sent_epoch_s":%s}\n' \
        "$_adv_sig" "${_adv_ts:-0}" > "$_adv_tmp" 2>/dev/null; then
        mv -f "$_adv_tmp" "$ADVISORY_STATE_FILE" 2>/dev/null || rm -f "$_adv_tmp" 2>/dev/null || true
    else
        rm -f "$_adv_tmp" 2>/dev/null || true
    fi
}

# --- Step 1: Probe connectivity and measure latency ---

PROBE_START_MS=$(now_ms)
if ! dolt_sql -q "SELECT active_branch()" >/dev/null 2>&1; then
    if send_mayor_mail \
        -s "ESCALATION: Dolt server unreachable on port $PORT [CRITICAL]" \
        -m "Doctor probe failed: server did not respond to active_branch() query."; then
        gc session nudge deacon/ "DOG_DONE: doctor — server: UNREACHABLE (escalated)" 2>/dev/null || true
        echo "doctor: server unreachable on port $PORT (escalated)"
    else
        gc session nudge deacon/ "DOG_DONE: doctor — server: UNREACHABLE (mail failed)" 2>/dev/null || true
        echo "doctor: server unreachable on port $PORT (mail failed)"
    fi
    exit 0
fi
PROBE_END_MS=$(now_ms)
LATENCY_MS=$((PROBE_END_MS - PROBE_START_MS))
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

# Backup freshness: scope mirrors mol-dog-backup.sh — only DBs with a
# configured <db>-backup remote are eligible. For each eligible DB,
# prefer Path A (per-DB named backup URL discovered via `dolt backup
# -v`); fall back to Path B (legacy local artifact dir) when no URL is
# discovered, for back-compat with Path B-only configs. Cities with
# user DBs but no backup remotes (legitimate config) produce no
# backup-related noise at all.
BACKUP_ELIGIBLE_DBS=""
for db in $USER_DBS; do
    db_dir="$DOLT_DATA_DIR/$db"
    if [ -d "$db_dir/.dolt" ]; then
        if (cd "$db_dir" && dolt backup 2>/dev/null | awk '{print $1}' | grep -qx "${db}-backup"); then
            BACKUP_ELIGIBLE_DBS="$BACKUP_ELIGIBLE_DBS $db"
        fi
    fi
done
BACKUP_ELIGIBLE_DBS=$(printf '%s\n' "$BACKUP_ELIGIBLE_DBS" | tr ' ' '\n' | grep -v '^$' || true)

BACKUP_STALE=""
if [ -n "$BACKUP_ELIGIBLE_DBS" ]; then
    BACKUP_STALE_ITEMS=""
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
    if [ -n "$BACKUP_STALE_ITEMS" ]; then
        BACKUP_STALE=" [WARN: backup freshness: $BACKUP_STALE_ITEMS]"
    fi
fi

# --- Step 3: Compose report and escalate if critical ---

WARNINGS="${LATENCY_WARN}${CONN_WARN}${ORPHAN_WARN}${BACKUP_STALE}"

# Build a stable signature of WHICH warning categories are active (not their
# exact values), so value jitter on a sustained condition (e.g. 1500ms vs
# 1520ms latency) does not read as a new condition and re-alert.
ADVISORY_SIGNATURE=""
[ -n "$LATENCY_WARN" ] && ADVISORY_SIGNATURE="${ADVISORY_SIGNATURE}latency," || true
[ -n "$CONN_WARN" ] && ADVISORY_SIGNATURE="${ADVISORY_SIGNATURE}conn," || true
[ -n "$ORPHAN_WARN" ] && ADVISORY_SIGNATURE="${ADVISORY_SIGNATURE}orphan," || true
[ -n "$BACKUP_STALE" ] && ADVISORY_SIGNATURE="${ADVISORY_SIGNATURE}backup," || true

LAST_SIGNATURE=$(read_runtime_state_string "$ADVISORY_STATE_FILE" signature)
LAST_SENT_S=$(read_runtime_state_number "$ADVISORY_STATE_FILE" last_sent_epoch_s)
NOW_S=$(date +%s)

if [ -n "$WARNINGS" ]; then
    # Send on the rising/changed edge, or once per cooldown window while the
    # same condition persists. Otherwise suppress to protect the data plane.
    SEND_ADVISORY=0
    if [ "$ADVISORY_SIGNATURE" != "$LAST_SIGNATURE" ]; then
        SEND_ADVISORY=1
    elif [ -z "$LAST_SENT_S" ] || [ "$(( NOW_S - LAST_SENT_S ))" -ge "$ADVISORY_COOLDOWN_S" ]; then
        SEND_ADVISORY=1
    fi
    if [ "$SEND_ADVISORY" -eq 1 ]; then
        if send_mayor_mail \
            -s "Dolt health advisory [MEDIUM]" \
            -m "Latency: ${LATENCY_MS}ms${LATENCY_WARN}
Connections: ${CONN_COUNT}/${CONN_MAX}${CONN_WARN}
Disk: ${DISK_USAGE}
Orphan DBs: ${ORPHAN_COUNT}${ORPHAN_WARN}${BACKUP_STALE}"; then
            # Record only on success so a failed send retries next tick instead
            # of starting the cooldown on a mail nobody received.
            write_advisory_state "$ADVISORY_SIGNATURE" "$NOW_S"
        fi
    fi
elif [ -n "$LAST_SIGNATURE" ]; then
    # Falling edge: the condition cleared. Announce recovery exactly once, then
    # reset state so the next onset re-triggers a fresh advisory.
    if send_mayor_mail \
        -s "Dolt health advisory cleared [OK]" \
        -m "Dolt health recovered: latency ${LATENCY_MS}ms, connections ${CONN_COUNT}/${CONN_MAX}, disk ${DISK_USAGE}, orphan DBs ${ORPHAN_COUNT}."; then
        write_advisory_state "" "$NOW_S"
    fi
fi

SUMMARY="doctor — server: ok, latency: ${LATENCY_MS}ms, conns: ${CONN_COUNT}/${CONN_MAX}, disk: ${DISK_USAGE}, orphans: ${ORPHAN_COUNT}"
gc session nudge deacon/ "DOG_DONE: $SUMMARY" 2>/dev/null || true
echo "doctor: $SUMMARY"
