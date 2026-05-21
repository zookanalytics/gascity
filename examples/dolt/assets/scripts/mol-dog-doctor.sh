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

PORT="$GC_DOLT_PORT"
HOST="${GC_DOLT_HOST:-127.0.0.1}"
USER="${GC_DOLT_USER:-root}"
LATENCY_WARN_S="${GC_DOCTOR_LATENCY_WARN_S:-1}"
CONN_MAX="${GC_DOCTOR_CONN_MAX:-50}"
CONN_WARN_PCT="${GC_DOCTOR_CONN_WARN_PCT:-80}"
BACKUP_STALE_S="${GC_DOCTOR_BACKUP_STALE_S:-43200}"  # 2x 6h backup interval
BACKUP_ARTIFACT_DIR="${GC_BACKUP_ARTIFACT_DIR:-$GC_CITY_PATH/.dolt-backup}"

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

newest_backup_mtime_in_dir() {
    backup_dir_path="$1"
    newest_mtime=0
    if [ ! -d "$backup_dir_path" ]; then
        printf '%s\n' "$newest_mtime"
        return
    fi
    while IFS= read -r -d '' backup_path; do
        backup_mtime=$(file_mtime "$backup_path")
        if [ "$backup_mtime" -gt "$newest_mtime" ]; then
            newest_mtime="$backup_mtime"
        fi
    done < <(find -L "$backup_dir_path" -type f -print0 2>/dev/null)
    printf '%s\n' "$newest_mtime"
}

# discover_named_backup_url echoes the URL portion of `dolt backup -v`
# for the named backup `<db>-backup`, run from the DB's data directory.
# Echoes empty when no DB data dir exists, no named backup is
# configured, or the dolt invocation fails — the caller distinguishes
# "configured with file:// URL" from "configured with remote URL" from
# "not configured" via the case-match on the result.
discover_named_backup_url() {
    db_name="$1"
    db_data_dir="$DOLT_DATA_DIR/$db_name"
    if [ ! -d "$db_data_dir/.dolt" ]; then
        printf ''
        return
    fi
    backup_line=$(cd "$db_data_dir" 2>/dev/null && dolt backup -v 2>/dev/null || true)
    printf '%s\n' "$backup_line" \
        | awk -v want="${db_name}-backup" '$1 == want {print $2; exit}'
}

append_backup_stale() {
    backup_stale_item="$1"
    if [ -n "$BACKUP_STALE_ITEMS" ]; then
        BACKUP_STALE_ITEMS="$BACKUP_STALE_ITEMS, $backup_stale_item"
    else
        BACKUP_STALE_ITEMS="$backup_stale_item"
    fi
}

# --- Step 1: Probe connectivity and measure latency ---

PROBE_START=$(date +%s)
if ! dolt_sql -q "SELECT active_branch()" >/dev/null 2>&1; then
    gc mail send mayor/ \
        -s "ESCALATION: Dolt server unreachable on port $PORT [CRITICAL]" \
        -m "Doctor probe failed: server did not respond to active_branch() query." \
        2>/dev/null || true
    gc session nudge deacon/ "DOG_DONE: doctor — server: UNREACHABLE (escalated)" 2>/dev/null || true
    echo "doctor: server unreachable on port $PORT (escalated)"
    exit 0
fi
PROBE_END=$(date +%s)
LATENCY_S=$((PROBE_END - PROBE_START))
LATENCY_WARN=""
if [ "$LATENCY_S" -ge "$LATENCY_WARN_S" ]; then
    LATENCY_WARN=" [WARN: latency ${LATENCY_S}s >= threshold ${LATENCY_WARN_S}s]"
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

# Backup freshness: prefer the `<db>-backup` URL recorded in dolt itself
# (auto-discovered via `dolt backup -v`). file:// URLs are stat'd at the
# URL path — this is the Path A flow where the offsite location lives
# outside any local artifact directory. Remote URLs (s3://, aws://,
# gs://, http(s)://) are skipped because remote freshness is the
# storage backend's responsibility. When no named backup is configured
# for a DB, fall back to scanning $BACKUP_ARTIFACT_DIR for back-compat
# with legacy Path B layouts that drop artifacts there directly; if
# that directory is also absent, the DB is treated as not enrolled and
# skipped silently (no advisory).
BACKUP_STALE=""
if [ -n "$USER_DBS" ]; then
    BACKUP_STALE_ITEMS=""
    NOW_S=$(date +%s)
    for db in $USER_DBS; do
        backup_url=$(discover_named_backup_url "$db")
        case "$backup_url" in
            file://*)
                backup_path="${backup_url#file://}"
                NEWEST_BACKUP_MTIME=$(newest_backup_mtime_in_dir "$backup_path")
                ;;
            "")
                if [ ! -d "$BACKUP_ARTIFACT_DIR" ]; then
                    continue
                fi
                NEWEST_BACKUP_MTIME=$(newest_backup_mtime_for_db "$db")
                ;;
            *)
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
if [ -n "$WARNINGS" ]; then
    gc mail send mayor/ \
        -s "Dolt health advisory [MEDIUM]" \
        -m "Latency: ${LATENCY_S}s${LATENCY_WARN}
Connections: ${CONN_COUNT}/${CONN_MAX}${CONN_WARN}
Disk: ${DISK_USAGE}
Orphan DBs: ${ORPHAN_COUNT}${ORPHAN_WARN}${BACKUP_STALE}" \
        2>/dev/null || true
fi

SUMMARY="doctor — server: ok, latency: ${LATENCY_S}s, conns: ${CONN_COUNT}/${CONN_MAX}, disk: ${DISK_USAGE}, orphans: ${ORPHAN_COUNT}"
gc session nudge deacon/ "DOG_DONE: $SUMMARY" 2>/dev/null || true
echo "doctor: $SUMMARY"
