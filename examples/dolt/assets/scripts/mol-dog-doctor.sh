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
# Connection-limit denominator: operator override only. When unset, the
# live @@GLOBAL.max_connections is read from the server (below). The old
# hardcoded default of 50 silently mislabeled the advisory — it matched
# the listener's back_log while the managed config sets max_connections
# to 1000, so "Connections: N/50" both misreported the limit and warned
# at 80% of the wrong number.
CONN_MAX_OVERRIDE="${GC_DOCTOR_CONN_MAX:-}"
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

# --- Step 1: Probe connectivity and measure latency ---

PROBE_START=$(date +%s)
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
PROBE_END=$(date +%s)
LATENCY_S=$((PROBE_END - PROBE_START))
LATENCY_WARN=""
if [ "$LATENCY_S" -ge "$LATENCY_WARN_S" ]; then
    LATENCY_WARN=" [WARN: latency ${LATENCY_S}s >= threshold ${LATENCY_WARN_S}s]"
fi

# --- Step 2: Check resource conditions ---

# Resolve the connection-limit denominator: explicit operator override
# wins; otherwise read the live server value so the advisory reports the
# actual configured limit.
if [ -n "$CONN_MAX_OVERRIDE" ]; then
    CONN_MAX="$CONN_MAX_OVERRIDE"
else
    CONN_MAX=$(dolt_sql -r csv -q "SELECT @@GLOBAL.max_connections" 2>/dev/null \
        | grep -E '^[0-9]+$' | head -1 || true)
fi

# Connection count. A failed probe on a reachable server must surface as
# an explicit unknown plus a WARN — during the 2026-06 pool wedge this
# query was itself rejected and the old `|| echo 0` fallback reported
# "Connections: 0/50", masking total pool saturation as an idle server.
CONN_COUNT=$(dolt_sql -r csv -q "SELECT COUNT(*) FROM information_schema.PROCESSLIST" 2>/dev/null \
    | grep -E '^[0-9]+$' | head -1 || true)
CONN_WARN=""
if [ -z "$CONN_COUNT" ]; then
    CONN_COUNT="unknown"
    CONN_WARN=" [WARN: connection-count probe failed on a reachable server — possible pool saturation]"
elif [ -z "$CONN_MAX" ]; then
    CONN_MAX="unknown"
    CONN_WARN=" [WARN: max_connections probe failed; cannot evaluate pool headroom]"
else
    CONN_WARN_AT=$(( (CONN_MAX * CONN_WARN_PCT) / 100 ))
    if [ "$CONN_COUNT" -ge "$CONN_WARN_AT" ]; then
        CONN_WARN=" [WARN: ${CONN_COUNT} connections >= ${CONN_WARN_PCT}% of max ${CONN_MAX}]"
    fi
fi
if [ -z "$CONN_MAX" ]; then
    CONN_MAX="unknown"
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
if [ -n "$WARNINGS" ]; then
    if ! send_mayor_mail \
        -s "Dolt health advisory [MEDIUM]" \
        -m "Latency: ${LATENCY_S}s${LATENCY_WARN}
Connections: ${CONN_COUNT}/${CONN_MAX}${CONN_WARN}
Disk: ${DISK_USAGE}
Orphan DBs: ${ORPHAN_COUNT}${ORPHAN_WARN}${BACKUP_STALE}"; then
        :
    fi
fi

SUMMARY="doctor — server: ok, latency: ${LATENCY_S}s, conns: ${CONN_COUNT}/${CONN_MAX}, disk: ${DISK_USAGE}, orphans: ${ORPHAN_COUNT}"
gc session nudge deacon/ "DOG_DONE: $SUMMARY" 2>/dev/null || true
echo "doctor: $SUMMARY"
