#!/bin/sh
# compact-gain-drift-proof.sh — row-preservation proof for post-flatten value
# hash drift (gastownhall/gascity#2846, generalized in gc-800l).
#
# When verify_counts sees a table's value hash drift, the safety property at
# stake ("pre-flight rows remain reachable") cannot be inferred from HEAD
# movement alone: a concurrent writer whose commit is ABSORBED into the flatten
# commit moves no HEAD, so the HEAD-proven gate misses it and a benign race is
# hard-quarantined — which then blocks all future GC of a busy DB (the
# memory-exhaustion failure the code calls out).
#
# This proves preservation DIRECTLY: for each drifted table, diff the pre-flight
# snapshot HEAD against the flatten commit and count only `removed` rows. Zero
# removals means every pre-flight row is still reachable, so the drift is
# concurrent-writer data — defer, exactly as the HEAD-proven path does. It is
# strictly more rigorous than the HEAD proxy: it proves reachability instead of
# inferring it. Any removed row, or any probe failure, fails closed and falls
# through to quarantine.
#
# Why removals and not "everything except added" (gc-800l): a live city writes
# in place as well as appending. `bd update` rewrites an existing row, which
# DOLT_DIFF reports as `modified` — the row is still reachable, nothing was
# lost. Counting `modified` as unprovable made the single most common bd write
# indistinguishable from corruption, so a 24h unattended compaction quarantined
# on essentially every pass and each one cost a human review to clear. Removal
# is the only diff_type that answers the question the check actually asks.
#
# Scope note: this reads `removed` as "a pre-flight row is gone", which holds
# for keyed tables — the shape bd uses. A KEYLESS table has no identity to
# track a row by, so Dolt reports an in-place update there as a removed/added
# pair; such a table still fails this proof and falls through to quarantine.
# That is the safe direction and matches the behaviour before this proof
# existed, so it is a limitation rather than a regression.
#
# Depends on `query_single_cell` and `valid_table_name` from run.sh.

# drift_preserves_preflight_rows <db> <from_head> <to_head> <space-separated tables>
# Returns 0 iff every listed table's <from>..<to> content diff contains no
# `removed` rows. Returns non-zero (fail closed) if either commit endpoint is
# missing, the table list is empty, a table name is invalid, a diff probe
# fails or returns a non-numeric result, or any table shows removed rows.
drift_preserves_preflight_rows() {
  _gd_db="$1"
  _gd_from="$2"
  _gd_to="$3"
  _gd_tables="$4"
  # Without both commit endpoints there is nothing to diff against — fail closed.
  [ -n "$_gd_from" ] && [ -n "$_gd_to" ] || return 1
  _gd_seen=0
  for _gd_t in $_gd_tables; do
    _gd_seen=1
    valid_table_name "$_gd_t" || return 1
    # Count rows that stopped being reachable between the pre-flight snapshot
    # and the flatten commit. Zero means every pre-flight row survived; added
    # and modified rows are ordinary concurrent-writer traffic.
    if ! _gd_removed=$(query_single_cell "$_gd_db" \
      "drift preservation diff probe failed for table=$_gd_t" \
      "SELECT COUNT(*) FROM DOLT_DIFF('$_gd_from', '$_gd_to', '$_gd_t') WHERE diff_type = 'removed'"); then
      return 1
    fi
    case "$_gd_removed" in
      0) ;;                    # no removals — this table's pre-flight rows preserved
      ''|*[!0-9]*) return 1 ;; # empty/non-numeric probe result — fail closed
      *) return 1 ;;           # one or more removed rows — not preservable
    esac
  done
  # An empty table list is not a proof of preservation.
  [ "$_gd_seen" = "1" ] || return 1
  return 0
}
