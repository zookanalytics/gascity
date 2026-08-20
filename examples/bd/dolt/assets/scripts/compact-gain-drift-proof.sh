#!/bin/sh
# compact-gain-drift-proof.sh — row-preservation proofs for post-flatten value
# hash drift (gastownhall/gascity#2846, generalized in gc-800l/gc-i52hj).
#
# When verify_counts sees a table's value hash drift, the safety property at
# stake ("pre-flight rows remain reachable") cannot be inferred from HEAD
# movement alone: a concurrent writer whose commit is ABSORBED into the flatten
# commit moves no HEAD, so the HEAD-proven gate misses it and a benign race is
# hard-quarantined — which then blocks all future GC of a busy DB (the
# memory-exhaustion failure the code calls out).
#
# These prove preservation DIRECTLY: for each drifted table, diff the pre-flight
# snapshot HEAD against the flatten commit. That is strictly more rigorous than
# the HEAD proxy — it proves reachability instead of inferring it — and any
# probe failure fails closed and falls through to quarantine.
#
# Two predicates, because the two callers ask different questions:
#
#   * drift_preserves_preflight_rows — counts `removed` rows only. Used for
#     tables that existed at the pre-flight root, where the question is whether
#     the flatten dropped any of the rows it was asked to preserve.
#   * diff_is_additive_only — counts every non-`added` row. Used for a table
#     the flatten FIRST-COMMITTED, which has no pre-flight rows to preserve;
#     there anything but a pure add is unexplained.
#
# Why the drift proof counts removals and not "everything except added"
# (gc-800l): a live city writes in place as well as appending. `bd update`
# rewrites an existing row, which DOLT_DIFF reports as `modified` — the row is
# still reachable, nothing was lost. Counting `modified` as unprovable made the
# single most common bd write indistinguishable from corruption, so a 24h
# unattended compaction quarantined on essentially every pass and each one cost
# a human review to clear. Removal is the only diff_type that answers the
# question the check actually asks.
#
# Scope note: this reads `removed` as "a pre-flight row is gone", which holds
# for keyed tables — the shape bd uses. A KEYLESS table has no identity to
# track a row by, so Dolt reports an in-place update there as a removed/added
# pair; such a table still fails this proof and falls through to quarantine.
# That is the safe direction and matches the behaviour before this proof
# existed, so it is a limitation rather than a regression.
#
# Depends on `query_single_cell` and `valid_table_name` from run.sh.

# diff_has_no_removed_rows <db> <from_head> <to_head> <table>
# Returns 0 iff the table's <from>..<to> content diff contains no `removed`
# rows. Returns non-zero (fail closed) if either commit endpoint is missing,
# the table name is missing or invalid, the diff probe fails or returns a
# non-numeric result, or the table shows removed rows.
#
# Deliberately weaker than diff_is_additive_only: `added` and `modified` rows
# are ordinary concurrent-writer traffic on a live store and leave every
# pre-flight row reachable. Only a removal answers "a row we were asked to
# preserve is gone".
diff_has_no_removed_rows() {
  _dr_db="$1"
  _dr_from="$2"
  _dr_to="$3"
  _dr_t="$4"
  # Without both commit endpoints there is nothing to diff against — fail closed.
  [ -n "$_dr_from" ] && [ -n "$_dr_to" ] && [ -n "$_dr_t" ] || return 1
  valid_table_name "$_dr_t" || return 1
  # Count rows that stopped being reachable between the two commits. Zero means
  # every row present at <from> is still present at <to>, however its values
  # may have been rewritten in place since.
  if ! _dr_removed=$(query_single_cell "$_dr_db" \
    "drift preservation diff probe failed for table=$_dr_t" \
    "SELECT COUNT(*) FROM DOLT_DIFF('$_dr_from', '$_dr_to', '$_dr_t') WHERE diff_type = 'removed'"); then
    return 1
  fi
  case "$_dr_removed" in
    0) return 0 ;;             # no removals — this table's <from> rows preserved
    ''|*[!0-9]*) return 1 ;;   # empty/non-numeric probe result — fail closed
    *) return 1 ;;             # one or more removed rows — not preservable
  esac
}

# diff_is_additive_only <db> <from_head> <to_head> <table>
# Returns 0 iff the table's <from>..<to> content diff contains only `added`
# rows. Returns non-zero (fail closed) if either commit endpoint is missing,
# the table name is missing or invalid, the diff probe fails or returns a
# non-numeric result, or the table shows removed/modified rows. Used by the
# committed-root drift proof's first-committed table case (run.sh
# db_root_drift_within_verified_tables), where the table exists in no
# pre-flatten commit so every row must be an add.
diff_is_additive_only() {
  _da_db="$1"
  _da_from="$2"
  _da_to="$3"
  _da_t="$4"
  # Without both commit endpoints there is nothing to diff against — fail closed.
  [ -n "$_da_from" ] && [ -n "$_da_to" ] && [ -n "$_da_t" ] || return 1
  valid_table_name "$_da_t" || return 1
  # Count rows that are NOT purely additive between the two commits. Zero means
  # every row present at <from> is reachable unchanged at <to> and the only
  # change was added rows.
  if ! _da_nonadded=$(query_single_cell "$_da_db" \
    "preservation diff probe failed for table=$_da_t" \
    "SELECT COUNT(*) FROM DOLT_DIFF('$_da_from', '$_da_to', '$_da_t') WHERE diff_type <> 'added'"); then
    return 1
  fi
  case "$_da_nonadded" in
    0) return 0 ;;             # only added rows — this table's <from> rows preserved
    ''|*[!0-9]*) return 1 ;;   # empty/non-numeric probe result — fail closed
    *) return 1 ;;             # one or more removed/modified rows — not preservable
  esac
}

# drift_preserves_preflight_rows <db> <from_head> <to_head> <space-separated tables>
# Returns 0 iff every listed table's <from>..<to> content diff contains no
# `removed` rows. Returns non-zero (fail closed) if the table list is empty or
# any table fails diff_has_no_removed_rows.
drift_preserves_preflight_rows() {
  _gd_db="$1"
  _gd_from="$2"
  _gd_to="$3"
  _gd_tables="$4"
  _gd_seen=0
  for _gd_t in $_gd_tables; do
    _gd_seen=1
    diff_has_no_removed_rows "$_gd_db" "$_gd_from" "$_gd_to" "$_gd_t" || return 1
  done
  # An empty table list is not a proof of preservation.
  [ "$_gd_seen" = "1" ] || return 1
  return 0
}
