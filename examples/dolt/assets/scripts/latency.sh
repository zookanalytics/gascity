#!/bin/sh
# latency.sh — millisecond-resolution latency measurement for dolt-pack
# health probes. Sourced by mol-dog-doctor.sh; unit-tested by
# test/dolt/latency_test.sh.
#
# Replaces whole-second 'date +%s' timing, which quantizes a sub-second probe
# to 0s or 1s depending on whether it straddles a wall-clock second tick —
# producing false latency WARNs (and MEDIUM advisory mail) at a 1s threshold.

# now_ms — echo the current time in epoch milliseconds.
# Uses PLAIN %N (no width spec): GNU coreutils zero-pads it to 9 digits and
# uutils coreutils emits the exact nanosecond value unpadded — either way the
# digits after the 10-digit epoch seconds are the exact nanosecond count.
# Do NOT use %3N here: GNU truncates it to milliseconds, but uutils date
# reads the width as a printf minimum-width spec and emits unpadded
# nanoseconds at variable width — two samples then land on different scales
# whenever the wall-clock fraction dips below 0.1s (~10% of calls), and the
# subtraction produced garbage latencies (observed: ~ -1.6e18 "ms").
# On platforms whose date lacks %N entirely (BSD/macOS without coreutils),
# fall back to second-resolution * 1000 — no worse than the prior
# whole-second behavior. Epoch seconds are 10 digits from 2001..2286.
now_ms() {
  _now_raw=$(date +%s%N 2>/dev/null)
  case "$_now_raw" in
    ''|*[!0-9]*) _now_raw="" ;;    # %N printed literally: no ns support
  esac
  if [ -n "$_now_raw" ] && [ "${#_now_raw}" -ge 11 ] && [ "${#_now_raw}" -le 19 ]; then
    _now_s=$(printf '%s' "$_now_raw" | cut -c1-10)
    _now_ns=$(printf '%s' "$_now_raw" | cut -c11-)
    # Strip leading zeros so $(( )) cannot read a padded fraction as octal.
    _now_ns=${_now_ns#"${_now_ns%%[!0]*}"}
    printf '%s\n' "$(( _now_s * 1000 + ${_now_ns:-0} / 1000000 ))"
  else
    printf '%s\n' "$(( $(date +%s) * 1000 ))"
  fi
}

# latency_should_warn ELAPSED_MS THRESHOLD_MS — exit 0 (warn) when measured
# latency meets or exceeds the threshold, 1 otherwise. Preserves the original
# '>=' semantics, now in milliseconds.
latency_should_warn() {
  [ "${1:-0}" -ge "${2:-0}" ]
}
