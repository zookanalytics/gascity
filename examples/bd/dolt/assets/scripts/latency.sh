#!/bin/sh
# latency.sh — millisecond-resolution latency measurement for dolt-pack
# health probes. Sourced by mol-dog-doctor.sh; unit-tested by
# test/dolt/latency_test.sh.
#
# Replaces whole-second 'date +%s' timing, which quantizes a sub-second probe
# to 0s or 1s depending on whether it straddles a wall-clock second tick —
# producing false latency WARNs (and MEDIUM advisory mail) at a 1s threshold.

# _now_ms_plausible VALUE — exit 0 when VALUE looks like an epoch-millisecond
# reading: all digits, 13 or 14 of them (epoch-ms is 13 digits from 2001-09-09
# through 2286-11-20, then 14). An overlong reading (16 = microseconds, 18-19 =
# nanoseconds) is rejected: it means a date(1) that ignored the %3N width
# modifier and emitted sub-millisecond precision. Accepting it would have now_ms
# return nanoseconds-as-milliseconds, inflating every probe latency by ~1e6x and
# false-tripping the [MEDIUM] advisory — so the bound is two-sided, not just a
# lower floor, forcing the cascade onward to a backend that returns true ms.
_now_ms_plausible() {
  case "${1:-}" in
    ''|*[!0-9]*) return 1 ;;
  esac
  [ "${#1}" -ge 13 ] && [ "${#1}" -le 14 ]
}

# now_ms — echo the current time in epoch milliseconds.
#
# Implementation cascade; the first plausible reading wins:
#   1. date +%s%3N      — GNU/coreutils date (%3N = milliseconds). BSD/macOS
#                         date has no %N and prints a literal '3N' suffix; a
#                         date that ignores the %3N width modifier instead
#                         emits epoch nanoseconds (18-19 digits). Both are
#                         rejected by the plausibility check's digit-width
#                         bound, so a wrong-precision clock degrades to the
#                         next backend rather than to garbage latency.
#   2. perl Time::HiRes — core module since perl 5.8; present on stock macOS
#                         and virtually every Linux.
#   3. python3          — time.time() carries sub-millisecond resolution.
#   4. date +%s × 1000  — whole seconds; no worse than the pre-fix behavior.
#
# The cascade exists because a GNU-only implementation silently degrades to
# whole seconds on BSD/macOS, where a sub-second probe that straddles a
# wall-clock second tick measures 1000ms and false-trips the default 1000ms
# warn threshold — the same advisory storm the millisecond rewrite was meant
# to stop, in different units.
now_ms() {
  _now_ms_v=$(date +%s%3N 2>/dev/null)
  if _now_ms_plausible "$_now_ms_v"; then
    printf '%s\n' "$_now_ms_v"
    return 0
  fi
  _now_ms_v=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time() * 1000' 2>/dev/null)
  if _now_ms_plausible "$_now_ms_v"; then
    printf '%s\n' "$_now_ms_v"
    return 0
  fi
  _now_ms_v=$(python3 -c 'import time; print(int(time.time() * 1000))' 2>/dev/null)
  if _now_ms_plausible "$_now_ms_v"; then
    printf '%s\n' "$_now_ms_v"
    return 0
  fi
  printf '%s\n' "$(( $(date +%s) * 1000 ))"
}

# latency_should_warn ELAPSED_MS THRESHOLD_MS — exit 0 (warn) when measured
# latency meets or exceeds the threshold, 1 otherwise. Preserves the original
# '>=' semantics, now in milliseconds.
latency_should_warn() {
  [ "${1:-0}" -ge "${2:-0}" ]
}

# latency_delta START_MS END_MS — echo a sane, non-negative probe latency in
# milliseconds, or 0 when the reading cannot be trusted. Defense-in-depth around
# the raw END-START subtraction: even if now_ms degrades, an impossible value
# can never reach a [MEDIUM] advisory. Collapses to 0 ("not measured this tick",
# the same sentinel the health command uses for an unmeasured probe — so it
# never trips the latency warn) when any of these hold:
#   * either operand is empty or non-numeric (now_ms returned garbage),
#   * either operand is <= 0 (a real epoch-ms reading is never zero),
#   * END < START (the clock moved backward: server/host restart, NTP step), or
#   * the delta exceeds GC_LATENCY_SANE_MAX_MS (default 600000 = 10 min) — far
#     above any run_bounded-capped probe, yet far below the ~1.78e12 ms a stale
#     or seconds-vs-milliseconds-mismatched start timestamp would produce.
latency_delta() {
  _lat_start="${1:-}"
  _lat_end="${2:-}"
  _lat_max="${GC_LATENCY_SANE_MAX_MS:-600000}"
  case "$_lat_max" in ''|*[!0-9]*) _lat_max=600000 ;; esac
  case "$_lat_start" in ''|*[!0-9]*) printf '0\n'; return 0 ;; esac
  case "$_lat_end" in ''|*[!0-9]*) printf '0\n'; return 0 ;; esac
  if [ "$_lat_start" -le 0 ] || [ "$_lat_end" -le 0 ]; then
    printf '0\n'
    return 0
  fi
  _lat_d=$((_lat_end - _lat_start))
  if [ "$_lat_d" -lt 0 ] || [ "$_lat_d" -gt "$_lat_max" ]; then
    printf '0\n'
    return 0
  fi
  printf '%s\n' "$_lat_d"
}
