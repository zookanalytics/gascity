#!/bin/sh
# Unit test for examples/bd/dolt/assets/scripts/advisory_state.sh.
#
# Proves the dedup fix for the dolt-health advisory storm in mol-dog-doctor.sh
# (#3409): a persistent condition sends exactly one advisory (not one per tick),
# a changed condition set re-alerts, and a healthy server clears the state so the
# next occurrence alerts again.
#
# Run: sh test/dolt/advisory_dedup_test.sh
set -u
HERE=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
ADVISORY_LIB="${ADVISORY_LIB:-$HERE/../../examples/bd/dolt/assets/scripts/advisory_state.sh}"

if [ ! -f "$ADVISORY_LIB" ]; then
  echo "FAIL: advisory helper not found at $ADVISORY_LIB"
  exit 1
fi
# shellcheck disable=SC1090
. "$ADVISORY_LIB"

fail=0
pass() { echo "PASS: $1"; }
bad()  { echo "FAIL: $1"; fail=1; }

command -v advisory_changed >/dev/null 2>&1 || { echo "FAIL: advisory_changed not defined"; exit 1; }
command -v advisory_record  >/dev/null 2>&1 || { echo "FAIL: advisory_record not defined"; exit 1; }
command -v advisory_clear   >/dev/null 2>&1 || { echo "FAIL: advisory_clear not defined"; exit 1; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
STATE="$WORK/doctor-advisory-state"

# No state recorded yet -> first advisory must send.
if advisory_changed "latency " "$STATE"; then pass "first advisory (no state) -> send"; else bad "first advisory suppressed with no state on file"; fi

# Recording persists the signature.
advisory_record "latency " "$STATE"
if [ -f "$STATE" ]; then pass "record creates the state file"; else bad "record did not create the state file"; fi

# Same signature after a record -> suppressed (the storm fix).
if advisory_changed "latency " "$STATE"; then bad "identical advisory re-sent (storm not deduped)"; else pass "identical advisory -> suppressed"; fi

# A simulated 50-tick run with an unchanged condition sends nothing further.
resends=0
i=0
while [ "$i" -lt 50 ]; do
  if advisory_changed "latency " "$STATE"; then resends=$((resends + 1)); advisory_record "latency " "$STATE"; fi
  i=$((i + 1))
done
if [ "$resends" -eq 0 ]; then pass "50 steady ticks -> 0 re-sends"; else bad "$resends/50 steady ticks re-sent"; fi

# A changed condition set -> re-alert.
if advisory_changed "latency conn " "$STATE"; then pass "changed condition set -> re-alert"; else bad "changed condition set did not re-alert"; fi
advisory_record "latency conn " "$STATE"

# Healthy server clears state; the next occurrence alerts again.
advisory_clear "$STATE"
if [ -f "$STATE" ]; then bad "clear did not remove the state file"; else pass "clear removes the state file"; fi
if advisory_changed "latency " "$STATE"; then pass "post-clear recurrence -> re-alert"; else bad "post-clear recurrence suppressed"; fi

# Fail-open: with no state-file path, never suppress (degrade to pre-fix alert,
# never to silence).
if advisory_changed "latency " ""; then pass "empty state path -> fail open (send)"; else bad "empty state path suppressed the advisory"; fi

# record into a not-yet-existing directory creates the path (mkdir -p).
NESTED="$WORK/runtime/packs/dolt/doctor-advisory-state"
advisory_record "orphan " "$NESTED"
if [ -f "$NESTED" ]; then pass "record creates missing parent directories"; else bad "record did not create nested state path"; fi

# The recorded signature round-trips exactly.
got=$(cat "$NESTED" 2>/dev/null || true)
if [ "$got" = "orphan " ]; then pass "recorded signature round-trips"; else bad "recorded signature mismatch: got '$got'"; fi

# --- advisory_compact (cleanup arm) ----------------------------------------
# Closes the open advisory wisps the send-time dedup cannot reach: the pre-dedup
# pile and advisories superseded by a changed/cleared condition set. A recording
# `gc` stub (injected via GC_BIN) captures the invocation, one arg per line.
command -v advisory_compact >/dev/null 2>&1 || { echo "FAIL: advisory_compact not defined"; exit 1; }

GC_STUB="$WORK/gc-stub"
cat > "$GC_STUB" <<'STUB'
#!/bin/sh
for a in "$@"; do printf '%s\n' "$a" >> "$GC_STUB_LOG"; done
exit 0
STUB
chmod +x "$GC_STUB"
export GC_STUB_LOG="$WORK/gc-archive.log"
arg_logged() { grep -Fxq -- "$1" "$GC_STUB_LOG"; }

# Issues `gc mail archive` with the expected open-advisory filter flags.
: > "$GC_STUB_LOG"
( export GC_BIN="$GC_STUB"; advisory_compact "human" "Dolt health advisory" 50 )
if arg_logged "mail" && arg_logged "archive" && arg_logged "--to" && arg_logged "human" \
   && arg_logged "--subject-prefix" && arg_logged "Dolt health advisory" \
   && arg_logged "--include-read" && arg_logged "--limit" && arg_logged "50"; then
  pass "advisory_compact issues gc mail archive with the expected filter flags"
else
  bad "advisory_compact invocation missing expected flags: $(tr '\n' '|' < "$GC_STUB_LOG")"
fi

# Omitted limit defaults to 1000 (a generous one-run bound; larger piles drain
# over later ticks).
: > "$GC_STUB_LOG"
( export GC_BIN="$GC_STUB"; advisory_compact "human" "Dolt health advisory" )
if arg_logged "1000"; then pass "advisory_compact defaults --limit to 1000"; else bad "advisory_compact missing default limit: $(tr '\n' '|' < "$GC_STUB_LOG")"; fi

# A non-numeric limit is coerced to the default, never passed through as junk
# (gc rejects --limit <= 0).
: > "$GC_STUB_LOG"
( export GC_BIN="$GC_STUB"; advisory_compact "human" "Dolt health advisory" "abc" )
if arg_logged "1000" && ! arg_logged "abc"; then pass "advisory_compact coerces invalid limit to 1000"; else bad "advisory_compact did not coerce invalid limit: $(tr '\n' '|' < "$GC_STUB_LOG")"; fi

# Refuses to run without BOTH a recipient and a subject prefix — never archives
# unrelated mail.
: > "$GC_STUB_LOG"
( export GC_BIN="$GC_STUB"; advisory_compact "" "Dolt health advisory" )
( export GC_BIN="$GC_STUB"; advisory_compact "human" "" )
if [ ! -s "$GC_STUB_LOG" ]; then pass "advisory_compact no-ops without recipient+prefix"; else bad "advisory_compact ran with missing args: $(tr '\n' '|' < "$GC_STUB_LOG")"; fi

# Best-effort: a failing gc must not propagate — compaction never blocks or
# fails the health probe.
GC_FAIL="$WORK/gc-fail"
printf '#!/bin/sh\nexit 3\n' > "$GC_FAIL"
chmod +x "$GC_FAIL"
if ( export GC_BIN="$GC_FAIL"; advisory_compact "human" "Dolt health advisory" ); then
  pass "advisory_compact swallows a failing gc (best-effort)"
else
  bad "advisory_compact propagated a gc failure"
fi

echo "----"
if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES PRESENT"; fi
exit "$fail"
