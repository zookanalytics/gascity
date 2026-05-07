#!/bin/sh
# gc dolt status-tick — Health-check tick used by the dolt-health order.
#
# Calls `gc dolt status` (bounded). On success, clears the consecutive-
# failure counter. On failure, increments the counter and only triggers
# `gc dolt start` once it reaches THRESHOLD (2) consecutive failures.
# This eliminates the single-tick false positive during the connection-
# bounce window of `gc dolt sync`, which produced a second restart
# ~8s after sync's own restart on every 15-minute sync cycle.
#
# Environment: GC_CITY_PATH
set -e

: "${GC_CITY_PATH:?GC_CITY_PATH must be set}"
PACK_DIR="${GC_PACK_DIR:-$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)}"
. "$PACK_DIR/assets/scripts/runtime.sh"

COUNTER="$DOLT_STATE_DIR/health-failures"
THRESHOLD=2

if run_bounded 5 gc dolt status >/dev/null 2>&1; then
  rm -f "$COUNTER"
  exit 0
fi

n=$(cat "$COUNTER" 2>/dev/null || echo 0)
n=$((n + 1))

if [ "$n" -ge "$THRESHOLD" ]; then
  rm -f "$COUNTER"
  exec gc dolt start
fi

printf '%s\n' "$n" > "$COUNTER"
exit 0
