#!/usr/bin/env bash
# Accelerated repro probe for the "Enter becomes newline" submit failure on
# tmux nudge (bead gc-kq4ia). See cmd/gc/live_nudge_submit_probe_test.go.
#
# This script validates prerequisites, exports the GC_LIVE_* knobs, and
# invokes the liveprobe-tagged test. It does NOT create or destroy a city —
# the operator is expected to have a city with a Claude session reachable
# at $GC_LIVE_CITY and template $GC_LIVE_TARGET.
#
# Defaults match cmd/gc/live_submit_probe_test.go so the existing
# /tmp/gc-claude-it city can be reused. Override via env vars:
#
#   GC_LIVE_CITY                  path to the city                (default /tmp/gc-claude-it)
#   GC_LIVE_TARGET                agent template to drive          (default mayor)
#   GC_LIVE_SESSION_ID            reuse an existing session id     (optional)
#   GC_LIVE_NUDGE_ITERATIONS      iterations per mode              (default 200)
#   GC_LIVE_NUDGE_MODE            detached | attached | both       (default detached)
#   GC_LIVE_NUDGE_DEBOUNCE_MS     debounce override                (default 500)
#   GC_LIVE_NUDGE_WAKE_BEFORE_TEXT true|false (hypothesis variant) (default false)
#   GC_LIVE_NUDGE_ARTIFACTS_DIR   where to write stuck captures    (default mktemp)
#
# Example:
#   GC_LIVE_NUDGE_MODE=both GC_LIVE_NUDGE_ITERATIONS=200 \
#     scripts/live-probe-nudge-submit.sh

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

for bin in claude tmux bd go; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "ERROR: $bin not found on PATH" >&2
    exit 1
  fi
done

city="${GC_LIVE_CITY:-/tmp/gc-claude-it}"
if [[ ! -d "$city" ]]; then
  echo "ERROR: city directory $city does not exist" >&2
  echo "       set GC_LIVE_CITY to a working city, or initialize one first:" >&2
  echo "         gc city init $city" >&2
  echo "         (then configure city.toml for the claude provider and start a session)" >&2
  exit 1
fi
export GC_LIVE_CITY="$city"

# Surface chosen knobs for log readability.
echo "=== nudge submit probe ==="
echo "GC_LIVE_CITY=$GC_LIVE_CITY"
echo "GC_LIVE_TARGET=${GC_LIVE_TARGET:-mayor}"
echo "GC_LIVE_SESSION_ID=${GC_LIVE_SESSION_ID:-<unset, will materialize>}"
echo "GC_LIVE_NUDGE_ITERATIONS=${GC_LIVE_NUDGE_ITERATIONS:-200}"
echo "GC_LIVE_NUDGE_MODE=${GC_LIVE_NUDGE_MODE:-detached}"
echo "GC_LIVE_NUDGE_DEBOUNCE_MS=${GC_LIVE_NUDGE_DEBOUNCE_MS:-500}"
echo "GC_LIVE_NUDGE_WAKE_BEFORE_TEXT=${GC_LIVE_NUDGE_WAKE_BEFORE_TEXT:-false}"
echo "GC_LIVE_NUDGE_ARTIFACTS_DIR=${GC_LIVE_NUDGE_ARTIFACTS_DIR:-<temp dir>}"
echo "=========================="

# GC_FAST_UNIT=0 disables skipSlowCmdGCTest's testing.Short fast-path.
GC_FAST_UNIT=0 \
  exec go test -tags liveprobe -count=1 -timeout 30m -v \
    -run TestLiveNudgeSubmitProbe ./cmd/gc
