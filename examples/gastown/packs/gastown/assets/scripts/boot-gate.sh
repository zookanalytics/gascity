#!/usr/bin/env bash
# boot-gate — mechanical staleness gate for the deacon watchdog (boot).
#
# Replaces boot's `mode = "always"` per-tick polling. The controller used to
# revive the always-on boot session on essentially every patrol tick (~30s),
# and each revival wrote boot's session bead — a Dolt commit per tick, the
# dominant source of churn on the city store even on a healthy, idle town.
#
# This gate moves boot onto the standard observer -> routed-work ->
# on_demand-worker pattern (the same shape the refinery uses):
#
#   1. Find the deacon's in-progress patrol wisp.
#   2. If it is fresh (younger than the staleness threshold), do nothing.
#      A healthy deacon pours a new patrol wisp every cycle and backs off at
#      most ~300s, so its in-progress wisp is never older than that — the
#      gate creates zero beads and boot never spawns.
#   3. If it is stale, create ONE work bead assigned to boot. The controller's
#      on_demand named-session demand check (build_desired_state.go) wakes
#      boot on the assignee match; boot then applies the judgment that ZFC
#      keeps out of Go (wedged vs. legitimate backoff) and resolves the bead.
#
# The gate decides only the mechanical FACT "wisp age > threshold" — a timer
# comparison, like the gate-sweep and reaper orders. The "is the deacon
# actually stuck?" judgment stays with boot's LLM.
#
# Idempotent: while a wake bead is still unresolved, the gate does not create
# another, so a deacon stuck across several cooldowns wakes boot at most once
# per bead lifetime rather than every tick.
#
# Runs as an exec order (no LLM, no agent, no wisp).
#
# Env:
#   GC_BOOT_GATE_STALENESS  wisp-age threshold in seconds (default 600 — ~2x
#                           the deacon's 300s backoff cap, conservative).
#   GC_BOOT_GATE_DEACON     exact deacon assignee to match (default: any
#                           in-progress wisp whose assignee base name is
#                           "deacon", which auto-adapts to the binding prefix).
#   GC_BOOT_GATE_BOOT       exact boot identity to wake (default: derived from
#                           the matched deacon assignee by swapping the
#                           trailing "deacon" for "boot", preserving prefix).
#   GC_BOOT_GATE_LABEL      label stamped on wake beads (default "boot-gate").
set -euo pipefail

# jq is a hard dependency: the gate decodes bead JSON and computes wisp age
# with it. Fail loud rather than silently never gating.
if ! command -v jq >/dev/null 2>&1; then
    echo "boot-gate: jq is required but not found in PATH" >&2
    exit 1
fi

STALENESS="${GC_BOOT_GATE_STALENESS:-600}"
LABEL="${GC_BOOT_GATE_LABEL:-boot-gate}"
DEACON_MATCH="${GC_BOOT_GATE_DEACON:-}"
BOOT_OVERRIDE="${GC_BOOT_GATE_BOOT:-}"

# Step 1: discover the deacon's in-progress patrol wisp. Patrol iterations are
# wisps (mol-deacon-patrol pours and burns one per cycle), so the in-progress
# wisp set is bounded by the number of live patrol agents — a cheap query.
# Wisps are ephemeral beads and `bd` has no "wisp" issue type — filtering a
# bead list by that nonexistent type exits non-zero, which combined with the
# `|| exit 0` below would silently no-op the gate and leave on_demand boot with
# no wake source. Discover via the ephemeral query tier instead and let the jq
# below narrow the rows to the deacon's patrol. A read failure (API down) must
# not crash the controller's order loop.
WISPS_JSON="$(gc bd query --json 'ephemeral=true AND status=in_progress' --limit=0 2>/dev/null)" || exit 0
[ -n "$WISPS_JSON" ] || exit 0

# Select the freshest matching wisp and emit "<assignee>\t<id>\t<age_seconds>".
# When GC_BOOT_GATE_DEACON is set we match it exactly; otherwise we match any
# assignee whose final path/binding segment is "deacon" (e.g. "deacon",
# "gastown.deacon", "rig/gastown.deacon"). Fractional seconds are trimmed so
# jq's whole-second fromdateiso8601 parses the RFC3339 Z timestamp.
SELECTED="$(printf '%s' "$WISPS_JSON" | jq -r --arg match "$DEACON_MATCH" '
    [ .[]
      | select((.issue_type // .type // "") != "message")
      | select(
          if $match != "" then (.assignee // "") == $match
          else (.assignee // "") | test("(^|[./])deacon$")
          end
        )
      | { assignee: (.assignee // ""), id: .id, ts: (.updated_at // .created_at // "") }
      | select(.ts != "")
    ]
    | sort_by(.ts) | last
    | select(. != null)
    | [ .assignee, .id, ((now - (.ts | sub("\\.[0-9]+";"") | fromdateiso8601)) | floor | tostring) ]
    | @tsv
' 2>/dev/null)" || SELECTED=""

# No in-progress deacon wisp: nothing to gate. A dead deacon (no wisp, no
# session) is the controller's liveness job, not boot's — boot only judges an
# alive-but-wedged deacon, which always has a frozen in-progress wisp.
[ -n "$SELECTED" ] || exit 0

DEACON_ID="$(printf '%s' "$SELECTED" | cut -f1)"
WISP_ID="$(printf '%s' "$SELECTED" | cut -f2)"
AGE="$(printf '%s' "$SELECTED" | cut -f3)"

# Guard against an unparseable timestamp (non-numeric age). Skip conservatively
# and surface it rather than waking boot on bad data.
case "$AGE" in
    ''|*[!0-9-]*)
        echo "boot-gate: unparseable wisp timestamp for $DEACON_ID; skipping" >&2
        exit 0
        ;;
esac

# Step 2: fresh wisp -> deacon is cycling normally. Do nothing.
if [ "$AGE" -le "$STALENESS" ]; then
    exit 0
fi

# Step 3: stale wisp. Derive the boot identity from the deacon's, preserving
# any binding prefix ("gastown.deacon" -> "gastown.boot", "deacon" -> "boot").
if [ -n "$BOOT_OVERRIDE" ]; then
    BOOT_ID="$BOOT_OVERRIDE"
else
    BOOT_ID="${DEACON_ID%deacon}boot"
fi

# Idempotency: if boot already has an unresolved wake bead, do not pile on a
# second. boot resolves (closes) the bead each wake, so the next cooldown can
# re-wake it only if the wisp is still stale.
EXISTING="$(gc bd list --assignee="$BOOT_ID" --label="$LABEL" --status=open,in_progress --json --limit=0 2>/dev/null)" || EXISTING=""
EXISTING_COUNT="$(printf '%s' "$EXISTING" | jq 'length' 2>/dev/null)" || EXISTING_COUNT=0
if [ "${EXISTING_COUNT:-0}" -gt 0 ]; then
    exit 0
fi

# Step 4: create the wake bead. The assignee (not gc.routed_to) is what the
# controller's named on_demand demand check keys on, so set it directly.
META="$(jq -nc \
    --arg target "$DEACON_ID" \
    --arg age "$AGE" \
    --arg threshold "$STALENESS" \
    --arg wisp "$WISP_ID" \
    '{
        target: $target,
        reason: ("patrol wisp stale: " + $age + "s > " + $threshold + "s"),
        requester: "boot-gate",
        wisp_id: $wisp,
        wisp_age_seconds: $age
    }')"

gc bd create \
    --type=task \
    --labels="$LABEL" \
    --assignee="$BOOT_ID" \
    --title="Boot check: $DEACON_ID patrol wisp stale (${AGE}s)" \
    --metadata="$META" >/dev/null

echo "boot-gate: deacon $DEACON_ID wisp stale (${AGE}s > ${STALENESS}s) — woke $BOOT_ID"
