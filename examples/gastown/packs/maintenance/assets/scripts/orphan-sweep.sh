#!/usr/bin/env bash
# orphan-sweep — reset beads assigned to dead agents.
#
# Replaces the deacon patrol town-orphan-sweep step. Cross-references
# in-progress beads against all known agents. Beads assigned to agents
# that don't exist in ANY rig get reset to open/unassigned so the rig's
# witness picks them up on its next patrol.
#
# Does NOT do worktree salvage — that's the witness's job.
#
# Runs as an exec order (no LLM, no agent, no wisp).
set -euo pipefail

CITY="${GC_CITY:-.}"

# Step 1: Collect in-progress beads from HQ and every rig.
# `gc bd list` without --rig is HQ-scoped from the city cwd, so per-rig
# beads are invisible to a bare query — walk every rig explicitly.
TMP=$(mktemp) || exit 0
trap 'rm -f "$TMP"' EXIT

gc bd list --status=in_progress --json --limit=0 2>/dev/null >>"$TMP" || true

RIG_LIST=$(gc rig list --json 2>/dev/null) || RIG_LIST=""
if [ -n "$RIG_LIST" ]; then
    RIG_NAMES=$(echo "$RIG_LIST" | jq -r '.rigs[] | select(.hq == false) | .name' 2>/dev/null) || RIG_NAMES=""
    while IFS= read -r rig; do
        [ -z "$rig" ] && continue
        gc bd list --rig "$rig" --status=in_progress --json --limit=0 2>/dev/null >>"$TMP" || true
    done <<<"$RIG_NAMES"
fi

IN_PROGRESS=$(jq -c -s 'add // []' "$TMP" 2>/dev/null) || IN_PROGRESS="[]"
if [ "$IN_PROGRESS" = "[]" ]; then
    exit 0
fi

# Step 2: Get all known agent identities from resolved config.
# `gc config explain` prints Agent.QualifiedName(), including import binding
# and rig scope. Fall back to the older config-show parser for older binaries.
AGENTS=$(gc config explain 2>/dev/null | awk '/^Agent: /{print $2}') || AGENTS=""
if [ -z "$AGENTS" ]; then
    AGENTS=$(gc config show 2>/dev/null | awk '/^\[\[agent\]\]/{a=1} a && /^[[:space:]]*name[[:space:]]*=/{print; a=0}' | sed 's/.*=[[:space:]]*"\(.*\)"/\1/') || exit 0
fi
if [ -z "$AGENTS" ]; then
    exit 0
fi

agent_exists() {
    local candidate="$1"
    [ -n "$candidate" ] && printf '%s\n' "$AGENTS" | grep -Fxq -- "$candidate"
}

# Step 3: Find orphaned beads (assigned to non-existent agents).
# Handles three forms of assignee names:
#   1. Bare template name (e.g. "deacon")
#   2. PackV2 binding-qualified (e.g. "gastown.deacon", "gascity/refinery")
#   3. Pool instance with -N suffix, possibly combined with (2)
#      (e.g. "polecat-3", "signal-loom/polecat-3")
is_known_agent() {
    local name="$1"
    # Direct match.
    if agent_exists "$name"; then return 0; fi
    # Pool instance: strip trailing -<digits> and check template name.
    local base="${name%-[0-9]*}"
    if [ "$base" != "$name" ] && agent_exists "$base"; then return 0; fi
    # PackV2 binding-qualified (gastown.deacon, gascity/refinery): strip
    # everything through the last dot or slash and re-check. Defense-in-
    # depth for older binaries that fall through to `gc config show` and
    # emit unqualified names. Also covers pool patterns like
    # "gastown.dog-3" or "signal-loom/polecat-3" by re-stripping the
    # -N suffix.
    local short="${name##*[./]}"
    if [ "$short" != "$name" ]; then
        if agent_exists "$short"; then return 0; fi
        local short_base="${short%-[0-9]*}"
        if [ "$short_base" != "$short" ] && agent_exists "$short_base"; then return 0; fi
    fi
    return 1
}

ORPHANED=0
# Process substitution (not a pipe) keeps the loop body in the parent
# shell so $ORPHANED survives for the summary message below.
while IFS=$'\t' read -r bead_id assignee; do
    if ! is_known_agent "$assignee"; then
        # `gc bd update` auto-resolves the bead's prefix to the right rig
        # store, so HQ and rig beads update in the correct database.
        gc bd update "$bead_id" --status=open --assignee="" 2>/dev/null || true
        ORPHANED=$((ORPHANED + 1))
    fi
done < <(echo "$IN_PROGRESS" | jq -r '.[] | select(.assignee != null and .assignee != "") | "\(.id)\t\(.assignee)"' 2>/dev/null)

if [ "$ORPHANED" -gt 0 ]; then
    echo "orphan-sweep: reset $ORPHANED orphaned beads"
fi
