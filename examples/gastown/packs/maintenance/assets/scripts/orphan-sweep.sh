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

# Step 1: Get all in-progress beads with assignees.
IN_PROGRESS=$(bd list --status=in_progress --json --limit=0 2>/dev/null) || exit 0
if [ -z "$IN_PROGRESS" ] || [ "$IN_PROGRESS" = "[]" ]; then
    exit 0
fi

# Step 2: Get all known agent names (from config, scoped to [[agent]] blocks).
AGENTS=$(gc config show 2>/dev/null | awk '/^\[\[agent\]\]/{a=1} a && /^\s*name\s*=/{print; a=0}' | sed 's/.*=\s*"\(.*\)"/\1/') || exit 0
if [ -z "$AGENTS" ]; then
    exit 0
fi

# Build a lookup set of known agents.
declare -A KNOWN_AGENTS
while IFS= read -r agent; do
    KNOWN_AGENTS["$agent"]=1
done <<< "$AGENTS"

# Step 3: Find orphaned beads (assigned to non-existent agents).
# Handles three forms of assignee names:
#   1. Bare template name (e.g. "deacon")
#   2. PackV2 binding-qualified (e.g. "gastown.deacon", "gascity/refinery")
#   3. Pool instance with -N suffix, possibly combined with (2)
#      (e.g. "polecat-3", "signal-loom/polecat-3")
is_known_agent() {
    local name="$1"
    # Direct match.
    if [ -n "${KNOWN_AGENTS[$name]+x}" ]; then return 0; fi
    # PackV2 qualified: "binding.template" or "rig/template" — strip prefix.
    local tmpl="${name##*[./]}"
    if [ "$tmpl" != "$name" ] && [ -n "${KNOWN_AGENTS[$tmpl]+x}" ]; then return 0; fi
    # Pool instance: strip trailing -<digits> and check template name.
    local base="${tmpl%-[0-9]*}"
    if [ "$base" != "$tmpl" ] && [ -n "${KNOWN_AGENTS[$base]+x}" ]; then return 0; fi
    return 1
}

ORPHANED=0
echo "$IN_PROGRESS" | jq -r '.[] | select(.assignee != null and .assignee != "") | "\(.id)\t\(.assignee)"' 2>/dev/null | while IFS=$'\t' read -r bead_id assignee; do
    if ! is_known_agent "$assignee"; then
        bd update "$bead_id" --status=open --assignee="" 2>/dev/null || true
        ORPHANED=$((ORPHANED + 1))
    fi
done

if [ "$ORPHANED" -gt 0 ]; then
    echo "orphan-sweep: reset $ORPHANED orphaned beads"
fi
