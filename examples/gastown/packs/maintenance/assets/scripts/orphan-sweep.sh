#!/usr/bin/env bash
# orphan-sweep — reset beads assigned to dead agents, scoped to current rig.
#
# Per-scope script. Invoked from city cwd (GC_RIG empty) it sweeps HQ-scoped
# beads against city-scoped agents; invoked from rig cwd (GC_RIG set) it
# sweeps that rig's beads against agents bound to that rig. The order
# dispatcher registers one instance for HQ plus one per non-HQ rig, so each
# invocation's bd scope is bounded by its cwd. No shell fan-out, no temp
# file, no jq -s union.
#
# KNOWN_AGENTS is populated in canonical form for the current scope from
# `gc status --json` (qualified names with pool members enumerated and
# binding/rig prefixes applied) and `gc session list --json` (running
# session and agent names). For rig V2 entries we also insert the V1
# alternate "<rig>/<name>" composed from the agent's bare name + scope
# rig — this is structural composition, not stripping; the assignee is
# never modified at lookup time. is_known_agent is a direct hash lookup.
#
# Does NOT do worktree salvage — that's the witness's job.
#
# Runs as an exec order (no LLM, no agent, no wisp).
set -euo pipefail

SCOPE_RIG="${GC_RIG:-}"
SCOPE_LABEL="${SCOPE_RIG:-HQ}"

# Step 1: In-progress beads in the current scope.
IN_PROGRESS=$(bd list --status=in_progress --json --limit=0 2>/dev/null) || exit 0
if [ -z "$IN_PROGRESS" ] || [ "$IN_PROGRESS" = "[]" ]; then
    exit 0
fi

# Step 2: Canonical agent and session identities for this scope.
STATUS_JSON=$(gc status --json 2>/dev/null) || exit 0
[ -n "$STATUS_JSON" ] || exit 0
SESSIONS_JSON=$(gc session list --json 2>/dev/null) || SESSIONS_JSON="[]"
[ -n "$SESSIONS_JSON" ] || SESSIONS_JSON="[]"

declare -A KNOWN_AGENTS

if [ -z "$SCOPE_RIG" ]; then
    # HQ: configured agents with scope=city.
    while IFS= read -r qn; do
        [ -n "$qn" ] && KNOWN_AGENTS["$qn"]=1
    done < <(printf "%s" "$STATUS_JSON" | \
        jq -r '.agents[] | select(.scope == "city") | .qualified_name' 2>/dev/null)

    # HQ running sessions: AgentName has no "/" (rig prefix) and is
    # non-empty. Both AgentName and SessionName are accepted as assignees.
    while IFS=$'\t' read -r agent session; do
        [ -n "$agent" ] && KNOWN_AGENTS["$agent"]=1
        [ -n "$session" ] && KNOWN_AGENTS["$session"]=1
    done < <(printf "%s" "$SESSIONS_JSON" | \
        jq -r '.[] | select((.AgentName // "") != "" and ((.AgentName // "") | contains("/") | not)) | "\((.AgentName // ""))\t\((.SessionName // ""))"' 2>/dev/null)
else
    # Rig: configured agents whose qualified_name is "<rig>/...". Insert
    # the canonical name AND a V1 alternate composed from rig + bare name.
    while IFS=$'\t' read -r qn name; do
        [ -n "$qn" ] && KNOWN_AGENTS["$qn"]=1
        [ -n "$name" ] && KNOWN_AGENTS["$SCOPE_RIG/$name"]=1
    done < <(printf "%s" "$STATUS_JSON" | \
        jq -r --arg prefix "$SCOPE_RIG/" \
        '.agents[] | select(.qualified_name | startswith($prefix)) | "\(.qualified_name)\t\(.name)"' 2>/dev/null)

    # Rig running sessions: AgentName starts with "<rig>/".
    while IFS=$'\t' read -r agent session; do
        [ -n "$agent" ] && KNOWN_AGENTS["$agent"]=1
        [ -n "$session" ] && KNOWN_AGENTS["$session"]=1
    done < <(printf "%s" "$SESSIONS_JSON" | \
        jq -r --arg prefix "$SCOPE_RIG/" \
        '.[] | select((.AgentName // "") | startswith($prefix)) | "\((.AgentName // ""))\t\((.SessionName // ""))"' 2>/dev/null)
fi

# Abort cleanly when nothing is enumerable, rather than reset every assignee.
if [ "${#KNOWN_AGENTS[@]}" -eq 0 ]; then
    exit 0
fi

# Step 3: Direct lookup — present in KNOWN_AGENTS means known.
is_known_agent() {
    [ -n "${KNOWN_AGENTS[$1]+x}" ]
}

# Step 4: Reset in-progress beads whose assignee isn't a known agent.
ORPHANED=0
while IFS=$'\t' read -r bead_id assignee; do
    if ! is_known_agent "$assignee"; then
        bd update "$bead_id" --status=open --assignee="" 2>/dev/null || true
        ORPHANED=$((ORPHANED + 1))
    fi
done < <(echo "$IN_PROGRESS" | jq -r '.[] | select(.assignee != null and .assignee != "") | "\(.id)\t\(.assignee)"' 2>/dev/null)

if [ "$ORPHANED" -gt 0 ]; then
    echo "orphan-sweep[$SCOPE_LABEL]: reset $ORPHANED orphaned beads"
fi
