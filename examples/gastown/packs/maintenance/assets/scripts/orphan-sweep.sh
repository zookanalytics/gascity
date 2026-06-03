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

# Trace bd invocations to $GC_BD_TRACE when set (no-op otherwise).
__SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "$__SCRIPT_DIR/_bd_trace.sh" "orphan-sweep"

CITY="${GC_CITY:-.}"

# Step 1: Collect in-progress beads from HQ and every rig whose session
# liveness can be determined.
# `gc bd list` without --rig is HQ-scoped from the city cwd, so per-rig
# beads are invisible to a bare query — walk every rig explicitly.
TMP=$(mktemp) || exit 0
SESSION_TMP=$(mktemp) || {
    rm -f "$TMP"
    exit 0
}
trap 'rm -f "$TMP" "$SESSION_TMP"' EXIT

RIG_NAMES=""
RIG_LIST=$(gc rig list --json 2>/dev/null) || RIG_LIST=""
if [ -n "$RIG_LIST" ]; then
    RIG_NAMES=$(echo "$RIG_LIST" | jq -r '.rigs[] | select(.hq == false) | .name' 2>/dev/null) || RIG_NAMES=""
fi

append_session_list() {
    local session_fetch_tmp
    session_fetch_tmp=$(mktemp) || return 1
    if "$@" >"$session_fetch_tmp" 2>/dev/null; then
        cat "$session_fetch_tmp" >>"$SESSION_TMP"
        rm -f "$session_fetch_tmp"
        return 0
    fi
    rm -f "$session_fetch_tmp"
    return 1
}

append_hq_scope() {
    local bead_fetch_tmp
    bead_fetch_tmp=$(mktemp) || return 1
    append_session_list gc session list --json || {
        rm -f "$bead_fetch_tmp"
        return 1
    }
    gc bd list --status=in_progress --json --limit=0 2>/dev/null >"$bead_fetch_tmp" || true
    append_session_list gc session list --json || {
        rm -f "$bead_fetch_tmp"
        return 1
    }
    cat "$bead_fetch_tmp" >>"$TMP"
    rm -f "$bead_fetch_tmp"
}

append_rig_scope() {
    local rig="$1"
    local bead_fetch_tmp
    bead_fetch_tmp=$(mktemp) || return 1
    append_session_list gc --rig "$rig" session list --json || {
        rm -f "$bead_fetch_tmp"
        return 1
    }
    gc bd list --rig "$rig" --status=in_progress --json --limit=0 2>/dev/null >"$bead_fetch_tmp" || true
    append_session_list gc --rig "$rig" session list --json || {
        rm -f "$bead_fetch_tmp"
        return 1
    }
    cat "$bead_fetch_tmp" >>"$TMP"
    rm -f "$bead_fetch_tmp"
}

# Step 1b: Get all known live session identities around each bead-list query.
# The second liveness pass closes the session-list-before-bd-list race where a
# newly spawned session can claim work after the first pass but before bd-list.
# HQ liveness is required; per-rig failures only skip that rig's staged bead
# rows so one stale or unreachable rig does not disable cleanup elsewhere.
append_hq_scope || exit 0

while IFS= read -r rig; do
    [ -z "$rig" ] && continue
    if ! append_rig_scope "$rig"; then
        echo "orphan-sweep: skipping rig $rig after session-list failure" >&2
    fi
done <<<"$RIG_NAMES"

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

# Step 2b: Parse identities of every live session so that pool-spawned
# ephemeral assignees (e.g. gastown__polekitten-gc-q9j0om) are treated as
# known, mirroring the Go-side liveOpenSessionAssignmentExists guard
# (false-orphan loop history: ga-nvx, gastownhall/gascity#2363).
# `gc session list --json` has shipped two field generations:
#   gen A: {"sessions":[{closed,id,session_name,alias,agent_name}], "schema_version":...}
#   gen B: {"_cache_age_s","sessions":[{ID,SessionName,Alias,Title,Template,Running,...}]}
# Accept both; deriving liveness from whichever marker the object carries.
# Every captured response must carry a recognizable `sessions` array. A
# response without one (renamed/moved container, or empty output from a
# successful command) means the sweep is blind to liveness — refuse rather
# than mass-orphan. `sessions: []` remains a legitimate empty town.
TOTAL_SESSION_DOCS=$(jq -r -s 'length' "$SESSION_TMP" 2>/dev/null) || exit 0
RECOGNIZED_SESSION_DOCS=$(jq -r -s '[.[] | select((.sessions? | type) == "array")] | length' "$SESSION_TMP" 2>/dev/null) || exit 0
if [ "${TOTAL_SESSION_DOCS:-0}" -eq 0 ] || [ "$RECOGNIZED_SESSION_DOCS" != "$TOTAL_SESSION_DOCS" ]; then
    echo "orphan-sweep: session list response lacks a recognizable sessions container — refusing to sweep" >&2
    exit 0
fi

# A response whose recognized `sessions` array is empty but which carries
# session-LIKE objects elsewhere (in arrays, object maps, or nested
# containers) may be reporting live sessions under a renamed sibling
# container (mixed-container drift). An object is session-like when it has a
# session identity field, or an ID/Title field together with a
# liveness/state marker. Unrelated metadata objects (warnings, summaries)
# do not match, so they cannot disable empty-town cleanup.
SUSPECT_SIBLING_DOCS=$(jq -r -s '
    [.[]
     | select((.sessions? | type) == "array" and (.sessions | length) == 0)
     | del(.sessions)
     | [.. | objects
        | select(
            has("SessionName") or has("session_name") or has("Alias") or has("alias")
            or has("Template") or has("agent_name")
            or ((has("ID") or has("id") or has("Title"))
                and (has("Running") or has("closed") or has("State") or has("state")))
          )]
     | select(length > 0)
    ] | length
' "$SESSION_TMP" 2>/dev/null) || exit 0
if [ "${SUSPECT_SIBLING_DOCS:-1}" -gt 0 ]; then
    echo "orphan-sweep: session list reports an empty town but carries session-like objects outside the sessions array — refusing to sweep" >&2
    exit 0
fi
TOTAL_SESSION_OBJECTS=$(jq -r -s '[.[] | .sessions[]?] | length' "$SESSION_TMP" 2>/dev/null) || exit 0
LIVE_SESSION_IDS=$(jq -r -s '
    .[] | .sessions[]?
    | select((.Running == true) // (.closed == false))
    | [.ID // .id, .SessionName // .session_name, .Alias // .alias, .Title, .agent_name // .Template]
    | .[]
    | select(. != null and . != "")
' "$SESSION_TMP" 2>/dev/null) || exit 0

# Session objects present but zero identities extracted means a third field
# generation has shipped. Resetting on that blindness would mass-orphan every
# live pool assignee (the #2363 failure mode) — refuse to sweep instead.
if [ "${TOTAL_SESSION_OBJECTS:-0}" -gt 0 ] && [ -z "$LIVE_SESSION_IDS" ]; then
    echo "orphan-sweep: $TOTAL_SESSION_OBJECTS sessions listed but none matched a known schema — refusing to sweep" >&2
    exit 0
fi

agent_exists() {
    local candidate="$1"
    [ -n "$candidate" ] && printf '%s\n' "$AGENTS" | grep -Fxq -- "$candidate"
}

live_session_match() {
    local candidate="$1"
    [ -n "$candidate" ] && [ -n "$LIVE_SESSION_IDS" ] \
        && printf '%s\n' "$LIVE_SESSION_IDS" | grep -Fxq -- "$candidate"
}

# Step 3: Find orphaned beads (assigned to non-existent agents).
# Pool instances use names like "worker-3"; strip the -N suffix to match
# the template name from config.
is_known_agent() {
    local name="$1"
    # Direct match against a configured agent template name.
    if agent_exists "$name"; then return 0; fi
    # Pool instance: strip trailing -<digits> and check template name.
    local base="${name%-[0-9]*}"
    if [ "$base" != "$name" ] && agent_exists "$base"; then return 0; fi
    # City-qualified assignee (gastown.deacon): strip everything through the
    # last dot and re-check. This relies on flattened pack binding chains.
    # Defense-in-depth for older binaries that fall through to `gc config show`
    # and emit unqualified names. Also covers pool patterns like
    # "gastown.dog-3" by re-stripping the -N suffix.
    local short="${name##*.}"
    if [ "$short" != "$name" ]; then
        if agent_exists "$short"; then return 0; fi
        local short_base="${short%-[0-9]*}"
        if [ "$short_base" != "$short" ] && agent_exists "$short_base"; then return 0; fi
    fi
    # Live ephemeral session names like gastown__polekitten-gc-q9j0om won't
    # match any template form — accept them as known when a non-closed session
    # is currently running with a matching ID, SessionName, Alias, or
    # AgentName. Mirrors liveOpenSessionAssignmentExists in the Go path.
    if live_session_match "$name"; then return 0; fi
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
