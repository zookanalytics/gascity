#!/usr/bin/env bash
# work-holder — whether a live session other than this one holds a work bead's
# tree, so a second dispatch of the work does not adopt a tree that a run is
# still using.
#
#   work-holder.sh --bead <id> [--work-dir <path>] [--holder <session>] [--branch <name>]
#
# A session running a worktree workflow never claims its work bead, so the
# bead's assignee cannot show the run. The run records itself instead:
# workspace-setup sets the bead's metadata.work_dir to the tree it enters and
# metadata.work_holder to the session that entered it. The caller passes the
# work bead's metadata.work_dir, metadata.work_holder and metadata.branch as it
# read them.
#
# The recorded holder decides, never where the tree sits on disk. A holder
# that names this session is a resume. A holder listed by
# `gc session list --state all` holds the work, whether it is active, asleep
# or suspended. A holder absent from that list is gone, and its tree is this
# run's to adopt, which is how a re-dispatch recovers a crashed run. A bead
# that records no holder is unheld, the way a bead with no assignee is
# unclaimed. A holder is matched against each listed session's id,
# session_name and alias.
#
# The worktree with --branch checked out is judged by the holder recorded
# beside it: the work bead's own when it is the bead's work_dir, otherwise the
# holder of the unclosed bead on that branch whose work_dir it is, such as an
# anchor or a sibling rework child. A tree with the branch checked out that no
# unclosed bead on the branch records names no holder, so it is undecided, and
# the finding says how to free it. The worktrees are listed in $GC_RIG_ROOT, or
# in $GC_DIR when no rig root is set, never in the current directory, which may
# be neither. A directory that is not a git repository has no worktrees to
# list.
#
# An unreadable session list, bead listing or worktree listing is undecided.
#
# stdout: one sentence per finding, naming the tree and its count of
#         uncommitted paths, who holds it or why that is undecided, and what
#         clears it.
# exit:   0 no live session holds the work · 1 a live session holds it ·
#         2 undecided · 64 usage
set -uo pipefail

PROG="work-holder"

usage() {
  cat >&2 <<'USAGE'
usage: work-holder.sh --bead <id> [--work-dir <path>] [--holder <session>] [--branch <name>]

  --bead      the work bead (required)
  --work-dir  the bead's metadata.work_dir; empty when it records none
  --holder    the bead's metadata.work_holder; empty when it records none
  --branch    the bead's metadata.branch; empty when it records none

env: GC_SESSION_ID, GC_SESSION_NAME, GC_AGENT and GC_ALIAS name this session.
     GC_RIG_ROOT is the repository whose worktrees are listed, and GC_DIR is
     used when it is unset.

exit: 0 no live session holds the work · 1 a live session holds it · 2 undecided · 64 usage
USAGE
}

# `OPT="$2"; shift 2` hangs the parse loop when the option ends argv. An empty
# value is valid: the caller passes the bead's metadata as it finds it.
require_value() {
  if [ "$#" -lt 2 ]; then
    echo "$PROG: $1 requires a value" >&2
    usage
    exit 64
  fi
  case "$2" in
    --bead|--work-dir|--holder|--branch|-h|--help)
      echo "$PROG: $1 requires a value, but the next argument is the option '$2'" >&2
      usage
      exit 64 ;;
  esac
}

BEAD=""; WORK_DIR=""; HOLDER=""; BRANCH=""
while [ $# -gt 0 ]; do
  case "$1" in
    --bead)     require_value "$@"; BEAD="$2";     shift 2 ;;
    --work-dir) require_value "$@"; WORK_DIR="$2"; shift 2 ;;
    --holder)   require_value "$@"; HOLDER="$2";   shift 2 ;;
    --branch)   require_value "$@"; BRANCH="$2";   shift 2 ;;
    -h|--help)  usage; exit 64 ;;
    *)          echo "$PROG: unknown argument '$1'" >&2; usage; exit 64 ;;
  esac
done
if [ -z "$BEAD" ]; then
  echo "$PROG: --bead is required" >&2
  usage
  exit 64
fi

NL='
'
US=$(printf '\037')

# A raw C0 byte inside a JSON string aborts jq on the whole payload, so every
# C0 byte is scrubbed before jq. Dropping a structural LF or TAB only minifies.
scrub() { tr -d '\000-\037'; }

# is_self ID — whether ID names this session.
is_self() {
  case "$1" in
    "${GC_SESSION_ID:-}"|"${GC_SESSION_NAME:-}"|"${GC_AGENT:-}"|"${GC_ALIAS:-}") [ -n "$1" ] ;;
    *) return 1 ;;
  esac
}

# uncommitted DIR — how many paths git reports changed or untracked in DIR.
# The tree may be a live session's, so the read takes no optional lock: a plain
# status refreshes the index under index.lock, and a lock held at the wrong
# moment fails that session's own git add or commit.
uncommitted() {
  local status
  if status=$(git --no-optional-locks -C "$1" status --porcelain 2>/dev/null); then
    printf '%s' "$status" | grep -c .
  else
    echo unreadable
  fi
}

# describe TREE — the tree as a finding names it.
describe() {
  local dir
  if [ -z "$1" ]; then
    printf 'its tree (none recorded)'
  elif dir=$(cd "$1" 2>/dev/null && pwd -P); then
    printf 'worktree %s (uncommitted paths: %s)' "$dir" "$(uncommitted "$dir")"
  else
    printf 'tree %s (not on disk)' "$1"
  fi
}

SESSIONS=""
SESSIONS_READ=""
read_sessions() {
  if [ -z "$SESSIONS_READ" ]; then
    SESSIONS=$(gc session list --state all --json 2>/dev/null </dev/null | scrub)
    if printf '%s' "$SESSIONS" | jq -e '.sessions | type == "array" and all(.[]; type == "object")' >/dev/null 2>&1; then
      SESSIONS_READ=ok
    else
      SESSIONS_READ=failed
    fi
  fi
  [ "$SESSIONS_READ" = ok ]
}

# listed_name ID — the name of the listed session ID names, or nothing when no
# listed session matches it.
listed_name() {
  printf '%s' "$SESSIONS" | jq -r --arg h "$1" '[.sessions[] | select((.id // "") == $h or (.session_name // "") == $h or (.alias // "") == $h)] | first | if . == null then "" else (.session_name // .id // $h) end'
}

HELD=""
UNDECIDED=""

# judge RECORDER TREE HOLDER — judge the holder bead RECORDER records beside
# TREE.
judge() {
  local recorder="$1" tree="$2" holder="$3" name
  [ -n "$holder" ] || return 0
  is_self "$holder" && return 0
  if ! read_sessions; then
    UNDECIDED="$UNDECIDED${UNDECIDED:+$NL}$recorder records $holder as the holder of $(describe "$tree"), and the session list could not be read, so $holder cannot be ruled out. Retry once gc session list --state all answers."
    return 0
  fi
  name=$(listed_name "$holder")
  [ -n "$name" ] || return 0
  HELD="$HELD${HELD:+$NL}$recorder records live $name as the holder of $(describe "$tree"). If $name is not working $recorder, clear the record with gc bd update $recorder --unset-metadata work_holder."
}

# judge_branch_tree TREE — judge the tree with BRANCH checked out that is not
# the work bead's own recorded tree.
judge_branch_tree() {
  local tree="$1" rows id wd holder recorded=""
  # bd lists every bead that is not closed unless a status is named, so a
  # parked bead's record counts too.
  rows=$(gc bd list --metadata-field "branch=$BRANCH" --json --brief --limit 0 2>/dev/null </dev/null | scrub)
  if ! printf '%s' "$rows" | jq -e 'type == "array" and all(.[]; type == "object")' >/dev/null 2>&1; then
    UNDECIDED="$UNDECIDED${UNDECIDED:+$NL}$BRANCH is checked out in $(describe "$tree"), and the beads on $BRANCH could not be listed, so who holds it cannot be read. Retry once gc bd list answers."
    return 0
  fi
  # Fields are split on US (0x1f), which the scrub removed from every value. A
  # tab would not do: read folds a run of tabs into one separator, so an empty
  # work_dir would shift the holder into its place.
  while IFS="$US" read -r id wd holder; do
    [ -n "$id" ] && [ -n "$wd" ] && [ "$wd" -ef "$tree" ] || continue
    recorded=1
    judge "$id" "$tree" "$holder"
  done <<EOF
$(printf '%s' "$rows" | jq -r '.[] | [(.id // ""), (.metadata.work_dir // ""), (.metadata.work_holder // "")] | map(tostring) | join("\u001f")')
EOF
  [ -n "$recorded" ] && return 0
  UNDECIDED="$UNDECIDED${UNDECIDED:+$NL}$BRANCH is checked out in $(describe "$tree"), which no unclosed bead on $BRANCH records as its work_dir, so nothing names who is working there. If nobody is, move the branch off it with git -C $tree switch --detach."
}

judge "$BEAD" "$WORK_DIR" "$HOLDER"

if [ -n "$BRANCH" ]; then
  REPO="${GC_RIG_ROOT:-${GC_DIR:-}}"
  if [ -n "$REPO" ] && git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1; then
    if WORKTREES=$(git -C "$REPO" worktree list --porcelain 2>/dev/null); then
      BRANCH_TREE=$(printf '%s\n' "$WORKTREES" | awk -v ref="branch refs/heads/$BRANCH" 'index($0, "worktree ") == 1 { tree = substr($0, 10) } $0 == ref { print tree; exit }')
      # A listed tree with no directory behind it holds nothing to adopt, and
      # the work bead's own tree was judged above.
      if [ -n "$BRANCH_TREE" ] && BRANCH_DIR=$(cd "$BRANCH_TREE" 2>/dev/null && pwd -P) \
        && ! { [ -n "$WORK_DIR" ] && [ "$WORK_DIR" -ef "$BRANCH_DIR" ]; }; then
        judge_branch_tree "$BRANCH_DIR"
      fi
    else
      UNDECIDED="$UNDECIDED${UNDECIDED:+$NL}git could not list the worktrees of $REPO, so a tree with $BRANCH checked out cannot be ruled out. Retry once git -C $REPO worktree list --porcelain succeeds."
    fi
  fi
fi

if [ -n "$HELD" ]; then
  printf '%s\n' "$HELD${UNDECIDED:+$NL$UNDECIDED}"
  exit 1
fi
if [ -n "$UNDECIDED" ]; then
  printf '%s\n' "$UNDECIDED"
  exit 2
fi
exit 0
