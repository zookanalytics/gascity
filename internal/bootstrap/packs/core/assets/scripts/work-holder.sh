#!/usr/bin/env bash
# work-holder — whether a live session other than this one holds a work bead's
# tree, so a second dispatch of the work does not adopt a tree that a run is
# still using, and with --claim, taking the work for this session when none
# does.
#
#   work-holder.sh --bead <id> [--work-dir <path>] [--holder <session>] [--branch <name>] [--claim]
#
# A session running a worktree workflow never claims its work bead, so the
# bead's assignee cannot show the run. The run records itself instead, as the
# bead's metadata.work_holder: load-context's claim records the session that
# took the work, and workspace-setup sets metadata.work_dir to the tree it
# enters and records the session that entered it. The caller passes the work
# bead's metadata.work_dir, metadata.work_holder and metadata.branch as it read
# them.
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
# The trees a dispatch could adopt are the bead's work_dir and the worktree with
# --branch checked out. Each of them on disk is judged by every holder recorded
# beside it: the work bead's own, and that of each other unclosed bead on the
# branch whose work_dir it is, such as an anchor or a sibling rework child. A
# live holder in any one of those records holds the tree, whatever the others
# record. Without --branch, the work bead's own record is the only one read. A
# tree with the branch checked out that no unclosed bead on the branch records
# names no holder, so it is undecided, and the finding says how to free it. The
# worktrees are listed in $GC_RIG_ROOT, or in $GC_DIR when no rig root is set,
# never in the current directory, which may be neither. A directory that is not
# a git repository has no worktrees to list.
#
# An unreadable session list, bead listing or worktree listing is undecided.
#
# With --claim, work that no live session holds is taken for this session in
# the same run that judged it free: one compare-and-set (gc beads metadata-cas)
# records GC_SESSION_ID as the bead's work_holder only while the record still
# holds the --holder value the caller judged. Of two dispatches that judged the
# work free from the same record, the second one's compare-and-set finds the
# first one's record and conflicts, so only one of them takes the work. The
# holder a conflict finds is judged the way the caller's record was: a live
# one holds the work, and a gone one, or this session, is claimed over once
# more. The claim is written to the store of this session's rig ($GC_RIG), or
# to the city store when no rig is set. A store that cannot compare-and-set
# gets a read and a plain write instead, and the script says so on stderr,
# because a dispatch writing between the two can still take the work beside
# this one. A claim the store refuses for any other reason is undecided.
#
# stdout: one sentence per finding, naming the tree and its count of
#         uncommitted paths, who holds it or why that is undecided, and what
#         clears it.
# exit:   0 no live session holds the work, and with --claim this session's
#         claim on it is recorded · 1 a live session holds it ·
#         2 undecided · 64 usage
set -uo pipefail

PROG="work-holder"

usage() {
  cat >&2 <<'USAGE'
usage: work-holder.sh --bead <id> [--work-dir <path>] [--holder <session>] [--branch <name>] [--claim]

  --bead      the work bead (required)
  --work-dir  the bead's metadata.work_dir; empty when it records none
  --holder    the bead's metadata.work_holder; empty when it records none
  --branch    the bead's metadata.branch; empty when it records none
  --claim     when no live session holds the work, record this session as
              its holder, compare-and-set on the --holder value

env: GC_SESSION_ID, GC_SESSION_NAME, GC_AGENT and GC_ALIAS name this session,
     and --claim records GC_SESSION_ID. GC_RIG names the rig whose store
     --claim writes to; the city store is used when it is unset. GC_RIG_ROOT is
     the repository whose worktrees are listed, and GC_DIR is used when it is
     unset.

exit: 0 no live session holds the work, and with --claim this session's claim is
      recorded · 1 a live session holds it · 2 undecided · 64 usage
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
    --bead|--work-dir|--holder|--branch|--claim|-h|--help)
      echo "$PROG: $1 requires a value, but the next argument is the option '$2'" >&2
      usage
      exit 64 ;;
  esac
}

BEAD=""; WORK_DIR=""; HOLDER=""; BRANCH=""; CLAIM=""
while [ $# -gt 0 ]; do
  case "$1" in
    --bead)     require_value "$@"; BEAD="$2";     shift 2 ;;
    --work-dir) require_value "$@"; WORK_DIR="$2"; shift 2 ;;
    --holder)   require_value "$@"; HOLDER="$2";   shift 2 ;;
    --branch)   require_value "$@"; BRANCH="$2";   shift 2 ;;
    --claim)    CLAIM=1;                            shift ;;
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

ROWS=""
ROWS_READ=""
# read_rows — list the unclosed beads on BRANCH once, as one line per bead
# other than the work bead: its id, work_dir and work_holder. The work bead's
# own record is the one the caller passed.
read_rows() {
  local listed
  if [ -z "$ROWS_READ" ]; then
    # bd lists every bead that is not closed unless a status is named, so a
    # parked bead's record counts too.
    listed=$(gc bd list --metadata-field "branch=$BRANCH" --json --brief --limit 0 2>/dev/null </dev/null | scrub)
    if printf '%s' "$listed" | jq -e 'type == "array" and all(.[]; type == "object")' >/dev/null 2>&1; then
      # Fields are split on US (0x1f), which the scrub removed from every
      # value. A tab would not do: read folds a run of tabs into one
      # separator, so an empty work_dir would shift the holder into its place.
      ROWS=$(printf '%s' "$listed" | jq -r --arg bead "$BEAD" '.[] | select((.id // "") != $bead) | [(.id // ""), (.metadata.work_dir // ""), (.metadata.work_holder // "")] | map(tostring) | join("\u001f")')
      ROWS_READ=ok
    else
      ROWS_READ=failed
    fi
  fi
  [ "$ROWS_READ" = ok ]
}

# judge_tree TREE RECORDED — judge the holder each other unclosed bead on
# BRANCH records beside TREE, a tree on disk that the work could be adopted
# from. RECORDED is non-empty when the work bead's own record names TREE.
judge_tree() {
  local tree="$1" recorded="$2" id wd holder
  if ! read_rows; then
    UNDECIDED="$UNDECIDED${UNDECIDED:+$NL}Whether an unclosed bead on $BRANCH records a live holder of $(describe "$tree") cannot be read, because the beads on $BRANCH could not be listed. Retry once gc bd list answers."
    return 0
  fi
  while IFS="$US" read -r id wd holder; do
    [ -n "$id" ] && [ -n "$wd" ] && [ "$wd" -ef "$tree" ] || continue
    recorded=1
    judge "$id" "$tree" "$holder"
  done <<EOF
$ROWS
EOF
  [ -n "$recorded" ] && return 0
  UNDECIDED="$UNDECIDED${UNDECIDED:+$NL}$BRANCH is checked out in $(describe "$tree"), which no unclosed bead on $BRANCH records as its work_dir, so nothing names who is working there. If nobody is, move the branch off it with git -C $tree switch --detach."
}

# store_ref — the exact store the claim is written to: the store of this
# session's rig, or the city store when no rig is set.
store_ref() {
  local name
  if [ -n "${GC_RIG:-}" ]; then
    printf 'rig:%s' "$GC_RIG"
    return 0
  fi
  name=$(gc rig list --json 2>/dev/null </dev/null | scrub | jq -r '.city_name // empty' 2>/dev/null)
  [ -n "$name" ] || return 1
  printf 'city:%s' "$name"
}

# read_holder — print the work_holder the work bead records now.
read_holder() {
  gc bd show "$BEAD" --json 2>/dev/null </dev/null | scrub | jq -er '(if type == "array" then .[0] else . end) | if type == "object" then (.metadata.work_holder // "" | tostring) else error("no bead") end' 2>/dev/null
}

STORE=""
PLAIN=""
OUTCOME=""
CAS_ERROR=""
CAS_CMD=""
# cas EXPECTED — set the work bead's work_holder to this session only while it
# holds EXPECTED, leaving the outcome in OUTCOME: swapped, already_next or
# conflict. A store that cannot compare-and-set gets the same comparison as a
# read and a plain write. A command that fails returns non-zero, and leaves
# which one in CAS_CMD and why in CAS_ERROR.
cas() {
  local expected="$1" out current
  OUTCOME=""
  if [ -z "$PLAIN" ]; then
    CAS_CMD="gc beads metadata-cas"
    if out=$(gc beads metadata-cas "$BEAD" --store-ref="$STORE" --key=work_holder --expected="$expected" --next="$GC_SESSION_ID" --json 2>&1 </dev/null); then
      OUTCOME=$(printf '%s\n' "$out" | grep '^{' | tail -n 1 | jq -r '.outcome // empty' 2>/dev/null)
      return 0
    fi
    case "$out" in
      *"conditional writes unsupported"*) ;;
      *)
        CAS_ERROR=$(printf '%s\n' "$out" | grep -v '^{' | head -n 1)
        return 1 ;;
    esac
    PLAIN=1
    echo "$PROG: $STORE cannot compare-and-set metadata, so the claim on $BEAD is a read and a plain write, and a dispatch writing between the two can still take the work beside this one." >&2
  fi
  CAS_CMD="gc bd show $BEAD"
  if ! current=$(read_holder); then
    CAS_ERROR="$BEAD could not be read"
    return 1
  fi
  if [ "$current" = "$GC_SESSION_ID" ]; then
    OUTCOME=already_next
  elif [ "$current" != "$expected" ]; then
    OUTCOME=conflict
  else
    CAS_CMD="gc bd update $BEAD"
    if ! out=$(gc bd update "$BEAD" --set-metadata work_holder="$GC_SESSION_ID" 2>&1 </dev/null); then
      CAS_ERROR=$(printf '%s\n' "$out" | head -n 1)
      return 1
    fi
    OUTCOME=swapped
  fi
}

# claim — take the work for this session: compare-and-set its holder from the
# record the caller judged. The holder a conflict finds is judged the same way,
# so a live one holds the work, and a gone one, or this session, is claimed over
# once more.
claim() {
  local expected="$HOLDER" current
  if [ -z "${GC_SESSION_ID:-}" ]; then
    UNDECIDED="$BEAD cannot be claimed, because GC_SESSION_ID is unset, so this run has no session to record as the holder of $(describe "$WORK_DIR"). Retry from a session gc started."
    return 0
  fi
  if ! STORE=$(store_ref); then
    UNDECIDED="$BEAD cannot be claimed, because GC_RIG is unset and gc rig list --json named no city, so there is no store to record this session's claim in. Retry once gc rig list --json answers."
    return 0
  fi
  for _ in 1 2; do
    if ! cas "$expected"; then
      UNDECIDED="The claim on $BEAD could not be written to $STORE (${CAS_ERROR:-no reason given}), so whether another dispatch holds the work cannot be ruled out. Retry once $CAS_CMD answers."
      return 0
    fi
    case "$OUTCOME" in
      swapped|already_next) return 0 ;;
      conflict) ;;
      *)
        UNDECIDED="gc beads metadata-cas answered no outcome for the claim on $BEAD in $STORE, so whether another dispatch holds the work cannot be ruled out. Retry once gc beads metadata-cas answers."
        return 0 ;;
    esac
    if ! current=$(read_holder); then
      UNDECIDED="The holder recorded on $BEAD changed while this dispatch claimed it, and $BEAD could not be read back, so whether another dispatch holds the work cannot be ruled out. Retry once gc bd show $BEAD answers."
      return 0
    fi
    judge "$BEAD" "$WORK_DIR" "$current"
    [ -z "$HELD$UNDECIDED" ] || return 0
    expected="$current"
  done
  UNDECIDED="The holder recorded on $BEAD changed again while this dispatch claimed it, so another dispatch is claiming the work at the same moment. Retry once one claim on $BEAD stands."
}

judge "$BEAD" "$WORK_DIR" "$HOLDER"

if [ -n "$BRANCH" ]; then
  # A tree with no directory behind it holds nothing to adopt.
  OWN_DIR=""
  if [ -n "$WORK_DIR" ]; then
    OWN_DIR=$(cd "$WORK_DIR" 2>/dev/null && pwd -P) || OWN_DIR=""
  fi
  BRANCH_DIR=""
  REPO="${GC_RIG_ROOT:-${GC_DIR:-}}"
  if [ -n "$REPO" ] && git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1; then
    if WORKTREES=$(git -C "$REPO" worktree list --porcelain 2>/dev/null); then
      BRANCH_TREE=$(printf '%s\n' "$WORKTREES" | awk -v ref="branch refs/heads/$BRANCH" 'index($0, "worktree ") == 1 { tree = substr($0, 10) } $0 == ref { print tree; exit }')
      if [ -n "$BRANCH_TREE" ]; then
        BRANCH_DIR=$(cd "$BRANCH_TREE" 2>/dev/null && pwd -P) || BRANCH_DIR=""
      fi
    else
      UNDECIDED="$UNDECIDED${UNDECIDED:+$NL}git could not list the worktrees of $REPO, so a tree with $BRANCH checked out cannot be ruled out. Retry once git -C $REPO worktree list --porcelain succeeds."
    fi
  fi
  if [ -n "$OWN_DIR" ]; then
    judge_tree "$OWN_DIR" own
  fi
  if [ -n "$BRANCH_DIR" ] && ! { [ -n "$OWN_DIR" ] && [ "$OWN_DIR" -ef "$BRANCH_DIR" ]; }; then
    judge_tree "$BRANCH_DIR" ""
  fi
fi

if [ -n "$CLAIM" ] && [ -z "$HELD$UNDECIDED" ]; then
  claim
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
