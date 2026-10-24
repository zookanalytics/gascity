package core

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// workHolderScriptPath is the on-disk copy of the holder check that both
// worktree formulas' load-context gates call through `gc core scripts-dir`.
const workHolderScriptPath = "assets/scripts/work-holder.sh"

// workHolderEnvKeys are the settings each work-holder test supplies itself. A
// live Gas City session exports its own identity, rig and directories, and an
// inherited git setting would point the fixture's git at the host's
// repositories or config.
var workHolderEnvKeys = map[string]struct{}{
	"GC_AGENT": {}, "GC_ALIAS": {}, "GC_DIR": {}, "GC_RIG": {}, "GC_RIG_ROOT": {}, "GC_SESSION_ID": {}, "GC_SESSION_NAME": {},
	"GIT_ALTERNATE_OBJECT_DIRECTORIES": {}, "GIT_COMMON_DIR": {}, "GIT_CONFIG_GLOBAL": {},
	"GIT_CONFIG_NOSYSTEM": {}, "GIT_DIR": {}, "GIT_INDEX_FILE": {}, "GIT_OBJECT_DIRECTORY": {},
	// An inherited GIT_OPTIONAL_LOCKS=0 keeps every status from writing the
	// index, and the held-index check would then pass a count that locks.
	"GIT_OPTIONAL_LOCKS": {}, "GIT_WORK_TREE": {},
}

// workHolderRepoSetup lays out $ROOT/repo, the repository whose worktrees the
// script lists, and $ROOT/elsewhere, a directory outside it that the script
// runs from.
const workHolderRepoSetup = `set -e
git init -q -b main "$ROOT/repo"
echo tracked > "$ROOT/repo/tracked.txt"
git -C "$ROOT/repo" add tracked.txt
git -C "$ROOT/repo" -c user.name=gate -c user.email=gate@example.invalid -c commit.gpgsign=false commit -q -m init
mkdir -p "$ROOT/elsewhere"
`

// Two sessions of one rig-scoped agent with no work_dir of its own both run in
// the rig root, so they share $ROOT/repo as their directory.
const (
	holderSessionMe   = `{"id":"s-me","session_name":"pool__worker-s-me","state":"active","work_dir":"$ROOT/repo"}`
	holderSessionPeer = `{"id":"s-peer","session_name":"pool__worker-s-peer","state":"asleep","work_dir":"$ROOT/repo"}`
)

// TestWorkHolderScriptJudgesTheRecordedHolder pins the holder check behind the
// duplicate-dispatch gate, run against a real repository.
//
// A session running a worktree workflow never claims its work bead, so the
// gate cannot read the run from the assignee. workspace-setup records it
// instead: the tree as metadata.work_dir and the session that entered it as
// metadata.work_holder. The recorded holder decides who holds the work, never
// where the tree sits on disk. Sessions can share a directory (every
// rig-scoped agent with no work_dir runs in the rig root), and a pool slot
// directory outlives the session that made a tree in it, so a path rule lets
// a live peer through in the first case and parks legitimate work in the
// second.
//
// The trees a dispatch could adopt, the work bead's work_dir and the worktree
// with its branch checked out, are each judged by every holder recorded beside
// them: the work bead's own and that of each other unclosed bead on the branch
// whose work_dir it is. One live holder among those records holds the tree, so
// a gone holder, or this session, recorded on the work bead never frees a tree
// that a live sibling records. A branch tree no unclosed bead records names no
// holder, so it is undecided, and the finding says how to free it. Worktrees
// are listed in the rig repository, never in the current directory, which
// every case here sets outside the repository. A holder is judged against a
// session list read after the record that names it, so a sibling whose
// session started after the work bead's own holder was judged is still live.
//
// A finding may name a live session's tree, so counting its uncommitted paths
// must not write to it: a status that takes the optional index lock refreshes
// a stale index entry and writes the index back under index.lock, and a lock
// held at that moment fails the holder's own git add or commit. Each tree a
// finding names carries a tracked file whose stat data no longer matches its
// index entry, and its index must come out of the script byte-identical.
func TestWorkHolderScriptJudgesTheRecordedHolder(t *testing.T) {
	for _, tool := range []string{"git", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available; work-holder.sh requires it", tool)
		}
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("locating git: %v", err)
	}
	script, err := filepath.Abs(workHolderScriptPath)
	if err != nil {
		t.Fatalf("resolving %s: %v", workHolderScriptPath, err)
	}
	both := `{"sessions":[` + holderSessionMe + `,` + holderSessionPeer + `]}`
	cases := []struct {
		name string
		// setup lays out the work's trees after workHolderRepoSetup has run.
		setup string
		// sessions is the `gc session list --state all --json` answer and rows
		// the `gc bd list` answer; empty means the read fails. lateSessions,
		// when set, answers every session list read after the first.
		sessions, lateSessions, rows string
		// workDir, holder and branch are the work bead's metadata.
		workDir, holder, branch string
		// env overrides the defaults: this session is s-me, and both GC_DIR
		// and GC_RIG_ROOT are $ROOT/repo.
		env []string
		// failWorktreeList makes `git worktree list` fail.
		failWorktreeList bool
		wantRC           int
		// tree is the tree the findings name, relative to $ROOT; its index
		// must come out unchanged.
		tree      string
		wantInOut []string
	}{
		{
			name: "a live peer sharing this session's directory holds the tree it recorded",
			setup: `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-w"
echo wip > "$ROOT/repo/worktrees/gc-w/wip.txt"`,
			sessions: both,
			rows:     `[{"id":"gc-w","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-peer"}}]`,
			workDir:  "$ROOT/repo/worktrees/gc-w",
			holder:   "s-peer",
			branch:   "polecat/gc-w",
			wantRC:   1,
			tree:     "repo/worktrees/gc-w",
			wantInOut: []string{
				"gc-w records live pool__worker-s-peer as the holder of worktree",
				"(uncommitted paths: 1)",
				"gc bd update gc-w --unset-metadata work_holder",
			},
		},
		{
			name: "a reused slot's new session does not hold the tree its gone predecessor recorded",
			setup: `mkdir -p "$ROOT/slot"
git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/slot/worktrees/gc-w"`,
			sessions: `{"sessions":[` + holderSessionMe + `,{"id":"s-new","session_name":"pool__worker-s-new","state":"active","work_dir":"$ROOT/slot"}]}`,
			rows:     `[{"id":"gc-w","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/slot/worktrees/gc-w","work_holder":"s-old"}}]`,
			workDir:  "$ROOT/slot/worktrees/gc-w",
			holder:   "s-old",
			branch:   "polecat/gc-w",
		},
		{
			name:     "this session's own recorded tree is a resume, though a live peer shares its directory",
			setup:    `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-w"`,
			sessions: both,
			rows:     `[{"id":"gc-w","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-me"}}]`,
			workDir:  "$ROOT/repo/worktrees/gc-w",
			holder:   "s-me",
			branch:   "polecat/gc-w",
		},
		{
			name:     "a bead that records no holder is unheld",
			setup:    `git -C "$ROOT/repo" worktree add -q --detach "$ROOT/repo/worktrees/gc-w"`,
			sessions: both,
			workDir:  "$ROOT/repo/worktrees/gc-w",
		},
		{
			name:     "a live holder holds the work after its tree is gone from disk",
			sessions: both,
			workDir:  "$ROOT/repo/worktrees/gc-w",
			holder:   "s-peer",
			wantRC:   1,
			wantInOut: []string{
				"gc-w records live pool__worker-s-peer as the holder of tree",
				"(not on disk)",
			},
		},
		{
			name:    "an unreadable session list leaves a recorded holder undecided",
			setup:   `git -C "$ROOT/repo" worktree add -q --detach "$ROOT/repo/worktrees/gc-w"`,
			workDir: "$ROOT/repo/worktrees/gc-w",
			holder:  "s-peer",
			wantRC:  2,
			tree:    "repo/worktrees/gc-w",
			wantInOut: []string{
				"gc-w records s-peer as the holder of worktree",
				"the session list could not be read",
				"Retry once gc session list --state all answers.",
			},
		},
		{
			name: "from outside the repository, the branch tree a live sibling recorded holds the work",
			setup: `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-anchor"
echo wip > "$ROOT/repo/worktrees/gc-anchor/wip.txt"`,
			sessions: both,
			rows:     `[{"id":"gc-anchor","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-anchor","work_holder":"s-peer"}},{"id":"gc-w","status":"in_progress","metadata":{"branch":"polecat/gc-w"}}]`,
			branch:   "polecat/gc-w",
			wantRC:   1,
			tree:     "repo/worktrees/gc-anchor",
			wantInOut: []string{
				"gc-anchor records live pool__worker-s-peer as the holder of worktree",
				"gc bd update gc-anchor --unset-metadata work_holder",
			},
		},
		{
			name: "a live sibling that started after the session list was read holds the branch tree it recorded",
			setup: `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-anchor"
echo wip > "$ROOT/repo/worktrees/gc-anchor/wip.txt"`,
			sessions:     `{"sessions":[` + holderSessionMe + `]}`,
			lateSessions: both,
			rows:         `[{"id":"gc-anchor","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-anchor","work_holder":"s-peer"}}]`,
			holder:       "s-gone",
			branch:       "polecat/gc-w",
			wantRC:       1,
			tree:         "repo/worktrees/gc-anchor",
			wantInOut: []string{
				"gc-anchor records live pool__worker-s-peer as the holder of worktree",
			},
		},
		{
			name:   "from outside the repository, a branch no tree has checked out holds nothing",
			branch: "polecat/gc-w",
		},
		{
			name:   "a session directory that is not a repository has no worktrees to list",
			setup:  `mkdir -p "$ROOT/home"`,
			branch: "polecat/gc-w",
			env:    []string{"GC_RIG_ROOT=", "GC_DIR=$ROOT/home"},
		},
		{
			name:     "a sibling's gone holder leaves the branch tree to adopt",
			setup:    `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-anchor"`,
			sessions: both,
			rows:     `[{"id":"gc-anchor","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-anchor","work_holder":"s-gone"}}]`,
			branch:   "polecat/gc-w",
		},
		{
			name: "a live sibling holds the work's own branch tree, though the work bead's own holder is gone",
			setup: `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-w"
echo wip > "$ROOT/repo/worktrees/gc-w/wip.txt"`,
			sessions: both,
			rows:     `[{"id":"gc-w","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-gone"}},{"id":"gc-rework","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-peer"}}]`,
			workDir:  "$ROOT/repo/worktrees/gc-w",
			holder:   "s-gone",
			branch:   "polecat/gc-w",
			wantRC:   1,
			tree:     "repo/worktrees/gc-w",
			wantInOut: []string{
				"gc-rework records live pool__worker-s-peer as the holder of worktree",
				"(uncommitted paths: 1)",
				"gc bd update gc-rework --unset-metadata work_holder",
			},
		},
		{
			name: "a live sibling holds the work's own tree with the branch checked out nowhere",
			setup: `git -C "$ROOT/repo" worktree add -q --detach "$ROOT/repo/worktrees/gc-w"
echo wip > "$ROOT/repo/worktrees/gc-w/wip.txt"`,
			sessions: both,
			rows:     `[{"id":"gc-rework","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-peer"}}]`,
			workDir:  "$ROOT/repo/worktrees/gc-w",
			holder:   "s-gone",
			branch:   "polecat/gc-w",
			wantRC:   1,
			tree:     "repo/worktrees/gc-w",
			wantInOut: []string{
				"gc-rework records live pool__worker-s-peer as the holder of worktree",
				"(uncommitted paths: 1)",
			},
		},
		{
			name:     "this session's own record does not free a tree a live sibling records",
			setup:    `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-w"`,
			sessions: both,
			rows:     `[{"id":"gc-w","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-me"}},{"id":"gc-rework","status":"open","metadata":{"branch":"polecat/gc-w","work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-peer"}}]`,
			workDir:  "$ROOT/repo/worktrees/gc-w",
			holder:   "s-me",
			branch:   "polecat/gc-w",
			wantRC:   1,
			tree:     "repo/worktrees/gc-w",
			wantInOut: []string{
				"gc-rework records live pool__worker-s-peer as the holder of worktree",
			},
		},
		{
			name:     "an unreadable bead listing leaves the work's own tree undecided",
			setup:    `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-w"`,
			sessions: both,
			workDir:  "$ROOT/repo/worktrees/gc-w",
			holder:   "s-gone",
			branch:   "polecat/gc-w",
			wantRC:   2,
			tree:     "repo/worktrees/gc-w",
			wantInOut: []string{
				"the beads on polecat/gc-w could not be listed",
				"Retry once gc bd list answers.",
			},
		},
		{
			name:     "the branch checked out in the main worktree is undecided, and the finding says how to free it",
			setup:    `git -C "$ROOT/repo" checkout -q -b polecat/gc-w`,
			sessions: both,
			rows:     `[]`,
			branch:   "polecat/gc-w",
			wantRC:   2,
			tree:     "repo",
			wantInOut: []string{
				"polecat/gc-w is checked out in worktree",
				"which no unclosed bead on polecat/gc-w records as its work_dir",
				"switch --detach",
			},
		},
		{
			name:     "an unreadable bead listing leaves the branch tree undecided",
			setup:    `git -C "$ROOT/repo" worktree add -q -b polecat/gc-w "$ROOT/repo/worktrees/gc-anchor"`,
			sessions: both,
			branch:   "polecat/gc-w",
			wantRC:   2,
			tree:     "repo/worktrees/gc-anchor",
			wantInOut: []string{
				"the beads on polecat/gc-w could not be listed",
				"Retry once gc bd list answers.",
			},
		},
		{
			name:             "a worktree listing git cannot produce is undecided",
			sessions:         both,
			branch:           "polecat/gc-w",
			failWorktreeList: true,
			wantRC:           2,
			wantInOut: []string{
				"git could not list the worktrees of",
				"worktree list --porcelain succeeds",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			expand := strings.NewReplacer("$ROOT", root)
			gitEnv := []string{"ROOT=" + root, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull}

			setup := workHolderRepoSetup + tc.setup + "\n"
			if tc.tree != "" {
				// Backdating the tree's tracked file leaves its index entry's
				// stat data stale, so a status that refreshes and writes the
				// index changes the index file.
				setup += `touch -t 200001010000 "$ROOT/` + tc.tree + `/tracked.txt"
git -C "$ROOT/` + tc.tree + `" rev-parse --path-format=absolute --git-path index > "$ROOT/held-index"
`
			}
			setupPath := filepath.Join(root, "setup.sh")
			writeGateFixture(t, setupPath, setup)
			if out, err := runPackScript(t, setupPath, t.TempDir(), workHolderEnvKeys, gitEnv); err != nil {
				t.Fatalf("laying out the repository: %v\n%s", err, out)
			}
			var heldIndex string
			var heldIndexBefore []byte
			if tc.tree != "" {
				pathData, err := os.ReadFile(filepath.Join(root, "held-index"))
				if err != nil {
					t.Fatalf("reading the tree's index path: %v", err)
				}
				heldIndex = strings.TrimSpace(string(pathData))
				if heldIndexBefore, err = os.ReadFile(heldIndex); err != nil {
					t.Fatalf("reading the tree's index: %v", err)
				}
			}

			answer := func(name, content string) string {
				if content == "" {
					return "exit 1"
				}
				path := filepath.Join(root, name)
				writeGateFixture(t, path, expand.Replace(content))
				return "cat '" + path + "'"
			}
			sessionsAnswer := answer("sessions.json", tc.sessions)
			if tc.lateSessions != "" {
				answered := filepath.Join(root, "sessions-answered")
				sessionsAnswer = "if [ -f '" + answered + "' ]; then " + answer("sessions-late.json", tc.lateSessions) +
					"; else : > '" + answered + "'; " + sessionsAnswer + "; fi"
			}
			binDir, logPath := fakeGCBin(t, `case "$1 $2" in
"session list") `+sessionsAnswer+` ;;
"bd list") `+answer("rows.json", tc.rows)+` ;;
esac
exit 0
`)
			if tc.failWorktreeList {
				writeGateFixture(t, filepath.Join(binDir, "git"), `#!/bin/sh
case " $* " in *" worktree list "*) echo "fatal: worktree list refused" >&2; exit 128 ;; esac
exec '`+realGit+`' "$@"
`)
				if err := os.Chmod(filepath.Join(binDir, "git"), 0o755); err != nil {
					t.Fatalf("making the git shim executable: %v", err)
				}
			}
			driverPath := filepath.Join(root, "driver.sh")
			writeGateFixture(t, driverPath, `cd "$ROOT/elsewhere" || exit 97
exec bash '`+script+`' "$@"
`)
			env := slices.Concat(gitEnv, []string{
				"GC_SESSION_ID=s-me",
				"GC_SESSION_NAME=pool__worker-s-me",
				"GC_AGENT=s-me",
				"GC_ALIAS=",
				"GC_DIR=" + filepath.Join(root, "repo"),
				"GC_RIG_ROOT=" + filepath.Join(root, "repo"),
			})
			for _, entry := range tc.env {
				env = append(env, expand.Replace(entry))
			}
			out, runErr := runPackScript(t, driverPath, binDir, workHolderEnvKeys, env,
				"--bead", "gc-w",
				"--work-dir", expand.Replace(tc.workDir),
				"--holder", tc.holder,
				"--branch", tc.branch)
			rc := 0
			if runErr != nil {
				var exitErr *exec.ExitError
				if !errors.As(runErr, &exitErr) {
					t.Fatalf("running work-holder.sh: %v\n%s", runErr, out)
				}
				rc = exitErr.ExitCode()
			}
			gcLog, _ := os.ReadFile(logPath)
			if rc != tc.wantRC {
				t.Fatalf("work-holder.sh exited %d, want %d\noutput:\n%s\ngc log:\n%s", rc, tc.wantRC, out, gcLog)
			}
			// Judging alone takes nothing: only --claim writes to the work bead.
			if strings.Contains(string(gcLog), "gc beads metadata-cas") || strings.Contains(string(gcLog), " update gc-w") {
				t.Errorf("work-holder.sh wrote to the store without --claim\ngc log:\n%s", gcLog)
			}
			if tc.wantRC == 0 {
				if strings.TrimSpace(out) != "" {
					t.Errorf("work-holder.sh reported a finding on work nothing holds:\n%s", out)
				}
				return
			}
			if lines := strings.Count(strings.TrimRight(out, "\n"), "\n") + 1; lines != 1 {
				t.Errorf("work-holder.sh reported %d findings, want 1:\n%s", lines, out)
			}
			wants := tc.wantInOut
			if tc.tree != "" {
				// A finding names the tree by the resolved path git reports, so
				// its work can be salvaged.
				tree, err := filepath.EvalSymlinks(filepath.Join(root, tc.tree))
				if err != nil {
					t.Fatalf("resolving the tree: %v", err)
				}
				wants = append([]string{"worktree " + tree + " (uncommitted paths: "}, wants...)
			}
			for _, want := range wants {
				if !strings.Contains(out, want) {
					t.Errorf("the finding does not mention %q:\n%s", want, out)
				}
			}
			if tc.tree != "" {
				heldIndexAfter, err := os.ReadFile(heldIndex)
				if err != nil {
					t.Fatalf("reading the tree's index after the script: %v", err)
				}
				if !bytes.Equal(heldIndexAfter, heldIndexBefore) {
					t.Error("work-holder.sh rewrote the tree's index: its count took the optional index lock, which can fail the live holder's own git add or commit on index.lock")
				}
			}
		})
	}
}

// workHolderClaimFakeGC is the gc the claim tests run against. Its store is one
// file, $ROOT/holder, holding the work_holder of the work bead, $FAKE_BEAD or
// gc-w when that is unset. `gc beads metadata-cas` compares and sets it under a
// mkdir lock, so concurrent claims land one at a time the way the store lands
// them; `gc bd show` reads it back and `gc bd update` writes it plainly,
// whichever store gc bd's --city and --rig name. FAKE_CAS makes the
// compare-and-set answer as a store that cannot compare-and-set (unsupported),
// as one that fails (fail), or as one whose record changes before every write
// (conflict). `gc session list` answers $ROOT/sessions.json, and every later
// call answers $ROOT/sessions-late.json instead when that file exists, failing
// when it is empty. `gc rig list` answers $ROOT/rigs.json, and fails without
// it. It answers the rest of what the load-context gate asks of gc too:
// FAKE_SCRIPTS_DIR is `gc core scripts-dir`, and each session's current step
// is gc-step-<session id>.
const workHolderClaimFakeGC = `holder="$ROOT/holder"
bead="${FAKE_BEAD:-gc-w}"
if [ "$1" = bd ]; then
  shift
  while [ "$1" = --city ] || [ "$1" = --rig ]; do shift 2; done
  set -- bd "$@"
fi
case "$1 $2" in
"core scripts-dir") [ -n "${FAKE_SCRIPTS_DIR:-}" ] || exit 1; echo "$FAKE_SCRIPTS_DIR" ;;
"hook current") echo "gc-step-$GC_SESSION_ID" ;;
"session list")
  if [ -f "$ROOT/sessions-late.json" ] && [ -f "$ROOT/sessions-answered" ]; then
    [ -s "$ROOT/sessions-late.json" ] || exit 1
    cat "$ROOT/sessions-late.json"
  else
    : > "$ROOT/sessions-answered"
    cat "$ROOT/sessions.json"
  fi ;;
"bd list") echo '[]' ;;
"rig list") [ -f "$ROOT/rigs.json" ] || exit 1; cat "$ROOT/rigs.json" ;;
"bd show")
  if [ "$3" = "$bead" ]; then
    printf '[{"id":"%s","status":"open","metadata":{"work_holder":"%s"}}]\n' "$bead" "$(cat "$holder" 2>/dev/null)"
  else
    printf '[{"id":"%s","metadata":{"gc.root_bead_id":"gc-root"}}]\n' "$3"
  fi ;;
"bd update")
  for arg in "$@"; do
    case "$arg" in work_holder=*) printf '%s' "${arg#work_holder=}" > "$holder" ;; esac
  done ;;
"beads metadata-cas")
  case "${FAKE_CAS:-}" in
  unsupported)
    echo 'gc beads metadata-cas: metadata CAS for "gc-w" key "work_holder": conditional writes unsupported' >&2
    echo '{"schema_version":"1","ok":false,"error":{"code":"command_failed","message":"command failed; see stderr for diagnostics","exit_code":1}}'
    exit 1 ;;
  fail)
    echo 'gc beads metadata-cas: opening rig:demo: dial tcp 127.0.0.1:3307: connection refused' >&2
    exit 1 ;;
  conflict)
    printf '{"schema_version":"1","ok":true,"bead_id":"%s","store_ref":"rig:demo","key":"work_holder","outcome":"conflict"}\n' "$3"
    exit 0 ;;
  esac
  expected=""; next=""
  for arg in "$@"; do
    case "$arg" in
    --expected=*) expected="${arg#--expected=}" ;;
    --next=*) next="${arg#--next=}" ;;
    esac
  done
  until mkdir "$holder.lock" 2>/dev/null; do :; done
  current=$(cat "$holder" 2>/dev/null)
  if [ "$current" = "$expected" ]; then
    printf '%s' "$next" > "$holder"
    outcome=swapped
  elif [ "$current" = "$next" ]; then
    outcome=already_next
  else
    outcome=conflict
  fi
  rmdir "$holder.lock"
  printf '{"schema_version":"1","ok":true,"bead_id":"%s","store_ref":"rig:demo","key":"work_holder","outcome":"%s"}\n' "$3" "$outcome" ;;
esac
exit 0
`

// workHolderRigs is the `gc rig list --json` answer the claim tests read
// unless a case gives its own, with $ROOT standing for the test's root: city
// metro, whose HQ prefix is mt, rig demo, whose prefix gc makes it the store
// that owns work bead gc-w, and rig tributary.
const workHolderRigs = `{"schema_version":"1","city_name":"metro","city_path":"$ROOT","rigs":[` +
	`{"name":"metro","path":"$ROOT","prefix":"mt","hq":true},` +
	`{"name":"demo","path":"$ROOT/demo","prefix":"gc"},` +
	`{"name":"tributary","path":"$ROOT/tributary","prefix":"tr"}]}`

// writeWorkHolderClaimAnswers writes what the claim fake answers from under
// root: this session and live s-peer as the session list, and workHolderRigs
// as the rig list.
func writeWorkHolderClaimAnswers(t *testing.T, root string) {
	t.Helper()
	expand := strings.NewReplacer("$ROOT", root)
	writeGateFixture(t, filepath.Join(root, "sessions.json"), expand.Replace(`{"sessions":[`+holderSessionMe+`,`+holderSessionPeer+`]}`))
	writeGateFixture(t, filepath.Join(root, "rigs.json"), expand.Replace(workHolderRigs))
}

// workHolderClaimEnv is the environment a claim test's session runs in: the
// fake's store under root, the session id, its session name, and rig demo.
func workHolderClaimEnv(root, id string) []string {
	return []string{
		"ROOT=" + root,
		"GC_SESSION_ID=" + id,
		"GC_SESSION_NAME=pool__worker-" + id,
		"GC_AGENT=" + id,
		"GC_ALIAS=",
		"GC_RIG=demo",
		"GC_DIR=" + root,
		"GC_RIG_ROOT=" + root,
	}
}

// runWorkHolderClaim runs work-holder.sh --claim for work bead bead, whose
// work_holder the caller read as holder. It returns the combined output and
// the exit code, and an error only when the script could not be run at all.
func runWorkHolderClaim(t *testing.T, script, binDir string, env []string, bead, holder string) (string, int, error) {
	t.Helper()
	out, err := runPackScript(t, script, binDir, workHolderEnvKeys, append(env, "FAKE_BEAD="+bead),
		"--claim", "--bead", bead, "--work-dir", "", "--holder", holder)
	if err == nil {
		return out, 0, nil
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return out, 0, err
	}
	return out, exitErr.ExitCode(), nil
}

// TestWorkHolderScriptClaimsTheWork pins --claim, which takes the work in the
// step that judges it free.
//
// The claim records this session as the work bead's work_holder with one
// compare-and-set whose expected value is the record the caller judged, so a
// dispatch that judged the same record and wrote first leaves this one's
// write to conflict. The holder a conflict finds is judged the way the
// caller's record was, against a session list read after the conflict: a live
// one holds the work, though it started after this run first read the list,
// and a gone one is claimed over once more. A store that cannot compare-and-set
// gets a read and a plain write, and the script says so. A claim the store
// refuses for any other reason is undecided, and so is a session with no
// identity to record or a list that cannot be read again.
//
// The claim, the read-back and the plain write all go to the store that owns
// the work bead, which gc rig list --json names by the longest prefix the
// bead's id begins with. The session's own rig plays no part: a session no rig
// scopes, and a session of another rig, claim the work in its owner's store. A
// rig list that cannot be read, or that names no single bound owner, leaves
// the claim undecided.
func TestWorkHolderScriptClaimsTheWork(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; work-holder.sh requires it")
	}
	script, err := filepath.Abs(workHolderScriptPath)
	if err != nil {
		t.Fatalf("resolving %s: %v", workHolderScriptPath, err)
	}
	onlyMe := `{"sessions":[` + holderSessionMe + `]}`
	both := `{"sessions":[` + holderSessionMe + `,` + holderSessionPeer + `]}`
	cases := []struct {
		name string
		// bead is the work bead, gc-w when empty.
		bead string
		// stored is the work_holder the store holds when the script starts,
		// and holder the record the caller read and judged.
		stored, holder string
		// sessions is the first `gc session list --state all --json` answer,
		// this session and live s-peer when empty. lateSessions is every later
		// answer, the first one again when empty, and lateSessionsFail makes
		// every later read fail.
		sessions, lateSessions string
		lateSessionsFail       bool
		// fakeCAS is the fake's FAKE_CAS mode.
		fakeCAS string
		// rigs is the `gc rig list --json` answer, workHolderRigs when empty,
		// and noRigList makes that read fail.
		rigs      string
		noRigList bool
		// env overrides the defaults: this session is s-me, in rig demo.
		env        []string
		wantRC     int
		wantStored string
		// wantInOut must appear in the output; an exit 0 with none expected
		// must print nothing.
		wantInOut []string
		// wantInLog must appear in the gc log, and wantNotInLog nowhere in it.
		// $ROOT stands for the test's root in both.
		wantInLog    []string
		wantNotInLog string
	}{
		{
			name:       "an unheld bead is claimed for this session in the store that owns it",
			wantStored: "s-me",
			wantInLog:  []string{"gc beads metadata-cas gc-w --store-ref=rig:demo --key=work_holder --expected= --next=s-me --json"},
		},
		{
			name:       "a gone holder's record is claimed over",
			stored:     "s-gone",
			holder:     "s-gone",
			wantStored: "s-me",
			wantInLog:  []string{"--expected=s-gone --next=s-me"},
		},
		{
			name:       "this session's own record stays this session's",
			stored:     "s-me",
			holder:     "s-me",
			wantStored: "s-me",
		},
		{
			name:         "a live holder is not claimed over",
			stored:       "s-peer",
			holder:       "s-peer",
			wantRC:       1,
			wantStored:   "s-peer",
			wantInOut:    []string{"gc-w records live pool__worker-s-peer as the holder of its tree (none recorded)"},
			wantNotInLog: "gc beads metadata-cas",
		},
		{
			name:       "a live dispatch that claimed the work since the read holds it",
			stored:     "s-peer",
			wantRC:     1,
			wantStored: "s-peer",
			wantInOut: []string{
				"gc-w records live pool__worker-s-peer as the holder of its tree (none recorded)",
				"gc bd update gc-w --unset-metadata work_holder",
			},
			wantInLog: []string{"gc bd --city $ROOT --rig demo show gc-w --json"},
		},
		{
			name:         "a dispatch that started after this one read the session list holds the work it claimed",
			stored:       "s-peer",
			holder:       "s-gone",
			sessions:     onlyMe,
			lateSessions: both,
			wantRC:       1,
			wantStored:   "s-peer",
			wantInOut:    []string{"gc-w records live pool__worker-s-peer as the holder of its tree (none recorded)"},
			wantInLog:    []string{"--expected=s-gone --next=s-me"},
			wantNotInLog: "--expected=s-peer",
		},
		{
			name:             "a session list that cannot be read again after a conflict leaves the claim undecided",
			stored:           "s-peer",
			holder:           "s-gone",
			sessions:         onlyMe,
			lateSessionsFail: true,
			wantRC:           2,
			wantStored:       "s-peer",
			wantInOut:        []string{"gc-w records s-peer as the holder of its tree (none recorded), and the session list could not be read"},
			wantNotInLog:     "--expected=s-peer",
		},
		{
			name:       "a gone dispatch's claim made since the read is claimed over",
			stored:     "s-gone",
			wantStored: "s-me",
			wantInLog:  []string{"--expected=s-gone --next=s-me"},
		},
		{
			name:       "a record that changes again under the claim is undecided",
			stored:     "s-gone",
			fakeCAS:    "conflict",
			wantRC:     2,
			wantStored: "s-gone",
			wantInOut:  []string{"changed again while this dispatch claimed it"},
		},
		{
			name:       "a store that cannot compare-and-set gets a read and a plain write in the store that owns the bead, and says so",
			fakeCAS:    "unsupported",
			wantStored: "s-me",
			wantInOut:  []string{"rig:demo cannot compare-and-set"},
			wantInLog: []string{
				"gc bd --city $ROOT --rig demo show gc-w --json",
				"gc bd --city $ROOT --rig demo update gc-w --set-metadata work_holder=s-me",
			},
		},
		{
			name:         "on a store that cannot compare-and-set, a live dispatch's claim since the read still holds the work",
			stored:       "s-peer",
			fakeCAS:      "unsupported",
			wantRC:       1,
			wantStored:   "s-peer",
			wantInOut:    []string{"gc-w records live pool__worker-s-peer as the holder of its tree (none recorded)"},
			wantNotInLog: "update gc-w",
		},
		{
			name:         "a claim the store refuses for another reason is undecided",
			fakeCAS:      "fail",
			wantRC:       2,
			wantInOut:    []string{"connection refused", "Retry once gc beads metadata-cas answers."},
			wantNotInLog: "update gc-w",
		},
		{
			name:         "a session with no GC_SESSION_ID has nothing to record and is undecided",
			env:          []string{"GC_SESSION_ID="},
			wantRC:       2,
			wantInOut:    []string{"GC_SESSION_ID is unset"},
			wantNotInLog: "gc beads metadata-cas",
		},
		{
			name:       "a session no rig scopes claims rig work in the store that owns it",
			env:        []string{"GC_RIG="},
			wantStored: "s-me",
			wantInLog:  []string{"gc beads metadata-cas gc-w --store-ref=rig:demo --key=work_holder --expected= --next=s-me --json"},
		},
		{
			name:       "a session of one rig claims another rig's work in the store that owns it",
			env:        []string{"GC_RIG=tributary"},
			wantStored: "s-me",
			wantInLog:  []string{"gc beads metadata-cas gc-w --store-ref=rig:demo --key=work_holder --expected= --next=s-me --json"},
		},
		{
			name: "work under the HQ's prefix is claimed and read back in the city store",
			rigs: `{"schema_version":"1","city_name":"metro","city_path":"$ROOT","rigs":[` +
				`{"name":"metro","path":"$ROOT","prefix":"gc","hq":true},` +
				`{"name":"demo","path":"$ROOT/demo","prefix":"dm"}]}`,
			stored:     "s-gone",
			wantStored: "s-me",
			wantInLog: []string{
				"gc beads metadata-cas gc-w --store-ref=city:metro --key=work_holder --expected= --next=s-me --json",
				"gc bd --city $ROOT show gc-w --json",
				"gc beads metadata-cas gc-w --store-ref=city:metro --key=work_holder --expected=s-gone --next=s-me --json",
			},
		},
		{
			name: "the longest listed prefix the bead's id begins with names the store that owns it",
			bead: "gc-w-1",
			rigs: `{"schema_version":"1","city_name":"metro","city_path":"$ROOT","rigs":[` +
				`{"name":"metro","path":"$ROOT","prefix":"mt","hq":true},` +
				`{"name":"demo","path":"$ROOT/demo","prefix":"gc"},` +
				`{"name":"tributary","path":"$ROOT/tributary","prefix":"gc-w"}]}`,
			wantStored: "s-me",
			wantInLog:  []string{"gc beads metadata-cas gc-w-1 --store-ref=rig:tributary --key=work_holder --expected= --next=s-me --json"},
		},
		{
			name:         "a rig list that cannot be read leaves the claim undecided",
			noRigList:    true,
			wantRC:       2,
			wantInOut:    []string{"gc rig list --json could not be read", "Retry once gc rig list --json answers."},
			wantNotInLog: "gc beads metadata-cas",
		},
		{
			name: "work whose prefix no listed store holds is undecided",
			rigs: `{"schema_version":"1","city_name":"metro","city_path":"$ROOT","rigs":[` +
				`{"name":"metro","path":"$ROOT","prefix":"mt","hq":true},` +
				`{"name":"tributary","path":"$ROOT/tributary","prefix":"tr"}]}`,
			wantRC:       2,
			wantInOut:    []string{"no one bound store in gc rig list --json holds the prefix gc-w carries"},
			wantNotInLog: "gc beads metadata-cas",
		},
		{
			name: "work whose prefix only an unbound rig holds is undecided",
			rigs: `{"schema_version":"1","city_name":"metro","city_path":"$ROOT","rigs":[` +
				`{"name":"metro","path":"$ROOT","prefix":"mt","hq":true},` +
				`{"name":"demo","path":"","prefix":"gc"}]}`,
			wantRC:       2,
			wantInOut:    []string{"no one bound store in gc rig list --json holds the prefix gc-w carries"},
			wantNotInLog: "gc beads metadata-cas",
		},
		{
			name: "work whose prefix the HQ and a rig both hold is undecided",
			rigs: `{"schema_version":"1","city_name":"metro","city_path":"$ROOT","rigs":[` +
				`{"name":"metro","path":"$ROOT","prefix":"gc","hq":true},` +
				`{"name":"demo","path":"$ROOT/demo","prefix":"gc"}]}`,
			wantRC:       2,
			wantInOut:    []string{"no one bound store in gc rig list --json holds the prefix gc-w carries"},
			wantNotInLog: "gc beads metadata-cas",
		},
		{
			name: "work whose prefix two rigs hold, whatever its case, is undecided",
			rigs: `{"schema_version":"1","city_name":"metro","city_path":"$ROOT","rigs":[` +
				`{"name":"metro","path":"$ROOT","prefix":"mt","hq":true},` +
				`{"name":"demo","path":"$ROOT/demo","prefix":"gc"},` +
				`{"name":"tributary","path":"$ROOT/tributary","prefix":"GC"}]}`,
			wantRC:       2,
			wantInOut:    []string{"no one bound store in gc rig list --json holds the prefix gc-w carries"},
			wantNotInLog: "gc beads metadata-cas",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			expand := strings.NewReplacer("$ROOT", root)
			writeWorkHolderClaimAnswers(t, root)
			if tc.sessions != "" {
				writeGateFixture(t, filepath.Join(root, "sessions.json"), expand.Replace(tc.sessions))
			}
			switch {
			case tc.lateSessionsFail:
				writeGateFixture(t, filepath.Join(root, "sessions-late.json"), "")
			case tc.lateSessions != "":
				writeGateFixture(t, filepath.Join(root, "sessions-late.json"), expand.Replace(tc.lateSessions))
			}
			switch {
			case tc.noRigList:
				if err := os.Remove(filepath.Join(root, "rigs.json")); err != nil {
					t.Fatalf("removing the rig list answer: %v", err)
				}
			case tc.rigs != "":
				writeGateFixture(t, filepath.Join(root, "rigs.json"), expand.Replace(tc.rigs))
			}
			if tc.stored != "" {
				writeGateFixture(t, filepath.Join(root, "holder"), tc.stored)
			}
			bead := tc.bead
			if bead == "" {
				bead = "gc-w"
			}
			binDir, logPath := fakeGCBin(t, workHolderClaimFakeGC)
			env := append(workHolderClaimEnv(root, "s-me"), "FAKE_CAS="+tc.fakeCAS)
			out, rc, err := runWorkHolderClaim(t, script, binDir, append(env, tc.env...), bead, tc.holder)
			if err != nil {
				t.Fatalf("running work-holder.sh --claim: %v\n%s", err, out)
			}
			gcLog, _ := os.ReadFile(logPath)
			if rc != tc.wantRC {
				t.Fatalf("work-holder.sh --claim exited %d, want %d\noutput:\n%s\ngc log:\n%s", rc, tc.wantRC, out, gcLog)
			}
			stored, _ := os.ReadFile(filepath.Join(root, "holder"))
			if string(stored) != tc.wantStored {
				t.Errorf("the store holds work_holder=%q, want %q\ngc log:\n%s", stored, tc.wantStored, gcLog)
			}
			if tc.wantRC == 0 && len(tc.wantInOut) == 0 && strings.TrimSpace(out) != "" {
				t.Errorf("work-holder.sh --claim reported a finding on work it claimed:\n%s", out)
			}
			for _, want := range tc.wantInOut {
				if !strings.Contains(out, want) {
					t.Errorf("the output does not mention %q:\n%s", want, out)
				}
			}
			for _, want := range tc.wantInLog {
				if want = expand.Replace(want); !strings.Contains(string(gcLog), want) {
					t.Errorf("the gc log does not show %q:\n%s", want, gcLog)
				}
			}
			if notWant := expand.Replace(tc.wantNotInLog); notWant != "" && strings.Contains(string(gcLog), notWant) {
				t.Errorf("the script ran %q:\n%s", notWant, gcLog)
			}
		})
	}
}

// TestWorkHolderScriptClaimLetsOneOfTwoRacingDispatchesHoldTheWork pins the
// race --claim closes. Two dispatches read the same unheld record, so both
// judge the work free, and each claims it at the same moment. The store lands
// one compare-and-set at a time, so exactly one claim lands: that dispatch
// holds the work, and the other finds it as the live holder and stays off.
func TestWorkHolderScriptClaimLetsOneOfTwoRacingDispatchesHoldTheWork(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; work-holder.sh requires it")
	}
	script, err := filepath.Abs(workHolderScriptPath)
	if err != nil {
		t.Fatalf("resolving %s: %v", workHolderScriptPath, err)
	}
	ids := []string{"s-me", "s-peer"}
	for round := range 8 {
		root := t.TempDir()
		writeWorkHolderClaimAnswers(t, root)
		binDir, _ := fakeGCBin(t, workHolderClaimFakeGC)
		outs := make([]string, len(ids))
		rcs := make([]int, len(ids))
		errs := make([]error, len(ids))
		var wg sync.WaitGroup
		for i, id := range ids {
			wg.Go(func() {
				outs[i], rcs[i], errs[i] = runWorkHolderClaim(t, script, binDir, workHolderClaimEnv(root, id), "gc-w", "")
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d: running %s's claim: %v\n%s", round, ids[i], err, outs[i])
			}
		}
		stored, _ := os.ReadFile(filepath.Join(root, "holder"))
		var winners []string
		for i, id := range ids {
			switch rcs[i] {
			case 0:
				winners = append(winners, id)
			case 1:
			default:
				t.Errorf("round %d: %s's claim exited %d, want 0 or 1:\n%s", round, id, rcs[i], outs[i])
			}
		}
		if len(winners) != 1 {
			t.Fatalf("round %d: %d dispatches hold the work (%v), want exactly one\n%s: %s\n%s: %s", round, len(winners), winners, ids[0], outs[0], ids[1], outs[1])
		}
		if string(stored) != winners[0] {
			t.Errorf("round %d: the store holds work_holder=%q, want the winner %q", round, stored, winners[0])
		}
		for i, id := range ids {
			if id != winners[0] && !strings.Contains(outs[i], "gc-w records live pool__worker-"+winners[0]+" as the holder") {
				t.Errorf("round %d: %s lost the claim without naming %s as the live holder:\n%s", round, id, winners[0], outs[i])
			}
		}
	}
}

// TestWorkHolderScriptRejectsAMissingBead pins the usage contract: the bead is
// required, and an option given no value is a usage error rather than a hang.
func TestWorkHolderScriptRejectsAMissingBead(t *testing.T) {
	for _, args := range [][]string{
		{"--work-dir", "/tmp/x"},
		{"--bead"},
		{"--bead", "--holder", "s-peer"},
		{"--bead", "gc-w", "--holder", "--claim"},
	} {
		out, err := runPackScript(t, workHolderScriptPath, t.TempDir(), workHolderEnvKeys, nil, args...)
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 64 {
			t.Errorf("work-holder.sh %v: got %v, want exit 64\n%s", args, err, out)
		}
	}
}

// TestCoreScriptsDirCommandNamesThePackScripts pins the command a formula step
// reaches core's scripts through. gc runs a pack command with GC_PACK_DIR set
// to the pack it loaded, so the directory printed is the copy of core the city
// composes, wherever it is served from. The embedded pack has to carry both
// the command and the scripts it names, because that is what the cache
// serves.
func TestCoreScriptsDirCommandNamesThePackScripts(t *testing.T) {
	for _, path := range []string{"commands/scripts-dir/run.sh", "commands/scripts-dir/command.toml", workHolderScriptPath} {
		if _, err := PackFS.Open(path); err != nil {
			t.Errorf("the embedded core pack does not carry %s: %v", path, err)
		}
	}
	packDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolving the pack directory: %v", err)
	}
	skip := map[string]struct{}{"GC_PACK_DIR": {}}
	out, err := runPackScript(t, "commands/scripts-dir/run.sh", t.TempDir(), skip, []string{"GC_PACK_DIR=" + packDir})
	if err != nil {
		t.Fatalf("gc core scripts-dir failed: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(out), filepath.Join(packDir, "assets", "scripts"); got != want {
		t.Errorf("gc core scripts-dir printed %q, want %q", got, want)
	}
	out, err = runPackScript(t, "commands/scripts-dir/run.sh", t.TempDir(), skip, nil)
	if err == nil || !strings.Contains(out, "GC_PACK_DIR is unset") {
		t.Errorf("gc core scripts-dir without GC_PACK_DIR: got %v, want a failure naming GC_PACK_DIR\n%s", err, out)
	}
}
