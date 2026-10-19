package core

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/BurntSushi/toml"
)

// formulaFile is the subset of a formula TOML these tests inspect. Steps carry
// the agent-facing instructions, so asserting on a step description is how the
// pack pins behavior that lives in prompt text rather than in Go.
type formulaFile struct {
	Formula string `toml:"formula"`
	Steps   []struct {
		ID          string `toml:"id"`
		Title       string `toml:"title"`
		Description string `toml:"description"`
	} `toml:"steps"`
}

// readFormula decodes a formula TOML from the embedded core pack.
//
// file stays parameterized even though every current caller passes
// mol-polecat-base.toml: the pack ships sibling formulas (mol-polecat-commit,
// mol-polecat-report) that inherit these steps, and the next test to pin one of
// them reads it through this same helper.
//
//nolint:unparam // see above
func readFormula(t *testing.T, file string) formulaFile {
	t.Helper()
	data, err := fs.ReadFile(PackFS, "formulas/"+file)
	if err != nil {
		t.Fatalf("reading formulas/%s: %v", file, err)
	}
	var parsed formulaFile
	if _, err := toml.Decode(string(data), &parsed); err != nil {
		t.Fatalf("decoding formulas/%s: %v", file, err)
	}
	return parsed
}

// formulaStep returns the description of the named step, failing the test when
// the step is absent.
func formulaStep(t *testing.T, f formulaFile, id string) string {
	t.Helper()
	for _, step := range f.Steps {
		if step.ID == id {
			return step.Description
		}
	}
	t.Fatalf("formula %s has no step %q", f.Formula, id)
	return ""
}

// TestMolDoWorkDrainClaimsCurrentContinuation pins the distinction between a
// process's immutable startup bead and the continuation that became ready
// while that process was running.
func TestMolDoWorkDrainClaimsCurrentContinuation(t *testing.T) {
	step := formulaStep(t, readFormula(t, "mol-do-work.toml"), "drain")

	if !strings.Contains(step, "gc ready --json --limit=2") {
		t.Fatal("drain must resolve the current continuation from ready work")
	}
	if !strings.Contains(step, "--include-ephemeral") {
		t.Fatal("drain must include ephemeral rows; a wisp-tier continuation is invisible to bd ready without it")
	}
	if !strings.Contains(step, `--metadata-field "gc.root_bead_id=$ROOT_BEAD_ID"`) || !strings.Contains(step, `--metadata-field "gc.step_ref=mol-do-work.drain"`) {
		t.Fatal("drain must select the ready drain step from the current workflow root")
	}
	if strings.Contains(step, `DRAIN_BEAD_ID="${GC_BEAD_ID`) {
		t.Fatal("drain must not treat the immutable startup bead as the continuation bead")
	}
	if !strings.Contains(step, "if [ -z \"$ROOT_BEAD_ID\" ]") {
		t.Fatal("drain must fail closed when the startup bead has no workflow root")
	}
	if !strings.Contains(step, "if length == 1") || !strings.Contains(step, "could not resolve one ready continuation bead") {
		t.Fatal("drain must fail closed unless exactly one matching continuation is ready")
	}
	// The current-claim branch must stay gated on gc.step_ref. An unguarded
	// `gc hook current` re-introduces gh-5141 on the deferred path, where the
	// claim stamp still names the do-work step rather than the drain step.
	currentAt := strings.Index(step, "gc hook current --id-only")
	if currentAt < 0 {
		t.Fatal("drain must consult the current claim before falling back to a ready query")
	}
	readyAt := strings.Index(step, "gc ready --json --limit=2")
	if readyAt < currentAt {
		t.Fatal("drain must consult the current claim before the ready query, not after it")
	}
	if !strings.Contains(step[currentAt:readyAt], `.metadata["gc.step_ref"] == "mol-do-work.drain"`) {
		t.Fatal("drain must gate the current-claim branch on gc.step_ref; an unguarded claim restores the stale-identity bug")
	}
	// The startup-bead id is only needed by the ready-query fallback. Requiring
	// it up front aborts drain on any seat that is not demand-spawned:
	// GC_BEAD_ID exists only in the dispatch condition environment, never in a
	// session shell, and GC_TRIGGER_BEAD_ID is pool-seat-only (see
	// cmd/gc/cmd_hook_current.go). Same shape as ga-2q2r0.
	startupAt := strings.Index(step, `STARTUP_BEAD_ID="${GC_BEAD_ID`)
	if startupAt < 0 || startupAt < currentAt {
		t.Fatal("drain must not require a startup bead id before consulting the current claim")
	}
	// The current claim carries gc.root_bead_id, and on a warm seat it is the
	// ONLY source of it: GC_BEAD_ID never reaches a session shell and
	// GC_TRIGGER_BEAD_ID is demand-spawn-only (cmd/gc/cmd_hook_current.go).
	// Reading the root off the startup bead first strands every warm seat on
	// the deferred path — the ga-2q2r0 failure, one layer in.
	rootFromCurrent := strings.Index(step, `ROOT_BEAD_ID=$(printf '%s' "$CURRENT"`)
	if rootFromCurrent < 0 {
		t.Fatal("drain must derive the workflow root from the current claim it already fetched")
	}
	if rootFromCurrent > startupAt {
		t.Fatal("drain must try the current claim's root before falling back to startup env vars")
	}

	updateAt := strings.Index(step, "gc bd update")
	drainAckAt := strings.Index(step, "gc runtime drain-ack")
	if drainAckAt < 0 {
		t.Fatal("drain must still acknowledge runtime drain after closing its continuation bead")
	}
	if updateAt < 0 || !strings.Contains(step[updateAt:drainAckAt], "|| exit 1") {
		t.Fatal("drain must not acknowledge runtime drain after a failed bead close")
	}
}

// TestPolecatPreflightSearchesLedgerBeforeFiling pins the search-before-file
// contract in the polecat preflight step.
//
// Concurrent polecats all run a baseline against the same base branch, so they
// all observe the same pre-existing failure within seconds of each other. With
// no ledger search in front of the create, each one files its own bug: five
// duplicates inside three minutes from three polecats, and one duplicate that
// sat ready in the pool after its original had already merged, one sling away
// from dispatching a polecat to redo merged work.
func TestPolecatPreflightSearchesLedgerBeforeFiling(t *testing.T) {
	step := formulaStep(t, readFormula(t, "mol-polecat-base.toml"), "preflight-tests")

	searchAt := strings.Index(step, "gc bd list --title-contains")
	if searchAt < 0 {
		t.Fatal("preflight-tests must search the ledger with `gc bd list --title-contains` before filing a pre-existing-failure bead")
	}
	createAt := strings.Index(step, "gc bd create")
	if createAt < 0 {
		t.Fatal("preflight-tests must still describe how to file a pre-existing-failure bead")
	}
	if searchAt > createAt {
		t.Error("preflight-tests searches the ledger after `gc bd create`; the search must gate the create, not follow it")
	}

	// The original report is usually already claimed by the polecat or refinery
	// fixing it, so an open-only filter misses the very bead it should match.
	searchCmd := step[searchAt:createAt]
	if !strings.Contains(searchCmd, "in_progress") {
		t.Error("the dedupe lookup must include --status in_progress; the existing bead is frequently already claimed")
	}

	// A lookup that errors is not an all-clear. The refinery's earlier attempt at
	// this check invoked a flag that does not exist (`gc bd list --search`), so it
	// errored every run and the "no duplicate found" branch filed anyway.
	if !strings.Contains(step, "LOOKUP_RC") || !strings.Contains(step, `"$LOOKUP_RC" -ne 0`) {
		t.Error("the dedupe lookup must capture its exit status in LOOKUP_RC and fail closed on a non-zero result")
	}

	// Round-trip matchability: agents write different prose for one defect, so the
	// filed title has to carry the same stable key the next agent searches for.
	if !strings.Contains(searchCmd, `--title-contains "$SYMPTOM_KEY"`) {
		t.Error("the dedupe lookup must search by the stable $SYMPTOM_KEY, not by free-text description")
	}
	if !strings.Contains(step[createAt:], "$SYMPTOM_KEY") {
		t.Error("the filed title must embed $SYMPTOM_KEY verbatim so the next polecat's lookup matches it")
	}
}

// TestPolecatPreflightKeysOnTestFunctionNotSubtest pins the two widenings that
// an exact-title match alone does not deliver.
//
// Go subtests make the reported name unstable across agents: two concurrent
// polecats often fail different subtests of the same function
// (`TestX/clean_config_with_residual_files_is_a_conflict` versus
// `TestX/peer_successor_cross-device_tree`), search different strings, and both
// file. Stripping at the first `/` keys them together.
//
// Sibling functions in one package are the second gap: three different
// `TestDisableAndPurge*` functions can share a single root cause (for example
// an environment leak), and no exact-name key groups them. Eight
// productmetrics beads landed in ~38h that way.
func TestPolecatPreflightKeysOnTestFunctionNotSubtest(t *testing.T) {
	step := formulaStep(t, readFormula(t, "mol-polecat-base.toml"), "preflight-tests")

	if !strings.Contains(step, "%%/*") {
		t.Error("the symptom key must strip the Go subtest suffix (${NAME%%/*}) so sibling subtests of one function share a key")
	}

	familyAt := strings.Index(step, `--title-contains "$SYMPTOM_FAMILY"`)
	if familyAt < 0 {
		t.Error("preflight-tests must widen to a $SYMPTOM_FAMILY lookup; sibling tests in one package usually share one root cause")
	}
	createAt := strings.Index(step, "gc bd create")
	if createAt >= 0 && familyAt > createAt {
		t.Error("the family lookup must run before `gc bd create`, not after it")
	}

	// The family branch is a judgement call, so nothing auto-assigns the match —
	// but the step must still tell the agent how to hand a family hit to 3c, or
	// 3c's `gc bd comment "$EXISTING"` runs with an empty id.
	if !strings.Contains(step, `EXISTING="<the bead id you judged to be the same defect>"`) {
		t.Error("3b2 must show how to set $EXISTING for a family match; 3c cannot comment without it")
	}
	if !strings.Contains(step, "FAMILY_RC") {
		t.Error("the family lookup must capture its exit status; a failed lookup is not an all-clear")
	}
}

// TestPolecatPreflightChecksRecentlyClosedBeforeFiling pins the staleness gate.
//
// Re-filing a defect that already merged puts a ready bead in the pool, and the
// next sling dispatches a polecat to redo merged work.
func TestPolecatPreflightChecksRecentlyClosedBeforeFiling(t *testing.T) {
	step := formulaStep(t, readFormula(t, "mol-polecat-base.toml"), "preflight-tests")

	closedAt := strings.Index(step, "--closed-after")
	if closedAt < 0 {
		t.Fatal("preflight-tests must check recently-closed beads before filing; a stale baseline otherwise re-files merged work")
	}
	createAt := strings.Index(step, "gc bd create")
	if createAt >= 0 && closedAt > createAt {
		t.Error("the recently-closed check must run before `gc bd create`, not after it")
	}
	if !strings.Contains(step, "CLOSED_RC") {
		t.Error("the recently-closed lookup must capture its exit status; a failed lookup is not proof the fix has not landed")
	}
}

// TestPolecatSelfReviewDefersToPreflightDedupeProtocol keeps the second
// pre-existing-failure filing path pointed at the one protocol.
//
// self-review also tells the polecat to file a bead when a failure turns out to
// be pre-existing. Restating the protocol there would let the two copies drift;
// referring to preflight-tests keeps one definition.
func TestPolecatSelfReviewDefersToPreflightDedupeProtocol(t *testing.T) {
	step := formulaStep(t, readFormula(t, "mol-polecat-base.toml"), "self-review")

	if !strings.Contains(step, "preflight-tests") {
		t.Error("self-review's pre-existing-failure path must point at the preflight-tests search-first protocol instead of filing directly")
	}
	if strings.Contains(step, "gc bd create") {
		t.Error("self-review must not carry its own `gc bd create` for pre-existing failures; that bypasses the dedupe protocol")
	}
}

// TestCoreShippedAssetsAvoidNonexistentBDListSearchFlag guards the failure mode
// that made the sibling fix a no-op: `gc bd list` has no `--search` flag, so a
// dedupe lookup written against it exits 1 and returns nothing, which reads as
// "no duplicate exists" to the branch that follows. Search by --title-contains.
func TestCoreShippedAssetsAvoidNonexistentBDListSearchFlag(t *testing.T) {
	err := fs.WalkDir(PackFS, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := fs.ReadFile(PackFS, path)
		if err != nil {
			return err
		}
		body := string(data)
		for _, line := range strings.Split(body, "\n") {
			if strings.Contains(line, "bd list") && strings.Contains(line, "--search") {
				t.Errorf("%s: `bd list --search` is not a real flag and exits 1; use --title-contains: %s", path, strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking embedded core pack: %v", err)
	}
}

// TestMolPolecatCommitResolvesRepoBeforeRemovingWorktree pins the fix for a
// stranded-worktree bug: `git worktree remove` resolves the repo from cwd,
// and this step `cd`s away from the worktree before removing it, so the bare
// form exits 128 having unregistered nothing while `rm -rf` deletes the
// directory anyway, leaving the registration behind forever. These are
// bootstrap templates, so every city seeded by `gc city init` inherited the
// defect (ga-x1u5cr; contributing cause of ga-lc9yx's 396 dead worktrees).
func TestMolPolecatCommitResolvesRepoBeforeRemovingWorktree(t *testing.T) {
	step := formulaStep(t, readFormula(t, "mol-polecat-commit.toml"), "commit-and-push")

	if strings.Contains(step, `git worktree remove "$WORKTREE_PATH" --force`) {
		t.Error("commit-and-push calls bare `git worktree remove` after `cd ..`; resolve the repo via --git-common-dir first and remove via `git -C \"$REPO\" worktree remove`")
	}
	if !strings.Contains(step, "--git-common-dir") {
		t.Error("commit-and-push must resolve REPO via `git rev-parse --path-format=absolute --git-common-dir` before `cd ..`, so worktree removal does not depend on cwd")
	}
	if !strings.Contains(step, `git -C "$REPO" worktree remove`) {
		t.Error(`commit-and-push must remove the worktree via git -C "$REPO" worktree remove, not a bare invocation`)
	}

	// `git -C ""` is a no-op that silently resolves the repo from cwd, and this
	// step has already `cd ..`'d away from the worktree by then. An unresolved
	// REPO must short-circuit rather than degrade back to cwd-dependent removal.
	if !strings.Contains(step, `[ -z "$REPO" ]`) {
		t.Error(`commit-and-push must bail on an empty $REPO; git -C "" silently resolves from cwd, which is exactly the bug this step fixes`)
	}

	// WORKTREE_PATH is $(pwd), and `git worktree remove` exits 128 on a main
	// working tree. Without the guard the failure path rm -rf's the whole repo.
	guardAt := strings.Index(step, `[ -f "$WORKTREE_PATH/.git" ]`)
	if guardAt < 0 {
		t.Fatal(`commit-and-push must guard the rm -rf fallback with [ -f "$WORKTREE_PATH/.git" ]; a linked worktree's .git is a file, a main checkout's is a directory`)
	}
	// Match the delete command itself, not the word: the surrounding comment and
	// the refusal message both mention `rm -rf` and would otherwise be found first.
	if got := strings.Count(step, `rm -rf "$WORKTREE_PATH"`); got != 1 {
		t.Fatalf(`commit-and-push must delete the worktree exactly once behind the guard; found %d occurrences of rm -rf "$WORKTREE_PATH"`, got)
	}
	removeAt := strings.Index(step, `rm -rf "$WORKTREE_PATH"`)
	if guardAt > removeAt {
		t.Error("commit-and-push runs rm -rf before the linked-worktree check; the check must gate the delete, not follow it")
	}
}

// TestMolScopedWorkResolvesRepoBeforeRemovingWorktree pins the same fix for
// mol-scoped-work's cleanup step, which is worse than mol-polecat-commit's:
// its `|| rm -rf` fallback makes the stranded git registration the designed
// outcome of the bare form's failure path, not just an incidental risk.
func TestMolScopedWorkResolvesRepoBeforeRemovingWorktree(t *testing.T) {
	step := formulaStep(t, readFormula(t, "mol-scoped-work.toml"), "cleanup-worktree")

	if strings.Contains(step, `git worktree remove --force "$WORKTREE" || rm -rf "$WORKTREE"`) {
		t.Error("cleanup-worktree calls bare `git worktree remove --force ... || rm -rf`; resolve the repo via --git-common-dir first and remove via `git -C \"$REPO\" worktree remove`")
	}
	if !strings.Contains(step, "--git-common-dir") {
		t.Error(`cleanup-worktree must resolve REPO via git -C "$WORKTREE" rev-parse --path-format=absolute --git-common-dir; cwd is not guaranteed inside the repo at this step`)
	}
	if !strings.Contains(step, `git -C "$REPO" worktree remove`) {
		t.Error(`cleanup-worktree must remove the worktree via git -C "$REPO" worktree remove, not a bare invocation`)
	}

	// A stale directory that still passes [ -d ], or a git too old for
	// --path-format, leaves REPO empty; `git -C ""` then resolves from a cwd
	// this step explicitly does not guarantee is inside the repo.
	if !strings.Contains(step, `[ -z "$REPO" ]`) {
		t.Error(`cleanup-worktree must bail on an empty $REPO; git -C "" silently resolves from cwd, which this step cannot assume`)
	}

	guardAt := strings.Index(step, `[ -f "$WORKTREE/.git" ]`)
	if guardAt < 0 {
		t.Fatal(`cleanup-worktree must guard the rm -rf fallback with [ -f "$WORKTREE/.git" ]; a linked worktree's .git is a file, a main checkout's is a directory`)
	}
	if got := strings.Count(step, `rm -rf "$WORKTREE"`); got != 1 {
		t.Fatalf(`cleanup-worktree must delete the worktree exactly once behind the guard; found %d occurrences of rm -rf "$WORKTREE"`, got)
	}
	removeAt := strings.Index(step, `rm -rf "$WORKTREE"`)
	if guardAt > removeAt {
		t.Error("cleanup-worktree runs rm -rf before the linked-worktree check; the check must gate the delete, not follow it")
	}
}

// TestMolDoWorkWorkBeadCloseFailsClosed pins that a refused work-bead close
// (for example, the typed work-record close gate) stops the step instead of
// falling through to close the formula step over still-open work.
func TestMolDoWorkWorkBeadCloseFailsClosed(t *testing.T) {
	step := formulaStep(t, readFormula(t, "mol-do-work.toml"), "do-work")
	closes := 0
	for _, line := range strings.Split(step, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, `gc bd close "$WORK_BEAD_ID"`) {
			continue
		}
		closes++
		if !strings.HasSuffix(line, "|| exit 1") {
			t.Errorf("work-bead close must fail closed with `|| exit 1`: %s", line)
		}
	}
	if closes == 0 {
		t.Fatal("expected mol-do-work to close $WORK_BEAD_ID in the do-work step")
	}
}

// TestWorktreeFormulasHoldOnALiveOwnerBeforeWorkspaceSetup pins the fail-closed
// duplicate-dispatch gate in every core formula that derives its work bead from
// an input convoy and then creates a worktree for it.
//
// The pour-side fix (sling retires the direct pool route when a graph.v2
// workflow starts) closes the one observed producer of two live dispatch
// surfaces. This gate is the independent backstop: it holds for ANY producer,
// including pours that never go through `gc sling`. It has to run before
// workspace-setup, because that step is what recreates the branch — the step
// that turns a duplicate dispatch into destroyed uncommitted work.
//
// Fail-closed is the load-bearing half. An unreadable bead or an unreadable
// session list is not proof that nobody holds the work, and the earlier
// generation of this check (a bare `gc bd show | jq` capture with no
// validation) let a transient read failure read as "unowned" and proceed.
//
// Holding is a PARK, not a close and not a bare drain. Closing the step with
// any outcome satisfies workspace-setup's dependency (load-context sits in no
// scope, so a failed close does not stop the workflow); acknowledging drain
// with the step still claimed hands it back to the pool, where a fresh session
// runs it again (upstream #6992, TestCoreFormulaDrainAckStepsCloseTheirOwnStepFirst).
// `status=blocked` is the one state every consumer already refuses to advance:
// dependency resolution (not closed), the hook claim (blocked candidates are
// filtered), and the drain-ack release (in_progress claims only). The session
// then leaves through the worker's own claim loop instead of idling.
func TestWorktreeFormulasHoldOnALiveOwnerBeforeWorkspaceSetup(t *testing.T) {
	for _, file := range []string{"mol-polecat-base.toml", "mol-scoped-work.toml"} {
		t.Run(file, func(t *testing.T) {
			step := formulaStep(t, readFormula(t, file), "load-context")

			if !strings.Contains(step, "gc session list --state all") {
				t.Error("load-context must resolve the current owner's session liveness; a stale-looking worktree or an idle owner is not proof of death")
			}
			if !strings.Contains(step, "OWNER_LIVE=1") {
				t.Error("an unreadable session list must default OWNER_LIVE=1 (fail closed), not fall through as unowned")
			}
			if !strings.Contains(step, "WORK_STATUS=unknown") {
				t.Error("an unreadable work bead must be treated as blocked, not as unowned")
			}
			// The gate resolves an assignee against the session list, so it has
			// to match the form the claim path actually writes. `bd update
			// --claim` sets assignee to the session NAME
			// (<binding>__<agent>-<session-id>), which `gc session list --json`
			// exposes as .session_name; the record carries no .name field at
			// all, and .alias holds the agent address instead. Matching only
			// .alias/.name resolves a live owner to zero sessions, so the gate
			// reports "unowned" and fails OPEN in exactly the case it exists
			// for.
			if !strings.Contains(step, "session_name") {
				t.Error("the owner-liveness query must match the assignee against .session_name; that is the form --claim writes, and the session record has no .name field")
			}
			// Holding means the step bead is parked, never closed: closing it
			// is what advances the workflow into workspace-setup.
			if !strings.Contains(step, "NOT close this step") {
				t.Error("load-context must say explicitly that the step bead is not closed when the gate trips")
			}
			// The park is the session's OWN step, resolved from its claim, and it
			// is fail-closed: an unparked step must not fall through to the
			// escalation and the exit.
			claimAt := strings.Index(step, `STEP_BEAD_ID=$(gc hook current --id-only) || exit 1`)
			parkAt := strings.Index(step, `gc bd update "$STEP_BEAD_ID" --status=blocked || exit 1`)
			if claimAt < 0 || parkAt < 0 || parkAt < claimAt {
				t.Error("the gate must park the claimed step (`gc hook current --id-only` then `gc bd update \"$STEP_BEAD_ID\" --status=blocked || exit 1`) before anything else")
			}
			// A bare drain acknowledgement releases every in_progress claim the
			// session holds, so a step acked while still claimed is re-pooled
			// to a fresh session; upstream's drain-ack contract requires the
			// session's own step to be CLOSED before any `gc runtime drain-ack`,
			// which is exactly the write this gate must not perform. The gate
			// therefore never acks from inside the step: it parks, and leaves
			// through the claim loop, which drains only when nothing else is
			// claimable.
			if strings.Contains(step, "gc runtime drain-ack") {
				t.Error("load-context must not run `gc runtime drain-ack`: with the step parked rather than closed, an ack would release it back to the pool")
			}
			leaveAt := strings.Index(step, "gc hook --claim --drain-ack --json")
			if leaveAt < 0 {
				t.Error("the held session must leave through the claim loop (`gc hook --claim --drain-ack --json`) rather than idle on a pool slot it cannot use")
			} else if parkAt >= 0 && leaveAt < parkAt {
				t.Error("the gate must park its step before leaving through the claim loop")
			}
			// The gate is worthless if the agent has already made the branch.
			ownerAt := strings.Index(step, "OWNER_LIVE")
			if ownerAt < 0 {
				t.Fatal("load-context carries no owner-liveness gate")
			}
			if setupAt := strings.Index(step, "git worktree add"); setupAt >= 0 && setupAt < ownerAt {
				t.Error("load-context creates a worktree before the owner-liveness gate; the gate must run first")
			}
		})
	}
}

// TestWorktreeFormulasParkOnTheWorkHolderFinding pins how the load-context
// gate uses the work's recorded holder, by running the gate itself.
//
// A session running this workflow never claims the work bead, so the
// assignee check passes a second dispatch while the first is mid-flight. The
// gate then asks the core pack's work-holder.sh whether a live session holds
// the work's tree, reaching it through `gc core scripts-dir`, the one path a
// formula step has to core's scripts, and has it claim the work when none
// does. A gate that passes has claimed the work for this session with a
// compare-and-set on the holder it read. A finding parks the step the way a
// live assignee does, and the note and the escalation carry the finding, which
// names the holder and the tree and says what clears it; a claim another
// dispatch made after the gate's read is such a finding. A script the gate
// cannot reach fails closed. A live assignee parks before the tree is
// consulted, and its release names that assignee.
func TestWorktreeFormulasParkOnTheWorkHolderFinding(t *testing.T) {
	for _, tool := range []string{"git", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available; the load-context gate requires it", tool)
		}
	}
	scriptsDir, err := filepath.Abs("assets/scripts")
	if err != nil {
		t.Fatalf("resolving the core scripts directory: %v", err)
	}
	const repoSetup = `set -e
git init -q -b main "$ROOT/repo"
echo tracked > "$ROOT/repo/tracked.txt"
git -C "$ROOT/repo" add tracked.txt
git -C "$ROOT/repo" -c user.name=gate -c user.email=gate@example.invalid -c commit.gpgsign=false commit -q -m init
git -C "$ROOT/repo" worktree add -q --detach "$ROOT/repo/worktrees/gc-w"
`
	sessions := `{"sessions":[` + holderSessionMe + `,` + holderSessionPeer + `]}`
	cases := []struct {
		name string
		bead string
		// scriptsDir is the `gc core scripts-dir` answer; empty means it fails.
		scriptsDir string
		// claim is the outcome `gc beads metadata-cas` answers, swapped when
		// empty, and reread the work bead `gc bd show gc-w` returns after it.
		claim, reread string
		wantHold      bool
		// wantInMail must appear in the escalation, and its first entry in the
		// note on the work bead too.
		wantInMail []string
		// wantInLog must appear in the gc log, and wantNotInLog nowhere in it.
		wantInLog    string
		wantNotInLog string
	}{
		{
			name:       "a live recorded holder parks the step, and the escalation carries the finding",
			bead:       `[{"id":"gc-w","status":"open","assignee":"","metadata":{"work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-peer"}}]`,
			scriptsDir: scriptsDir,
			wantHold:   true,
			wantInMail: []string{
				"gc-w records live pool__worker-s-peer as the holder of worktree",
				"gc bd update gc-w --unset-metadata work_holder",
				"Once each finding above is resolved: gc bd update gc-step --status=open",
			},
			wantNotInLog: "gc beads metadata-cas",
		},
		{
			name:       "a gone recorded holder leaves the tree to adopt, and the gate claims it for this session",
			bead:       `[{"id":"gc-w","status":"open","assignee":"","metadata":{"work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-old"}}]`,
			scriptsDir: scriptsDir,
			wantInLog:  "gc beads metadata-cas gc-w --store-ref=rig:demo --key=work_holder --expected=s-old --next=s-me --json",
		},
		{
			name:       "a dispatch that claimed the work after the gate's read parks the step on that holder",
			bead:       `[{"id":"gc-w","status":"open","assignee":"","metadata":{"work_dir":"$ROOT/repo/worktrees/gc-w"}}]`,
			scriptsDir: scriptsDir,
			claim:      "conflict",
			reread:     `[{"id":"gc-w","status":"open","assignee":"","metadata":{"work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-peer"}}]`,
			wantHold:   true,
			wantInMail: []string{
				"gc-w records live pool__worker-s-peer as the holder of worktree",
				"gc bd update gc-w --unset-metadata work_holder",
				"Once each finding above is resolved: gc bd update gc-step --status=open",
			},
			wantInLog: "gc beads metadata-cas gc-w --store-ref=rig:demo --key=work_holder --expected= --next=s-me --json",
		},
		{
			name:     "a work-holder.sh the gate cannot reach fails closed",
			bead:     `[{"id":"gc-w","status":"open","assignee":"","metadata":{}}]`,
			wantHold: true,
			wantInMail: []string{
				"could not be run, because gc core scripts-dir did not name a directory holding it",
			},
		},
		{
			name:       "a live assignee parks before the tree is consulted, and its release names the assignee",
			bead:       `[{"id":"gc-w","status":"in_progress","assignee":"pool__worker-s-peer","metadata":{"work_dir":"$ROOT/repo/worktrees/gc-w","work_holder":"s-peer"}}]`,
			scriptsDir: scriptsDir,
			wantHold:   true,
			wantInMail: []string{
				"assigned to pool__worker-s-peer, which is live or not provably gone",
				"Only if pool__worker-s-peer is truly gone: gc bd update gc-step --status=open",
			},
			wantNotInLog: "gc core scripts-dir",
		},
	}
	vars := strings.NewReplacer("{{convoy_id}}", "gc-convoy", "{{escalation_target}}", "human")
	for _, file := range []string{"mol-polecat-base.toml", "mol-scoped-work.toml"} {
		gate := vars.Replace(bashFenceContaining(t, formulaStep(t, readFormula(t, file), "load-context"), "OWNER_LIVE=0"))
		if strings.Contains(gate, "{{") {
			t.Fatalf("%s: the load-context gate carries an unsubstituted formula variable", file)
		}
		for _, tc := range cases {
			t.Run(file+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				expand := strings.NewReplacer("$ROOT", root)
				env := []string{"ROOT=" + root, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull}
				setupPath := filepath.Join(root, "setup.sh")
				writeGateFixture(t, setupPath, repoSetup)
				if out, err := runPackScript(t, setupPath, t.TempDir(), workHolderEnvKeys, env); err != nil {
					t.Fatalf("laying out the repository: %v\n%s", err, out)
				}
				sessionsPath := filepath.Join(root, "sessions.json")
				writeGateFixture(t, sessionsPath, expand.Replace(sessions))
				scriptsAnswer := "exit 1"
				if tc.scriptsDir != "" {
					scriptsAnswer = "echo '" + tc.scriptsDir + "'"
				}
				claim := tc.claim
				if claim == "" {
					claim = "swapped"
				}
				beadPath := filepath.Join(root, "bead.json")
				writeGateFixture(t, beadPath, expand.Replace(tc.bead))
				rereadPath := filepath.Join(root, "reread.json")
				reread := tc.reread
				if reread == "" {
					reread = tc.bead
				}
				writeGateFixture(t, rereadPath, expand.Replace(reread))
				binDir, logPath := fakeGCBin(t, `case "$1 $2" in
"core scripts-dir") `+scriptsAnswer+` ;;
"session list") cat '`+sessionsPath+`' ;;
"bd list") echo '[]' ;;
"hook current") echo gc-step ;;
"beads metadata-cas") echo '{"schema_version":"1","ok":true,"bead_id":"gc-w","store_ref":"rig:demo","key":"work_holder","outcome":"`+claim+`"}' ;;
"bd show")
  case "$3" in
  gc-w) cat '`+rereadPath+`' ;;
  *) echo '[{"id":"gc-step","metadata":{"gc.root_bead_id":"gc-root"}}]' ;;
  esac ;;
esac
exit 0
`)
				gatePath := filepath.Join(root, "gate.sh")
				writeGateFixture(t, gatePath, `WORK_BEAD_ID=gc-w
WORK_BEAD_JSON=$(cat '`+beadPath+`')
`+gate+`
echo GATE-PASSED
`)
				out, err := runPackScript(t, gatePath, binDir, workHolderEnvKeys, append(env,
					"GC_DIR="+filepath.Join(root, "repo"),
					"GC_RIG=demo",
					"GC_RIG_ROOT="+filepath.Join(root, "repo"),
					"GC_SESSION_ID=s-me",
					"GC_SESSION_NAME=pool__worker-s-me",
					"GC_AGENT=s-me",
					"GC_ALIAS=",
				))
				logData, readErr := os.ReadFile(logPath)
				if readErr != nil && !os.IsNotExist(readErr) {
					t.Fatalf("reading the gc log: %v", readErr)
				}
				gcLog := string(logData)
				parked := strings.Contains(gcLog, "gc bd update gc-step --status=blocked")
				if tc.wantInLog != "" && !strings.Contains(gcLog, tc.wantInLog) {
					t.Errorf("the gate did not run %q\ngc log:\n%s", tc.wantInLog, gcLog)
				}
				if tc.wantNotInLog != "" && strings.Contains(gcLog, tc.wantNotInLog) {
					t.Errorf("the gate ran %q\ngc log:\n%s", tc.wantNotInLog, gcLog)
				}
				if !tc.wantHold {
					if err != nil || !strings.Contains(out, "GATE-PASSED") || parked {
						t.Fatalf("the gate held a dispatch it should pass (err=%v)\noutput:\n%s\ngc log:\n%s", err, out, gcLog)
					}
					if !strings.Contains(gcLog, "gc core scripts-dir") {
						t.Errorf("the gate passed without asking work-holder.sh\ngc log:\n%s", gcLog)
					}
					return
				}
				if err == nil || strings.Contains(out, "GATE-PASSED") || !parked {
					t.Fatalf("the gate passed a dispatch it must hold (err=%v)\noutput:\n%s\ngc log:\n%s", err, out, gcLog)
				}
				noteAt := strings.Index(gcLog, "gc bd update gc-w --append-notes")
				mailAt := strings.Index(gcLog, "gc mail send")
				if noteAt < 0 || mailAt < noteAt {
					t.Fatalf("the gate held without noting the work bead and then escalating\ngc log:\n%s", gcLog)
				}
				if !strings.Contains(gcLog[noteAt:mailAt], tc.wantInMail[0]) {
					t.Errorf("the note on the work bead does not mention %q\ngc log:\n%s", tc.wantInMail[0], gcLog)
				}
				for _, want := range tc.wantInMail {
					if !strings.Contains(gcLog[mailAt:], want) {
						t.Errorf("the escalation does not mention %q\ngc log:\n%s", want, gcLog)
					}
				}
			})
		}
	}
}

// TestWorktreeFormulasLetOneOfTwoRacingDispatchesThrough runs the race the
// load-context gate's claim closes. Two dispatches of one work bead both read
// it before either has claimed it, so both judge it free, and both reach the
// claim at once. The store lands one compare-and-set at a time, so exactly one
// gate passes, holding the work, and the other parks its own step on the
// holder that won.
func TestWorktreeFormulasLetOneOfTwoRacingDispatchesThrough(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; the load-context gate requires it")
	}
	scriptsDir, err := filepath.Abs("assets/scripts")
	if err != nil {
		t.Fatalf("resolving the core scripts directory: %v", err)
	}
	vars := strings.NewReplacer("{{convoy_id}}", "gc-convoy", "{{escalation_target}}", "human")
	ids := []string{"s-me", "s-peer"}
	for _, file := range []string{"mol-polecat-base.toml", "mol-scoped-work.toml"} {
		gate := vars.Replace(bashFenceContaining(t, formulaStep(t, readFormula(t, file), "load-context"), "OWNER_LIVE=0"))
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			for round := range 4 {
				root := t.TempDir()
				writeGateFixture(t, filepath.Join(root, "sessions.json"),
					strings.ReplaceAll(`{"sessions":[`+holderSessionMe+`,`+holderSessionPeer+`]}`, "$ROOT", root))
				gatePath := filepath.Join(root, "gate.sh")
				writeGateFixture(t, gatePath, `WORK_BEAD_ID=gc-w
WORK_BEAD_JSON='[{"id":"gc-w","status":"open","assignee":"","metadata":{}}]'
`+gate+`
echo GATE-PASSED
`)
				binDir, logPath := fakeGCBin(t, workHolderClaimFakeGC)
				outs := make([]string, len(ids))
				errs := make([]error, len(ids))
				var wg sync.WaitGroup
				for i, id := range ids {
					wg.Go(func() {
						outs[i], errs[i] = runPackScript(t, gatePath, binDir, workHolderEnvKeys,
							append(workHolderClaimEnv(root, id), "FAKE_SCRIPTS_DIR="+scriptsDir))
					})
				}
				wg.Wait()
				gcLog, _ := os.ReadFile(logPath)
				var passed []string
				for i, id := range ids {
					parked := strings.Contains(string(gcLog), "gc bd update gc-step-"+id+" --status=blocked")
					switch {
					case errs[i] == nil && strings.Contains(outs[i], "GATE-PASSED") && !parked:
						passed = append(passed, id)
					case errs[i] != nil && !strings.Contains(outs[i], "GATE-PASSED") && parked:
					default:
						t.Errorf("round %d: %s's gate neither passed nor parked its own step (err=%v)\noutput:\n%s", round, id, errs[i], outs[i])
					}
				}
				if len(passed) != 1 {
					t.Fatalf("round %d: %d gates passed (%v), want exactly one\ngc log:\n%s", round, len(passed), passed, gcLog)
				}
				if stored, _ := os.ReadFile(filepath.Join(root, "holder")); string(stored) != passed[0] {
					t.Errorf("round %d: the store holds work_holder=%q, want the gate that passed, %q", round, stored, passed[0])
				}
			}
		})
	}
}

// TestWorktreeFormulasShareOneDuplicateDispatchGate pins the two worktree
// formulas to one gate. Its judgment of the work's tree lives in
// work-holder.sh, and what each load-context step keeps (the assignee check,
// the park and the escalation) must read the same in both, apart from the
// escalation's recipient: mol-polecat-base mails {{escalation_target}} and
// mol-scoped-work mails human.
func TestWorktreeFormulasShareOneDuplicateDispatchGate(t *testing.T) {
	gate := func(file string) string {
		return bashFenceContaining(t, formulaStep(t, readFormula(t, file), "load-context"), "OWNER_LIVE=0")
	}
	base := strings.ReplaceAll(gate("mol-polecat-base.toml"), "gc mail send {{escalation_target}}", "gc mail send human")
	if scoped := gate("mol-scoped-work.toml"); scoped != base {
		t.Errorf("the load-context gates of mol-polecat-base and mol-scoped-work have drifted apart; change both together\nmol-polecat-base:\n%s\nmol-scoped-work:\n%s", base, scoped)
	}
}

// TestWorktreeFormulasRecordTheWorkHolderBesideTheTree pins the record the
// gate reads. workspace-setup records the tree it enters and the session that
// entered it in one write, on every entry, so a resumed or adopted tree names
// its current holder, and the step that removes the tree clears both. The
// base formula's placeholder step tells a variant to do the same.
func TestWorktreeFormulasRecordTheWorkHolderBesideTheTree(t *testing.T) {
	scoped := readFormula(t, "mol-scoped-work.toml")
	setup := formulaStep(t, scoped, "workspace-setup")
	const record = `gc bd update "$WORK_BEAD_ID" --set-metadata work_dir="$WORKTREE" --set-metadata work_holder="${GC_SESSION_ID:-}" || exit 1`
	enterAt := strings.Index(setup, `cd "$WORKTREE" || exit 1`)
	if recordAt := strings.Index(setup, record); enterAt < 0 || recordAt < enterAt {
		t.Errorf("mol-scoped-work workspace-setup must enter the tree and then record it with its holder, on a resume as well as a create: %s", record)
	}
	if !strings.Contains(formulaStep(t, scoped, "cleanup-worktree"), "--unset-metadata work_dir --unset-metadata work_holder") {
		t.Error("mol-scoped-work cleanup-worktree must clear work_holder with work_dir")
	}

	commit := readFormula(t, "mol-polecat-commit.toml")
	if !strings.Contains(formulaStep(t, commit, "workspace-setup"), `--set-metadata work_dir="$(pwd)" --set-metadata work_holder="${GC_SESSION_ID:-}"`) {
		t.Error("mol-polecat-commit workspace-setup must record the tree and its holder in one write")
	}
	if !strings.Contains(formulaStep(t, commit, "commit-and-push"), "--unset-metadata work_dir --unset-metadata work_holder") {
		t.Error("mol-polecat-commit commit-and-push must clear work_holder with work_dir")
	}

	if !strings.Contains(formulaStep(t, readFormula(t, "mol-polecat-base.toml"), "workspace-setup"), "`metadata.work_holder` (`$GC_SESSION_ID`)") {
		t.Error("mol-polecat-base workspace-setup must tell a variant to record the session holding its tree as metadata.work_holder")
	}
}

// bashFenceContaining returns the body of the first bash fence in text whose
// body contains marker.
func bashFenceContaining(t *testing.T, text, marker string) string {
	t.Helper()
	const open, closing = "```bash\n", "\n```"
	rest := text
	for {
		start := strings.Index(rest, open)
		if start < 0 {
			t.Fatalf("no bash fence contains %q", marker)
		}
		rest = rest[start+len(open):]
		end := strings.Index(rest, closing)
		if end < 0 {
			t.Fatal("unterminated bash fence")
		}
		if body := rest[:end]; strings.Contains(body, marker) {
			return body
		}
		rest = rest[end+len(closing):]
	}
}

func writeGateFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}
