//go:build acceptance_a

// Lifecycle-example worktree acceptance tests.
//
// worktree-setup.sh in the "lifecycle" example pack
// (examples/lifecycle/packs/lifecycle/assets/scripts/worktree-setup.sh) is
// an independently maintained script, not the same file as the gastown
// pack's embedded copy exercised by worktree_test.go -- the two happen to
// share a name and structure but have separate histories. Kept in its own
// file so the two script sources are never conflated.
package acceptance_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	helpers "github.com/gastownhall/gascity/test/acceptance/helpers"
)

// lifecycleWorktreeSetupScript returns the path to the lifecycle example
// pack's worktree-setup.sh as checked into this repo.
func lifecycleWorktreeSetupScript(t *testing.T) string {
	t.Helper()
	return filepath.Join(helpers.FindModuleRoot(), "examples", "lifecycle", "packs", "lifecycle", "assets", "scripts", "worktree-setup.sh")
}

// runLifecycleScript runs the lifecycle worktree-setup.sh script. Unlike
// runScript (used for the gastown pack's copy), this does not fail the
// test on a non-zero exit: this script currently exits 128 on every
// invocation from an unrelated, separately-tracked issue (ga-g8lt3x's
// "Out of scope" note) that is not this bead's deliverable. The exit is
// logged for visibility; the actual pass/fail signal is the .beads/redirect
// assertion each test makes afterward, exactly as the shell-level repro
// (investigations/ga-58xwg1/repro_gascity_clean.sh) evaluates it.
func runLifecycleScript(t *testing.T, script, repoDir, wt, agent string) {
	t.Helper()
	if out, err := runScriptCommand(script, repoDir, wt, agent); err != nil {
		t.Logf("worktree-setup.sh exited non-zero (tracked separately, not asserted here): %v\n%s", err, out)
	}
}

// TestLifecycleWorktreeSetupBeadRedirect verifies that the lifecycle
// example's worktree-setup.sh creates a .beads/redirect file pointing to
// the rig's .beads directory on a fresh worktree. Control case -- must
// keep passing across the ga-g8lt3x fix.
func TestLifecycleWorktreeSetupBeadRedirect(t *testing.T) {
	repoDir := t.TempDir()
	git(t, repoDir, "init")
	git(t, repoDir, "commit", "--allow-empty", "-m", "initial")

	script := lifecycleWorktreeSetupScript(t)

	wt := filepath.Join(t.TempDir(), "worktree")
	runLifecycleScript(t, script, repoDir, wt, "polecat")

	redirect := filepath.Join(wt, ".beads", "redirect")
	data, err := os.ReadFile(redirect)
	if err != nil {
		t.Fatalf(".beads/redirect not created: %v", err)
	}

	want := repoDir + "/.beads"
	if got := strings.TrimSpace(string(data)); got != want {
		t.Fatalf(".beads/redirect = %q, want %q", got, want)
	}
}

// TestLifecycleWorktreeSetupRedirectAppliesToPreExistingWorktree is a
// regression test for ga-g8lt3x: a worktree created by any means other
// than this script (a plain "git worktree add", an older script version,
// or a redirect later clobbered) used to hit the early-exit branch and
// skip the bead-redirect / local-excludes provisioning forever -- no
// convergence, even across repeated pre_start invocations on the same
// worktree.
func TestLifecycleWorktreeSetupRedirectAppliesToPreExistingWorktree(t *testing.T) {
	repoDir := t.TempDir()
	git(t, repoDir, "init")
	git(t, repoDir, "commit", "--allow-empty", "-m", "initial")

	script := lifecycleWorktreeSetupScript(t)

	wt := filepath.Join(t.TempDir(), "worktree")
	// Simulate a worktree created by some other path -- the setup
	// script has never touched it yet.
	git(t, repoDir, "worktree", "add", "-b", "gc-agent-b", wt)

	if _, err := os.Stat(filepath.Join(wt, ".beads", "redirect")); err == nil {
		t.Fatal("redirect already present before setup script ran -- test setup is wrong")
	}

	// pre_start runs the script on every session start; a converging
	// fix must not need a special first-time case, so run it 3x on the
	// same already-existing worktree.
	for i := 0; i < 3; i++ {
		runLifecycleScript(t, script, repoDir, wt, "agent-b")
	}

	redirect := filepath.Join(wt, ".beads", "redirect")
	data, err := os.ReadFile(redirect)
	if err != nil {
		t.Fatalf(".beads/redirect not created for pre-existing worktree after 3 runs: %v", err)
	}

	want := repoDir + "/.beads"
	if got := strings.TrimSpace(string(data)); got != want {
		t.Fatalf(".beads/redirect = %q, want %q", got, want)
	}
}

// divergedWorktree builds the shape that wedges an agent worktree: a
// persistent branch tracking the rig's default branch, carrying a local
// commit the default branch never took, where the default branch has
// since evolved the same file. Returns the rig root (the fetch origin)
// and the worktree path.
//
// The worktree's parent is a clone rather than the rig itself, so it has
// a real "origin" remote to fetch from, exactly as a rig checkout does.
func divergedWorktree(t *testing.T, file string) (rigRoot, wt string) {
	t.Helper()

	rigRoot = t.TempDir()
	git(t, rigRoot, "init")
	mustWriteTestFile(t, filepath.Join(rigRoot, file), "base\n")
	git(t, rigRoot, "add", file)
	git(t, rigRoot, "commit", "-m", "initial")

	cloneParent := t.TempDir()
	clone := filepath.Join(cloneParent, "clone")
	git(t, cloneParent, "clone", rigRoot, clone)

	wt = filepath.Join(t.TempDir(), "home")
	git(t, clone, "worktree", "add", "-b", "gc-agent-home", wt, "origin/"+currentBranch(t, clone))

	// The agent's persistent branch accumulates a local commit...
	mustWriteTestFile(t, filepath.Join(wt, file), "agent-local\n")
	git(t, wt, "commit", "-am", "local commit the default branch later sheds")

	// ...that the default branch never takes, and then supersedes.
	// Replaying the local commit onto the new tip now conflicts.
	mustWriteTestFile(t, filepath.Join(rigRoot, file), "upstream\n")
	git(t, rigRoot, "commit", "-am", "default branch evolves the same file")

	return rigRoot, wt
}

// assertNoInterruptedRebase fails if the worktree is parked mid-rebase,
// holds a conflicted index, or has conflict markers in file.
func assertNoInterruptedRebase(t *testing.T, wt, file string) {
	t.Helper()

	for _, state := range []string{"rebase-merge", "rebase-apply"} {
		dir := git(t, wt, "rev-parse", "--git-path", state)
		if _, err := os.Stat(dir); err == nil {
			t.Errorf("worktree left mid-rebase: %s exists", state)
		}
	}

	if status := git(t, wt, "status", "--porcelain"); status != "" {
		t.Errorf("worktree not clean after sync:\n%s", status)
	}

	data, err := os.ReadFile(filepath.Join(wt, file))
	if err != nil {
		t.Fatalf("reading %s: %v", file, err)
	}
	if strings.Contains(string(data), "<<<<<<<") || strings.Contains(string(data), ">>>>>>>") {
		t.Errorf("%s holds conflict markers after sync:\n%s", file, data)
	}
}

// TestLifecycleWorktreeSetupSyncDoesNotWedgeDivergedBranch is a
// regression test for gc-16sh5: --sync ran "git pull --rebase" in the
// agent's persistent worktree, which replays every local commit on that
// scratch branch onto the freshly fetched tip. Once the default branch
// sheds those commits the first pick conflicts, and because the failure
// was swallowed the worktree was left mid-rebase with a conflicted index
// and conflict markers in tracked files -- including .githooks/pre-commit,
// which core.hooksPath makes an executable hook, so the next commit from
// that worktree runs a syntactically broken hook.
func TestLifecycleWorktreeSetupSyncDoesNotWedgeDivergedBranch(t *testing.T) {
	const hook = ".githooks/pre-commit"
	rigRoot, wt := divergedWorktree(t, hook)

	before := git(t, wt, "rev-parse", "HEAD")

	runLifecycleScript(t, lifecycleWorktreeSetupScript(t), rigRoot, wt, "agent")

	assertNoInterruptedRebase(t, wt, hook)

	// A branch that cannot fast-forward is left exactly as it was: the
	// local commits are the agent's, not ours to replay or discard.
	if after := git(t, wt, "rev-parse", "HEAD"); after != before {
		t.Errorf("diverged branch moved during sync: %s -> %s", before, after)
	}
}

// TestLifecycleWorktreeSetupSyncClearsWedgedWorktree covers the
// fail-safe half of gc-16sh5: a worktree found already parked mid-rebase
// (from an earlier cycle, or a session that died mid-conflict) must be
// cleared before the session starts, because pre_start runs before any
// agent can commit and the conflicted files include the hook that commit
// would run.
func TestLifecycleWorktreeSetupSyncClearsWedgedWorktree(t *testing.T) {
	const hook = ".githooks/pre-commit"
	rigRoot, wt := divergedWorktree(t, hook)

	// Wedge it exactly the way the old sync did.
	git(t, wt, "fetch", "origin")
	if out, err := runGitCommand(wt, "pull", "--rebase"); err == nil {
		t.Fatalf("test setup: pull --rebase was expected to conflict, got:\n%s", out)
	}
	rebaseDir := git(t, wt, "rev-parse", "--git-path", "rebase-merge")
	if _, err := os.Stat(rebaseDir); err != nil {
		t.Fatalf("test setup: worktree is not mid-rebase: %v", err)
	}

	runLifecycleScript(t, lifecycleWorktreeSetupScript(t), rigRoot, wt, "agent")

	assertNoInterruptedRebase(t, wt, hook)
}

// TestLifecycleWorktreeSetupSyncFastForwardsCleanBranch is the control
// for the two tests above: refusing to rebase a diverged branch must not
// turn --sync into a no-op. A worktree with no local commits still
// converges on the fetched tip.
func TestLifecycleWorktreeSetupSyncFastForwardsCleanBranch(t *testing.T) {
	rigRoot := t.TempDir()
	git(t, rigRoot, "init")
	mustWriteTestFile(t, filepath.Join(rigRoot, "file.txt"), "base\n")
	git(t, rigRoot, "add", "file.txt")
	git(t, rigRoot, "commit", "-m", "initial")

	cloneParent := t.TempDir()
	clone := filepath.Join(cloneParent, "clone")
	git(t, cloneParent, "clone", rigRoot, clone)

	wt := filepath.Join(t.TempDir(), "home")
	git(t, clone, "worktree", "add", "-b", "gc-agent-home", wt, "origin/"+currentBranch(t, clone))

	mustWriteTestFile(t, filepath.Join(rigRoot, "file.txt"), "advanced\n")
	git(t, rigRoot, "commit", "-am", "default branch advances")
	want := git(t, rigRoot, "rev-parse", "HEAD")

	runLifecycleScript(t, lifecycleWorktreeSetupScript(t), rigRoot, wt, "agent")

	if got := git(t, wt, "rev-parse", "HEAD"); got != want {
		t.Errorf("clean branch did not fast-forward: HEAD = %s, want %s", got, want)
	}
}
