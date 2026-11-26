package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/git"
	"github.com/gastownhall/gascity/internal/pathutil"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/worktree"
)

// livePruneAllowed is a successfully-scanned liveness snapshot that shows no
// process or session in any worktree, so the liveness gate never blocks — the
// implicit shape every prune test assumed before the gate existed. Shared by
// both the raw and session.Info prune test forms.
func livePruneAllowed() worktreeLivenessInputs {
	return worktreeLivenessInputs{live: liveWorktreeState{scanned: true}}
}

// liveAt returns a successfully-scanned liveness snapshot reporting a live
// process cwd at each given directory, normalized the way the production
// process-table scan records them.
func liveAt(dirs ...string) worktreeLivenessInputs {
	cwds := make([]string, 0, len(dirs))
	for _, d := range dirs {
		cwds = append(cwds, pathutil.NormalizePathForCompare(d))
	}
	return worktreeLivenessInputs{live: liveWorktreeState{scanned: true, cwds: cwds}}
}

// liveViaSession returns a successfully-scanned liveness snapshot with no live
// process cwd but the given working directories recorded by the open session
// id, exercising the session-dir cross-check independently of the /proc scan.
func liveViaSession(id string, dirs ...string) worktreeLivenessInputs {
	in := worktreeLivenessInputs{live: liveWorktreeState{scanned: true}}
	for _, d := range dirs {
		in.sessionDirs = append(in.sessionDirs, sessionWorkDir{sessionID: id, dir: d})
	}
	return in
}

// liveScanUnavailable is the indeterminate liveness snapshot — the process
// table could not be enumerated — which must fail closed and block every prune.
func liveScanUnavailable() worktreeLivenessInputs {
	return worktreeLivenessInputs{live: liveWorktreeState{scanned: false}}
}

// fakeGitProbe is a hand-rolled gitProbe stub. Each field controls one
// probe; worktrees and worktreeListErr answer the repository's worktree
// list; WorktreeRemoveErr controls the destructive call, and removed
// records the (path, force) of every WorktreeRemove invocation so tests
// can assert which directory the removal targeted. gitDir is the private
// git directory the prune reads worktree provenance from.
type fakeGitProbe struct {
	isRepo           bool
	gitDir           string
	gitDirErr        error
	currentBranch    string
	currentBranchErr error
	hasUncommitted   bool
	hasUnpushed      bool
	unpushedErr      error
	worktrees        []git.Worktree
	worktreeListErr  error
	worktreeRemove   func(path string, force bool) error
	removedPath      string
	removedForce     bool
	removeInvoked    bool
}

func (f *fakeGitProbe) IsRepo() bool { return f.isRepo }
func (f *fakeGitProbe) GitDir() (string, error) {
	return f.gitDir, f.gitDirErr
}

func (f *fakeGitProbe) CurrentBranch() (string, error) {
	return f.currentBranch, f.currentBranchErr
}
func (f *fakeGitProbe) HasUncommittedWork() bool { return f.hasUncommitted }
func (f *fakeGitProbe) HasUnpushedCommitsResult() (bool, error) {
	return f.hasUnpushed, f.unpushedErr
}

func (f *fakeGitProbe) WorktreeList() ([]git.Worktree, error) {
	return f.worktrees, f.worktreeListErr
}

func (f *fakeGitProbe) WorktreeRemove(path string, force bool) error {
	f.removeInvoked = true
	f.removedPath = path
	f.removedForce = force
	if f.worktreeRemove != nil {
		return f.worktreeRemove(path, force)
	}
	return nil
}

// assertWorktreeStaleMarker fails the test unless workerDir contains a
// .worktree-stale marker recording the given branch and reason. Shared by
// both the raw and session.Info test forms.
func assertWorktreeStaleMarker(t *testing.T, workerDir, wantBranch, wantReason string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(workerDir, worktreeStaleFileName))
	if err != nil {
		t.Fatalf("reading %s marker: %v", worktreeStaleFileName, err)
	}
	content := string(data)
	if !strings.Contains(content, "branch="+wantBranch) {
		t.Errorf("marker content = %q, want to contain %q", content, "branch="+wantBranch)
	}
	if !strings.Contains(content, "reason="+wantReason) {
		t.Errorf("marker content = %q, want to contain %q", content, "reason="+wantReason)
	}
}

// assertNoWorktreeStaleMarker fails the test if workerDir contains a
// .worktree-stale marker.
func assertNoWorktreeStaleMarker(t *testing.T, workerDir string) {
	t.Helper()
	if _, err := os.ReadFile(filepath.Join(workerDir, worktreeStaleFileName)); err == nil {
		t.Errorf("%s marker unexpectedly present in %s", worktreeStaleFileName, workerDir)
	} else if !os.IsNotExist(err) {
		t.Fatalf("checking %s marker: %v", worktreeStaleFileName, err)
	}
}

// pruneTestFixture wires a temp city directory, a writable worker_dir
// under .gc/worktrees with a .git marker, and a config with a rig that
// points at a separate "rig root" directory. Every probe lists the rig root
// first among its repository's worktrees unless a test says otherwise, so
// the rig root is the main worktree the prune call uses as the git -C
// target. gitDir is the worker_dir's private git directory: it holds no
// worktree provenance, so the worker_dir is an unmanaged worktree unless a
// test says otherwise.
type pruneTestFixture struct {
	t          *testing.T
	cityPath   string
	rigRoot    string
	workerDir  string
	gitDir     string
	cfg        *config.City
	probesByWD map[string]*fakeGitProbe
}

func newPruneFixture(t *testing.T) *pruneTestFixture {
	t.Helper()
	cityPath := t.TempDir()
	rigRoot := filepath.Join(cityPath, "repos", "demo")
	workerDir := filepath.Join(cityPath, ".gc", "worktrees", "demo", "polecats", "polecat-3")
	gitDir := filepath.Join(rigRoot, ".git", "worktrees", "polecat-3")

	if err := os.MkdirAll(rigRoot, 0o755); err != nil {
		t.Fatalf("mkdir rigRoot: %v", err)
	}
	if err := os.MkdirAll(workerDir, 0o755); err != nil {
		t.Fatalf("mkdir workerDir: %v", err)
	}
	if err := os.MkdirAll(gitDir, 0o755); err != nil {
		t.Fatalf("mkdir gitDir: %v", err)
	}
	// .git marker so the existence check passes; contents don't matter
	// because gitProbe is faked.
	if err := os.WriteFile(filepath.Join(workerDir, ".git"), []byte("gitdir: /fake\n"), 0o644); err != nil {
		t.Fatalf("write .git marker: %v", err)
	}

	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "demo", Path: rigRoot},
		},
	}

	fx := &pruneTestFixture{
		t:          t,
		cityPath:   cityPath,
		rigRoot:    rigRoot,
		workerDir:  workerDir,
		gitDir:     gitDir,
		cfg:        cfg,
		probesByWD: make(map[string]*fakeGitProbe),
	}

	orig := newGitProbe
	t.Cleanup(func() { newGitProbe = orig })
	newGitProbe = func(workDir string) gitProbe {
		probe, ok := fx.probesByWD[workDir]
		if !ok {
			// Default: a healthy probe. The WorktreeRemove call goes
			// through this default for the rig-root invocation.
			probe = &fakeGitProbe{isRepo: true, gitDir: fx.gitDir, worktrees: fx.repoWorktrees()}
			fx.probesByWD[workDir] = probe
		}
		return probe
	}

	return fx
}

// repoWorktrees is the worktree list of the fixture's repository: the rig
// root, its main worktree, then the worker_dir.
func (fx *pruneTestFixture) repoWorktrees() []git.Worktree {
	return []git.Worktree{{Path: fx.rigRoot}, {Path: fx.workerDir}}
}

// setProbe installs probe for workDir. A probe that names no git dir and
// no git dir error reads the fixture's provenance-free gitDir, and one that
// names no worktree list and no list error lists the fixture's repository.
func (fx *pruneTestFixture) setProbe(workDir string, probe *fakeGitProbe) {
	if probe.gitDir == "" && probe.gitDirErr == nil {
		probe.gitDir = fx.gitDir
	}
	if probe.worktrees == nil && probe.worktreeListErr == nil {
		probe.worktrees = fx.repoWorktrees()
	}
	fx.probesByWD[workDir] = probe
}

// managedPruneWorktree publishes a real managed worktree under cityPath's
// worktrees root for rig "demo", as `gc worktree ensure` does for a
// provisioner that is not the session: its provenance names owner
// "provisioner" and bead gc-work. Its checkout is clean and its HEAD is
// pushed, so every git safety gate of the prune passes and only its
// ownership can keep it. It returns the rig root and the spec the owner
// holds, BaseSHA included, which worktree.Verify accepts while the
// worktree is intact.
func managedPruneWorktree(t *testing.T, cityPath string) (string, worktree.Spec) {
	t.Helper()
	bare := t.TempDir()
	runGit(t, bare, "init", "--bare")

	rigRoot := t.TempDir()
	runGit(t, rigRoot, "clone", bare, ".")
	runGit(t, rigRoot, "config", "user.email", "test@test.com")
	runGit(t, rigRoot, "config", "user.name", "Test")
	runGit(t, rigRoot, "commit", "--allow-empty", "-m", "init")
	runGit(t, rigRoot, "push", "origin", "HEAD")
	base := strings.TrimSpace(runGit(t, rigRoot, "rev-parse", "--abbrev-ref", "HEAD"))

	root := filepath.Join(cityPath, ".gc", "worktrees", "demo")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir worktree root: %v", err)
	}
	spec := worktree.Spec{
		RepoDir: rigRoot, Root: root, Path: filepath.Join(root, "gc-work-g1"),
		Branch: "work/gc-work-g1", Base: base,
		BeadID: "gc-work", StoreRef: "rig:demo", Creator: "provisioner", Owner: "provisioner",
		Generation: "g1", Lifecycle: worktree.LifecycleActive,
	}
	report, err := worktree.Ensure(spec)
	if err != nil {
		t.Fatalf("Ensure managed worktree: %v", err)
	}
	spec.BaseSHA = report.Provenance.BaseSHA
	if hasUnpushed, err := git.New(spec.Path).HasUnpushedCommitsResult(); err != nil || hasUnpushed {
		t.Fatalf("test setup invalid: managed worktree must probe pushed so only ownership keeps it; unpushed=%v err=%v", hasUnpushed, err)
	}
	return rigRoot, spec
}

// assertManagedWorktreeIntact fails the test unless the owner's worktree
// still verifies against the spec the owner holds: present, registered,
// on its branch, and carrying its provenance.
func assertManagedWorktreeIntact(t *testing.T, spec worktree.Spec, stderr string) {
	t.Helper()
	if _, err := worktree.Verify(spec); err != nil {
		t.Fatalf("closing session's prune removed or damaged a worktree another owner holds: %v; stderr=%q", err, stderr)
	}
	if strings.Contains(stderr, "pruned worker_dir") {
		t.Fatalf("prune reported removing a worktree another owner holds; stderr=%q", stderr)
	}
}

func (fx *pruneTestFixture) sessionBead() beads.Bead {
	return beads.Bead{
		ID: "session-1",
		Metadata: map[string]string{
			"worker_dir":   fx.workerDir,
			"template":     "demo/polecat",
			"session_name": "demo/polecat-3",
		},
	}
}

func TestPruneAgentHomeWorktreeIfSafe_DisabledByConfig(t *testing.T) {
	fx := newPruneFixture(t)
	off := false
	fx.cfg.Daemon.AutoPruneWorkerDir = &off

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true while disabled")
	}
	if rigProbe := fx.probesByWD[fx.rigRoot]; rigProbe != nil && rigProbe.removeInvoked {
		t.Fatal("WorktreeRemove called while config disabled prune")
	}
}

func TestPruneAgentHomeWorktreeIfSafe_NoWorkerDir(t *testing.T) {
	fx := newPruneFixture(t)
	session := fx.sessionBead()
	delete(session.Metadata, "worker_dir")

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(session, fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true with no worker_dir")
	}
}

func TestPruneAgentHomeWorktreeIfSafe_LegacyWorkDirKey(t *testing.T) {
	fx := newPruneFixture(t)
	session := fx.sessionBead()
	delete(session.Metadata, "worker_dir")
	session.Metadata["work_dir"] = fx.workerDir

	probe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.workerDir, probe)
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)

	var stderr bytes.Buffer
	if !pruneAgentHomeWorktreeIfSafe(session, fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatalf("prune returned false on legacy work_dir; stderr=%s", stderr.String())
	}
	if !rigProbe.removeInvoked || rigProbe.removedPath != fx.workerDir || !rigProbe.removedForce {
		t.Fatalf("expected WorktreeRemove(%q, true) on rig root; got invoked=%v path=%q force=%v",
			fx.workerDir, rigProbe.removeInvoked, rigProbe.removedPath, rigProbe.removedForce)
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_OutsideWorktreesTree(t *testing.T) {
	fx := newPruneFixture(t)
	session := fx.sessionBead()
	outside := filepath.Join(fx.cityPath, "elsewhere", "polecat-3")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outside, ".git"), []byte("gitdir: /fake\n"), 0o644); err != nil {
		t.Fatalf("write .git: %v", err)
	}
	session.Metadata["worker_dir"] = outside

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(session, fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true for path outside .gc/worktrees")
	}
}

func TestPruneAgentHomeWorktreeIfSafe_UsesConfiguredWorktreesRoot(t *testing.T) {
	fx := newPruneFixture(t)
	configuredRoot := filepath.Join(fx.cityPath, "configured-worktrees")
	configuredWorker := filepath.Join(configuredRoot, "demo", "polecats", "polecat-3")
	t.Setenv("GC_WORKTREES_DIR", configuredRoot)
	if err := os.MkdirAll(configuredWorker, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configuredWorker, ".git"), []byte("gitdir: /fake\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	session := fx.sessionBead()
	session.Metadata["worker_dir"] = configuredWorker
	fx.setProbe(configuredWorker, &fakeGitProbe{isRepo: true})
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)

	if !pruneAgentHomeWorktreeIfSafe(session, fx.cityPath, fx.cfg, livePruneAllowed(), io.Discard) {
		t.Fatal("configured-root worktree was not pruned")
	}
	if !rigProbe.removeInvoked || rigProbe.removedPath != configuredWorker {
		t.Fatalf("WorktreeRemove path = %q, invoked=%v", rigProbe.removedPath, rigProbe.removeInvoked)
	}
}

func TestPruneAgentHomeWorktreeIfSafe_RejectsWorktreesRoot(t *testing.T) {
	fx := newPruneFixture(t)
	session := fx.sessionBead()
	wtRoot := filepath.Join(fx.cityPath, ".gc", "worktrees")
	if err := os.WriteFile(filepath.Join(wtRoot, ".git"), []byte("gitdir: /fake\n"), 0o644); err != nil {
		t.Fatalf("write .git on wtRoot: %v", err)
	}
	session.Metadata["worker_dir"] = wtRoot

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(session, fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true for .gc/worktrees root itself")
	}
}

func TestPruneAgentHomeWorktreeIfSafe_RelativeWorkerDir(t *testing.T) {
	fx := newPruneFixture(t)
	session := fx.sessionBead()
	session.Metadata["worker_dir"] = "relative/path"

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(session, fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true for relative worker_dir")
	}
}

func TestPruneAgentHomeWorktreeIfSafe_MissingDotGit(t *testing.T) {
	fx := newPruneFixture(t)
	if err := os.Remove(filepath.Join(fx.workerDir, ".git")); err != nil {
		t.Fatalf("remove .git: %v", err)
	}

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true with missing .git pointer")
	}
}

func TestPruneAgentHomeWorktreeIfSafe_NotARepo(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: false})

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true when IsRepo=false")
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_HasUncommitted(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true, hasUncommitted: true, currentBranch: "builder/ga-abc123"})

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true with uncommitted work")
	}
	if !strings.Contains(stderr.String(), "uncommitted changes") {
		t.Errorf("expected uncommitted-reason log; got %q", stderr.String())
	}
	assertWorktreeStaleMarker(t, fx.workerDir, "builder/ga-abc123", "uncommitted-work")
}

func TestPruneAgentHomeWorktreeIfSafe_HasUnpushed(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true, hasUnpushed: true, currentBranch: "builder/ga-def456"})

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true with unpushed commits")
	}
	if !strings.Contains(stderr.String(), "unpushed commits") {
		t.Errorf("expected unpushed-reason log; got %q", stderr.String())
	}
	assertWorktreeStaleMarker(t, fx.workerDir, "builder/ga-def456", "unpushed-commits")
}

func TestPruneAgentHomeWorktreeIfSafe_UnpushedProbeError(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true, unpushedErr: errors.New("boom")})

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true after unpushed probe error")
	}
	if !strings.Contains(stderr.String(), "unpushed probe failed") {
		t.Errorf("expected unpushed-error log; got %q", stderr.String())
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

// TestPruneAgentHomeWorktreeIfSafe_UnrelatedStashDoesNotBlock is a real-git
// regression test for the repo-global stash aliasing bug (ga-pyp2oh):
// `git stash list` reads refs/stash from the shared git admin dir, so its
// result used to be identical for every worktree of the same repo
// regardless of which worktree's own state was probed. pruneAgentHomeWorktreeIfSafe
// no longer probes stashes at all; this test exercises a real repo to prove
// an unrelated stash elsewhere in the shared repo never blocks reclaim.
func TestPruneAgentHomeWorktreeIfSafe_UnrelatedStashDoesNotBlock(t *testing.T) {
	bare := t.TempDir()
	runGit(t, bare, "init", "--bare")

	rigRoot := t.TempDir()
	runGit(t, rigRoot, "clone", bare, ".")
	runGit(t, rigRoot, "config", "user.email", "test@test.com")
	runGit(t, rigRoot, "config", "user.name", "Test")
	runGit(t, rigRoot, "commit", "--allow-empty", "-m", "init")
	runGit(t, rigRoot, "push", "origin", "HEAD")

	// A stash rooted at the rig root — simulating some unrelated agent
	// session having stashed work anywhere in the shared repo.
	if err := os.WriteFile(filepath.Join(rigRoot, "wip.txt"), []byte("wip"), 0o644); err != nil {
		t.Fatalf("write wip file: %v", err)
	}
	runGit(t, rigRoot, "add", "wip.txt")
	runGit(t, rigRoot, "stash")

	cityPath := t.TempDir()
	workerDir := filepath.Join(cityPath, ".gc", "worktrees", "demo", "polecat-1")
	if err := os.MkdirAll(filepath.Dir(workerDir), 0o755); err != nil {
		t.Fatalf("mkdir worktree parent: %v", err)
	}
	runGit(t, rigRoot, "worktree", "add", "-b", "polecat-1-branch", workerDir)

	// Sanity: prove the aliasing precondition. The stash created at
	// rigRoot must be visible from workerDir too, or this test proves
	// nothing about the bug.
	if out := runGit(t, workerDir, "stash", "list"); strings.TrimSpace(out) == "" {
		t.Fatalf("test setup invalid: rig-root stash not visible from workerDir — aliasing precondition not met")
	}

	cfg := &config.City{Rigs: []config.Rig{{Name: "demo", Path: rigRoot}}}
	session := beads.Bead{
		ID: "session-1",
		Metadata: map[string]string{
			"worker_dir":   workerDir,
			"template":     "demo/polecat",
			"session_name": "demo/polecat-1",
		},
	}

	var stderr bytes.Buffer
	if !pruneAgentHomeWorktreeIfSafe(session, cityPath, cfg, livePruneAllowed(), &stderr) {
		t.Fatalf("prune returned false for a clean, fully-pushed worktree merely because an unrelated stash exists elsewhere in the shared repo; stderr=%s", stderr.String())
	}
	if _, err := os.Stat(workerDir); !os.IsNotExist(err) {
		t.Errorf("workerDir %s should have been removed; stat err=%v", workerDir, err)
	}
}

// TestPruneAgentHomeWorktreeIfSafe_StashInsideWorktreeSurvivesPrune locks in
// the premise the stash-gate removal rests on (ga-pyp2oh): a stash created
// from inside the worktree being pruned is not lost when the worktree is
// removed. Stash commits live in the shared object store and are held by
// refs/stash in the shared git admin dir, neither of which
// `git worktree remove` touches — so the gate could only ever have produced
// false positives, never prevented a real loss. This test proves the stash
// entry and its full diff are both still readable from the rig root after
// the worktree directory is gone.
func TestPruneAgentHomeWorktreeIfSafe_StashInsideWorktreeSurvivesPrune(t *testing.T) {
	bare := t.TempDir()
	runGit(t, bare, "init", "--bare")

	rigRoot := t.TempDir()
	runGit(t, rigRoot, "clone", bare, ".")
	runGit(t, rigRoot, "config", "user.email", "test@test.com")
	runGit(t, rigRoot, "config", "user.name", "Test")
	runGit(t, rigRoot, "commit", "--allow-empty", "-m", "init")
	runGit(t, rigRoot, "push", "origin", "HEAD")

	cityPath := t.TempDir()
	workerDir := filepath.Join(cityPath, ".gc", "worktrees", "demo", "polecat-1")
	if err := os.MkdirAll(filepath.Dir(workerDir), 0o755); err != nil {
		t.Fatalf("mkdir worktree parent: %v", err)
	}
	runGit(t, rigRoot, "worktree", "add", "-b", "polecat-1-branch", workerDir)

	// Stash work from *inside* the worktree about to be pruned — the case
	// the removed gate nominally existed to protect. Stashing leaves the
	// worktree clean, so no other safety gate keeps it alive.
	const stashedLine = "work stashed inside the worktree"
	if err := os.WriteFile(filepath.Join(workerDir, "inside.txt"), []byte(stashedLine+"\n"), 0o644); err != nil {
		t.Fatalf("write in-worktree file: %v", err)
	}
	runGit(t, workerDir, "add", "inside.txt")
	runGit(t, workerDir, "stash")
	if out := runGit(t, workerDir, "status", "--porcelain"); strings.TrimSpace(out) != "" {
		t.Fatalf("test setup invalid: worktree not clean after stash; status=%q", out)
	}

	cfg := &config.City{Rigs: []config.Rig{{Name: "demo", Path: rigRoot}}}
	session := beads.Bead{
		ID: "session-1",
		Metadata: map[string]string{
			"worker_dir":   workerDir,
			"template":     "demo/polecat",
			"session_name": "demo/polecat-1",
		},
	}

	var stderr bytes.Buffer
	if !pruneAgentHomeWorktreeIfSafe(session, cityPath, cfg, livePruneAllowed(), &stderr) {
		t.Fatalf("prune returned false for a clean, fully-pushed worktree; stderr=%s", stderr.String())
	}
	if _, err := os.Stat(workerDir); !os.IsNotExist(err) {
		t.Fatalf("workerDir %s should have been removed; stat err=%v", workerDir, err)
	}

	// The load-bearing assertion: the stash outlived the worktree it was
	// made in, and its contents are still recoverable from the rig root.
	if out := runGit(t, rigRoot, "stash", "list"); strings.TrimSpace(out) == "" {
		t.Fatalf("stash made inside the worktree was lost when the worktree was pruned")
	}
	if out := runGit(t, rigRoot, "stash", "show", "-p"); !strings.Contains(out, stashedLine) {
		t.Fatalf("stashed content unrecoverable after prune; `git stash show -p` =\n%s", out)
	}
}

// TestPruneAgentHomeWorktreeIfSafe_LeavesManagedWorktreeOfAnotherOwner is
// the raw form of the managed-worktree ownership gate (ga-tlus3e.22): a
// pool seat the controller started inside a work bead's verified managed
// worktree records that worktree as its work dir, and closing the seat must
// not remove a worktree whose provenance names another owner.
func TestPruneAgentHomeWorktreeIfSafe_LeavesManagedWorktreeOfAnotherOwner(t *testing.T) {
	cityPath := t.TempDir()
	rigRoot, spec := managedPruneWorktree(t, cityPath)
	cfg := &config.City{Rigs: []config.Rig{{Name: "demo", Path: rigRoot}}}
	session := beads.Bead{
		ID: "session-1",
		Metadata: map[string]string{
			"work_dir":     spec.Path,
			"gc.work_dir":  spec.Path,
			"template":     "demo/worker",
			"session_name": "demo/worker-1",
		},
	}

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(session, cityPath, cfg, livePruneAllowed(), &stderr) {
		t.Fatalf("prune returned true for a managed worktree owned by %q; stderr=%s", spec.Owner, stderr.String())
	}
	assertManagedWorktreeIntact(t, spec, stderr.String())
	if !strings.Contains(stderr.String(), "managed worktree") {
		t.Errorf("expected managed-worktree refusal log; got %q", stderr.String())
	}
}

// TestPruneAgentHomeWorktreeIfSafe_RemovesThroughTheMainWorktree pins where
// the removal runs: from the main worktree the worker_dir's own worktree list
// names first, with no rig configured and a template that names none. A list
// that cannot be read, or a worker_dir that is the main worktree, keeps the
// folder.
func TestPruneAgentHomeWorktreeIfSafe_RemovesThroughTheMainWorktree(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mainIsWD  bool
		listErr   error
		wantPrune bool
		wantLog   string
	}{
		{name: "the main worktree the list names first", wantPrune: true, wantLog: "pruned worker_dir"},
		{name: "an unreadable worktree list", listErr: errors.New("git worktree list failed"), wantLog: "cannot list its repository's worktrees"},
		{name: "a worker_dir that is its repository's main worktree", mainIsWD: true, wantLog: "its repository's main worktree"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newPruneFixture(t)
			fx.cfg.Rigs = nil
			main := filepath.Join(fx.cityPath, "repos", "main")
			list := []git.Worktree{{Path: main}, {Path: fx.workerDir}}
			if tc.mainIsWD {
				list = []git.Worktree{{Path: fx.workerDir}}
			}
			wdProbe := &fakeGitProbe{isRepo: true, worktrees: list, worktreeListErr: tc.listErr}
			fx.setProbe(fx.workerDir, wdProbe)
			mainProbe := &fakeGitProbe{isRepo: true}
			fx.setProbe(main, mainProbe)
			// A city-scoped agent's session, whose record asks for the removal.
			session := fx.sessionBead()
			session.Metadata["template"] = "helper"
			session.Metadata["session_name"] = "helper"
			session.Metadata[sessionpkg.PruneWorkerDirMetadataKey] = "true"

			var stderr bytes.Buffer
			if got := pruneAgentHomeWorktreeIfSafe(session, fx.cityPath, fx.cfg, livePruneAllowed(), &stderr); got != tc.wantPrune {
				t.Fatalf("prune returned %v, want %v; stderr=%q", got, tc.wantPrune, stderr.String())
			}
			if mainProbe.removeInvoked != tc.wantPrune || (tc.wantPrune && mainProbe.removedPath != fx.workerDir) || wdProbe.removeInvoked {
				t.Fatalf("removal: through the main worktree %v on %q, through the worker_dir %v; want %v through the main worktree alone; stderr=%q",
					mainProbe.removeInvoked, mainProbe.removedPath, wdProbe.removeInvoked, tc.wantPrune, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantLog) {
				t.Fatalf("stderr = %q, want it to say %q", stderr.String(), tc.wantLog)
			}
			assertNoWorktreeStaleMarker(t, fx.workerDir)
		})
	}
}

func TestPruneAgentHomeWorktreeIfSafe_RemoveFails(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true})
	fx.setProbe(fx.rigRoot, &fakeGitProbe{
		isRepo:         true,
		worktreeRemove: func(_ string, _ bool) error { return errors.New("locked") },
	})

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true when WorktreeRemove failed")
	}
	if !strings.Contains(stderr.String(), "pruning worker_dir") || !strings.Contains(stderr.String(), "locked") {
		t.Errorf("expected removal-error log; got %q", stderr.String())
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_HappyPath(t *testing.T) {
	fx := newPruneFixture(t)
	wdProbe := &fakeGitProbe{isRepo: true}
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.workerDir, wdProbe)
	fx.setProbe(fx.rigRoot, rigProbe)

	var stderr bytes.Buffer
	if !pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, livePruneAllowed(), &stderr) {
		t.Fatalf("prune returned false on happy path; stderr=%s", stderr.String())
	}
	if wdProbe.removeInvoked {
		t.Error("WorktreeRemove invoked on worker_dir; should be invoked on rig root only")
	}
	if !rigProbe.removeInvoked {
		t.Fatal("WorktreeRemove not invoked on rig root")
	}
	if rigProbe.removedPath != fx.workerDir {
		t.Errorf("WorktreeRemove path = %q, want %q", rigProbe.removedPath, fx.workerDir)
	}
	if !rigProbe.removedForce {
		t.Error("WorktreeRemove force flag = false, want true")
	}
	if !strings.Contains(stderr.String(), "pruned worker_dir") {
		t.Errorf("expected success log; got %q", stderr.String())
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_NilConfig(t *testing.T) {
	fx := newPruneFixture(t)
	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, nil, livePruneAllowed(), &stderr) {
		t.Fatal("prune returned true with nil cfg")
	}
}

func TestPruneAgentHomeWorktreeIfSafe_LivenessScanUnavailable(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true})
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, liveScanUnavailable(), &stderr) {
		t.Fatal("prune returned true when the liveness scan was indeterminate")
	}
	if rigProbe.removeInvoked {
		t.Error("WorktreeRemove invoked despite indeterminate liveness")
	}
	if !strings.Contains(stderr.String(), "liveness scan unavailable") {
		t.Errorf("expected fail-closed reason log; got %q", stderr.String())
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_LiveProcessCWD(t *testing.T) {
	fx := newPruneFixture(t)
	// Clean tree — the normal resting state of a healthy agent between commits.
	// Without the liveness gate this passes every git probe and the worktree is
	// force-removed out from under the running process.
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true})
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, liveAt(fx.workerDir), &stderr) {
		t.Fatal("prune returned true for a worktree with a live process cwd")
	}
	if rigProbe.removeInvoked {
		t.Error("WorktreeRemove invoked on a live worktree")
	}
	if !strings.Contains(stderr.String(), "live process cwd") {
		t.Errorf("expected live-process reason log; got %q", stderr.String())
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_LiveProcessInSubdir(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true})
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)
	subdir := filepath.Join(fx.workerDir, "internal", "build")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, liveAt(subdir), &stderr) {
		t.Fatal("prune returned true while a process runs in a subdir of the worktree")
	}
	if rigProbe.removeInvoked {
		t.Error("WorktreeRemove invoked while a process runs under the worktree")
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_OwnRecordedSessionDirDoesNotBlockSelf(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true})
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)

	// The reconciler gathers the active-session set from the snapshot taken at
	// tick start, which still lists the session being pruned as open — so its
	// own worker_dir appears in sessionDirs. That self-reference must not read
	// as a live signal against its own worktree, or a clean retired pool
	// worktree is never reclaimed.
	var stderr bytes.Buffer
	if !pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, liveViaSession(fx.sessionBead().ID, fx.workerDir), &stderr) {
		t.Fatalf("prune skipped a clean worktree whose only session-dir signal was its own retired session; log: %q", stderr.String())
	}
	if !rigProbe.removeInvoked {
		t.Error("WorktreeRemove not invoked for a prunable worktree")
	}
}

func TestPruneAgentHomeWorktreeIfSafe_OtherSessionNestedDirBlocks(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true})
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)

	// A different open session records a directory strictly beneath the
	// worktree (a nested tree it is working in). That is not the retired
	// session's own dir, so it is not self-filtered and still blocks removal.
	nested := filepath.Join(fx.workerDir, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, liveViaSession("session-2", nested), &stderr) {
		t.Fatal("prune returned true while a different open session works beneath the worktree")
	}
	if rigProbe.removeInvoked {
		t.Error("WorktreeRemove invoked while a different open session works beneath the worktree")
	}
	if !strings.Contains(stderr.String(), "active session dir") {
		t.Errorf("expected active-session reason log; got %q", stderr.String())
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_OtherSessionSameDirBlocks(t *testing.T) {
	fx := newPruneFixture(t)
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true})
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)

	// A different open session records the retired session's worker_dir
	// itself, as the next session in the same pool slot does. Only the
	// retired session's own entries are dropped, so the other session's
	// identical entry still blocks removal.
	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, liveViaSession("session-2", fx.workerDir), &stderr) {
		t.Fatal("prune returned true while a different open session records the same worktree")
	}
	if rigProbe.removeInvoked {
		t.Error("WorktreeRemove invoked while a different open session records the same worktree")
	}
	if !strings.Contains(stderr.String(), "active session dir") {
		t.Errorf("expected active-session reason log; got %q", stderr.String())
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

func TestPruneAgentHomeWorktreeIfSafe_LiveTakesPrecedenceOverGitState(t *testing.T) {
	fx := newPruneFixture(t)
	// Uncommitted work AND a live process. Liveness is checked first, so the
	// worktree is refused as live and no .worktree-stale marker is written into
	// the tree the live process is using.
	fx.setProbe(fx.workerDir, &fakeGitProbe{isRepo: true, hasUncommitted: true, currentBranch: "builder/ga-live1"})
	rigProbe := &fakeGitProbe{isRepo: true}
	fx.setProbe(fx.rigRoot, rigProbe)

	var stderr bytes.Buffer
	if pruneAgentHomeWorktreeIfSafe(fx.sessionBead(), fx.cityPath, fx.cfg, liveAt(fx.workerDir), &stderr) {
		t.Fatal("prune returned true for a live worktree")
	}
	if !strings.Contains(stderr.String(), "live process cwd") {
		t.Errorf("expected live reason to win over git-state; got %q", stderr.String())
	}
	if strings.Contains(stderr.String(), "uncommitted") {
		t.Errorf("git-state probe ran before liveness; got %q", stderr.String())
	}
	assertNoWorktreeStaleMarker(t, fx.workerDir)
}

// cliPruneCity is a city for the worker_dir prune of the session close and
// prune commands: rig "demo" is a real repository whose HEAD is pushed to its
// origin and which ignores the worktrees folders made inside its worktrees,
// and agent demo/helper sets nothing. Its worktrees live under the city's
// worktrees root, so every git safety gate of the prune passes on a clean one.
type cliPruneCity struct {
	t        *testing.T
	cityDir  string
	rigRoot  string
	helperWT string // the folder each helper session's worktree sits under
	store    beads.Store
}

func newCLIPruneCity(t *testing.T) *cliPruneCity {
	t.Helper()
	origScan := collectLiveWorktreeStateFn
	collectLiveWorktreeStateFn = func() liveWorktreeState { return liveWorktreeState{scanned: true} }
	t.Cleanup(func() { collectLiveWorktreeStateFn = origScan })

	c := &cliPruneCity{t: t, cityDir: t.TempDir(), rigRoot: filepath.Join(t.TempDir(), "demo")}
	c.helperWT = filepath.Join(c.cityDir, ".gc", "worktrees", "demo", "helper")
	bare := t.TempDir()
	runGit(t, bare, "init", "--bare")
	if err := os.MkdirAll(c.rigRoot, 0o755); err != nil {
		t.Fatalf("mkdir rig root: %v", err)
	}
	runGit(t, c.rigRoot, "clone", bare, ".")
	runGit(t, c.rigRoot, "config", "user.email", "test@test.com")
	runGit(t, c.rigRoot, "config", "user.name", "Test")
	runGit(t, c.rigRoot, "commit", "--allow-empty", "-m", "init")
	runGit(t, c.rigRoot, "push", "origin", "HEAD")
	if err := os.WriteFile(filepath.Join(c.rigRoot, ".git", "info", "exclude"), []byte("worktrees/\n"), 0o644); err != nil {
		t.Fatalf("write info/exclude: %v", err)
	}
	writePhase0InterfaceCity(t, c.cityDir, fmt.Sprintf(`[workspace]
name = "test-city"

[beads]
provider = "file"

[[rigs]]
name = "demo"
path = %q

[[agent]]
name = "helper"
dir = "demo"
start_command = "true"
`, c.rigRoot))
	t.Setenv("GC_CITY", c.cityDir)
	t.Setenv("GC_DIR", t.TempDir())
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_SESSION", "fake")
	store, err := openCityStoreAt(c.cityDir)
	if err != nil {
		t.Fatalf("openCityStoreAt: %v", err)
	}
	c.store = store
	return c
}

// worktree adds a detached worktree of the rig at path.
func (c *cliPruneCity) worktree(path string) string {
	c.t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		c.t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	runGit(c.t, c.rigRoot, "worktree", "add", "--detach", path)
	return path
}

// session creates a manual helper session working in dir, with meta added.
func (c *cliPruneCity) session(name, dir string, meta map[string]string) beads.Bead {
	c.t.Helper()
	return c.sessionIn(c.store, name, dir, meta)
}

// sessionIn is session, written through store.
func (c *cliPruneCity) sessionIn(store beads.Store, name, dir string, meta map[string]string) beads.Bead {
	c.t.Helper()
	m := map[string]string{"session_name": name, "template": "demo/helper", "session_origin": "manual", "work_dir": dir}
	maps.Copy(m, meta)
	b, err := store.Create(beads.Bead{Title: name, Type: sessionpkg.BeadType, Labels: []string{sessionpkg.LabelSession}, Metadata: m})
	if err != nil {
		c.t.Fatalf("create session %s: %v", name, err)
	}
	return b
}

// assertFolder fails unless dir exists exactly when want says it stays.
func assertFolder(t *testing.T, dir string, stays bool, stderr string) {
	t.Helper()
	_, err := os.Stat(dir)
	if gone := errors.Is(err, os.ErrNotExist); gone == stays {
		t.Fatalf("%s gone = %v, want stays = %v; stderr=%q", dir, gone, stays, stderr)
	}
}

// TestPruneClosedRowWorkerDirKeepsAFolderAnotherOpenSessionRecords runs the
// legacy reconciler's prune of a row it closed (a failed create, a dead
// named-session phantom, an orphan) with the liveness its pass gathers from
// the snapshot taken before the close: a folder another open session records
// stays, and the same folder with the closing session alone in the snapshot
// goes.
func TestPruneClosedRowWorkerDirKeepsAFolderAnotherOpenSessionRecords(t *testing.T) {
	asks := map[string]string{sessionpkg.PruneWorkerDirMetadataKey: "true"}
	for _, tc := range []struct {
		name   string
		shared bool
	}{
		{"another open session records the folder", true},
		{"the closing session alone records it", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCLIPruneCity(t)
			dir := c.worktree(filepath.Join(c.helperWT, "shared"))
			closing := c.session("helper-old", dir, asks)
			open := []sessionpkg.Info{sessionInfoFromBead(closing)}
			if tc.shared {
				open = append(open, sessionInfoFromBead(c.session("helper-new", dir, nil)))
			}
			pass := worktreeLivenessInputs{live: liveWorktreeState{scanned: true}, sessionDirs: openSessionWorkDirs(open)}
			if err := c.store.Close(closing.ID); err != nil {
				t.Fatalf("Close(%s): %v", closing.ID, err)
			}
			cfg, err := loadCityConfig(c.cityDir)
			if err != nil {
				t.Fatalf("loadCityConfig: %v", err)
			}
			var stderr bytes.Buffer
			pruneClosedRowWorkerDir(sessionFrontDoor(c.store), closing.ID, c.cityDir, cfg, func() worktreeLivenessInputs { return pass }, &stderr)
			assertFolder(t, dir, tc.shared, stderr.String())
		})
	}
}

// TestControllerStateReclaimClosedSessionsPrunesAsTheCLIDoes pins the API's
// side of the worker_dir prune: the API's worker factories hand the rows an API
// close closed to controllerState.ReclaimClosedSessions, which runs the prune
// gc session close's session manager runs (closedSessionWorkerDirReclaim). A
// closed session whose record asks for it loses its clean folder, one whose
// folder holds uncommitted changes keeps it, and so does one whose agent sets
// nothing. The sessions are written through the controller's sessions store,
// where the API's closes write them.
func TestControllerStateReclaimClosedSessionsPrunesAsTheCLIDoes(t *testing.T) {
	c := newCLIPruneCity(t)
	cfg, err := loadCityConfig(c.cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	cs := &controllerState{cityPath: c.cityDir, cityBeadStore: c.store, cfg: cfg}
	sessions := cs.Stores().Sessions().Store
	asks := map[string]string{sessionpkg.PruneWorkerDirMetadataKey: "true"}
	clean := c.worktree(filepath.Join(c.helperWT, "clean"))
	dirty := c.worktree(filepath.Join(c.helperWT, "dirty"))
	if err := os.WriteFile(filepath.Join(dirty, "draft.txt"), []byte("unsaved\n"), 0o644); err != nil {
		t.Fatalf("write draft: %v", err)
	}
	unset := c.worktree(filepath.Join(c.helperWT, "unset"))
	var ids []string
	for _, s := range []struct {
		dir  string
		meta map[string]string
	}{{clean, asks}, {dirty, asks}, {unset, nil}} {
		b := c.sessionIn(sessions, "helper-"+filepath.Base(s.dir), s.dir, s.meta)
		if err := sessions.Close(b.ID); err != nil {
			t.Fatalf("Close(%s): %v", b.ID, err)
		}
		ids = append(ids, b.ID)
	}
	cs.ReclaimClosedSessions(ids)
	assertFolder(t, clean, false, "")
	assertFolder(t, dirty, true, "")
	assertFolder(t, unset, true, "")
}

// TestPruneClosedSessionWorkerDirsKeepsAnotherRepositorysDirtyWorktree runs
// the prune over a real clean and pushed worktree of the rig whose ignored
// worktrees/ folder holds a worktree of another repository, which the rig's
// repository does not list: with uncommitted changes in it, the folder stays
// and so do the changes, clean, the folder goes, and clean but provisioned by
// gc worktree ensure, whose path check reads only its own repository's
// worktrees, the folder stays and the provisioned worktree stays intact.
func TestPruneClosedSessionWorkerDirsKeepsAnotherRepositorysDirtyWorktree(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dirty   bool
		managed bool
		stays   bool
	}{
		{name: "with uncommitted changes", dirty: true, stays: true},
		{name: "clean"},
		{name: "clean and provisioned by gc worktree ensure", managed: true, stays: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCLIPruneCity(t)
			dir := c.worktree(filepath.Join(c.helperWT, "outer"))
			bare := t.TempDir()
			runGit(t, bare, "init", "--bare")
			other := t.TempDir()
			runGit(t, other, "clone", bare, ".")
			runGit(t, other, "config", "user.email", "test@test.com")
			runGit(t, other, "config", "user.name", "Test")
			runGit(t, other, "commit", "--allow-empty", "-m", "init")
			runGit(t, other, "push", "origin", "HEAD")
			root := filepath.Join(dir, "worktrees")
			if err := os.MkdirAll(root, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", root, err)
			}
			nested := filepath.Join(root, "other-change")
			var spec worktree.Spec
			if tc.managed {
				spec = worktree.Spec{
					RepoDir: other, Root: root, Path: nested, Branch: "work/other-change",
					Base:   strings.TrimSpace(runGit(t, other, "rev-parse", "--abbrev-ref", "HEAD")),
					BeadID: "gc-work", StoreRef: "rig:other", Creator: "provisioner", Owner: "provisioner",
					Generation: "g1", Lifecycle: worktree.LifecycleActive,
				}
				report, err := worktree.Ensure(spec)
				if err != nil {
					t.Fatalf("Ensure managed worktree: %v", err)
				}
				spec.BaseSHA = report.Provenance.BaseSHA
			} else {
				runGit(t, other, "worktree", "add", "--detach", nested)
			}
			draft := filepath.Join(nested, "draft.txt")
			if tc.dirty {
				if err := os.WriteFile(draft, []byte("unsaved\n"), 0o644); err != nil {
					t.Fatalf("write draft: %v", err)
				}
			}
			row := c.session("helper-outer", dir, map[string]string{sessionpkg.PruneWorkerDirMetadataKey: "true"})
			if err := c.store.Close(row.ID); err != nil {
				t.Fatalf("Close(%s): %v", row.ID, err)
			}
			cfg, err := loadCityConfig(c.cityDir)
			if err != nil {
				t.Fatalf("loadCityConfig: %v", err)
			}

			var stderr bytes.Buffer
			pruneClosedSessionWorkerDirs(c.cityDir, cfg, sessionFrontDoor(c.store), []string{row.ID}, &stderr)
			assertFolder(t, dir, tc.stays, stderr.String())
			if tc.dirty {
				if data, err := os.ReadFile(draft); err != nil || string(data) != "unsaved\n" {
					t.Fatalf("the nested worktree's change = %q, %v; want it kept; stderr=%q", data, err, stderr.String())
				}
			}
			if tc.managed {
				assertManagedWorktreeIntact(t, spec, stderr.String())
			}
		})
	}
}

// TestPruneClosedSessionWorkerDirsRemovesACityScopedSessionsFolder runs the
// prune of a closed session of a city-scoped agent, whose template names no
// rig, over a real clean and pushed worktree whose record asks for the
// removal: the folder goes, through the repository the worktree belongs to,
// which no longer lists it.
func TestPruneClosedSessionWorkerDirsRemovesACityScopedSessionsFolder(t *testing.T) {
	c := newCLIPruneCity(t)
	dir := c.worktree(filepath.Join(c.helperWT, "city-helper"))
	row := c.session("city-helper", dir, map[string]string{"template": "helper", sessionpkg.PruneWorkerDirMetadataKey: "true"})
	if err := c.store.Close(row.ID); err != nil {
		t.Fatalf("Close(%s): %v", row.ID, err)
	}
	cfg, err := loadCityConfig(c.cityDir)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}
	cfg.Agents = []config.Agent{{Name: "helper"}}

	var stderr bytes.Buffer
	pruneClosedSessionWorkerDirs(c.cityDir, cfg, sessionFrontDoor(c.store), []string{row.ID}, &stderr)
	assertFolder(t, dir, false, stderr.String())
	if list := runGit(t, c.rigRoot, "worktree", "list", "--porcelain"); strings.Contains(list, filepath.Base(dir)) {
		t.Fatalf("the repository still lists the removed worktree:\n%s", list)
	}
}
