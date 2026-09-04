package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/contract"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/git"
	"github.com/gastownhall/gascity/internal/pathutil"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
	"github.com/gastownhall/gascity/internal/workdir"
	"github.com/gastownhall/gascity/internal/worktree"
)

// gitProbe is the slice of internal/git.Git used by the worker-dir
// auto-prune path. Defined as an interface so tests can inject a fake
// without standing up real git worktrees.
type gitProbe interface {
	IsRepo() bool
	GitDir() (string, error)
	CurrentBranch() (string, error)
	HasUncommittedWork() bool
	HasUnpushedCommitsResult() (bool, error)
	WorktreeRemove(path string, force bool) error
}

// newGitProbe returns a gitProbe scoped to the given directory. Indirected
// through a package-level var so tests can stub the git invocations.
var newGitProbe = func(workDir string) gitProbe { return git.New(workDir) }

// managedWorktreeRefusal reports why the prune must leave the worktree gp
// probes, or "" when the worktree is the closing session's own. A worktree
// whose private git directory holds the provenance a completed
// `gc worktree ensure` published is a managed worktree: the provenance names
// the one owner that provisioned it, and only that owner removes it, through
// `gc worktree cleanup` with the attempt id that created it. A closing
// session is never that owner, even when the controller started it inside the
// worktree because its work bead names one (verifiedPoolTriggerWorkDir). Only
// a record that does not exist makes the worktree the session's; a git dir or
// record that cannot be read keeps the worktree.
func managedWorktreeRefusal(gp gitProbe) string {
	gitDir, err := gp.GitDir()
	if err != nil {
		return fmt.Sprintf("cannot read its worktree provenance: %v", err)
	}
	provenance, err := worktree.ReadProvenanceAt(gitDir)
	if errors.Is(err, fs.ErrNotExist) {
		return ""
	}
	if err != nil {
		return fmt.Sprintf("cannot read its worktree provenance: %v", err)
	}
	return fmt.Sprintf("managed worktree of bead %s owned by %q; only its owner removes it (gc worktree cleanup)", provenance.BeadID, provenance.Owner)
}

// worktreeLivenessInputs bundles the liveness signals the worker_dir auto-prune
// consults before removing a closed session's worktree: the authoritative
// process-table scan (live) and the recorded working directories of every open
// session (sessionDirs). The reconciler gathers both once per pass and shares
// the same value across every prune decision in that pass, matching the
// closed-bead reaper's own once-per-pass gather — the process-table walk is far
// more expensive than the per-worktree git probes.
type worktreeLivenessInputs struct {
	live        liveWorktreeState
	sessionDirs []string
}

// worktreeLivenessBlocksPrune reports whether the pass's liveness snapshot
// forbids removing workerDir, logging the reason to stderr when it does. It is
// the shared gate both prune forms consult before the git-state probes, so a
// clean tree — the normal resting state of a healthy agent between commits —
// can no longer read as prunable while a process is running in it.
//
// selfSessionDirs are the retired session's own recorded worktree dirs. They
// are dropped from the active-session cross-check before it runs: the pass
// gathers sessionDirs from the snapshot taken at tick start, which still lists
// the session being pruned as open, so its own recorded directory would
// otherwise read as a live signal against its own worktree and no clean pool
// worktree would ever be reclaimed. The /proc cwd scan is consulted unchanged —
// a process genuinely still running in the tree (a drain in flight, a crash
// adoption, a reaped-but-live runtime) is a real liveness signal and still
// blocks — so only the recorded-metadata signal is self-filtered.
//
// It fails closed: an indeterminate scan (scanned=false) blocks removal,
// because a scan that observed nothing running is indistinguishable from one
// that could not run at all. A live process cwd, or a different open session
// recorded as working at or beneath workerDir, also blocks. This is the same
// shared boundary (bead_worktree_liveness.go) the closed-bead reaper and the
// doctor prune check read; liveness is a fact about which directory a running
// process occupies and names no role.
//
// No .worktree-stale marker is written for a liveness block: unlike the
// git-state gates, liveness is transient and self-resolving once the process
// exits, and writing into a tree a process is actively using would race that
// process.
func worktreeLivenessBlocksPrune(workerDir string, liveness worktreeLivenessInputs, selfSessionDirs []string, stderr io.Writer) bool {
	if !liveness.live.scanned {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: liveness scan unavailable (failing closed)\n", workerDir) //nolint:errcheck
		return true
	}
	sessionDirs := excludeNormalizedDirs(liveness.sessionDirs, selfSessionDirs)
	if isLive, why := worktreeIsLive(workerDir, liveness.live, sessionDirs); isLive {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: %s\n", workerDir, why) //nolint:errcheck
		return true
	}
	return false
}

// excludeNormalizedDirs returns the entries of dirs whose normalized path does
// not match any normalized entry of exclude. It is how the worker_dir auto-prune
// drops the retired session's own recorded dirs from the active-session liveness
// set before the cross-check compares it against that session's worker_dir.
// Comparison is on pathutil.NormalizePathForCompare so it matches worktreeIsLive's
// own normalization; a dir a different session recorded strictly beneath the
// worktree is not an exact match, so it survives and still blocks. Empty exclude
// entries never match.
func excludeNormalizedDirs(dirs, exclude []string) []string {
	if len(dirs) == 0 || len(exclude) == 0 {
		return dirs
	}
	skip := make(map[string]struct{}, len(exclude))
	for _, e := range exclude {
		if n := pathutil.NormalizePathForCompare(e); n != "" {
			skip[n] = struct{}{}
		}
	}
	if len(skip) == 0 {
		return dirs
	}
	out := make([]string, 0, len(dirs))
	for _, d := range dirs {
		if _, ok := skip[pathutil.NormalizePathForCompare(d)]; ok {
			continue
		}
		out = append(out, d)
	}
	return out
}

// writeWorktreeStaleMarker records why workerDir was left in place instead of
// pruned, so cleanupClosedBeadAgentHomeWorktrees (agent_home_worktree_cleanup.go)
// can later detect when it's safe to reclaim. Best-effort: write failures are
// logged but never alter the caller's control flow.
func writeWorktreeStaleMarker(gp gitProbe, workerDir, reason string, stderr io.Writer) {
	branch, err := gp.CurrentBranch()
	if err != nil {
		branch = ""
	}
	content := fmt.Sprintf("branch=%s\nreason=%s\n", branch, reason)
	if err := os.WriteFile(filepath.Join(workerDir, worktreeStaleFileName), []byte(content), 0o644); err != nil {
		fmt.Fprintf(stderr, "session reconciler: writing %s marker for %s: %v\n", worktreeStaleFileName, workerDir, err) //nolint:errcheck
	}
}

// pruneAgentHomeWorktreeIfSafe removes the worktree at the closed session's
// worker_dir, after applying doctor's NestedWorktreePruneCheck safety gates
// minus its repo-global stash probe (refs/stash is shared across worktrees
// and survives `git worktree remove`; ga-pyp2oh), plus the liveness gate, so
// a closed session whose process is still running (a drain in flight, a crash
// adoption, a reaped-but-live runtime) does not have its worker_dir
// force-removed out from under it. Returns true when the removal actually
// happened.
//
// The decision is mechanical, never role-coupled: any pool-managed agent
// worktree that lives under the configured worktrees root
// (workdir.WorktreesRoot), is a git worktree no provisioner owns, has nothing
// live working in it, and probes clean is safe to reclaim. Pool sessions are
// transient by design — their own worktrees were never meant to outlive the
// session bead.
//
// liveness is the pass's once-gathered liveness snapshot (the process-table
// cwd scan plus the open sessions' recorded working directories); the caller
// gathers it once and shares it across every prune decision in the pass.
//
// No-op when:
//   - cfg.Daemon.AutoPruneWorkerDir is false
//   - the session bead has no worker_dir metadata
//   - the worker_dir does not live under the configured worktrees root
//     (workdir.WorktreesRoot)
//   - the worker_dir is missing on disk or has no .git pointer
//   - the worker_dir is a managed worktree, or its provenance cannot be read
//     (managedWorktreeRefusal)
//   - a live process, or a different open session, is working at or beneath the
//     worker_dir, or the liveness scan is indeterminate (fails closed)
//   - the worktree has uncommitted changes or unpushed commits
//   - the rig that owns the session cannot be resolved to a filesystem path
//
// Removal failures are logged but never surfaced — an orphaned worktree
// still shows up via `gc doctor` later, which is the operator's existing
// reclaim path.
func pruneAgentHomeWorktreeIfSafe(session beads.Bead, cityPath string, cfg *config.City, liveness worktreeLivenessInputs, stderr io.Writer) bool {
	if cfg == nil || !cfg.Daemon.AutoPruneWorkerDirEnabled() {
		return false
	}
	workerDir := strings.TrimSpace(contract.WorkerDirFromMetadata(session.Metadata))
	if workerDir == "" {
		return false
	}
	if !filepath.IsAbs(workerDir) {
		return false
	}

	wtRoot := workdir.WorktreesRoot(cityPath)
	if !pathutil.PathWithin(wtRoot, workerDir) || pathutil.SamePath(wtRoot, workerDir) {
		return false
	}

	if _, err := os.Stat(filepath.Join(workerDir, ".git")); err != nil {
		// Already gone, or never a worktree — nothing to do.
		return false
	}

	gp := newGitProbe(workerDir)
	if !gp.IsRepo() {
		return false
	}
	if reason := managedWorktreeRefusal(gp); reason != "" {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: %s\n", workerDir, reason) //nolint:errcheck
		return false
	}
	// Liveness before the git-state probes: a clean tree is the normal resting
	// state of a healthy agent between commits, so those probes alone cannot
	// see a live worktree, and checking first also avoids writing a stale
	// marker into a tree a process is still using.
	if worktreeLivenessBlocksPrune(workerDir, liveness, []string{workerDir}, stderr) {
		return false
	}
	if gp.HasUncommittedWork() {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: has uncommitted changes\n", workerDir) //nolint:errcheck
		writeWorktreeStaleMarker(gp, workerDir, "uncommitted-work", stderr)
		return false
	}
	hasUnpushed, err := gp.HasUnpushedCommitsResult()
	if err != nil {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: unpushed probe failed: %v\n", workerDir, err) //nolint:errcheck
		return false
	}
	if hasUnpushed {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: has unpushed commits\n", workerDir) //nolint:errcheck
		writeWorktreeStaleMarker(gp, workerDir, "unpushed-commits", stderr)
		return false
	}

	// Run `git worktree remove` from the rig root rather than from the
	// worktree being removed: git refuses to remove a worktree whose path
	// equals cwd in some configurations, and operating from cwd of a
	// directory we are about to delete is fragile in general.
	rigRoot := lookupRigRootForSession(session, cfg)
	if rigRoot == "" {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: rig path unresolved\n", workerDir) //nolint:errcheck
		return false
	}
	if err := newGitProbe(rigRoot).WorktreeRemove(workerDir, true); err != nil {
		fmt.Fprintf(stderr, "session reconciler: pruning worker_dir %s: %v\n", workerDir, err) //nolint:errcheck
		return false
	}
	fmt.Fprintf(stderr, "session reconciler: pruned worker_dir %s (session %s)\n", workerDir, session.Metadata["session_name"]) //nolint:errcheck
	return true
}

// pruneAgentHomeWorktreeIfSafeInfo is the session.Info form of
// pruneAgentHomeWorktreeIfSafe: the worker_dir read routes through
// session.WorkerDirFromInfo (the canonical→legacy Info fallback equivalent to
// contract.WorkerDirFromMetadata), the rig-root lookup reads Info.Template via
// lookupRigRootForSessionInfo, and the log line reads Info.SessionNameMetadata —
// every safety gate (including the shared liveness gate) and the removal itself
// are unchanged. Byte-identical to the raw form, which survives for its test
// callers.
func pruneAgentHomeWorktreeIfSafeInfo(info sessionpkg.Info, cityPath string, cfg *config.City, liveness worktreeLivenessInputs, stderr io.Writer) {
	if cfg == nil || !cfg.Daemon.AutoPruneWorkerDirEnabled() {
		return
	}
	workerDir := strings.TrimSpace(sessionpkg.WorkerDirFromInfo(info))
	if workerDir == "" {
		return
	}
	if !filepath.IsAbs(workerDir) {
		return
	}

	wtRoot := workdir.WorktreesRoot(cityPath)
	if !pathutil.PathWithin(wtRoot, workerDir) || pathutil.SamePath(wtRoot, workerDir) {
		return
	}

	if _, err := os.Stat(filepath.Join(workerDir, ".git")); err != nil {
		return
	}

	gp := newGitProbe(workerDir)
	if !gp.IsRepo() {
		return
	}
	if reason := managedWorktreeRefusal(gp); reason != "" {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: %s\n", workerDir, reason) //nolint:errcheck
		return
	}
	// Liveness before the git-state probes: a clean tree is the normal resting
	// state of a healthy agent between commits, so those probes alone cannot
	// see a live worktree, and checking first also avoids writing a stale
	// marker into a tree a process is still using.
	if worktreeLivenessBlocksPrune(workerDir, liveness, sessionRecordedWorktreeDirs(info), stderr) {
		return
	}
	if gp.HasUncommittedWork() {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: has uncommitted changes\n", workerDir) //nolint:errcheck
		writeWorktreeStaleMarker(gp, workerDir, "uncommitted-work", stderr)
		return
	}
	hasUnpushed, err := gp.HasUnpushedCommitsResult()
	if err != nil {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: unpushed probe failed: %v\n", workerDir, err) //nolint:errcheck
		return
	}
	if hasUnpushed {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: has unpushed commits\n", workerDir) //nolint:errcheck
		writeWorktreeStaleMarker(gp, workerDir, "unpushed-commits", stderr)
		return
	}

	rigRoot := lookupRigRootForSessionInfo(info, cfg)
	if rigRoot == "" {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: rig path unresolved\n", workerDir) //nolint:errcheck
		return
	}
	if err := newGitProbe(rigRoot).WorktreeRemove(workerDir, true); err != nil {
		fmt.Fprintf(stderr, "session reconciler: pruning worker_dir %s: %v\n", workerDir, err) //nolint:errcheck
		return
	}
	fmt.Fprintf(stderr, "session reconciler: pruned worker_dir %s (session %s)\n", workerDir, info.SessionNameMetadata) //nolint:errcheck
}

// lookupRigRootForSession returns the filesystem path of the rig that owns
// the given session bead, derived from the qualified template metadata
// ("<rig>/<template>"). Returns "" when the rig cannot be identified or
// has no configured path.
func lookupRigRootForSession(session beads.Bead, cfg *config.City) string {
	qt := strings.TrimSpace(session.Metadata["template"])
	slash := strings.IndexByte(qt, '/')
	if slash <= 0 {
		return ""
	}
	rigName := qt[:slash]
	for i := range cfg.Rigs {
		if cfg.Rigs[i].Name == rigName {
			return strings.TrimSpace(cfg.Rigs[i].Path)
		}
	}
	return ""
}

// lookupRigRootForSessionInfo is the session.Info form of
// lookupRigRootForSession: it reads the qualified template off Info.Template (the
// verbatim raw mirror of b.Metadata["template"]), so the rig resolution is
// byte-identical to the raw form.
func lookupRigRootForSessionInfo(info sessionpkg.Info, cfg *config.City) string {
	qt := strings.TrimSpace(info.Template)
	slash := strings.IndexByte(qt, '/')
	if slash <= 0 {
		return ""
	}
	rigName := qt[:slash]
	for i := range cfg.Rigs {
		if cfg.Rigs[i].Name == rigName {
			return strings.TrimSpace(cfg.Rigs[i].Path)
		}
	}
	return ""
}
