package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

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
	WorktreeList() ([]git.Worktree, error)
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
// process-table scan (live) and the working directories open sessions record,
// each with the session that records it (sessionDirs). The reconciler gathers
// both once per pass and shares the same value across every prune decision in
// that pass, matching the closed-bead reaper's own once-per-pass gather — the
// process-table walk is far more expensive than the per-worktree git probes.
type worktreeLivenessInputs struct {
	live        liveWorktreeState
	sessionDirs []sessionWorkDir
}

// sessionWorkDir is one working directory an open session row records, with
// that row's id: the prune of a closed row drops the entries of that row alone,
// so another open row recording the same directory still blocks.
type sessionWorkDir struct {
	sessionID string
	dir       string
}

// openSessionWorkDirs lists the absolute working directories each of infos
// records (sessionRecordedWorktreeDirs), each with its session's id: the
// active-session side of the worker_dir prune's liveness inputs.
func openSessionWorkDirs(infos []sessionpkg.Info) []sessionWorkDir {
	var out []sessionWorkDir
	for _, info := range infos {
		var seen []string
		for _, dir := range sessionRecordedWorktreeDirs(info) {
			if dir = strings.TrimSpace(dir); dir == "" || !filepath.IsAbs(dir) || slices.Contains(seen, dir) {
				continue
			}
			seen = append(seen, dir)
			out = append(out, sessionWorkDir{sessionID: info.ID, dir: dir})
		}
	}
	return out
}

// worktreeLivenessBlocksPrune reports whether the liveness snapshot forbids
// removing workerDir, the worktree of the closed session selfID, logging the
// reason to stderr when it does. It is the shared gate both prune forms consult
// before the git-state probes, so a clean tree — the normal resting state of a
// healthy agent between commits — can no longer read as prunable while a
// process is running in it.
//
// The directories selfID records are dropped from the active-session
// cross-check before it runs: a pass gathers sessionDirs from the snapshot
// taken at tick start, which still lists the session being pruned as open, so
// its own recorded directory would otherwise read as a live signal against its
// own worktree and no clean pool worktree would ever be reclaimed. Only
// selfID's entries are dropped: another open session recording the same
// directory, such as the next session in the same pool slot, still blocks. The
// /proc cwd scan is consulted unchanged — a process genuinely still running in
// the tree (a drain in flight, a crash adoption, a reaped-but-live runtime) is a
// real liveness signal and still blocks.
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
func worktreeLivenessBlocksPrune(workerDir string, liveness worktreeLivenessInputs, selfID string, stderr io.Writer) bool {
	if !liveness.live.scanned {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: liveness scan unavailable (failing closed)\n", workerDir) //nolint:errcheck
		return true
	}
	var sessionDirs []string
	for _, d := range liveness.sessionDirs {
		if d.sessionID != selfID {
			sessionDirs = append(sessionDirs, d.dir)
		}
	}
	if isLive, why := worktreeIsLive(workerDir, liveness.live, sessionDirs); isLive {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: %s\n", workerDir, why) //nolint:errcheck
		return true
	}
	return false
}

// sessionRecordPrunesWorkerDir reports whether the closing session's worker_dir
// is removed, as sessionpkg.PrunesWorkerDirAtClose decides from the facts it
// gathers: the session's record, its agent's prune_worker_dir, and whether the
// session is pool-managed. The agent is resolved as the session was built
// (sessionWakeResolveAgentInfo), which finds a rig's stamped copy of a
// scope = "rig" agent; a template the config no longer has sets nothing. A
// record that does not parse keeps the folder, and says so on stderr.
func sessionRecordPrunesWorkerDir(info sessionpkg.Info, workerDir string, cfg *config.City, stderr io.Writer) bool {
	var agentSetting *bool
	if agent := sessionWakeResolveAgentInfo(info, cfg); agent != nil {
		agentSetting = agent.PruneWorkerDir
	}
	prune, err := sessionpkg.PrunesWorkerDirAtClose(info, agentSetting, isPoolManagedSessionInfo(info))
	if err != nil {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: session %s records %v\n", workerDir, info.ID, err) //nolint:errcheck
		return false
	}
	return prune
}

// nestedCheckoutRefusal reports why workerDir must stay because a repository
// or a checkout beneath it holds work that removing the folder would lose, or
// "" when none does. `git worktree remove --force` deletes everything under
// the folder, ignored paths included, and the folder's own status cannot see
// what an ignored path holds, so the walk reads every directory beneath the
// folder, whichever repository it belongs to.
//
// A repository stored beneath the folder keeps it, because removing the folder
// deletes that repository with every commit, branch and stash in it. The walk
// knows a repository by its git directory, whatever its name (isGitDir): a
// clone's .git, a bare clone, or the git directory a checkout's .git file
// points at.
//
// A worktree (a `.git` file) keeps the folder while it has uncommitted
// changes, or while its state cannot be read. Its commits live in its
// repository, which keeps the folder itself when it is stored beneath it. A
// worktree `gc worktree ensure` provisioned keeps the folder too, as
// managedWorktreeRefusal keeps the folder itself, because only its owner
// removes it, and the provisioner refuses a path inside a registered worktree
// of its own repository only. A session started by hand can cut a change's
// worktree, of any repository, inside its own folder.
//
// A directory the walk cannot read keeps the folder. Ignored files that belong
// to no repository, such as build output and caches, go with it.
func nestedCheckoutRefusal(workerDir string) string {
	root := filepath.Clean(workerDir)
	var refusal string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		if d.IsDir() {
			if path == filepath.Join(root, ".git") {
				return fs.SkipDir // the folder's own repository
			}
			repo, err := isGitDir(path)
			if err != nil {
				return err
			}
			if repo {
				refusal = fmt.Sprintf("repository %s beneath it would be deleted with every commit in it", path)
				return fs.SkipAll
			}
			return nil
		}
		checkout := filepath.Dir(path)
		if d.Name() != ".git" || checkout == root {
			return nil
		}
		gp := newGitProbe(checkout)
		if reason := managedWorktreeRefusal(gp); reason != "" {
			refusal = fmt.Sprintf("worktree %s beneath it: %s", checkout, reason)
			return fs.SkipAll
		}
		if gp.HasUncommittedWork() {
			refusal = fmt.Sprintf("checkout %s beneath it has uncommitted changes, or its state cannot be read", checkout)
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return fmt.Sprintf("cannot read every checkout beneath it: %v", err)
	}
	return refusal
}

// isGitDir reports whether dir is a repository's git directory: one named
// .git, or one that carries HEAD beside an objects directory, as a bare
// repository does, whatever its name. A probe that fails for any reason but a
// missing entry returns its error.
func isGitDir(dir string) (bool, error) {
	if filepath.Base(dir) == ".git" {
		return true, nil
	}
	head, err := os.Lstat(filepath.Join(dir, "HEAD"))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if head.IsDir() {
		return false, nil
	}
	objects, err := os.Stat(filepath.Join(dir, "objects"))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return objects.IsDir(), nil
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
// The decision is mechanical, never role-coupled: when the session's record
// asks for it (sessionRecordPrunesWorkerDir), a worktree that lives under the
// configured worktrees root (workdir.WorktreesRoot), is a git worktree no
// provisioner owns, has nothing live working in it, and probes clean is safe
// to reclaim, whichever rig the session's template names, or none. The removal
// runs through the repository the gates probed (worktreeRemovalRoot).
//
// liveness is the pass's once-gathered liveness snapshot (the process-table
// cwd scan plus the open sessions' recorded working directories); the caller
// gathers it once and shares it across every prune decision in the pass.
//
// No-op when:
//   - cfg.Daemon.AutoPruneWorkerDir is false
//   - the session bead has no worker_dir metadata
//   - the session's record does not ask for the removal
//   - the worker_dir does not live under the configured worktrees root
//     (workdir.WorktreesRoot)
//   - the worker_dir is missing on disk or has no .git pointer
//   - the worker_dir is a managed worktree, or its provenance cannot be read
//     (managedWorktreeRefusal)
//   - a live process, or a different open session, is working at or beneath the
//     worker_dir, or the liveness scan is indeterminate (fails closed)
//   - the worktree has uncommitted changes or unpushed commits
//   - a repository or a git checkout beneath it holds work the removal would
//     lose, or the folder cannot be read whole (nestedCheckoutRefusal)
//   - the worktree's repository cannot list its worktrees
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
	if !sessionRecordPrunesWorkerDir(sessionInfoFromBead(session), workerDir, cfg, stderr) {
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
	if worktreeLivenessBlocksPrune(workerDir, liveness, session.ID, stderr) {
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
	if reason := nestedCheckoutRefusal(workerDir); reason != "" {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: %s\n", workerDir, reason) //nolint:errcheck
		return false
	}

	root, err := worktreeRemovalRoot(gp, workerDir)
	if err != nil {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: %v\n", workerDir, err) //nolint:errcheck
		return false
	}
	if err := newGitProbe(root).WorktreeRemove(workerDir, true); err != nil {
		fmt.Fprintf(stderr, "session reconciler: pruning worker_dir %s: %v\n", workerDir, err) //nolint:errcheck
		return false
	}
	fmt.Fprintf(stderr, "session reconciler: pruned worker_dir %s (session %s)\n", workerDir, session.Metadata["session_name"]) //nolint:errcheck
	return true
}

// pruneAgentHomeWorktreeIfSafeInfo is the session.Info form of
// pruneAgentHomeWorktreeIfSafe: the worker_dir read routes through
// session.WorkerDirFromInfo (the canonical→legacy Info fallback equivalent to
// contract.WorkerDirFromMetadata) and the log line reads
// Info.SessionNameMetadata — every safety gate (including the shared liveness
// gate) and the removal itself are unchanged. Byte-identical to the raw form,
// which survives for its test callers.
func pruneAgentHomeWorktreeIfSafeInfo(info sessionpkg.Info, cityPath string, cfg *config.City, liveness worktreeLivenessInputs, stderr io.Writer) {
	if cfg == nil || !cfg.Daemon.AutoPruneWorkerDirEnabled() {
		return
	}
	workerDir := strings.TrimSpace(sessionpkg.WorkerDirFromInfo(info))
	if workerDir == "" {
		return
	}
	if !sessionRecordPrunesWorkerDir(info, workerDir, cfg, stderr) {
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
	if worktreeLivenessBlocksPrune(workerDir, liveness, info.ID, stderr) {
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
	if reason := nestedCheckoutRefusal(workerDir); reason != "" {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: %s\n", workerDir, reason) //nolint:errcheck
		return
	}

	root, err := worktreeRemovalRoot(gp, workerDir)
	if err != nil {
		fmt.Fprintf(stderr, "session reconciler: not pruning worker_dir %s: %v\n", workerDir, err) //nolint:errcheck
		return
	}
	if err := newGitProbe(root).WorktreeRemove(workerDir, true); err != nil {
		fmt.Fprintf(stderr, "session reconciler: pruning worker_dir %s: %v\n", workerDir, err) //nolint:errcheck
		return
	}
	fmt.Fprintf(stderr, "session reconciler: pruned worker_dir %s (session %s)\n", workerDir, info.SessionNameMetadata) //nolint:errcheck
}

// pruneClosedRowWorkerDir is the worker_dir prune a close path runs once it
// has closed the session row id: the legacy reconciler's closes, and the
// session manager's after a session close or prune
// (pruneClosedSessionWorkerDirs). It reads the row again, so the removal
// follows the record the closed row holds, a prune_worker_dir written after
// the caller's snapshot included, and hands that row to pruneWorktree
// (pruneAgentHomeWorktreeIfSafeInfo), which decides the removal and every
// safety gate. liveness is gathered only for a closed row that records a
// worker_dir, so another close costs no process-table walk. A row that
// reopened since or cannot be read keeps its worktree, and a city whose
// auto-prune is off reads nothing.
func pruneClosedRowWorkerDir(sessions *sessionpkg.Store, id, cityPath string, cfg *config.City, liveness func() worktreeLivenessInputs, stderr io.Writer) {
	if cfg == nil || !cfg.Daemon.AutoPruneWorkerDirEnabled() {
		return
	}
	info, err := sessions.Get(id)
	if err != nil {
		fmt.Fprintf(stderr, "worker_dir prune: reading closed session %s: %v\n", id, err) //nolint:errcheck
		return
	}
	if !info.Closed || sessionpkg.WorkerDirFromInfo(info) == "" {
		return
	}
	pruneWorktree(info, cityPath, cfg, liveness(), stderr)
}

// closedSessionWorkerDirReclaim is the cleanup a worker factory's session
// manager runs after it closes rows (worker.FactoryConfig.AfterClose), as a
// session close or prune does: pruneClosedSessionWorkerDirs over the closed
// rows, saying on stderr what it keeps and what it removes. Nil without a city
// path, its config or a session store, with which no removal can be judged.
func closedSessionWorkerDirReclaim(cityPath string, cfg *config.City, sessions *sessionpkg.Store, stderr io.Writer) func(ids []string) {
	if cityPath == "" || cfg == nil || sessions == nil {
		return nil
	}
	return func(ids []string) { pruneClosedSessionWorkerDirs(cityPath, cfg, sessions, ids, stderr) }
}

// pruneClosedSessionWorkerDirs prunes the worker worktree of each session row
// in ids that a session manager closed (closedSessionWorkerDirReclaim), as
// pruneClosedRowWorkerDir does for one row, so a row's worktree goes only when
// its closed record asks for it and every safety gate passes. A worktree an
// open row records, such as a successor seat's in the same pool slot, stays.
// The liveness inputs, the process-table walk and the worktree dirs every open
// row records, are gathered once, on the first closed row that records a
// worker_dir. The open rows are every open session row (session.Store.ListAll),
// a row that lost its session label included. An open-row list that cannot be
// read, or reads only in part, leaves the inputs unscanned, which keeps every
// worktree, and says so on stderr.
func pruneClosedSessionWorkerDirs(cityPath string, cfg *config.City, sessions *sessionpkg.Store, ids []string, stderr io.Writer) {
	liveness := sync.OnceValue(func() worktreeLivenessInputs {
		open, err := sessions.ListAll(sessionpkg.ListAllOptions{})
		if err != nil {
			fmt.Fprintf(stderr, "worker_dir prune: listing open sessions: %v\n", err) //nolint:errcheck
			return worktreeLivenessInputs{}
		}
		return worktreeLivenessInputs{live: collectLiveWorktreeStateFn(), sessionDirs: openSessionWorkDirs(open)}
	})
	for _, id := range ids {
		pruneClosedRowWorkerDir(sessions, id, cityPath, cfg, liveness, stderr)
	}
}

// worktreeRemovalRoot returns the directory the prune runs
// `git worktree remove` from for workerDir, the worktree gp probes: its
// repository's main worktree, which the worktree's own list names first. The
// removal so goes through the repository every gate probed, whichever rig the
// session's template names, or none, and never runs from the directory it
// deletes, which git refuses in some configurations. A list that cannot be
// read, or a worker_dir that is its repository's main worktree, which
// `git worktree remove` does not remove, keeps the folder.
func worktreeRemovalRoot(gp gitProbe, workerDir string) (string, error) {
	entries, err := gp.WorktreeList()
	if err != nil {
		return "", fmt.Errorf("cannot list its repository's worktrees: %w", err)
	}
	if len(entries) == 0 || strings.TrimSpace(entries[0].Path) == "" {
		return "", errors.New("its repository lists no main worktree")
	}
	if pathutil.SamePath(entries[0].Path, workerDir) {
		return "", errors.New("it is its repository's main worktree, not a linked worktree")
	}
	return entries[0].Path, nil
}
