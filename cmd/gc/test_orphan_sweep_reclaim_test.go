package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// completedTestTempRoot builds a root in the shape createActiveTestTempRoot
// leaves behind once setup finishes: an alive sentinel plus the active-root
// marker. The sentinel is returned still locked; closing it models the
// creator's death.
func completedTestTempRoot(t *testing.T, parent, prefix string, pid int) (string, *os.File) {
	t.Helper()
	dir := pidPrefixedTestDir(parent, prefix, pid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", dir, err)
	}
	sentinel, err := holdAliveSentinel(dir)
	if err != nil {
		t.Fatalf("holdAliveSentinel(%s): %v", dir, err)
	}
	t.Cleanup(func() { _ = sentinel.Close() })
	if err := os.WriteFile(filepath.Join(dir, testActiveTempRootMarker), []byte("1\n"), 0o644); err != nil {
		t.Fatalf("writing active-root marker in %s: %v", dir, err)
	}
	return dir, sentinel
}

// TestSweepReclaimsCompletedRootWithoutWaitingOutAgeGuard pins the reclaim
// that keeps the temp dir bounded. A root carrying the active-root marker
// beside a free sentinel is proof of a finished setup whose owner has since
// died: the marker is written only after the flock is held, so the two
// together cannot describe a run still in its startup window. Each abandoned
// root holds a copy of the test binary per testscript command, so holding one
// for testOrphanSweepMinAge is expensive.
func TestSweepReclaimsCompletedRootWithoutWaitingOutAgeGuard(t *testing.T) {
	parent := t.TempDir()
	dir, sentinel := completedTestTempRoot(t, parent, "pfx", nonLivePID(t))
	if err := sentinel.Close(); err != nil {
		t.Fatalf("close sentinel: %v", err)
	}

	sweepOrphanPIDPrefixedDirs(parent, "pfx")

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("completed root %s survived the sweep; want it reclaimed without waiting out the age guard", dir)
	}
}

// TestSweepKeepsIncompleteRootWithinAgeGuard is the other half of the reclaim
// rule: without the marker, a free sentinel cannot distinguish a dead owner
// from one still between opening the sentinel and taking its flock, so the
// age guard must still protect it.
func TestSweepKeepsIncompleteRootWithinAgeGuard(t *testing.T) {
	parent := t.TempDir()
	dir := pidPrefixedTestDir(parent, "pfx", nonLivePID(t))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", dir, err)
	}
	sentinel, err := holdAliveSentinel(dir)
	if err != nil {
		t.Fatalf("holdAliveSentinel: %v", err)
	}
	if err := sentinel.Close(); err != nil {
		t.Fatalf("close sentinel: %v", err)
	}

	sweepOrphanPIDPrefixedDirs(parent, "pfx")

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("root without an active-root marker was reclaimed inside the age guard: %v", err)
	}
}

// TestSweepKeepsCompletedRootWhoseSentinelIsHeld guards the reclaim against
// reaping a live sibling: a held flock means the owner is alive even when its
// PID is invisible from another PID namespace, and that outranks both the
// marker and any age.
func TestSweepKeepsCompletedRootWhoseSentinelIsHeld(t *testing.T) {
	parent := t.TempDir()
	dir, _ := completedTestTempRoot(t, parent, "pfx", nonLivePID(t))
	backdatePastSweepAge(t, dir)

	sweepOrphanPIDPrefixedDirs(parent, "pfx")

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("root with a held alive sentinel was reclaimed: %v", err)
	}
}

// TestCreateActiveTestTempRootProducesAnImmediatelyReclaimableShape checks the
// real setup function against the real sweep rule, which is what the ordering
// inside createActiveTestTempRoot exists to satisfy: a live root is
// untouchable, and once its owner dies the same root is reclaimable without
// waiting out the age guard.
//
// The sweep never reaps a dir bearing its own PID, so the dead owner is staged
// by renaming the finished root onto a non-live PID. rename(2) leaves the
// directory's own mtime alone, so the root stays inside the age guard and the
// reclaim under test is the marker rule rather than the age fallback.
func TestCreateActiveTestTempRootProducesAnImmediatelyReclaimableShape(t *testing.T) {
	parent := t.TempDir()
	t.Setenv("TMPDIR", parent)

	root, sentinel, err := createActiveTestTempRoot("pfx")
	if err != nil {
		t.Fatalf("createActiveTestTempRoot: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	if !hasActiveTempRootMarker(root) {
		t.Fatal("active-root marker missing after setup")
	}
	if exists, held := aliveSentinelHeld(root); !exists || !held {
		t.Fatalf("aliveSentinelHeld(%s) = (exists=%v, held=%v); want (true, true)", root, exists, held)
	}

	sweepOrphanPIDPrefixedDirs(parent, "pfx")
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("live root was reclaimed while its sentinel was held: %v", err)
	}

	dead := pidPrefixedTestDir(parent, "pfx", nonLivePID(t))
	if err := os.Rename(root, dead); err != nil {
		t.Fatalf("Rename(%s, %s): %v", root, dead, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dead) })
	if err := sentinel.Close(); err != nil {
		t.Fatalf("close sentinel: %v", err)
	}

	sweepOrphanPIDPrefixedDirs(parent, "pfx")
	if _, err := os.Stat(dead); !os.IsNotExist(err) {
		t.Errorf("root %s survived the sweep after its owner died", dead)
	}
}

// TestDoltLeakGuardTerminationSignalsCoverWatchdogQuit pins SIGQUIT into the
// handled set. scripts/lib/harness-reap.sh's watchdog sends SIGQUIT before
// SIGKILL so the Go runtime dumps goroutine stacks and names the wedged test;
// the runtime's default action for it is dump-and-die, so leaving SIGQUIT
// unhandled means every watchdog escalation abandons the test temp root.
func TestDoltLeakGuardTerminationSignalsCoverWatchdogQuit(t *testing.T) {
	want := map[syscall.Signal]bool{
		syscall.SIGINT:  false,
		syscall.SIGTERM: false,
		syscall.SIGQUIT: false,
	}
	for _, sig := range doltLeakGuardTerminationSignals {
		if _, ok := want[sig]; !ok {
			t.Errorf("unexpected termination signal %v in the handled set", sig)
			continue
		}
		want[sig] = true
	}
	for sig, seen := range want {
		if !seen {
			t.Errorf("termination signal %v is not handled; the guard leaks its temp root on that path", sig)
		}
	}
}

// TestHandleTerminationSignalRemovesTemporaryPaths covers what the handler
// owes a run it is about to end: every temporary path this process created is
// gone before the signal is re-raised.
func TestHandleTerminationSignalRemovesTemporaryPaths(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	stray := filepath.Join(parent, "stray")
	for _, dir := range []string{root, stray} {
		if err := os.MkdirAll(filepath.Join(dir, "payload"), 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", dir, err)
		}
	}

	g := &doltLeakGuardedTestingM{tempRoot: root, cleanupPaths: []string{root, stray}}
	captureSweepStderr(t, func() { g.handleTerminationSignal(syscall.SIGQUIT) })

	for _, dir := range []string{root, stray} {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("%s survived the termination handler", dir)
		}
	}
}

// TestSweepIsSilentWhenDeathIsProven pins the one removal shape that says
// nothing. A marker beside a free sentinel proves the owner died after setup,
// so the removal cannot be reaping a live dir and needs no forensic record.
// It is also the shape nearly every reclaim takes, and a test binary's
// subprocesses inherit stderr, so announcing it puts harness chatter inside
// output that tests assert belongs to the child command.
func TestSweepIsSilentWhenDeathIsProven(t *testing.T) {
	parent := t.TempDir()
	dir, sentinel := completedTestTempRoot(t, parent, "pfx", nonLivePID(t))
	if err := sentinel.Close(); err != nil {
		t.Fatalf("close sentinel: %v", err)
	}

	got := captureSweepStderr(t, func() { sweepOrphanPIDPrefixedDirs(parent, "pfx") })

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("sweepOrphanPIDPrefixedDirs did not remove %s: %v", dir, err)
	}
	if got != "" {
		t.Errorf("sweep stderr = %q, want empty for a removal whose owner is provably dead", got)
	}
}

// TestCleanupTemporaryPathsReclaimsReadOnlyFixtureTree pins the guard's own
// reclaim. cleanupTemporaryPaths is what removes this run's temp root, so a
// read-only fixture the run never restored has to be reclaimed here or it
// outlives the process as a husk no later sweep can remove either.
func TestCleanupTemporaryPathsReclaimsReadOnlyFixtureTree(t *testing.T) {
	skipWhenRootIgnoresPermissions(t)
	root := filepath.Join(t.TempDir(), "root")
	readOnlyFixtureTree(t, root)

	g := &doltLeakGuardedTestingM{cleanupPaths: []string{root}}
	_ = captureSweepStderr(t, g.cleanupTemporaryPaths)

	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("cleanupTemporaryPaths left %s behind: stat err = %v", root, err)
	}
}

// TestCleanupTemporaryPathsReportsAPathItCannotRemove pins the announcement
// for the paths the reclaim cannot finish. Discarding that error is what let
// a stranded root go unreported for the life of the run.
func TestCleanupTemporaryPathsReportsAPathItCannotRemove(t *testing.T) {
	skipWhenRootIgnoresPermissions(t)
	target := unremovableFixtureDir(t, filepath.Join(t.TempDir(), "parent"))

	g := &doltLeakGuardedTestingM{cleanupPaths: []string{"", target}}
	got := captureSweepStderr(t, g.cleanupTemporaryPaths)

	if _, err := os.Stat(target); err != nil {
		t.Fatalf("Stat(%s) = %v, want the unremovable path still present", target, err)
	}
	if !strings.Contains(got, target) || !strings.Contains(got, "permission denied") {
		t.Errorf("cleanup stderr = %q, want a report naming %s and the removal error", got, target)
	}
}
