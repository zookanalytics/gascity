package tmuxtest

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// completedSocketParentDir builds a socket parent in the shape
// NewSocketParentDir leaves behind once setup finishes: an alive sentinel plus
// the setup-complete marker. The sentinel is returned still locked; closing it
// models the creator's death.
func completedSocketParentDir(t *testing.T, root, prefix string, pid int) (string, *os.File) {
	t.Helper()
	dir := pidPrefixedTestDir(t, root, prefix, pid)
	sentinel, err := HoldAliveSentinel(dir)
	if err != nil {
		t.Fatalf("HoldAliveSentinel(%s): %v", dir, err)
	}
	t.Cleanup(func() { _ = sentinel.Close() })
	if err := os.WriteFile(filepath.Join(dir, socketParentSetupCompleteMarkerName), []byte("1\n"), 0o644); err != nil {
		t.Fatalf("writing setup-complete marker in %s: %v", dir, err)
	}
	return dir, sentinel
}

// TestSweepReclaimsCompletedSocketParentWithoutWaitingOutAgeGuard pins the
// reclaim that keeps /tmp's entry count bounded. A socket parent carrying the
// setup-complete marker beside a free sentinel is proof of a finished setup
// whose owner has since died: the marker is written only after the flock is
// held, so the two together cannot describe a run still in its startup window.
// The uncatchable exits that leave these behind -- SIGKILL and the go test
// timeout panic -- cannot run in-process cleanup, so the sweep is the only
// thing that reclaims them.
func TestSweepReclaimsCompletedSocketParentWithoutWaitingOutAgeGuard(t *testing.T) {
	root := t.TempDir()
	dir, sentinel := completedSocketParentDir(t, root, "pfx-", nonLivePID(t))
	if err := sentinel.Close(); err != nil {
		t.Fatalf("close sentinel: %v", err)
	}

	var diagnostics bytes.Buffer
	SweepOrphanPIDPrefixedDirs(root, "pfx-", &diagnostics)

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("completed socket parent %s survived the sweep; want it reclaimed without waiting out the age guard", dir)
	}
	wantDiagnostics := fmt.Sprintf("tmuxtest: removing orphaned socket parent %s (setup complete, owner gone)\n", dir)
	if got := diagnostics.String(); got != wantDiagnostics {
		t.Errorf("diagnostics = %q, want %q", got, wantDiagnostics)
	}
}

// TestSweepKeepsIncompleteSocketParentWithinAgeGuard is the other half of the
// reclaim rule: without the marker, a free sentinel cannot distinguish a dead
// owner from one still between opening the sentinel and taking its flock, so
// the age guard must still protect it.
func TestSweepKeepsIncompleteSocketParentWithinAgeGuard(t *testing.T) {
	root := t.TempDir()
	dir := pidPrefixedTestDir(t, root, "pfx-", nonLivePID(t))
	sentinel, err := HoldAliveSentinel(dir)
	if err != nil {
		t.Fatalf("HoldAliveSentinel: %v", err)
	}
	if err := sentinel.Close(); err != nil {
		t.Fatalf("close sentinel: %v", err)
	}

	SweepOrphanPIDPrefixedDirs(root, "pfx-", io.Discard)

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("socket parent without a setup-complete marker was reclaimed inside the age guard: %v", err)
	}
}

// TestSweepKeepsCompletedSocketParentWhoseSentinelIsHeld guards the reclaim
// against reaping a live sibling: a held flock means the owner is alive even
// when its PID is invisible from another PID namespace, and that outranks both
// the marker and any age.
func TestSweepKeepsCompletedSocketParentWhoseSentinelIsHeld(t *testing.T) {
	root := t.TempDir()
	dir, _ := completedSocketParentDir(t, root, "pfx-", nonLivePID(t))
	backdatePastSweepAge(t, dir)

	SweepOrphanPIDPrefixedDirs(root, "pfx-", io.Discard)

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("socket parent with a held alive sentinel was reclaimed: %v", err)
	}
}

// TestNewSocketParentDirProducesAnImmediatelyReclaimableShape checks the real
// creation function against the real sweep rule, which is what the ordering
// inside NewSocketParentDir exists to satisfy: a live socket parent is
// untouchable, and once its owner dies the same directory is reclaimable
// without waiting out the age guard.
//
// The sweep never reaps a dir bearing its own PID, so the dead owner is staged
// by renaming the finished directory onto a non-live PID. rename(2) leaves the
// directory's own mtime alone, so it stays inside the age guard and the reclaim
// under test is the marker rule rather than the age fallback.
func TestNewSocketParentDirProducesAnImmediatelyReclaimableShape(t *testing.T) {
	root := t.TempDir()

	dir, sentinel, err := NewSocketParentDir(root, io.Discard)
	if err != nil {
		t.Fatalf("NewSocketParentDir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	if !hasSetupCompleteMarker(dir) {
		t.Fatal("setup-complete marker missing after NewSocketParentDir")
	}
	if exists, held := aliveSentinelHeld(dir); !exists || !held {
		t.Fatalf("aliveSentinelHeld(%s) = (exists=%v, held=%v), want (true, true)", dir, exists, held)
	}

	dead := filepath.Join(root, SocketParentDirPrefix+strconv.Itoa(nonLivePID(t))+"-dead")
	if err := os.Rename(dir, dead); err != nil {
		t.Fatalf("Rename(%s, %s): %v", dir, dead, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dead) })
	if err := sentinel.Close(); err != nil {
		t.Fatalf("close sentinel: %v", err)
	}

	SweepOrphanPIDPrefixedDirs(root, SocketParentDirPrefix, io.Discard)

	if _, err := os.Stat(dead); !os.IsNotExist(err) {
		t.Errorf("socket parent %s survived the sweep after its owner died", dead)
	}
}
