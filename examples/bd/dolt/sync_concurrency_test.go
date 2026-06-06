package dolt_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests cover the concurrency guard in commands/sync/run.sh: a second
// `gc dolt sync` must not run while one is already in flight (the
// dolt-remotes-patrol order fires every 15m and a slow push would otherwise let
// each tick stack another concurrent DOLT_PUSH — incident 2026-06-05). The
// guard is a non-blocking flock; they are skipped where flock is unavailable
// because the guard there deliberately degrades to a warn-and-proceed no-op,
// leaving nothing to assert.

// requireFlock skips the calling test when flock is not on PATH.
func requireFlock(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock unavailable; the concurrency guard degrades to a warn-and-proceed no-op here")
	}
}

// writeSyncFakeDoltBlockingPush installs a fake dolt that answers the SQL-mode
// remote-lookup and active-branch queries, then BLOCKS on DOLT_PUSH: it creates
// startMarker (signaling the push — and thus the held lock — has been reached)
// and spins until releaseMarker appears, then exits 0. This pins one
// `gc dolt sync` mid-push (holding the concurrency lock) while a test races a
// second sync, then releases it deterministically. The spin is bounded (~60s)
// so a forgotten release cannot leak the process indefinitely.
func writeSyncFakeDoltBlockingPush(t *testing.T, dir, startMarker, releaseMarker string) {
	t.Helper()
	logPath := filepath.Join(dir, "dolt.log")
	body := `#!/bin/sh
printf '%s\n' "$*" >> "` + logPath + `"
case "$*" in
  *"SELECT name, url FROM dolt_remotes LIMIT 1"*)
    printf 'name,url\norigin,https://example.invalid/repo\n'
    exit 0
    ;;
  *"SELECT active_branch()"*)
    printf 'active_branch()\nmain\n'
    exit 0
    ;;
  *DOLT_PUSH*)
    : > "` + startMarker + `"
    i=0
    while [ ! -f "` + releaseMarker + `" ]; do
      i=$((i + 1))
      [ "$i" -ge 1200 ] && break
      sleep 0.05
    done
    exit 0
    ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(dir, "dolt"), []byte(body), 0o755); err != nil {
		t.Fatalf("write blocking fake dolt: %v", err)
	}
}

// syncOutcome carries the combined output and exit error of a backgrounded sync.
type syncOutcome struct {
	out []byte
	err error
}

// startSyncCmd launches `sh <script> <args...>` with the given env in the
// background, capturing combined output, and returns the command plus a
// buffered channel that receives the outcome when the process exits.
func startSyncCmd(t *testing.T, script string, env []string, args ...string) (*exec.Cmd, <-chan syncOutcome) {
	t.Helper()
	cmd := exec.Command("sh", append([]string{script}, args...)...)
	cmd.Env = env
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	done := make(chan syncOutcome, 1)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sync %v: %v", args, err)
	}
	go func() {
		err := cmd.Wait()
		done <- syncOutcome{out: buf.Bytes(), err: err}
	}()
	return cmd, done
}

// waitForFile polls for path until it exists or the timeout elapses.
func waitForFile(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSyncConcurrentRunSkipsWhileLockHeld pins one sync mid-push (holding the
// lock) and proves a competing `gc dolt sync` skips cleanly (exit 0, no push)
// rather than stacking a second concurrent DOLT_PUSH. Releasing the first sync
// then confirms it completes its own push.
func TestSyncConcurrentRunSkipsWhileLockHeld(t *testing.T) {
	requireFlock(t)
	root := repoRoot(t)
	script := filepath.Join(root, syncScript)

	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	cityPath := t.TempDir()
	dataDir := filepath.Join(cityPath, "data")
	if err := os.MkdirAll(filepath.Join(dataDir, "app", ".dolt"), 0o755); err != nil {
		t.Fatalf("mkdir db: %v", err)
	}

	startMarker := filepath.Join(cityPath, "push-started")
	releaseMarker := filepath.Join(cityPath, "push-release")

	// sync #1 blocks mid-push, holding the lock.
	binDir1 := t.TempDir()
	writeSyncFakeDoltBlockingPush(t, binDir1, startMarker, releaseMarker)
	_ = writeSyncFakeBeadsBD(t, cityPath)

	env1 := append(syncFilteredEnv(),
		"PATH="+binDir1+":"+os.Getenv("PATH"),
		"GC_CITY_PATH="+cityPath,
		"GC_PACK_DIR="+root,
		"GC_DOLT_DATA_DIR="+dataDir,
		fmt.Sprintf("GC_DOLT_PORT=%d", port),
		"GC_DOLT_USER=root",
		"GC_DOLT_PASSWORD=",
	)
	cmd1, done1 := startSyncCmd(t, script, env1, "--db", "app")
	// Always let the blocking sync exit, even if an assertion fails first.
	defer func() {
		_ = os.WriteFile(releaseMarker, []byte("x"), 0o644)
		_ = cmd1.Process.Kill()
	}()

	if !waitForFile(startMarker, 30*time.Second) {
		select {
		case r := <-done1:
			t.Fatalf("sync #1 never reached push (exited early: %v)\n%s", r.err, r.out)
		default:
			t.Fatal("timed out waiting for sync #1 to reach push (lock not held in time)")
		}
	}

	// sync #2 is a competing real sync. With the lock held it must skip and
	// must NOT push.
	binDir2 := t.TempDir()
	doltLog2 := writeSyncFakeDoltActiveBranch(t, binDir2, "main")
	cmd2 := exec.Command("sh", script, "--db", "app")
	cmd2.Env = append(syncFilteredEnv(),
		"PATH="+binDir2+":"+os.Getenv("PATH"),
		"GC_CITY_PATH="+cityPath,
		"GC_PACK_DIR="+root,
		"GC_DOLT_DATA_DIR="+dataDir,
		fmt.Sprintf("GC_DOLT_PORT=%d", port),
		"GC_DOLT_USER=root",
		"GC_DOLT_PASSWORD=",
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("competing sync should skip cleanly (exit 0), got error: %v\n%s", err2, out2)
	}
	if !strings.Contains(string(out2), "another sync is already in flight") {
		t.Fatalf("competing sync should report it skipped due to an in-flight sync, got:\n%s", out2)
	}
	if data, rerr := os.ReadFile(doltLog2); rerr == nil && strings.Contains(string(data), "DOLT_PUSH") {
		t.Fatalf("competing sync must NOT push while another sync holds the lock, dolt log:\n%s", data)
	}

	// Release sync #1 and confirm it completed its own push.
	if err := os.WriteFile(releaseMarker, []byte("x"), 0o644); err != nil {
		t.Fatalf("write release marker: %v", err)
	}
	select {
	case r := <-done1:
		if r.err != nil {
			t.Fatalf("sync #1 (lock holder) failed after release: %v\n%s", r.err, r.out)
		}
		if !strings.Contains(string(r.out), "app: pushed") {
			t.Fatalf("sync #1 should have pushed after releasing, got:\n%s", r.out)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("sync #1 did not finish after release")
	}
}

// TestSyncReleasesLockOnExit runs two syncs sequentially against the same city.
// Both must push: the first has to release the lock on exit so the second can
// acquire it. A guard that never released would skip the second run.
func TestSyncReleasesLockOnExit(t *testing.T) {
	requireFlock(t)
	root := repoRoot(t)
	script := filepath.Join(root, syncScript)

	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	cityPath := t.TempDir()
	dataDir := filepath.Join(cityPath, "data")
	if err := os.MkdirAll(filepath.Join(dataDir, "app", ".dolt"), 0o755); err != nil {
		t.Fatalf("mkdir db: %v", err)
	}
	binDir := t.TempDir()
	doltLog := writeSyncFakeDoltActiveBranch(t, binDir, "main")
	_ = writeSyncFakeBeadsBD(t, cityPath)

	env := append(syncFilteredEnv(),
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"GC_CITY_PATH="+cityPath,
		"GC_PACK_DIR="+root,
		"GC_DOLT_DATA_DIR="+dataDir,
		fmt.Sprintf("GC_DOLT_PORT=%d", port),
		"GC_DOLT_USER=root",
		"GC_DOLT_PASSWORD=",
	)

	for i := 1; i <= 2; i++ {
		cmd := exec.Command("sh", script, "--db", "app")
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("sync #%d failed: %v\n%s", i, err, out)
		}
		if strings.Contains(string(out), "another sync is already in flight") {
			t.Fatalf("sync #%d was wrongly skipped — the prior run failed to release the lock:\n%s", i, out)
		}
		if !strings.Contains(string(out), "app: pushed") {
			t.Fatalf("sync #%d should have pushed, got:\n%s", i, out)
		}
	}

	if data, err := os.ReadFile(doltLog); err != nil || !strings.Contains(string(data), "DOLT_PUSH") {
		t.Fatalf("expected DOLT_PUSH in dolt log across the two runs, err=%v log:\n%s", err, data)
	}
}

// TestSyncDryRunBypassesLock proves a `--dry-run` is not blocked by an in-flight
// sync: a dry-run performs no push, so it neither needs the lock nor should be
// turned away by a held one.
func TestSyncDryRunBypassesLock(t *testing.T) {
	requireFlock(t)
	root := repoRoot(t)
	script := filepath.Join(root, syncScript)

	port, cleanup := startReachableTCPListener(t)
	defer cleanup()

	cityPath := t.TempDir()
	dataDir := filepath.Join(cityPath, "data")
	if err := os.MkdirAll(filepath.Join(dataDir, "app", ".dolt"), 0o755); err != nil {
		t.Fatalf("mkdir db: %v", err)
	}

	startMarker := filepath.Join(cityPath, "push-started")
	releaseMarker := filepath.Join(cityPath, "push-release")

	binDir1 := t.TempDir()
	writeSyncFakeDoltBlockingPush(t, binDir1, startMarker, releaseMarker)
	_ = writeSyncFakeBeadsBD(t, cityPath)

	env1 := append(syncFilteredEnv(),
		"PATH="+binDir1+":"+os.Getenv("PATH"),
		"GC_CITY_PATH="+cityPath,
		"GC_PACK_DIR="+root,
		"GC_DOLT_DATA_DIR="+dataDir,
		fmt.Sprintf("GC_DOLT_PORT=%d", port),
		"GC_DOLT_USER=root",
		"GC_DOLT_PASSWORD=",
	)
	cmd1, done1 := startSyncCmd(t, script, env1, "--db", "app")
	defer func() {
		_ = os.WriteFile(releaseMarker, []byte("x"), 0o644)
		_ = cmd1.Process.Kill()
	}()

	if !waitForFile(startMarker, 30*time.Second) {
		select {
		case r := <-done1:
			t.Fatalf("sync #1 never reached push (exited early: %v)\n%s", r.err, r.out)
		default:
			t.Fatal("timed out waiting for sync #1 to hold the lock")
		}
	}

	binDir2 := t.TempDir()
	_ = writeSyncFakeDoltActiveBranch(t, binDir2, "main")
	cmd2 := exec.Command("sh", script, "--db", "app", "--dry-run")
	cmd2.Env = append(syncFilteredEnv(),
		"PATH="+binDir2+":"+os.Getenv("PATH"),
		"GC_CITY_PATH="+cityPath,
		"GC_PACK_DIR="+root,
		"GC_DOLT_DATA_DIR="+dataDir,
		fmt.Sprintf("GC_DOLT_PORT=%d", port),
		"GC_DOLT_USER=root",
		"GC_DOLT_PASSWORD=",
	)
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("dry-run should not be blocked by an in-flight sync, got error: %v\n%s", err2, out2)
	}
	if strings.Contains(string(out2), "another sync is already in flight") {
		t.Fatalf("dry-run must bypass the concurrency guard, but it was skipped:\n%s", out2)
	}
	if !strings.Contains(string(out2), "would push") {
		t.Fatalf("dry-run should preview a push despite the in-flight sync, got:\n%s", out2)
	}
}
