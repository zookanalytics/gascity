package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	testGCBinaryDirPrefix        = "gc-test-binary-pid"
	testBDBinaryDirPrefix        = "bd-test-binary-pid"
	testCmdGCTempRootPrefix      = "gct"
	testCmdGCShardTempRootPrefix = "gcx"
	testShardIndexEnv            = "GC_TEST_SHARD_INDEX"
	testShardTotalEnv            = "GC_TEST_SHARD_TOTAL"
	testActiveTempRootMarker     = ".gc-test-active-root"
	testSharedFixtureDirPrefix   = "gascity-gc-test-fixtures-pid"
	testSlingFormulaDirPrefix    = "gc-sling-test-formulas-pid"
	testSlingCityDirPrefix       = "gc-sling-test-city-pid"
	testGCHomeDirPrefix          = "gascity-gc-home-pid"
	testRuntimeDirPrefix         = "gascity-runtime-pid"
	testProviderStubDirPrefix    = "gascity-provider-stubs-pid"
	// testAliveSentinelName is a lock file inside each test temp root. The
	// creating process holds an exclusive flock on it for its lifetime;
	// sweepers probe the lock instead of trusting PID visibility, which lies
	// across PID namespaces (ga-djbcqt: bwrap --unshare-pid sandboxes see
	// every host PID as dead while sharing the host /tmp).
	testAliveSentinelName = ".gc-test-alive.lock"
)

// testOrphanSweepMinAge is how long a dir whose liveness cannot be settled
// from its own contents is left alone. It covers the window where a sibling
// run has created its dir but not yet acquired the alive sentinel. A dir that
// carries both the sentinel and the active-root marker is not ambiguous and
// does not wait this out; see sweepOrphanPIDPrefixedDirs.
const testOrphanSweepMinAge = time.Hour

// holdAliveSentinel creates <dir>/.gc-test-alive.lock and takes an exclusive
// flock on it. The caller must keep the returned file referenced for as long
// as the dir must stay protected: the runtime finalizes unreachable os.Files,
// which closes the descriptor and releases the lock.
func holdAliveSentinel(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, testAliveSentinelName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening alive sentinel in %q: %w", dir, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("locking alive sentinel in %q: %w", dir, err)
	}
	return f, nil
}

// aliveSentinelHeld probes <dir>'s alive sentinel. exists reports whether the
// sentinel file is present; held reports whether some process still holds its
// flock. Probe failures are reported as held so the sweep stays conservative.
func aliveSentinelHeld(dir string) (exists, held bool) {
	f, err := os.OpenFile(filepath.Join(dir, testAliveSentinelName), os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return false, false
		}
		return true, true
	}
	defer f.Close() //nolint:errcheck
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true, true
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return true, false
}

// createActiveTestTempRoot sweeps stale prefix-matching roots under the
// inherited temp dir (honoring TMPDIR rather than hardcoding /tmp, so gate
// runners can isolate concurrent runs), creates this process's test temp
// root there, acquires the alive sentinel lock, and writes the active-root
// marker. The caller must keep the returned file referenced for the lifetime
// of the process so the flock is not released by a finalizer.
//
// The marker is written last on purpose, and sweepOrphanPIDPrefixedDirs
// depends on that order: a marker can only exist once the flock was held, so
// a marker beside a free sentinel proves the owner is gone rather than still
// starting up. That is what lets the sweep reclaim a dead root immediately
// instead of waiting out testOrphanSweepMinAge.
func createActiveTestTempRoot(prefix string) (string, *os.File, error) {
	parent := os.TempDir()
	sweepOrphanPIDPrefixedDirs(parent, prefix)
	root, err := os.MkdirTemp(parent, pidPrefixedTempPattern(prefix))
	if err != nil {
		return "", nil, fmt.Errorf("creating test temp root under %q: %w", parent, err)
	}
	sentinel, err := holdAliveSentinel(root)
	if err != nil {
		_ = os.RemoveAll(root)
		return "", nil, err
	}
	if err := os.WriteFile(filepath.Join(root, testActiveTempRootMarker), []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o644); err != nil {
		_ = sentinel.Close()
		_ = os.RemoveAll(root)
		return "", nil, fmt.Errorf("writing active test temp root marker: %w", err)
	}
	return root, sentinel, nil
}

// hasActiveTempRootMarker reports whether dir carries the active-root marker,
// which createActiveTestTempRoot writes only after taking the alive sentinel.
func hasActiveTempRootMarker(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, testActiveTempRootMarker))
	return err == nil
}

func pidPrefixedTempPattern(prefix string) string {
	return prefix + strconv.Itoa(os.Getpid()) + "-*"
}

func cmdGCTestTempRootPrefix() string {
	if strings.TrimSpace(os.Getenv(testShardIndexEnv)) != "" || strings.TrimSpace(os.Getenv(testShardTotalEnv)) != "" {
		return testCmdGCShardTempRootPrefix
	}
	return testCmdGCTempRootPrefix
}

func pidFromPrefixedDirName(name, prefix string) (int, bool) {
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	suffix := strings.TrimPrefix(name, prefix)
	end := 0
	for end < len(suffix) && suffix[end] >= '0' && suffix[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	if end < len(suffix) && suffix[end] != '-' {
		return 0, false
	}
	pid, err := strconv.Atoi(suffix[:end])
	if err != nil {
		return 0, false
	}
	return pid, true
}

// sweepOrphanPIDPrefixedDirs removes <root>/<prefix><PID> dirs whose creator
// is gone, including MkdirTemp names such as <prefix><PID>-<random>. Used by
// test setup to clean leftover test fixtures from prior crashed/SIGKILL'd
// runs. A dir that survives its removal is named on stderr and left in place;
// the sweep continues, because no later step depends on any single removal.
//
// Liveness is decided by the alive sentinel flock when present: flock state
// is visible across PID namespaces, whereas pidAlive reports every host PID
// as dead from inside a bwrap --unshare-pid sandbox that shares the host
// /tmp (ga-djbcqt). A free sentinel next to the active-root marker settles
// death outright, because createActiveTestTempRoot writes that marker only
// once the flock is held; such a dir is removed at any age. Every other
// shape is ambiguous — the owner may be mid-setup — so it must age past
// testOrphanSweepMinAge first, and a dir with no sentinel at all then falls
// back to PID liveness and the marker.
func sweepOrphanPIDPrefixedDirs(root, prefix string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	self := os.Getpid()
	now := time.Now()
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, ok := pidFromPrefixedDirName(e.Name(), prefix)
		if !ok || pid <= 0 || pid == self {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(root, e.Name())
		exists, held := aliveSentinelHeld(path)
		if held {
			// Creator (possibly in another PID namespace) is still alive.
			continue
		}
		aged := now.Sub(info.ModTime()) >= testOrphanSweepMinAge
		// An empty reason marks a removal that needs no announcement. Only the
		// shapes whose liveness was inferred get named on stderr, because those
		// are the ones that could have reaped a live dir.
		var reason string
		switch {
		case exists && hasActiveTempRootMarker(path):
			// createActiveTestTempRoot takes the flock before writing the
			// marker, so this pair describes a run that finished starting up
			// and has since died. No startup race is left to guard, so the
			// age guard does not apply and the root is reclaimed now. Death is
			// proven rather than inferred, and this is the shape nearly every
			// reclaim takes, so it stays silent: a subprocess inherits stderr,
			// and tests that assert on a child's output read the announcement
			// as the child's own.
		case !aged:
			// Not provably dead and younger than the guard: the owner may
			// still be between MkdirTemp and its flock.
			continue
		case exists:
			// A sentinel with no marker: setup never finished. The age guard
			// above has cleared the startup window, so the owner is gone.
			reason = "free sentinel, setup incomplete"
		default:
			// Legacy dir without a sentinel: fall back to PID liveness and
			// the active-root marker.
			if pidAlive(pid) {
				continue
			}
			if hasActiveTempRootMarker(path) {
				continue
			}
			reason = "legacy: pid dead, no active marker"
		}
		if reason != "" {
			// Name each inferred removal so a recurrence of ga-djbcqt is
			// attributable from run logs instead of gate-log forensics.
			fmt.Fprintf(os.Stderr, "cmd/gc test sweep: removing %s (%s)\n", path, reason)
		}
		if err := forceRemoveAll(path); err != nil {
			fmt.Fprintf(os.Stderr, "cmd/gc test sweep: removing %s: %v\n", path, err)
		}
	}
}

// forceRemoveAll removes path, retrying once with owner traversal restored on
// every directory underneath it. os.RemoveAll cannot unlink an entry whose
// parent directory denies writes, which is what a test that chmods a fixture
// read-only leaves behind when its process dies before t.Cleanup restores the
// mode. The first removal strips the rest of the tree, and the remnant then
// fails every later removal for the same reason it failed the first time.
// Widening the mode is safe: these trees are test fixtures created by this
// process or by a dead predecessor of it. The returned error is what still
// stands after the retry.
func forceRemoveAll(path string) error {
	if err := os.RemoveAll(path); err == nil {
		return nil
	}
	chmodErr := grantOwnerTraversal(path)
	if err := os.RemoveAll(path); err != nil {
		return errors.Join(err, chmodErr)
	}
	return nil
}

// grantOwnerTraversal adds owner read, write, and execute to path and to every
// directory beneath it, which is what unlinking their entries requires. Each
// directory is widened before the walk descends into it, so a mode that denies
// search does not hide the subtree under it. Symlinks are not followed, so the
// walk cannot widen a directory outside path. Errors are collected rather than
// raised: a mode this process cannot change is only worth reporting if the
// removal it was meant to unblock still fails.
func grantOwnerTraversal(path string) error {
	var errs []error
	walkErr := filepath.WalkDir(path, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			errs = append(errs, err)
			return nil
		}
		if !entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			errs = append(errs, err)
			return nil
		}
		if mode := info.Mode().Perm(); mode&0o700 != 0o700 {
			if err := os.Chmod(name, mode|0o700); err != nil {
				errs = append(errs, err)
			}
		}
		return nil
	})
	if walkErr != nil {
		errs = append(errs, walkErr)
	}
	return errors.Join(errs...)
}
