package tmuxtest

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gastownhall/gascity/internal/pidutil"
)

// SocketParentDirPrefix is the shared prefix for the tmux Unix-socket parent
// directories created by cmd/gc, internal/runtime/tmux, and test/integration
// TestMains. All three use the same root ("/tmp", for macOS socket-path
// length reasons -- see each call site) and prefix so a sweep triggered by
// any one of them reaps orphans left by any of the others.
const SocketParentDirPrefix = "gct-"

// socketParentAliveSentinelName is a lock file inside each socket parent
// dir. The creating process holds an exclusive flock on it for its
// lifetime; SweepOrphanPIDPrefixedDirs probes the lock instead of trusting
// PID visibility, which lies across PID namespaces (ga-djbcqt: bwrap
// --unshare-pid sandboxes see every host PID as dead while sharing the host
// /tmp). Ported from cmd/gc's identical test-temp-root sentinel mechanism
// (cmd/gc/test_orphan_sweep_test.go) so all three tmux socket parent
// creation sites share one policy instead of cmd/gc's copy being
// reimplemented per package -- package main cannot be imported, so this is
// the shared home.
const socketParentAliveSentinelName = ".gc-test-alive.lock"

// socketParentSetupCompleteMarkerName is written into a socket parent dir once
// its alive sentinel is held. Its presence beside a free sentinel proves the
// creator finished starting up and has since died, which is what lets the sweep
// reclaim the dir immediately instead of waiting out socketParentSweepMinAge.
// The name matches the active-root marker cmd/gc writes into its own test temp
// roots so both sweeps describe a finished setup the same way; the two scan
// disjoint prefixes, so neither reclaims the other's directories.
const socketParentSetupCompleteMarkerName = ".gc-test-active-root"

// socketParentSweepMinAge is how long a dir whose liveness cannot be settled
// from its own contents is left alone. It closes the window where a sibling run
// has created its dir but not yet acquired the alive sentinel. A dir carrying
// both the sentinel and the setup-complete marker is not ambiguous and does not
// wait this out; see SweepOrphanPIDPrefixedDirs.
const socketParentSweepMinAge = time.Hour

// PIDPrefixedTempPattern returns the os.MkdirTemp pattern for this
// process's own socket parent dir: "<prefix><pid>-*".
func PIDPrefixedTempPattern(prefix string) string {
	return prefix + strconv.Itoa(os.Getpid()) + "-*"
}

// HoldAliveSentinel creates <dir>/.gc-test-alive.lock and takes an
// exclusive flock on it. The caller must keep the returned file referenced
// for as long as dir must stay protected from SweepOrphanPIDPrefixedDirs:
// the runtime finalizes unreachable os.Files, which closes the descriptor
// and releases the lock.
func HoldAliveSentinel(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, socketParentAliveSentinelName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening alive sentinel in %q: %w", dir, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("locking alive sentinel in %q: %w", dir, err)
	}
	return f, nil
}

// aliveSentinelHeld probes <dir>'s alive sentinel. exists reports whether
// the sentinel file is present; held reports whether some process still
// holds its flock. Probe failures are reported as held so the sweep stays
// conservative.
func aliveSentinelHeld(dir string) (exists, held bool) {
	f, err := os.OpenFile(filepath.Join(dir, socketParentAliveSentinelName), os.O_RDWR, 0)
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

// hasSetupCompleteMarker reports whether dir carries the setup-complete marker,
// which NewSocketParentDir writes only after taking the alive sentinel.
func hasSetupCompleteMarker(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, socketParentSetupCompleteMarkerName))
	return err == nil
}

// pidFromPrefixedDirName parses the owner PID out of a socket-parent dir name
// of the form "<prefix><PID>-<random>" -- the shape NewSocketParentDir creates
// via os.MkdirTemp(root, "<prefix><PID>-*"). The "-" separator after the PID is
// required: a bare all-digit "<prefix><digits>" name is a legacy directory left
// by the pre-sweep harness (os.MkdirTemp(root, prefix)), whose trailing digits
// are a random suffix, not an owner PID. Parsing that random number as a PID
// could reap a still-live legacy sibling once it aged past the sweep guard, so
// such names are rejected here and left for a dedicated opt-in cleanup path.
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
	if end >= len(suffix) || suffix[end] != '-' {
		return 0, false
	}
	pid, err := strconv.Atoi(suffix[:end])
	if err != nil {
		return 0, false
	}
	return pid, true
}

// SweepOrphanPIDPrefixedDirs removes <root>/<prefix><PID>-<random> dirs
// whose creator is gone. Best-effort; ignores errors. Ported from cmd/gc's
// sweepOrphanPIDPrefixedDirs (test_orphan_sweep_test.go) so cmd/gc,
// internal/runtime/tmux, and test/integration share one policy for their
// tmux socket parent dirs instead of each reimplementing it.
//
// Liveness is decided by the alive sentinel flock when present: flock state
// is visible across PID namespaces, whereas raw PID liveness reports every
// host PID as dead from inside a bwrap --unshare-pid sandbox that shares
// the host /tmp (ga-djbcqt). A free sentinel beside the setup-complete marker
// settles death outright, because NewSocketParentDir writes that marker only
// once the flock is held; such a dir is removed at any age. Every other shape
// is ambiguous -- the owner may be mid-setup -- so it must age past
// socketParentSweepMinAge first, and a "<prefix><PID>-<random>" dir with no
// sentinel at all then falls back to PID liveness, covering a creator that
// crashed between MkdirTemp and HoldAliveSentinel. Legacy pre-sweep names with
// no "-" after the PID are rejected by pidFromPrefixedDirName and never swept
// here. Each removal is described on diagnostics; callers that do not surface
// cleanup logs should pass io.Discard.
func SweepOrphanPIDPrefixedDirs(root, prefix string, diagnostics io.Writer) {
	if diagnostics == nil {
		diagnostics = io.Discard
	}
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
		aged := now.Sub(info.ModTime()) >= socketParentSweepMinAge
		var reason string
		switch {
		case exists && hasSetupCompleteMarker(path):
			// NewSocketParentDir takes the flock before writing the marker, so
			// this pair describes a run that finished starting up and has since
			// died. No startup race is left to guard, so the age guard does not
			// apply.
			reason = "setup complete, owner gone"
		case !aged:
			// Not provably dead and younger than the guard: the owner may still
			// be between MkdirTemp and its flock.
			continue
		case exists:
			// A sentinel with no marker: setup never finished. The age guard
			// above has cleared the startup window, so the owner is gone.
			reason = "free sentinel, setup incomplete"
		default:
			// A "<prefix><PID>-<random>" dir with no sentinel: its creator
			// crashed between MkdirTemp and HoldAliveSentinel. Fall back to
			// PID liveness. (Legacy no-"-" names are rejected by
			// pidFromPrefixedDirName and never reach here.)
			if pidutil.Alive(pid) {
				continue
			}
			reason = "pid dead, no sentinel"
		}
		// Name each removal so a recurrence of ga-djbcqt is attributable
		// from run logs instead of gate-log forensics.
		_, _ = fmt.Fprintf(diagnostics, "tmuxtest: removing orphaned socket parent %s (%s)\n", path, reason)
		_ = os.RemoveAll(path)
	}
}

// NewSocketParentDir sweeps orphaned sibling socket parent directories
// under root (see SweepOrphanPIDPrefixedDirs), then creates and returns a
// fresh one plus the *os.File holding its alive sentinel. The caller must
// keep the returned file referenced for as long as dir must stay protected
// from a concurrent sibling's sweep -- the runtime finalizes unreachable
// os.Files, which releases the flock. Sweep removal messages are written to
// diagnostics.
//
// The setup-complete marker is written last on purpose, and
// SweepOrphanPIDPrefixedDirs depends on that order: a marker can only exist
// once the flock was held, so a marker beside a free sentinel proves the owner
// is gone rather than still starting up. That is what lets the sweep reclaim a
// dead socket parent immediately instead of waiting out
// socketParentSweepMinAge.
func NewSocketParentDir(root string, diagnostics io.Writer) (dir string, sentinel *os.File, err error) {
	SweepOrphanPIDPrefixedDirs(root, SocketParentDirPrefix, diagnostics)
	dir, err = os.MkdirTemp(root, PIDPrefixedTempPattern(SocketParentDirPrefix))
	if err != nil {
		return "", nil, err
	}
	sentinel, err = HoldAliveSentinel(dir)
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, err
	}
	marker := filepath.Join(dir, socketParentSetupCompleteMarkerName)
	if err := os.WriteFile(marker, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		_ = sentinel.Close()
		_ = os.RemoveAll(dir)
		return "", nil, fmt.Errorf("writing setup-complete marker in %q: %w", dir, err)
	}
	return dir, sentinel, nil
}
