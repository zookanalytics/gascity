package orders

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

// LastRunCursorFileName is the runtime-dir-relative file holding per-order
// last-run cursors: a JSON object mapping each cooldown/cron order's scoped name
// to the Unix-nanosecond timestamp of its most recent dispatch. One record per
// order, updated in place — cooldown/cron orders persist their cooldown clock
// here instead of minting a retained tracking bead per fire (gc-7hf34). A file,
// not a bead, so advancing it emits no bead event and cannot self-trigger the
// controller's bead-event watcher (gc-k8r4y).
const LastRunCursorFileName = "order-lastrun-cursors.json"

// lastRunCursorMu serializes read-modify-write of the cursor file within a process.
var lastRunCursorMu sync.Mutex

// lastRunCursorLockSuffix names the sidecar flock file that serializes cursor
// updates across processes. It is a separate, never-renamed file because the
// cursor file itself is replaced via os.Rename on every write, and an flock
// follows the open file description (inode), not the path.
const lastRunCursorLockSuffix = ".lock"

// LastRunCursorPath returns the cursor file path under a city runtime dir.
func LastRunCursorPath(runtimeDir string) string {
	return filepath.Join(runtimeDir, LastRunCursorFileName)
}

// ReadLastRun returns the most recent dispatch time for a scoped order name, or
// the zero time if the order has no cursor or the file does not exist. A corrupt
// or unreadable file surfaces as an error so callers can fall back to the legacy
// tracking-bead history rather than silently treating a recently-run order as
// never run.
func ReadLastRun(runtimeDir, scoped string) (time.Time, error) {
	m, err := loadLastRunCursors(LastRunCursorPath(runtimeDir))
	if err != nil {
		return time.Time{}, err
	}
	nanos := m[scoped]
	if nanos <= 0 {
		return time.Time{}, nil
	}
	return time.Unix(0, nanos).UTC(), nil
}

// AdvanceLastRun moves a scoped order's cursor to when if it is later than the
// stored value, writing the file atomically. The advance is monotonic so a
// stale writer cannot move the cursor backward and re-fire a cooldown order that
// already ran more recently. A zero (never-run) time is a no-op — the absence of
// a key is the canonical "never run" signal.
//
// The cursor file is written from multiple processes — controller dispatch and
// manual `gc order run` — so the in-process mutex alone cannot prevent a lost
// update: two processes could load the same map, each write a temp file, and the
// later os.Rename would drop the earlier process's advance, regressing a cursor
// and re-firing an order after a restart. The whole load/merge/write is
// therefore serialized across processes with an on-disk flock as well.
func AdvanceLastRun(runtimeDir, scoped string, when time.Time) error {
	if when.IsZero() {
		return nil
	}
	nanos := when.UnixNano()
	if nanos <= 0 {
		return nil
	}

	lastRunCursorMu.Lock()
	defer lastRunCursorMu.Unlock()
	path := LastRunCursorPath(runtimeDir)

	// The flock file lives in the runtime dir; ensure it exists before opening
	// the lock so the first write does not fail on a missing directory.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating runtime dir %q: %w", filepath.Dir(path), err)
	}
	lock := beads.NewFileFlock(path + lastRunCursorLockSuffix)
	if err := lock.Lock(); err != nil {
		return fmt.Errorf("locking last-run cursors %q: %w", path, err)
	}
	defer lock.Unlock() //nolint:errcheck // best-effort unlock

	m, err := loadLastRunCursors(path)
	if err != nil {
		return err
	}
	if nanos <= m[scoped] {
		return nil
	}
	m[scoped] = nanos
	return writeLastRunCursors(path, m)
}

func loadLastRunCursors(path string) (map[string]int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]int64{}, nil
		}
		return nil, fmt.Errorf("reading last-run cursors %q: %w", path, err)
	}
	m := map[string]int64{}
	if len(data) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parsing last-run cursors %q: %w", path, err)
	}
	return m, nil
}

func writeLastRunCursors(path string, m map[string]int64) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating runtime dir %q: %w", dir, err)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encoding last-run cursors: %w", err)
	}
	tmp, err := os.CreateTemp(dir, LastRunCursorFileName+".*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp cursor file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck // best-effort; no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close() //nolint:errcheck // already returning the write error
		return fmt.Errorf("writing temp cursor file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp cursor file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("renaming cursor file into place: %w", err)
	}
	return nil
}
