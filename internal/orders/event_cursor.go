package orders

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// EventCursorFileName is the runtime-dir-relative file holding per-order event
// cursors: a JSON object mapping each event order's scoped name to its
// last-processed event seq. One record per order, updated in place — event
// orders persist their cursor here instead of minting a tracking bead per fire.
const EventCursorFileName = "order-event-cursors.json"

// eventCursorMu serializes read-modify-write of the cursor file within a process.
var eventCursorMu sync.Mutex

// EventCursorPath returns the cursor file path under a city runtime dir.
func EventCursorPath(runtimeDir string) string {
	return filepath.Join(runtimeDir, EventCursorFileName)
}

// ReadEventCursor returns the last-processed event seq for a scoped order name,
// 0 if the order has no cursor or the file does not exist.
func ReadEventCursor(runtimeDir, scoped string) (uint64, error) {
	m, err := loadEventCursors(EventCursorPath(runtimeDir))
	if err != nil {
		return 0, err
	}
	return m[scoped], nil
}

// EventCursorFunc returns a CursorFunc backed by the cursor file. A read error
// surfaces as 0 (fail-open: checkEvent then treats matching events as
// unprocessed rather than skipping them).
func EventCursorFunc(runtimeDir string) CursorFunc {
	return func(scoped string) uint64 {
		seq, err := ReadEventCursor(runtimeDir, scoped)
		if err != nil {
			runtimeHelpersLogf("orders: event cursor read failed for %s: %v", scoped, err)
			return 0
		}
		return seq
	}
}

// AdvanceEventCursor moves a scoped order's cursor to seq when seq is higher,
// writing the file atomically. The advance is monotonic so a stale writer
// cannot move the cursor backward and replay events.
func AdvanceEventCursor(runtimeDir, scoped string, seq uint64) error {
	eventCursorMu.Lock()
	defer eventCursorMu.Unlock()
	path := EventCursorPath(runtimeDir)
	m, err := loadEventCursors(path)
	if err != nil {
		return err
	}
	if seq <= m[scoped] {
		return nil
	}
	m[scoped] = seq
	return writeEventCursors(path, m)
}

func loadEventCursors(path string) (map[string]uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]uint64{}, nil
		}
		return nil, fmt.Errorf("reading event cursors %q: %w", path, err)
	}
	m := map[string]uint64{}
	if len(data) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parsing event cursors %q: %w", path, err)
	}
	return m, nil
}

func writeEventCursors(path string, m map[string]uint64) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating runtime dir %q: %w", dir, err)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encoding event cursors: %w", err)
	}
	tmp, err := os.CreateTemp(dir, EventCursorFileName+".*.tmp")
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
