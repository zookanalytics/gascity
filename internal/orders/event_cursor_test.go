package orders

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

func TestReadEventCursorMissingFileIsZero(t *testing.T) {
	dir := t.TempDir()
	seq, err := ReadEventCursor(dir, "nudge-on-route")
	if err != nil {
		t.Fatalf("ReadEventCursor on missing file: %v", err)
	}
	if seq != 0 {
		t.Fatalf("missing cursor = %d, want 0", seq)
	}
}

func TestAdvanceEventCursorPersistsAndReads(t *testing.T) {
	dir := t.TempDir()
	if err := AdvanceEventCursor(dir, "nudge-on-route", 42); err != nil {
		t.Fatalf("AdvanceEventCursor: %v", err)
	}
	seq, err := ReadEventCursor(dir, "nudge-on-route")
	if err != nil {
		t.Fatalf("ReadEventCursor: %v", err)
	}
	if seq != 42 {
		t.Fatalf("cursor = %d, want 42", seq)
	}
}

// The cursor must survive a controller restart: a fresh read of the file
// (no in-memory state) returns the persisted value.
func TestAdvanceEventCursorDurableAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	if err := AdvanceEventCursor(dir, "cascade-nudge", 7); err != nil {
		t.Fatalf("AdvanceEventCursor: %v", err)
	}
	// Simulate restart: read straight off disk via EventCursorFunc.
	if got := EventCursorFunc(dir)("cascade-nudge"); got != 7 {
		t.Fatalf("post-restart cursor = %d, want 7", got)
	}
}

func TestAdvanceEventCursorMonotonic(t *testing.T) {
	dir := t.TempDir()
	if err := AdvanceEventCursor(dir, "o", 100); err != nil {
		t.Fatalf("AdvanceEventCursor 100: %v", err)
	}
	// A lower seq must not move the cursor backward (no re-processing).
	if err := AdvanceEventCursor(dir, "o", 50); err != nil {
		t.Fatalf("AdvanceEventCursor 50: %v", err)
	}
	if got := EventCursorFunc(dir)("o"); got != 100 {
		t.Fatalf("cursor moved backward: %d, want 100", got)
	}
	// An equal seq is also a no-op.
	if err := AdvanceEventCursor(dir, "o", 100); err != nil {
		t.Fatalf("AdvanceEventCursor 100 again: %v", err)
	}
	if got := EventCursorFunc(dir)("o"); got != 100 {
		t.Fatalf("cursor after equal advance: %d, want 100", got)
	}
}

func TestAdvanceEventCursorIndependentOrders(t *testing.T) {
	dir := t.TempDir()
	if err := AdvanceEventCursor(dir, "a", 1); err != nil {
		t.Fatalf("AdvanceEventCursor a: %v", err)
	}
	if err := AdvanceEventCursor(dir, "b", 2); err != nil {
		t.Fatalf("AdvanceEventCursor b: %v", err)
	}
	cf := EventCursorFunc(dir)
	if cf("a") != 1 || cf("b") != 2 {
		t.Fatalf("independent cursors: a=%d b=%d, want a=1 b=2", cf("a"), cf("b"))
	}
	// Rig-scoped names (containing ':') round-trip as JSON keys.
	if err := AdvanceEventCursor(dir, "dolt-health:rig:demo", 9); err != nil {
		t.Fatalf("AdvanceEventCursor scoped: %v", err)
	}
	if got := EventCursorFunc(dir)("dolt-health:rig:demo"); got != 9 {
		t.Fatalf("scoped cursor = %d, want 9", got)
	}
}

func TestAdvanceEventCursorWritesValidJSON(t *testing.T) {
	dir := t.TempDir()
	if err := AdvanceEventCursor(dir, "a", 1); err != nil {
		t.Fatalf("AdvanceEventCursor: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, EventCursorFileName))
	if err != nil {
		t.Fatalf("reading cursor file: %v", err)
	}
	var m map[string]uint64
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("cursor file is not valid JSON: %v (%s)", err, data)
	}
	if m["a"] != 1 {
		t.Fatalf("decoded cursor = %d, want 1", m["a"])
	}
}

// The cursor file is written from multiple processes (controller dispatch and
// manual `gc order run`). AdvanceEventCursor must serialize the whole
// load/merge/write with an on-disk lock so a concurrent process cannot load the
// same map and clobber an update via the later rename. We simulate the other
// process by holding the on-disk lock directly: AdvanceEventCursor must block
// until it is released. flock(2) LOCK_EX blocks across distinct open file
// descriptions even within one process, so this exercises the cross-process path.
func TestAdvanceEventCursorHoldsCrossProcessLock(t *testing.T) {
	dir := t.TempDir()
	path := EventCursorPath(dir)

	// Stand in for another process holding the cursor lock.
	external := beads.NewFileFlock(path + ".lock")
	if err := external.Lock(); err != nil {
		t.Fatalf("acquiring external lock: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- AdvanceEventCursor(dir, "o", 5) }()

	// While the external lock is held the advance must not complete.
	select {
	case err := <-done:
		_ = external.Unlock() //nolint:errcheck // test cleanup on the failure path
		t.Fatalf("AdvanceEventCursor completed while cross-process lock held (err=%v); write not serialized", err)
	case <-time.After(150 * time.Millisecond):
		// Expected: blocked on the on-disk lock.
	}

	if err := external.Unlock(); err != nil {
		t.Fatalf("releasing external lock: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AdvanceEventCursor after lock release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AdvanceEventCursor did not complete after lock release (possible deadlock)")
	}

	if got := EventCursorFunc(dir)("o"); got != 5 {
		t.Fatalf("cursor = %d, want 5", got)
	}
}

func TestEventCursorFuncReadErrorIsZero(t *testing.T) {
	dir := t.TempDir()
	// A corrupt cursor file fails the read; EventCursorFunc surfaces 0 so
	// checkEvent treats events as unprocessed (fail-open) rather than panicking.
	if err := os.WriteFile(filepath.Join(dir, EventCursorFileName), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("seeding corrupt file: %v", err)
	}
	if got := EventCursorFunc(dir)("a"); got != 0 {
		t.Fatalf("corrupt-file cursor = %d, want 0", got)
	}
}
