package orders

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

func TestReadLastRunMissingFileIsZero(t *testing.T) {
	dir := t.TempDir()
	last, err := ReadLastRun(dir, "dolt-health")
	if err != nil {
		t.Fatalf("ReadLastRun on missing file: %v", err)
	}
	if !last.IsZero() {
		t.Fatalf("missing cursor = %v, want zero time", last)
	}
}

func TestAdvanceLastRunPersistsAndReads(t *testing.T) {
	dir := t.TempDir()
	want := time.Unix(1_700_000_000, 0).UTC()
	if err := AdvanceLastRun(dir, "dolt-health", want); err != nil {
		t.Fatalf("AdvanceLastRun: %v", err)
	}
	got, err := ReadLastRun(dir, "dolt-health")
	if err != nil {
		t.Fatalf("ReadLastRun: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("cursor = %v, want %v", got, want)
	}
}

// The cursor must survive a controller restart: a fresh read of the file (no
// in-memory state) returns the persisted value.
func TestAdvanceLastRunDurableAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	want := time.Unix(1_700_000_500, 250).UTC()
	if err := AdvanceLastRun(dir, "gate-sweep", want); err != nil {
		t.Fatalf("AdvanceLastRun: %v", err)
	}
	// Simulate restart: read straight off disk, no in-process cache.
	got, err := ReadLastRun(dir, "gate-sweep")
	if err != nil {
		t.Fatalf("ReadLastRun post-restart: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("post-restart cursor = %v, want %v", got, want)
	}
}

func TestAdvanceLastRunMonotonic(t *testing.T) {
	dir := t.TempDir()
	high := time.Unix(1_700_000_100, 0).UTC()
	low := time.Unix(1_700_000_000, 0).UTC()
	if err := AdvanceLastRun(dir, "o", high); err != nil {
		t.Fatalf("AdvanceLastRun high: %v", err)
	}
	// An earlier time must not move the cursor backward (no re-fire of a
	// cooldown order that already ran more recently).
	if err := AdvanceLastRun(dir, "o", low); err != nil {
		t.Fatalf("AdvanceLastRun low: %v", err)
	}
	got, err := ReadLastRun(dir, "o")
	if err != nil {
		t.Fatalf("ReadLastRun: %v", err)
	}
	if !got.Equal(high) {
		t.Fatalf("cursor moved backward: %v, want %v", got, high)
	}
	// An equal time is also a no-op.
	if err := AdvanceLastRun(dir, "o", high); err != nil {
		t.Fatalf("AdvanceLastRun equal: %v", err)
	}
	if got, _ := ReadLastRun(dir, "o"); !got.Equal(high) {
		t.Fatalf("cursor after equal advance: %v, want %v", got, high)
	}
}

// A zero (never-run) time must never be stored: the absence of a key is the
// canonical "never run" signal, so a zero advance is a no-op.
func TestAdvanceLastRunIgnoresZeroTime(t *testing.T) {
	dir := t.TempDir()
	if err := AdvanceLastRun(dir, "o", time.Time{}); err != nil {
		t.Fatalf("AdvanceLastRun zero: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, LastRunCursorFileName)); !os.IsNotExist(err) {
		t.Fatalf("zero advance created a cursor file (err=%v); want no file", err)
	}
	got, err := ReadLastRun(dir, "o")
	if err != nil {
		t.Fatalf("ReadLastRun: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("cursor = %v after zero advance, want zero time", got)
	}
}

func TestAdvanceLastRunIndependentOrders(t *testing.T) {
	dir := t.TempDir()
	ta := time.Unix(1_700_000_001, 0).UTC()
	tb := time.Unix(1_700_000_002, 0).UTC()
	if err := AdvanceLastRun(dir, "a", ta); err != nil {
		t.Fatalf("AdvanceLastRun a: %v", err)
	}
	if err := AdvanceLastRun(dir, "b", tb); err != nil {
		t.Fatalf("AdvanceLastRun b: %v", err)
	}
	if got, _ := ReadLastRun(dir, "a"); !got.Equal(ta) {
		t.Fatalf("cursor a = %v, want %v", got, ta)
	}
	if got, _ := ReadLastRun(dir, "b"); !got.Equal(tb) {
		t.Fatalf("cursor b = %v, want %v", got, tb)
	}
	// Rig-scoped names (containing ':') round-trip as JSON keys.
	ts := time.Unix(1_700_000_009, 0).UTC()
	if err := AdvanceLastRun(dir, "doc-keeper-drift-audit:rig:gc-toolkit", ts); err != nil {
		t.Fatalf("AdvanceLastRun scoped: %v", err)
	}
	if got, _ := ReadLastRun(dir, "doc-keeper-drift-audit:rig:gc-toolkit"); !got.Equal(ts) {
		t.Fatalf("scoped cursor = %v, want %v", got, ts)
	}
}

func TestAdvanceLastRunWritesValidJSON(t *testing.T) {
	dir := t.TempDir()
	want := time.Unix(1_700_000_000, 123).UTC()
	if err := AdvanceLastRun(dir, "a", want); err != nil {
		t.Fatalf("AdvanceLastRun: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, LastRunCursorFileName))
	if err != nil {
		t.Fatalf("reading cursor file: %v", err)
	}
	var m map[string]int64
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("cursor file is not valid JSON: %v (%s)", err, data)
	}
	if m["a"] != want.UnixNano() {
		t.Fatalf("decoded cursor = %d, want %d", m["a"], want.UnixNano())
	}
}

func TestReadLastRunCorruptFileErrors(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, LastRunCursorFileName), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("seeding corrupt file: %v", err)
	}
	if _, err := ReadLastRun(dir, "a"); err == nil {
		t.Fatal("ReadLastRun on corrupt file: want error, got nil")
	}
}

// The cursor file is written from multiple processes (controller dispatch and
// manual `gc order run`). AdvanceLastRun must serialize the whole
// load/merge/write with an on-disk lock so a concurrent process cannot load the
// same map and clobber an update via the later rename. We simulate the other
// process by holding the on-disk lock directly: AdvanceLastRun must block until
// it is released.
func TestAdvanceLastRunHoldsCrossProcessLock(t *testing.T) {
	dir := t.TempDir()
	path := LastRunCursorPath(dir)

	external := beads.NewFileFlock(path + ".lock")
	if err := external.Lock(); err != nil {
		t.Fatalf("acquiring external lock: %v", err)
	}

	when := time.Unix(1_700_000_000, 0).UTC()
	done := make(chan error, 1)
	go func() { done <- AdvanceLastRun(dir, "o", when) }()

	select {
	case err := <-done:
		_ = external.Unlock() //nolint:errcheck // test cleanup on the failure path
		t.Fatalf("AdvanceLastRun completed while cross-process lock held (err=%v); write not serialized", err)
	case <-time.After(150 * time.Millisecond):
		// Expected: blocked on the on-disk lock.
	}

	if err := external.Unlock(); err != nil {
		t.Fatalf("releasing external lock: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AdvanceLastRun after lock release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AdvanceLastRun did not complete after lock release (possible deadlock)")
	}

	if got, _ := ReadLastRun(dir, "o"); !got.Equal(when) {
		t.Fatalf("cursor = %v, want %v", got, when)
	}
}
