package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/citylayout"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/orders"
)

// newLegacyEventCursorStore returns a store holding a single closed
// order:<scoped> + seq:<N> tracking bead — the pre-file-cursor durable cursor an
// existing city carries for an event order.
func newLegacyEventCursorStore(t *testing.T, scoped string, seq uint64) beads.Store {
	t.Helper()
	store := beads.NewMemStore()
	b, err := store.Create(beads.Bead{
		Title:     "order:" + scoped,
		Labels:    []string{"order:" + scoped, fmt.Sprintf("seq:%d", seq)},
		NoHistory: true,
	})
	if err != nil {
		t.Fatalf("creating legacy cursor bead: %v", err)
	}
	// Legacy cursor beads are closed; bdCursor reads them with IncludeClosed.
	if err := store.Close(b.ID); err != nil {
		t.Fatalf("closing legacy cursor bead: %v", err)
	}
	return store
}

func TestEventCursorWithLegacyFallback(t *testing.T) {
	t.Run("file cursor wins and legacy is not consulted", func(t *testing.T) {
		dir := t.TempDir()
		if err := orders.AdvanceEventCursor(dir, "watch", 7); err != nil {
			t.Fatalf("AdvanceEventCursor: %v", err)
		}
		// A store that errors on every matching list proves the legacy path is
		// never taken once the file cursor exists.
		failing := labelFailListStore{Store: beads.NewMemStore(), failLabel: "order:watch"}
		got, err := eventCursorWithLegacyFallback(dir, "watch", true, failing)
		if err != nil {
			t.Fatalf("eventCursorWithLegacyFallback: %v", err)
		}
		if got != 7 {
			t.Fatalf("cursor = %d, want 7 (file cursor)", got)
		}
	})

	t.Run("missing file falls back to legacy without seeding when seed is false", func(t *testing.T) {
		dir := t.TempDir()
		store := newLegacyEventCursorStore(t, "watch", 5)
		got, err := eventCursorWithLegacyFallback(dir, "watch", false, store)
		if err != nil {
			t.Fatalf("eventCursorWithLegacyFallback: %v", err)
		}
		if got != 5 {
			t.Fatalf("cursor = %d, want 5 (legacy fallback)", got)
		}
		// Read-only callers must not write the cursor file.
		if seq, err := orders.ReadEventCursor(dir, "watch"); err != nil || seq != 0 {
			t.Fatalf("file cursor = %d (err %v), want 0 (not seeded)", seq, err)
		}
	})

	t.Run("missing file seeds the file from legacy when seed is true", func(t *testing.T) {
		dir := t.TempDir()
		store := newLegacyEventCursorStore(t, "watch", 5)
		got, err := eventCursorWithLegacyFallback(dir, "watch", true, store)
		if err != nil {
			t.Fatalf("eventCursorWithLegacyFallback: %v", err)
		}
		if got != 5 {
			t.Fatalf("cursor = %d, want 5 (legacy fallback)", got)
		}
		// The durable cursor is now file-backed and survives legacy-bead pruning.
		if seq, err := orders.ReadEventCursor(dir, "watch"); err != nil || seq != 5 {
			t.Fatalf("seeded file cursor = %d (err %v), want 5", seq, err)
		}
	})

	t.Run("no legacy beads and no file yields zero", func(t *testing.T) {
		dir := t.TempDir()
		got, err := eventCursorWithLegacyFallback(dir, "watch", true, beads.NewMemStore())
		if err != nil {
			t.Fatalf("eventCursorWithLegacyFallback: %v", err)
		}
		if got != 0 {
			t.Fatalf("cursor = %d, want 0 (no file, no legacy)", got)
		}
	})

	t.Run("legacy read error propagates so the caller fails closed", func(t *testing.T) {
		dir := t.TempDir()
		failing := labelFailListStore{Store: beads.NewMemStore(), failLabel: "order:watch"}
		if _, err := eventCursorWithLegacyFallback(dir, "watch", false, failing); err == nil {
			t.Fatal("expected error when the legacy cursor read fails, got nil")
		}
	})
}

// An existing city carries its last-processed event seq in a legacy
// order:<scoped> + seq:<N> tracking bead. On the first dispatch after upgrading
// to the durable file cursor the file is absent; treating it as 0 would replay
// every historical matching event once. Dispatch must fall back to the legacy
// bead cursor (suppressing the replay) and seed the file so the durable cursor
// survives later pruning of the legacy bead. A genuinely new event still fires.
func TestOrderDispatchEventCursorMigratesFromLegacyTrackingBead(t *testing.T) {
	store := beads.NewMemStore()
	eventLog := events.NewFake()
	eventLog.Record(events.Event{Type: events.BeadClosed, Actor: "test"})
	headSeq, err := eventLog.LatestSeq()
	if err != nil {
		t.Fatalf("LatestSeq(): %v", err)
	}

	// Legacy cursor already covers the whole historical event window.
	legacy, err := store.Create(beads.Bead{
		Title:     "order:release-exec",
		Labels:    []string{"order:release-exec", fmt.Sprintf("seq:%d", headSeq)},
		NoHistory: true,
	})
	if err != nil {
		t.Fatalf("creating legacy cursor bead: %v", err)
	}
	if err := store.Close(legacy.ID); err != nil {
		t.Fatalf("closing legacy cursor bead: %v", err)
	}

	var calls int
	execRun := func(context.Context, string, string, []string) ([]byte, error) {
		calls++
		return []byte("ok"), nil
	}

	ad := buildOrderDispatcherFromListExec([]orders.Order{{
		Name:    "release-exec",
		Trigger: "event",
		On:      events.BeadClosed,
		Exec:    "scripts/release.sh",
	}}, store, eventLog, execRun, events.Discard)
	if ad == nil {
		t.Fatal("expected non-nil dispatcher")
	}

	cityPath := t.TempDir()
	ad.dispatch(context.Background(), cityPath, time.Now())
	ad.drain(context.Background())

	// Legacy cursor covers the window: the order is not due and must not replay.
	if calls != 0 {
		t.Fatalf("exec calls after first dispatch = %d, want 0 (legacy cursor suppresses replay)", calls)
	}
	// The file cursor is seeded from the legacy bead even though the order did
	// not fire, so future reads are file-only.
	if seq, err := orders.ReadEventCursor(citylayout.RuntimeDataDir(cityPath), "release-exec"); err != nil || seq != headSeq {
		t.Fatalf("seeded file cursor = %d (err %v), want %d", seq, err, headSeq)
	}

	// A genuinely new event past the migrated cursor fires exactly once and
	// advances the file cursor to the new head.
	eventLog.Record(events.Event{Type: events.BeadClosed, Actor: "test"})
	newHead, err := eventLog.LatestSeq()
	if err != nil {
		t.Fatalf("LatestSeq(): %v", err)
	}
	ad.dispatch(context.Background(), cityPath, time.Now().Add(10*time.Second))
	ad.drain(context.Background())
	if calls != 1 {
		t.Fatalf("exec calls after new event = %d, want 1 (fires once for the new event)", calls)
	}
	if seq, err := orders.ReadEventCursor(citylayout.RuntimeDataDir(cityPath), "release-exec"); err != nil || seq != newHead {
		t.Fatalf("file cursor after new event = %d (err %v), want %d", seq, err, newHead)
	}
}

func TestBdCursorUsesRowsFromPartialTierError(t *testing.T) {
	store := &partialListStore{
		Store: beads.NewMemStore(),
		rows: []beads.Bead{{
			ID:     "cursor-1",
			Labels: []string{"order:digest", "seq:42"},
		}},
		err: fmt.Errorf("wisps tier unavailable"),
	}

	got, err := bdCursor(store, "digest")
	if err != nil {
		t.Fatalf("bdCursor: %v", err)
	}
	if got != 42 {
		t.Fatalf("bdCursor() = %d, want 42 from surviving rows", got)
	}
}

// The legacy fallback fails closed: when the file cursor is absent and the
// legacy cursor read errors, gc order check reports the error and exits non-zero
// rather than treating the cursor as 0 and replaying the event window.
func TestOrderCheckWithStoresResolverFailsWhenLegacyEventCursorReadFails(t *testing.T) {
	rigStore := beads.NewMemStore()
	legacyStore := labelFailListStore{
		Store:     beads.NewMemStore(),
		failLabel: "order:watch:rig:frontend",
	}
	eventLog := events.NewFake()
	eventLog.Record(events.Event{Type: events.BeadClosed, Actor: "test"})

	aa := []orders.Order{{
		Name:    "watch",
		Rig:     "frontend",
		Trigger: "event",
		On:      events.BeadClosed,
		Formula: "mol-watch",
	}}
	resolver := func(a orders.Order) ([]beads.Store, error) {
		if a.Rig == "frontend" {
			return []beads.Store{rigStore, legacyStore}, nil
		}
		return []beads.Store{rigStore}, nil
	}

	var stdout, stderr bytes.Buffer
	code := doOrderCheckWithStoresResolver(aa, time.Now(), eventLog, resolver, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("doOrderCheckWithStoresResolver = %d, want 1 when legacy event cursor cannot be read; stdout: %s", code, stdout.String())
	}
	if !strings.Contains(stderr.String(), "event cursor") {
		t.Fatalf("stderr missing event cursor error:\n%s", stderr.String())
	}
}

// Dispatch is the mutating mirror of the check path: a failing legacy cursor
// read (with no file cursor yet) must skip the fire rather than replay.
func TestOrderDispatchSkipsRigEventWhenLegacyCursorReadFails(t *testing.T) {
	rigStore := beads.NewMemStore()
	legacyStore := labelFailListStore{
		Store:     beads.NewMemStore(),
		failLabel: "order:release-watch:rig:frontend",
	}
	eventLog := events.NewFake()
	eventLog.Record(events.Event{Type: events.BeadClosed, Actor: "test"})

	stderr := &bytes.Buffer{}
	m := &memoryOrderDispatcher{
		aa: []orders.Order{{
			Name:    "release-watch",
			Rig:     "frontend",
			Trigger: "event",
			On:      events.BeadClosed,
			Exec:    "true",
			Pool:    "worker",
			Timeout: "1m",
		}},
		storeFn: func(target execStoreTarget) (beads.Store, error) {
			if target.ScopeKind == "city" {
				return legacyStore, nil
			}
			return rigStore, nil
		},
		ep:      eventLog,
		execRun: successfulExec,
		rec:     events.Discard,
		stderr:  stderr,
		cfg: &config.City{
			Rigs: []config.Rig{{
				Name: "frontend",
				Path: "frontend",
			}},
		},
	}

	m.dispatch(context.Background(), t.TempDir(), time.Now())
	m.drain(context.Background())

	rigRuns := trackingBeads(t, rigStore, "order-run:release-watch:rig:frontend")
	if len(rigRuns) != 0 {
		t.Fatalf("rig store has %d new run bead(s), want 0 when legacy event cursor cannot be read", len(rigRuns))
	}
	if !strings.Contains(stderr.String(), "event cursor") {
		t.Fatalf("stderr missing event cursor error:\n%s", stderr.String())
	}
}
