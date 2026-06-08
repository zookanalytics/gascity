package main

import (
	"context"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
)

// waitForCondition polls cond until it returns true or the deadline elapses.
// The autoclose watcher runs asynchronously, so tests assert its effects
// with a bounded poll rather than a fixed sleep.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func mustStatus(t *testing.T, store beads.Store, id string) string {
	t.Helper()
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return b.Status
}

func mustMeta(t *testing.T, store beads.Store, id, key string) string {
	t.Helper()
	b, err := store.Get(id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return b.Metadata[key]
}

// --- runAutocloseCores parity with the bd on_close hook ---

// TestRunAutocloseCoresConvoyClosesWhenAllChildrenClosed pins parity with the
// hook's `gc convoy autoclose`: the parent convoy auto-closes exactly when the
// last child closes, never while a sibling is still open.
func TestRunAutocloseCoresConvoyClosesWhenAllChildrenClosed(t *testing.T) {
	store := beads.NewMemStore()
	convoy, _ := store.Create(beads.Bead{Title: "batch", Type: "convoy"})
	childA, _ := store.Create(beads.Bead{Title: "task A", ParentID: convoy.ID})
	childB, _ := store.Create(beads.Bead{Title: "task B", ParentID: convoy.ID})

	// First child closes — convoy must stay open (no premature close).
	_ = store.Close(childA.ID)
	runAutocloseCores(store, events.Discard, "", childA.ID)
	if got := mustStatus(t, store, convoy.ID); got != "open" {
		t.Fatalf("convoy closed prematurely with sibling open: status=%q", got)
	}

	// Last child closes — convoy must auto-close.
	_ = store.Close(childB.ID)
	runAutocloseCores(store, events.Discard, "", childB.ID)
	if got := mustStatus(t, store, convoy.ID); got != "closed" {
		t.Fatalf("convoy not auto-closed after all children closed: status=%q", got)
	}
}

// TestRunAutocloseCoresMoleculeClosesWhenAllStepsTerminal pins parity with the
// hook's `gc molecule autoclose` (gastownhall/gascity#1039): the molecule root
// auto-closes once every step is terminal.
func TestRunAutocloseCoresMoleculeClosesWhenAllStepsTerminal(t *testing.T) {
	store := beads.NewMemStore()
	root, _ := store.Create(beads.Bead{Title: "mol", Type: "molecule"})
	stepA, _ := store.Create(beads.Bead{Title: "load", Type: "step", ParentID: root.ID})
	stepB, _ := store.Create(beads.Bead{Title: "test", Type: "step", ParentID: root.ID})

	_ = store.Close(stepA.ID)
	runAutocloseCores(store, events.Discard, "", stepA.ID)
	if got := mustStatus(t, store, root.ID); got != "open" {
		t.Fatalf("molecule closed prematurely with open step: status=%q", got)
	}

	_ = store.Close(stepB.ID)
	runAutocloseCores(store, events.Discard, "", stepB.ID)
	if got := mustStatus(t, store, root.ID); got != "closed" {
		t.Fatalf("molecule not auto-closed after all steps terminal: status=%q", got)
	}
	if reason := mustMeta(t, store, root.ID, "close_reason"); reason != moleculeAutocloseReason {
		t.Errorf("close_reason = %q, want %q", reason, moleculeAutocloseReason)
	}
}

// TestRunAutocloseCoresWispClosesAttachedMolecule pins parity with the hook's
// `gc wisp autoclose`: an open molecule attached to the closed work bead is
// force-closed so it does not outlive its parent.
func TestRunAutocloseCoresWispClosesAttachedMolecule(t *testing.T) {
	store := beads.NewMemStore()
	work, _ := store.Create(beads.Bead{Title: "work item"})
	wisp, _ := store.Create(beads.Bead{Title: "wisp", Type: "molecule", ParentID: work.ID})

	_ = store.Close(work.ID)
	runAutocloseCores(store, events.Discard, "", work.ID)

	if got := mustStatus(t, store, wisp.ID); got != "closed" {
		t.Fatalf("attached wisp not force-closed: status=%q", got)
	}
}

// TestRunAutocloseCoresIdempotent asserts re-running the cores on an
// already-cascaded close is a no-op — the property that lets the consumer run
// safely alongside the hook (hook mode) and reprocess replayed events after a
// checkpoint resume.
func TestRunAutocloseCoresIdempotent(t *testing.T) {
	store := beads.NewMemStore()
	convoy, _ := store.Create(beads.Bead{Title: "batch", Type: "convoy"})
	child, _ := store.Create(beads.Bead{Title: "task", ParentID: convoy.ID})
	_ = store.Close(child.ID)

	runAutocloseCores(store, events.Discard, "", child.ID)
	first := mustStatus(t, store, convoy.ID)
	runAutocloseCores(store, events.Discard, "", child.ID)
	second := mustStatus(t, store, convoy.ID)

	if first != "closed" || second != "closed" {
		t.Fatalf("convoy status not stable across repeated runs: first=%q second=%q", first, second)
	}
}

// TestRunAutocloseCoresNilStoreAndEmptyID guards the defensive early returns so
// a malformed event never panics the watcher loop.
func TestRunAutocloseCoresNilStoreAndEmptyID(_ *testing.T) {
	runAutocloseCores(nil, events.Discard, "", "gc-1") // nil store
	store := beads.NewMemStore()
	runAutocloseCores(store, events.Discard, "", "") // empty id
	runAutocloseCores(store, nil, "", "gc-1")        // nil recorder defaults to Discard
}

// --- resolveAutocloseTargets per-rig routing ---

func twoRigCity() *config.City {
	return &config.City{
		Workspace: config.Workspace{Name: "tc", Prefix: "hq"},
		Rigs: []config.Rig{
			{Name: "alpha", Prefix: "al"},
			{Name: "beta", Prefix: "bt"},
		},
	}
}

// TestResolveAutocloseTargetsRoutesByPrefix asserts a bead resolves to exactly
// the store that owns its ID prefix — a close in one rig never scans another.
func TestResolveAutocloseTargetsRoutesByPrefix(t *testing.T) {
	cfg := twoRigCity()
	cityStore := beads.NewMemStore()
	alpha := beads.NewMemStore()
	beta := beads.NewMemStore()
	rigStores := map[string]beads.Store{"alpha": alpha, "beta": beta}

	cases := []struct {
		id        string
		wantStore beads.Store
		wantRef   string
	}{
		{"al-123", alpha, "rig:alpha"},
		{"bt-456", beta, "rig:beta"},
		{"hq-789", cityStore, "city:tc"},
	}
	for _, tc := range cases {
		got := resolveAutocloseTargets(cfg, "tc", cityStore, rigStores, tc.id)
		if len(got) != 1 {
			t.Fatalf("id %s: got %d targets, want 1", tc.id, len(got))
		}
		if got[0].store != tc.wantStore {
			t.Errorf("id %s: routed to wrong store", tc.id)
		}
		if got[0].storeRef != tc.wantRef {
			t.Errorf("id %s: storeRef = %q, want %q", tc.id, got[0].storeRef, tc.wantRef)
		}
	}
}

// TestResolveAutocloseTargetsLongestPrefixWins asserts overlapping prefixes
// resolve to the most specific store, matching gc bd's routing.
func TestResolveAutocloseTargetsLongestPrefixWins(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "tc", Prefix: "hq"},
		Rigs: []config.Rig{
			{Name: "short", Prefix: "gc"},
			{Name: "long", Prefix: "gctk"},
		},
	}
	short := beads.NewMemStore()
	long := beads.NewMemStore()
	got := resolveAutocloseTargets(cfg, "tc", nil, map[string]beads.Store{"short": short, "long": long}, "gctk-1")
	if len(got) != 1 || got[0].store != long {
		t.Fatalf("gctk-1 did not resolve to the longer-prefix store: %+v", got)
	}
}

// TestResolveAutocloseTargetsUnknownPrefixFansOut asserts an unrecognized ID
// falls back to every configured store (each core bails when the bead is
// absent), mirroring beadEventStoresLocked's unknown-ID fan-out.
func TestResolveAutocloseTargetsUnknownPrefixFansOut(t *testing.T) {
	cfg := twoRigCity()
	cityStore := beads.NewMemStore()
	alpha := beads.NewMemStore()
	beta := beads.NewMemStore()
	rigStores := map[string]beads.Store{"alpha": alpha, "beta": beta}

	got := resolveAutocloseTargets(cfg, "tc", cityStore, rigStores, "zz-999")
	if len(got) != 3 {
		t.Fatalf("unknown prefix fan-out: got %d targets, want 3 (2 rigs + city)", len(got))
	}
}

// --- cursor persistence (restart-safe checkpointing) ---

func TestAutocloseCursorRoundTrip(t *testing.T) {
	cityDir := t.TempDir()
	if _, ok := loadAutocloseCursor(cityDir); ok {
		t.Fatal("loadAutocloseCursor on fresh city should report no cursor")
	}
	if err := saveAutocloseCursor(cityDir, 42); err != nil {
		t.Fatalf("saveAutocloseCursor: %v", err)
	}
	seq, ok := loadAutocloseCursor(cityDir)
	if !ok || seq != 42 {
		t.Fatalf("loadAutocloseCursor = (%d, %v), want (42, true)", seq, ok)
	}
	// Overwrite advances the checkpoint.
	if err := saveAutocloseCursor(cityDir, 100); err != nil {
		t.Fatalf("saveAutocloseCursor overwrite: %v", err)
	}
	if seq, _ := loadAutocloseCursor(cityDir); seq != 100 {
		t.Fatalf("cursor after overwrite = %d, want 100", seq)
	}
}

func TestAutocloseCursorEmptyCityPath(t *testing.T) {
	if err := saveAutocloseCursor("", 5); err != nil {
		t.Fatalf("saveAutocloseCursor(\"\") = %v, want nil no-op", err)
	}
	if _, ok := loadAutocloseCursor(""); ok {
		t.Fatal("loadAutocloseCursor(\"\") should report no cursor")
	}
}

// --- routing + cores wired through the controller state ---

// TestRunAutocloseForClosedBeadScopesToOwningStore asserts the consumer runs
// autoclose only against the store that owns the closed bead. Both rigs hold an
// identically-complete convoy; closing a bead in alpha must close alpha's
// convoy and leave beta's open.
func TestRunAutocloseForClosedBeadScopesToOwningStore(t *testing.T) {
	alpha := completeConvoyStore(t)
	beta := completeConvoyStore(t)
	cfg := &config.City{
		Workspace: config.Workspace{Name: "tc", Prefix: "hq"},
		Rigs: []config.Rig{
			{Name: "alpha", Prefix: "gc"}, // matches MemStore's gc-N ids
			{Name: "beta", Prefix: "bt"},
		},
	}
	cs := &controllerState{
		cfg:        cfg,
		cityName:   "tc",
		beadStores: map[string]beads.Store{"alpha": alpha, "beta": beta},
		eventProv:  events.NewFake(),
	}

	cs.runAutocloseForClosedBead(events.Event{Type: events.BeadClosed, Subject: "gc-3"})

	if got := mustStatus(t, alpha, "gc-1"); got != "closed" {
		t.Errorf("alpha convoy = %q, want closed", got)
	}
	if got := mustStatus(t, beta, "gc-1"); got != "open" {
		t.Errorf("beta convoy = %q, want open (must not be scanned)", got)
	}
}

// TestRunAutocloseForClosedBeadCascadesLevelByLevel asserts the cores climb a
// nested convoy chain one level per close. In production each parent close
// re-enters the event stream (CachingStore's change notifier), so feeding the
// parent's close here mirrors what the watcher would receive next — closing the
// grandparent only once its sole child convoy is closed.
func TestRunAutocloseForClosedBeadCascadesLevelByLevel(t *testing.T) {
	store := beads.NewMemStore()
	_, _ = store.Create(beads.Bead{Title: "grandparent", Type: "convoy"})              // gc-1
	_, _ = store.Create(beads.Bead{Title: "parent", Type: "convoy", ParentID: "gc-1"}) // gc-2
	_, _ = store.Create(beads.Bead{Title: "leaf", ParentID: "gc-2"})                   // gc-3
	cfg := &config.City{
		Workspace: config.Workspace{Name: "tc", Prefix: "hq"},
		Rigs:      []config.Rig{{Name: "r", Prefix: "gc"}},
	}
	cs := &controllerState{
		cfg:        cfg,
		cityName:   "tc",
		beadStores: map[string]beads.Store{"r": store},
		eventProv:  events.NewFake(),
	}

	// Leaf closes → parent convoy closes; grandparent stays open (its child
	// convoy only just became terminal — the next stream event drives it).
	_ = store.Close("gc-3")
	cs.runAutocloseForClosedBead(events.Event{Type: events.BeadClosed, Subject: "gc-3"})
	if got := mustStatus(t, store, "gc-2"); got != "closed" {
		t.Fatalf("parent convoy = %q, want closed", got)
	}
	if got := mustStatus(t, store, "gc-1"); got != "open" {
		t.Fatalf("grandparent closed before parent's close was observed: status=%q", got)
	}

	// Parent's close re-enters the stream → grandparent closes.
	cs.runAutocloseForClosedBead(events.Event{Type: events.BeadClosed, Subject: "gc-2"})
	if got := mustStatus(t, store, "gc-1"); got != "closed" {
		t.Fatalf("grandparent convoy not closed after parent close observed: status=%q", got)
	}
}

// TestStartAutocloseWatcherActsOnCacheReconcileActor pins the key difference
// from startBeadEventWatcher: that watcher skips its own "cache-reconcile"
// emissions, but the autoclose watcher MUST act on them — in native store mode
// (event_hooks=false, no bd hook) bead.closed is emitted with exactly that
// actor and is the only autoclose trigger.
func TestStartAutocloseWatcherActsOnCacheReconcileActor(t *testing.T) {
	cityDir := t.TempDir()
	rigStore := completeConvoyStore(t)
	cfg := &config.City{
		Workspace: config.Workspace{Name: "tc", Prefix: "hq"},
		Rigs:      []config.Rig{{Name: "r", Prefix: "gc"}},
	}
	ep := events.NewFake()
	cs := &controllerState{
		cfg:        cfg,
		cityName:   "tc",
		cityPath:   cityDir,
		beadStores: map[string]beads.Store{"r": rigStore},
		eventProv:  ep,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs.startAutocloseWatcher(ctx)

	ep.Record(events.Event{Type: events.BeadClosed, Subject: "gc-3", Actor: "cache-reconcile"})

	if !waitForCondition(t, 2*time.Second, func() bool {
		b, _ := rigStore.Get("gc-1")
		return b.Status == "closed"
	}) {
		t.Fatalf("watcher ignored cache-reconcile close (native-mode trigger): status=%q",
			mustStatus(t, rigStore, "gc-1"))
	}
}

// completeConvoyStore builds a MemStore holding a convoy (gc-1) whose two
// children (gc-2, gc-3) are both closed — ready to auto-close.
func completeConvoyStore(t *testing.T) beads.Store {
	t.Helper()
	store := beads.NewMemStore()
	_, _ = store.Create(beads.Bead{Title: "batch", Type: "convoy"}) // gc-1
	_, _ = store.Create(beads.Bead{Title: "a", ParentID: "gc-1"})   // gc-2
	_, _ = store.Create(beads.Bead{Title: "b", ParentID: "gc-1"})   // gc-3
	_ = store.Close("gc-2")
	_ = store.Close("gc-3")
	return store
}

// --- watcher goroutine + checkpointing ---

// TestStartAutocloseWatcherProcessesCloseAndCheckpoints asserts the live
// watcher autocloses on a bead.closed event and advances the persisted cursor.
func TestStartAutocloseWatcherProcessesCloseAndCheckpoints(t *testing.T) {
	cityDir := t.TempDir()
	rigStore := completeConvoyStore(t)
	cfg := &config.City{
		Workspace: config.Workspace{Name: "tc", Prefix: "hq"},
		Rigs:      []config.Rig{{Name: "r", Prefix: "gc"}},
	}
	ep := events.NewFake()
	cs := &controllerState{
		cfg:        cfg,
		cityName:   "tc",
		cityPath:   cityDir,
		beadStores: map[string]beads.Store{"r": rigStore},
		eventProv:  ep,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs.startAutocloseWatcher(ctx)

	ep.Record(events.Event{Type: events.BeadClosed, Subject: "gc-3"})

	if !waitForCondition(t, 2*time.Second, func() bool {
		b, _ := rigStore.Get("gc-1")
		return b.Status == "closed"
	}) {
		t.Fatalf("convoy gc-1 not auto-closed by watcher: status=%q", mustStatus(t, rigStore, "gc-1"))
	}
	if !waitForCondition(t, time.Second, func() bool {
		seq, ok := loadAutocloseCursor(cityDir)
		return ok && seq >= 1
	}) {
		t.Fatal("watcher did not persist a checkpoint cursor")
	}
}

// TestStartAutocloseWatcherResumesFromCursor is the restart-safety regression:
// a bead.closed that landed while the controller was "down" (recorded before
// the watcher starts, at a seq above the persisted cursor) must still be
// processed on resume. The watcher seeds from the cursor, NOT from the live
// stream head (beadEventStartSeq), so the close is replayed rather than
// skipped.
func TestStartAutocloseWatcherResumesFromCursor(t *testing.T) {
	cityDir := t.TempDir()
	rigStore := completeConvoyStore(t)
	cfg := &config.City{
		Workspace: config.Workspace{Name: "tc", Prefix: "hq"},
		Rigs:      []config.Rig{{Name: "r", Prefix: "gc"}},
	}
	ep := events.NewFake()
	// seq 1: an unrelated event already acknowledged by the cursor.
	ep.Record(events.Event{Type: events.BeadUpdated, Subject: "gc-9"})
	// seq 2: the close that landed "during downtime", before the watcher runs.
	ep.Record(events.Event{Type: events.BeadClosed, Subject: "gc-3"})

	// Cursor sits at seq 1 (below the close). beadEventStartSeq is the live
	// head (2): if the watcher seeded from it, the close would be skipped.
	if err := saveAutocloseCursor(cityDir, 1); err != nil {
		t.Fatalf("saveAutocloseCursor: %v", err)
	}
	cs := &controllerState{
		cfg:               cfg,
		cityName:          "tc",
		cityPath:          cityDir,
		beadStores:        map[string]beads.Store{"r": rigStore},
		eventProv:         ep,
		beadEventStartSeq: 2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs.startAutocloseWatcher(ctx)

	if !waitForCondition(t, 2*time.Second, func() bool {
		b, _ := rigStore.Get("gc-1")
		return b.Status == "closed"
	}) {
		t.Fatalf("watcher skipped the pre-recorded close — resumed from head, not cursor: status=%q",
			mustStatus(t, rigStore, "gc-1"))
	}
	if seq, ok := loadAutocloseCursor(cityDir); !ok || seq < 2 {
		t.Fatalf("cursor not advanced past the replayed close: seq=%d ok=%v", seq, ok)
	}
}
