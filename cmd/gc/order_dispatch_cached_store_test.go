package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/orders"
)

// TestOrderDispatchReusesControllerCachedStore is the gc-t5rev regression: when
// the controller exposes a long-lived cached bead store for a dispatch target
// (controllerState.beadStores), memoryOrderDispatcher.dispatch must reuse that
// borrowed handle every tick instead of opening — and closing — a fresh native
// Dolt store per target. The per-tick reopen is the bulk of the connection
// storm (each open is a TCP dial + connection pool + preflight subprocess fork
// + migration pool), and closing a borrowed handle would stop its reconciler
// and latch the native store shut (gascity#3157).
func TestOrderDispatchReusesControllerCachedStore(t *testing.T) {
	cached := newLatchedCloseStore()
	// fallback models storeFn (openStoreAtForCity). Reaching it means dispatch
	// opened a per-tick store despite a cached handle being available.
	fallback := newLatchedCloseStore()
	var storeFnCalls int32
	var rec memRecorder

	fakeExec := func(context.Context, string, string, []string) ([]byte, error) {
		return []byte("ok\n"), nil
	}
	aa := []orders.Order{{
		Name:     "cached-health",
		Trigger:  "cooldown",
		Interval: "1h",
		Exec:     "scripts/health.sh",
	}}
	ad := buildOrderDispatcherFromListExec(aa, fallback, nil, fakeExec, &rec)
	if ad == nil {
		t.Fatal("expected non-nil dispatcher")
	}
	mad := ad.(*memoryOrderDispatcher)

	innerStoreFn := mad.storeFn
	mad.storeFn = func(target execStoreTarget) (beads.Store, error) {
		atomic.AddInt32(&storeFnCalls, 1)
		return innerStoreFn(target)
	}
	// Wire the controller-cached resolver exactly as CityRuntime installs it.
	mad.cachedStoreFn = func(execStoreTarget) beads.Store { return cached }

	cityPath := t.TempDir()
	base := time.Now()
	for i := 0; i < 3; i++ {
		mad.dispatch(context.Background(), cityPath, base.Add(time.Duration(i)*time.Second))
		drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		ok := mad.drain(drainCtx)
		cancel()
		if !ok {
			t.Fatalf("tick %d: drain timed out", i)
		}
	}

	if got := atomic.LoadInt32(&storeFnCalls); got != 0 {
		t.Fatalf("dispatch opened %d per-tick store(s) across 3 ticks; with a controller-cached handle it must open 0 (gc-t5rev: per-tick reopen drives the connection storm)", got)
	}
	if !rec.hasType(events.OrderFired) {
		t.Fatal("order never fired through the cached store; reusing the cached handle must not skip dispatch")
	}
	// Give any (buggy) deferred per-tick closer a chance to run before asserting.
	time.Sleep(50 * time.Millisecond)
	if cached.isClosed() {
		t.Fatal("dispatch closed the borrowed controller-cached store; cached handles are owned by controllerState and must survive across ticks (gc-t5rev / gascity#3157)")
	}
	if op, used := cached.usedAfterClose(); used {
		t.Fatalf("cached store saw use-after-close op %q; dispatch must never close a borrowed handle", op)
	}
	if fallback.isClosed() {
		t.Fatal("fallback per-tick store was closed but it should never have been opened")
	}
}

// TestOrderDispatchFallsBackToPerTickStoreWhenNoCachedHandle locks the other
// direction of the gc-t5rev owned/borrowed distinction: when no controller
// cached handle is available for a target (standalone/no-API mode, or the city
// store failed to open), dispatch must fall back to storeFn (opening a per-tick
// store) AND close that owned handle once the in-flight dispatch releases it —
// the close path must not silently turn into a handle leak.
func TestOrderDispatchFallsBackToPerTickStoreWhenNoCachedHandle(t *testing.T) {
	opened := newLatchedCloseStore()
	var storeFnCalls int32
	var rec memRecorder

	fakeExec := func(context.Context, string, string, []string) ([]byte, error) {
		return []byte("ok\n"), nil
	}
	aa := []orders.Order{{
		Name:     "fallback-health",
		Trigger:  "cooldown",
		Interval: "1h",
		Exec:     "scripts/health.sh",
	}}
	ad := buildOrderDispatcherFromListExec(aa, opened, nil, fakeExec, &rec)
	if ad == nil {
		t.Fatal("expected non-nil dispatcher")
	}
	mad := ad.(*memoryOrderDispatcher)

	innerStoreFn := mad.storeFn
	mad.storeFn = func(target execStoreTarget) (beads.Store, error) {
		atomic.AddInt32(&storeFnCalls, 1)
		return innerStoreFn(target)
	}
	// A cachedStoreFn that returns nil must be treated as "no cached handle"
	// and fall through to storeFn — identical to the nil-cachedStoreFn default.
	mad.cachedStoreFn = func(execStoreTarget) beads.Store { return nil }

	mad.dispatch(context.Background(), t.TempDir(), time.Now())
	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if !mad.drain(drainCtx) {
		t.Fatal("drain timed out waiting for in-flight dispatchOne to finish")
	}

	if atomic.LoadInt32(&storeFnCalls) == 0 {
		t.Fatal("no cached handle available but dispatch never opened a per-tick store via storeFn")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !opened.isClosed() {
		if time.Now().After(deadline) {
			t.Fatal("per-tick (owned) store was never closed after dispatch drained (handle leak)")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestControllerCachedOrderStoreResolvesScope verifies the CityRuntime resolver
// that gc-t5rev installs onto the dispatcher: a city-scoped target maps to the
// controller's city store, a rig-scoped target to that rig's cached store, and
// anything without a cached handle (unknown rig, empty rig name, unknown scope,
// or no controllerState) resolves to nil so dispatch falls back to a per-tick
// open.
func TestControllerCachedOrderStoreResolvesScope(t *testing.T) {
	cityStore := beads.NewMemStore()
	rigStore := beads.NewMemStore()
	cr := &CityRuntime{cs: &controllerState{
		cityBeadStore: cityStore,
		beadStores:    map[string]beads.Store{"myrig": rigStore},
	}}

	if got := cr.controllerCachedOrderStore(execStoreTarget{ScopeKind: "city", ScopeRoot: "/city"}); got != beads.Store(cityStore) {
		t.Errorf("city target: got %p, want city store %p", got, cityStore)
	}
	if got := cr.controllerCachedOrderStore(execStoreTarget{ScopeKind: "rig", RigName: "myrig"}); got != beads.Store(rigStore) {
		t.Errorf("rig target: got %p, want rig store %p", got, rigStore)
	}
	if got := cr.controllerCachedOrderStore(execStoreTarget{ScopeKind: "rig", RigName: "unknown"}); got != nil {
		t.Errorf("unknown rig: got %p, want nil", got)
	}
	if got := cr.controllerCachedOrderStore(execStoreTarget{ScopeKind: "rig", RigName: ""}); got != nil {
		t.Errorf("empty rig name: got %p, want nil", got)
	}
	if got := cr.controllerCachedOrderStore(execStoreTarget{ScopeKind: "other"}); got != nil {
		t.Errorf("unknown scope: got %p, want nil", got)
	}
	if got := (&CityRuntime{}).controllerCachedOrderStore(execStoreTarget{ScopeKind: "city"}); got != nil {
		t.Errorf("nil controllerState: got %p, want nil", got)
	}
}

// TestInstallOrderDispatcherCachedStoresWiresResolver verifies the installer
// points the production dispatcher's cachedStoreFn at the CityRuntime resolver
// (gc-t5rev) and is a no-op for non-memoryOrderDispatcher dispatchers.
func TestInstallOrderDispatcherCachedStoresWiresResolver(t *testing.T) {
	cityStore := beads.NewMemStore()
	cr := &CityRuntime{cs: &controllerState{cityBeadStore: cityStore}}
	mad := &memoryOrderDispatcher{}
	cr.od = mad

	cr.installOrderDispatcherCachedStores()

	if mad.cachedStoreFn == nil {
		t.Fatal("installOrderDispatcherCachedStores did not set cachedStoreFn on the dispatcher")
	}
	if got := mad.cachedStoreFn(execStoreTarget{ScopeKind: "city"}); got != beads.Store(cityStore) {
		t.Errorf("wired resolver: got %p, want city store %p", got, cityStore)
	}
	// The installer must also register the dispatcher so a config mutation's
	// store swap can drain its in-flight borrowers before closing retired cached
	// handles (gc-t5rev / gascity#3157).
	if got := cr.cs.inflightOrderDispatcher.Load(); got != mad {
		t.Errorf("installOrderDispatcherCachedStores did not register the dispatcher for store-retire drain: got %p, want %p", got, mad)
	}

	// Non-memoryOrderDispatcher: installer must not panic and must leave it alone.
	cr.od = nopOrderDispatcher{}
	cr.installOrderDispatcherCachedStores()
}

// nopOrderDispatcher is a non-memoryOrderDispatcher used to confirm the cached-
// store installer no-ops on dispatcher types that own their store seam.
type nopOrderDispatcher struct{}

func (nopOrderDispatcher) dispatch(context.Context, string, time.Time) {}
func (nopOrderDispatcher) drain(context.Context) bool                  { return true }

// TestOrderDispatchWaitsForInflightBeforeClosingStore is the gc-t5rev codex
// finding (PR#68): a config mutation that swaps the controller's cached bead
// stores must not close a handle an in-flight order dispatch borrowed. update()
// schedules the replaced handles to close after controllerStateStoreCloseDelay,
// but a dispatchOne can hold a borrowed handle across a long exec/formula and
// then write its tracking-bead outcome through it; closing underneath that write
// latches the native store shut and orphans the run (gascity#3157).
// scheduleCloseReplacedStores must wait for the dispatcher's in-flight drain
// before retiring the handle.
func TestOrderDispatchWaitsForInflightBeforeClosingStore(t *testing.T) {
	restore := controllerStateStoreCloseDelay
	controllerStateStoreCloseDelay = 0 // isolate the wait to the in-flight drain
	defer func() { controllerStateStoreCloseDelay = restore }()

	// A dispatcher with one in-flight dispatchOne still holding a borrowed handle.
	mad := &memoryOrderDispatcher{}
	mad.addInflight()

	cs := &controllerState{}
	cs.inflightOrderDispatcher.Store(mad)

	borrowed := newLatchedCloseStore()
	replacement := newLatchedCloseStore()
	cs.scheduleCloseReplacedStores(borrowed, replacement, nil, nil)

	// While the dispatch is in-flight, the retired borrowed handle must stay open.
	time.Sleep(50 * time.Millisecond)
	if borrowed.isClosed() {
		t.Fatal("retired borrowed store closed while an order dispatch was still in-flight (gc-t5rev use-after-close)")
	}

	// Release the in-flight dispatch; the retire must now close the handle.
	mad.doneInflight()
	waitStoreClosed(t, borrowed, "retired borrowed store never closed after in-flight order dispatch drained")
}

// TestOrderDispatchClosesStoreAfterInflightCompletes pairs with the wait test:
// once the in-flight dispatch finishes — writing its tracking-bead outcome
// through the borrowed handle — the retire must close the handle, without
// leaking it and without any use-after-close. It runs a real dispatchOne held
// mid-exec so the post-exec tracking-bead write is exercised against the live
// borrowed store before the close lands (gc-t5rev / gascity#3157).
func TestOrderDispatchClosesStoreAfterInflightCompletes(t *testing.T) {
	restore := controllerStateStoreCloseDelay
	controllerStateStoreCloseDelay = 0
	defer func() { controllerStateStoreCloseDelay = restore }()

	borrowed := newLatchedCloseStore()
	entered := make(chan struct{}) // closed once dispatchOne reaches exec holding `borrowed`
	release := make(chan struct{})
	var rec memRecorder
	fakeExec := func(context.Context, string, string, []string) ([]byte, error) {
		close(entered)
		<-release // hold the dispatch in-flight, still borrowing `borrowed`
		return []byte("ok\n"), nil
	}
	aa := []orders.Order{{
		Name:     "cached-health",
		Trigger:  "cooldown",
		Interval: "1h",
		Exec:     "scripts/health.sh",
	}}
	// The fallback store must never be opened/closed: the target resolves to the
	// cached handle, so reaching storeFn would be a bug.
	ad := buildOrderDispatcherFromListExec(aa, newLatchedCloseStore(), nil, fakeExec, &rec)
	if ad == nil {
		t.Fatal("expected non-nil dispatcher")
	}
	mad := ad.(*memoryOrderDispatcher)
	mad.cachedStoreFn = func(execStoreTarget) beads.Store { return borrowed }

	cs := &controllerState{}
	cs.inflightOrderDispatcher.Store(mad)

	// Tick once: borrows `borrowed`, creates its tracking bead, and launches a
	// dispatchOne that blocks in exec while holding the handle. addInflight runs
	// synchronously in dispatch, so the handle is in-flight once dispatch returns;
	// `entered` confirms the goroutine reached exec still holding it.
	mad.dispatch(context.Background(), t.TempDir(), time.Now())
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatchOne never reached exec; the cached-store dispatch must launch a goroutine that holds the handle")
	}

	// Retire `borrowed` mid-exec.
	cs.scheduleCloseReplacedStores(borrowed, newLatchedCloseStore(), nil, nil)
	time.Sleep(50 * time.Millisecond)
	if borrowed.isClosed() {
		t.Fatal("retired borrowed store closed while dispatchOne was still mid-exec (gc-t5rev use-after-close)")
	}

	// Finish the exec; dispatchOne writes its tracking-bead outcome through
	// `borrowed` (still open), then drain releases and the retire closes it.
	close(release)
	waitStoreClosed(t, borrowed, "retired borrowed store never closed after dispatchOne completed")
	if op, used := borrowed.usedAfterClose(); used {
		t.Fatalf("borrowed store saw use-after-close op %q; the retire closed it before the in-flight dispatch finished writing", op)
	}
}

// waitStoreClosed polls until the latched store reports closed or fails the test
// after a bounded deadline, so the close-coordination tests stay deterministic
// without sleeping on the detached drain+close goroutine.
func waitStoreClosed(t *testing.T, s *latchedCloseStore, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !s.isClosed() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
