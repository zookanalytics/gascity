package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/orders"
)

// TestOrderDispatchCooldownAdvancesCursorMintsNoBead is the canonical
// regression for gc-7hf34: a cooldown order records each fire by advancing the
// durable last-run file cursor instead of minting a retained per-tick
// order-tracking bead (those writes were the base bead-event-rate driver of the
// controller self-feeding loop in gc-k8r4y). It pins the full contract:
//   - the cursor is zero before the first fire,
//   - dispatch advances it to the tick time and mints ZERO tracking beads,
//   - a tick within the cooldown window does not re-fire (the advanced cursor
//     is the anti-re-fire gate the tracking bead used to be), and
//   - a tick after the interval elapses fires again — still minting no bead.
func TestOrderDispatchCooldownAdvancesCursorMintsNoBead(t *testing.T) {
	store := beads.NewMemStore()
	fired := 0
	countingExec := func(context.Context, string, string, []string) ([]byte, error) {
		fired++
		return []byte("ok\n"), nil
	}

	aa := []orders.Order{{
		Name:     "dolt-health",
		Trigger:  "cooldown",
		Interval: "30s",
		Exec:     "true",
	}}
	ad := buildOrderDispatcherFromListExec(aa, store, nil, countingExec, nil)
	if ad == nil {
		t.Fatal("expected non-nil dispatcher")
	}

	// One city runtime dir across all ticks so the cursor persists.
	cityPath := t.TempDir()
	t0 := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

	// Before any dispatch the order has never run: its cursor is unset.
	assertLastRunZero(t, cityPath, "dolt-health")

	// First tick: never run → due → fires once and advances the cursor to t0.
	ad.dispatch(context.Background(), cityPath, t0)
	ad.drain(context.Background())
	if fired != 1 {
		t.Fatalf("exec runs after first tick = %d, want 1", fired)
	}
	if got := assertLastRunAdvanced(t, cityPath, "dolt-health"); !got.Equal(t0) {
		t.Fatalf("cursor after first fire = %v, want tick time %v", got, t0)
	}
	assertNoOrderTrackingBeads(t, store, "dolt-health")

	// Second tick within the cooldown window (+10s < 30s): not due → no re-fire.
	ad.dispatch(context.Background(), cityPath, t0.Add(10*time.Second))
	ad.drain(context.Background())
	if fired != 1 {
		t.Fatalf("exec runs after cooldown-suppressed tick = %d, want 1 (must not re-fire)", fired)
	}

	// Third tick after the interval elapses (+31s > 30s): due → fires again and
	// advances the cursor, still without minting any tracking bead.
	t1 := t0.Add(31 * time.Second)
	ad.dispatch(context.Background(), cityPath, t1)
	ad.drain(context.Background())
	if fired != 2 {
		t.Fatalf("exec runs after interval elapsed = %d, want 2 (must re-fire)", fired)
	}
	if got := assertLastRunAdvanced(t, cityPath, "dolt-health"); !got.Equal(t1) {
		t.Fatalf("cursor after second fire = %v, want tick time %v", got, t1)
	}
	assertNoOrderTrackingBeads(t, store, "dolt-health")
}

// assertNoOrderTrackingBeads fails if any order-tracking bead exists for scoped,
// across both tiers — the gc-7hf34 invariant for cooldown/cron orders.
func assertNoOrderTrackingBeads(t *testing.T, store beads.Store, scoped string) {
	t.Helper()
	if got := trackingBeads(t, store, "order-run:"+scoped); len(got) != 0 {
		t.Fatalf("order-run beads for %q = %d, want 0 (cooldown records via the last-run cursor)", scoped, len(got))
	}
	if got := trackingBeads(t, store, labelOrderTracking); len(got) != 0 {
		t.Fatalf("order-tracking beads = %d, want 0 (cooldown mints none)", len(got))
	}
}

// TestOrderDispatchCooldownExecInflightGateBlocksConcurrentDispatch is the
// regression for gc-4nxy8: once cooldown/cron orders stopped minting a per-fire
// tracking bead (gc-7hf34), they lost the in-flight marker that bead provided.
// The advanced last-run cursor only suppresses re-fire WITHIN the interval, so a
// slow exec whose runtime exceeds the interval would be dispatched a second time
// while the first copy is still running. The in-memory per-order in-flight gate
// must block that concurrent dispatch and clear once the run completes so the
// order can fire again normally.
func TestOrderDispatchCooldownExecInflightGateBlocksConcurrentDispatch(t *testing.T) {
	store := beads.NewMemStore()

	// starts receives one signal each time the exec command begins. release
	// gates the (blocking) command so it outlives the order interval, modeling a
	// command whose runtime exceeds its cooldown.
	starts := make(chan struct{}, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseExec := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseExec() // never leak blocked goroutines, even on failure

	var execStarts int32
	slowExec := func(ctx context.Context, _ string, _ string, _ []string) ([]byte, error) {
		atomic.AddInt32(&execStarts, 1)
		starts <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte("ok\n"), nil
	}

	aa := []orders.Order{{
		Name:     "slow-health",
		Trigger:  "cooldown",
		Interval: "1s",
		Exec:     "true",
	}}
	ad := buildOrderDispatcherFromListExec(aa, store, nil, slowExec, nil)
	if ad == nil {
		t.Fatal("expected non-nil dispatcher")
	}

	cityPath := t.TempDir()
	t0 := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

	// First tick: never run → due → launches the blocking exec and advances the
	// cursor to t0. The command stays in flight (longer than the 1s interval).
	ad.dispatch(context.Background(), cityPath, t0)
	select {
	case <-starts:
	case <-time.After(5 * time.Second):
		t.Fatal("first exec did not start")
	}

	// Second tick AFTER the interval elapsed (+2s > 1s) while the first run is
	// still in flight. The cursor no longer suppresses it, so the in-flight gate
	// is the only thing that can prevent a concurrent second copy.
	ad.dispatch(context.Background(), cityPath, t0.Add(2*time.Second))
	select {
	case <-starts:
		t.Fatal("second exec started while the first run was in flight; in-flight gate failed to block concurrent cooldown dispatch (gc-4nxy8)")
	case <-time.After(500 * time.Millisecond):
		// No second start: the in-flight gate held.
	}
	if got := atomic.LoadInt32(&execStarts); got != 1 {
		t.Fatalf("exec starts while first run in flight = %d, want 1", got)
	}

	// Release the first run and drain so its in-flight marker clears.
	releaseExec()
	ad.drain(context.Background())

	// A later tick (interval elapsed, nothing in flight) must fire again: the
	// in-flight gate clears on completion and does not wedge the order.
	ad.dispatch(context.Background(), cityPath, t0.Add(10*time.Second))
	select {
	case <-starts:
	case <-time.After(5 * time.Second):
		t.Fatal("order did not re-fire after the in-flight run completed; in-flight gate did not clear")
	}
	ad.drain(context.Background())
	if got := atomic.LoadInt32(&execStarts); got != 2 {
		t.Fatalf("total exec starts = %d, want 2 (one blocked run + one re-fire)", got)
	}
}

// TestReplaceOrderDispatcherCarriesInflightGateAcrossReload is the regression
// for the reload/rescan gap in the in-flight gate (PR#73 review, gc-4nxy8): the
// per-order in-flight marker lived on the memoryOrderDispatcher instance, so a
// config reload/rescan that built a fresh dispatcher started with an empty gate
// while a dispatchOne goroutine launched by the now-retired dispatcher was still
// running its (slow) command. The next tick on the replacement dispatcher saw
// no tracking bead and orderInflight=false, and launched a second concurrent
// copy — reintroducing the very concurrency bug the gate exists to prevent.
//
// The reload drain is bounded by reloadOrderDrainTimeout, so a command whose
// runtime exceeds that bound outlives the drain. This drives the real
// replaceOrderDispatcher + drain path: the replacement dispatcher must inherit
// the live in-flight gate (shared by reference, not copied), block the
// concurrent re-dispatch while the original run is still in flight, then allow a
// fresh dispatch once that run completes and clears the shared marker.
func TestReplaceOrderDispatcherCarriesInflightGateAcrossReload(t *testing.T) {
	store := beads.NewMemStore()

	starts := make(chan struct{}, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseExec := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseExec() // never leak blocked goroutines, even on failure

	var execStarts int32
	slowExec := func(ctx context.Context, _ string, _ string, _ []string) ([]byte, error) {
		atomic.AddInt32(&execStarts, 1)
		starts <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte("ok\n"), nil
	}

	aa := []orders.Order{{
		Name:     "slow-health",
		Trigger:  "cooldown",
		Interval: "1s",
		Exec:     "true",
	}}

	cityPath := t.TempDir()
	t0 := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

	// Generation 0: first tick launches the blocking exec, advances the durable
	// last-run cursor to t0, and marks the in-memory in-flight gate. The command
	// stays in flight (longer than the 1s interval AND the reload drain bound).
	gen0 := buildOrderDispatcherFromListExec(aa, store, nil, slowExec, nil)
	if gen0 == nil {
		t.Fatal("expected non-nil gen0 dispatcher")
	}
	gen0.dispatch(context.Background(), cityPath, t0)
	select {
	case <-starts:
	case <-time.After(5 * time.Second):
		t.Fatal("first exec did not start")
	}

	// Simulate a config reload while the gen0 command is still in flight: drain
	// the outgoing dispatcher with the reload bound (it times out because the
	// goroutine is blocked, so gen0 is retained), then install a freshly built
	// replacement via replaceOrderDispatcher — the same path config reload takes.
	gen1 := buildOrderDispatcherFromListExec(aa, store, nil, slowExec, nil)
	cr := &CityRuntime{od: gen0}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	cr.drainOutgoingOrderDispatcher(drainCtx, cr.od)
	drainCancel()
	cr.replaceOrderDispatcher(gen1)
	if cr.od != gen1 {
		t.Fatal("replaceOrderDispatcher did not install the replacement dispatcher")
	}

	// A tick on the replacement dispatcher AFTER the interval elapsed (+2s > 1s)
	// must NOT launch a concurrent second copy: the in-flight gate carried across
	// the reload still sees gen0's run in flight.
	cr.od.dispatch(context.Background(), cityPath, t0.Add(2*time.Second))
	select {
	case <-starts:
		t.Fatal("second exec started after reload while the first run was in flight; in-flight gate did not survive dispatcher replacement (gc-4nxy8)")
	case <-time.After(500 * time.Millisecond):
		// No second start: the carried in-flight gate held across the reload.
	}
	if got := atomic.LoadInt32(&execStarts); got != 1 {
		t.Fatalf("exec starts after reload while first run in flight = %d, want 1", got)
	}

	// Release the gen0 run and drain every dispatcher (active + retired) so the
	// retired goroutine returns and clears the shared in-flight marker.
	releaseExec()
	cr.drainOrderDispatchers(context.Background())

	// A later tick on the replacement dispatcher must fire again now that the
	// shared marker has cleared: the carried gate must not wedge the order.
	cr.od.dispatch(context.Background(), cityPath, t0.Add(10*time.Second))
	select {
	case <-starts:
	case <-time.After(5 * time.Second):
		t.Fatal("order did not re-fire after the in-flight run completed; the carried gate did not clear")
	}
	cr.od.drain(context.Background())
	if got := atomic.LoadInt32(&execStarts); got != 2 {
		t.Fatalf("total exec starts = %d, want 2 (one blocked run + one post-reload re-fire)", got)
	}
}
