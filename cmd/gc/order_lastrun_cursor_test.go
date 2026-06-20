package main

import (
	"context"
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
