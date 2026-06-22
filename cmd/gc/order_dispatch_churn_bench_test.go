package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/orders"
)

// Per-tick order-dispatch store-open churn harness (epic gc-k8r4y).
//
// memoryOrderDispatcher.dispatch opens a bead store once per DISTINCT order
// target-scope it encounters during a tick (deduped by orderStoreTargetKey),
// via resolveDispatchStore -> storeFn. When the controller wires cachedStoreFn
// (the gc-t5rev / PR #68 fix), resolveDispatchStore returns the borrowed cached
// handle and storeFn is never called, so per-tick opens drop to zero. These
// tiers lock and quantify that behavior:
//
//   - Tier 0  TestOrderDispatchStoreOpenScalingLaw — CI assertion that per
//     tick, opens == distinct active target-scopes, and == 0 with a cached
//     handle.
//   - Tier 1  BenchmarkOrderDispatchStoreOpens — hermetic before/after across
//     target counts, reporting opens/tick.
//   - Tier 2  BenchmarkOrderDispatchDoltConnections — real embedded Dolt
//     connections-per-open, in the //go:build dolt_integration companion file
//     order_dispatch_churn_dolt_bench_test.go (gated so default CI needs no
//     Dolt; build tags require a separate file).
//
// Faithfulness note: orders are seeded NOT-due so the measurement isolates the
// per-tick store OPEN (which precedes the trigger check in dispatch) with no
// dispatch goroutines or tracking-bead writes. That is precisely the
// steady-state pathology the epic targets — stores reopened every tick even
// when no order is firing. The open count is independent of due-ness anyway
// (the open happens before the due check); not-due simply removes goroutine and
// bead-accumulation noise so the benchmark measures the open cost alone.

// churnTargetScopeCounts enumerates the distinct active order target-scope
// counts the harness sweeps. 37 mirrors a large reported town (one city scope
// plus 36 rig scopes).
var churnTargetScopeCounts = []int{1, 2, 5, 17, 37}

// buildChurnOrders returns scopes cooldown orders that resolve to scopes
// DISTINCT order target-scopes: one city-scoped order plus scopes-1 rig-scoped
// orders with distinct rig names. The rig orders each trigger the legacy
// city-store fallback, but that resolves to the same city scope as the city
// order, so it reuses the already-open handle and adds no extra open — leaving
// exactly scopes distinct scopes (city + scopes-1 rigs).
func buildChurnOrders(scopes int) []orders.Order {
	if scopes < 1 {
		scopes = 1
	}
	aa := make([]orders.Order, 0, scopes)
	aa = append(aa, orders.Order{
		Name:     "city-churn",
		Trigger:  "cooldown",
		Interval: "24h",
		Exec:     "scripts/noop.sh",
	})
	for i := 1; i < scopes; i++ {
		aa = append(aa, orders.Order{
			Name:     fmt.Sprintf("rig-churn-%02d", i),
			Trigger:  "cooldown",
			Interval: "24h",
			Exec:     "scripts/noop.sh",
			Rig:      fmt.Sprintf("rig%02d", i),
		})
	}
	return aa
}

// churnHarness bundles a real memoryOrderDispatcher driven by the churn tests
// with the seam needed to count per-tick store opens.
type churnHarness struct {
	mad      *memoryOrderDispatcher
	store    beads.Store
	rec      *memRecorder
	cityPath string
	now      time.Time
	opens    *int64
}

// newChurnHarness builds a real memoryOrderDispatcher over scopes distinct
// target-scopes with a counting storeFn, the per-tick dispatch cap removed (so
// a single dispatch() scans the full active order set rather than early-
// returning after maxDispatchesPerTick dispatches), and every order seeded
// not-due. It drives the REAL dispatch() — only the store-open seam and the
// dispatch cap are configured, never the dispatch logic itself.
func newChurnHarness(tb testing.TB, scopes int) *churnHarness {
	tb.Helper()
	rec := &memRecorder{}
	store := beads.NewMemStore()
	ad := buildOrderDispatcherFromListExec(buildChurnOrders(scopes), store, nil, nil, rec)
	if ad == nil {
		tb.Fatalf("buildOrderDispatcherFromListExec returned nil for scopes=%d", scopes)
	}
	mad := ad.(*memoryOrderDispatcher)

	var opens int64
	mad.storeFn = func(execStoreTarget) (beads.Store, error) {
		atomic.AddInt64(&opens, 1)
		return store, nil
	}
	// 0 = no per-tick dispatch cap: scan every order so per-tick opens equal the
	// distinct scope count. With the production default of 4 the loop would
	// early-return after 4 dispatches and never reach the remaining scopes.
	mad.maxDispatchesPerTick = 0

	h := &churnHarness{
		mad:      mad,
		store:    store,
		rec:      rec,
		cityPath: tb.TempDir(),
		now:      time.Now(),
		opens:    &opens,
	}
	h.seedNotDue(tb)
	return h
}

// seedNotDue marks every order recently-run in the dispatcher's last-run cache
// so its cooldown trigger evaluates not-due. It derives each order's cache key
// exactly as dispatch() does — via the real resolveOrderStoreTarget /
// orderStoreTargetKey / legacy-city-fallback path — so the seeded entry is the
// one dispatch reads.
func (h *churnHarness) seedNotDue(tb testing.TB) {
	tb.Helper()
	for _, a := range h.mad.aa {
		target, err := resolveOrderStoreTarget(h.cityPath, h.mad.cfg, a)
		if err != nil {
			tb.Fatalf("resolving target for %s: %v", a.ScopedName(), err)
		}
		keys := []string{orderStoreTargetKey(target)}
		if legacyOrderCityFallbackNeeded(h.cityPath, target) {
			keys = append(keys, orderStoreTargetKey(legacyOrderCityTarget(h.cityPath, h.mad.cfg)))
		}
		h.mad.rememberLastRun(a.ScopedName(), keys, h.now)
	}
}

// tick drives one real dispatch() and drains it (a no-op when nothing fired).
func (h *churnHarness) tick(tb testing.TB) {
	tb.Helper()
	h.mad.dispatch(context.Background(), h.cityPath, h.now)
	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if !h.mad.drain(drainCtx) {
		tb.Fatal("drain timed out")
	}
}

// TestOrderDispatchStoreOpenScalingLaw locks the invariant that per dispatch
// tick the number of store opens equals the number of distinct active order
// target-scopes (city + bound rigs with active orders), and drops to zero when
// the controller-cached store resolver is wired. It is the regression guard for
// the dedup + cached-reuse behavior; default CI runs it with no Dolt.
func TestOrderDispatchStoreOpenScalingLaw(t *testing.T) {
	for _, scopes := range churnTargetScopeCounts {
		scopes := scopes
		t.Run(fmt.Sprintf("scopes=%d", scopes), func(t *testing.T) {
			// Arm A — per-tick open (no cached handle): opens == distinct scopes.
			perTick := newChurnHarness(t, scopes)
			perTick.tick(t)
			if got := atomic.LoadInt64(perTick.opens); got != int64(scopes) {
				t.Fatalf("per-tick open: opened %d store(s); want %d (one per distinct active order target-scope)", got, scopes)
			}
			if perTick.rec.hasType(events.OrderFired) {
				t.Fatal("an order fired; orders are seeded not-due so the measurement isolates per-tick store opens")
			}

			// Arm B — cached reuse (controller handle wired): opens == 0.
			cachedArm := newChurnHarness(t, scopes)
			cached := beads.NewMemStore()
			cachedArm.mad.cachedStoreFn = func(execStoreTarget) beads.Store { return cached }
			cachedArm.tick(t)
			if got := atomic.LoadInt64(cachedArm.opens); got != 0 {
				t.Fatalf("cached reuse: opened %d per-tick store(s); want 0 (the borrowed cached handle is reused every tick)", got)
			}
		})
	}
}

// BenchmarkOrderDispatchStoreOpens quantifies per-tick store-open churn across
// target-scope counts, before (perTickOpen) and after (cachedReuse) the
// cached-store-reuse fix. Each iteration drives one real dispatch() tick. The
// opens/tick metric reports the headline number: it equals the distinct scope
// count without a cached handle (linear churn) and 0 with one (100%
// elimination). ns/op shows the wall-cost of that churn scaling.
func BenchmarkOrderDispatchStoreOpens(b *testing.B) {
	for _, scopes := range churnTargetScopeCounts {
		scopes := scopes
		b.Run(fmt.Sprintf("scopes=%d/perTickOpen", scopes), func(b *testing.B) {
			h := newChurnHarness(b, scopes)
			ctx := context.Background()
			atomic.StoreInt64(h.opens, 0)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.mad.dispatch(ctx, h.cityPath, h.now)
			}
			b.StopTimer()
			b.ReportMetric(float64(atomic.LoadInt64(h.opens))/float64(b.N), "opens/tick")
		})
		b.Run(fmt.Sprintf("scopes=%d/cachedReuse", scopes), func(b *testing.B) {
			h := newChurnHarness(b, scopes)
			cached := beads.NewMemStore()
			h.mad.cachedStoreFn = func(execStoreTarget) beads.Store { return cached }
			ctx := context.Background()
			atomic.StoreInt64(h.opens, 0)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.mad.dispatch(ctx, h.cityPath, h.now)
			}
			b.StopTimer()
			b.ReportMetric(float64(atomic.LoadInt64(h.opens))/float64(b.N), "opens/tick")
		})
	}
}
