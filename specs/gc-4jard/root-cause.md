---
name: Why every order in the city fires at 5-14 minutes (gc-4jard)
description: The lag is not refinery-reconcile's and not deferred-dispatch's — it is every order's, because order dispatch ran inline on the controller's single reconciler goroutine. The declared cadence was silently max(cadence, reconciler latency), and time.Ticker coalesced the difference away. Records the measurements that isolate it, the three hypotheses ruled out, and why the two mitigations already in the code could not work.
---

# gc-4jard: the cadence was never the order's, it was the reconciler's

## Verdict

**One shared clock, not two lagging orders.** `refinery-reconcile` (60s) and
`deferred-dispatch` (2m) are not individually slow. *Every* order in the city
fires on the same 5–14 minute rhythm, whatever cadence it declares, because
order dispatch ran inline at the top of the controller's reconciler tick. The
declared cadence was in practice `max(declared, reconciler-loop latency)`, and
`time.NewTicker` drops the patrol ticks that elapse while the loop is busy —
so the slip produced no error, no warning, and no metric.

The bead's framing — "look at the thing between *order due* and *order
fired*" — was right, and the answer is that nothing sat between them. The
dispatch pass itself is fine. What was slow was *getting to it*.

## The measurement that settles it

Every order fires in one batch per tick. Exact `gc order history` timestamps,
loomington, 2026-08-24:

| order | declared | fires |
|---|---|---|
| `dolt-health` | 30s | 04:25:54, 04:23:16, 04:16:29, 04:09:37, 03:59:46 |
| `beads-health` | 30s | 04:25:54, 04:23:16, 04:16:29, 04:11:23, 03:59:46 |
| `gate-sweep` | 30s | 04:26:04, 04:23:22, 04:18:30, 04:18:24, 04:11:34 |
| `order-tracking-sweep` | 1m | 04:26:13, 04:23:23, 04:18:30, 04:13:11, 04:01:16 |
| `boot-health` | 2m | 04:26:13, 04:23:31, 04:18:35, 04:13:20, 04:02:33 |

A 30s order and a 2m order sharing fire timestamps to the second is not four
rigs each falling behind. It is one pass emitting all of them.

Against `patrol_interval = "30s"`, successive intervals for `dolt-health` were
2m38s, 6m47s, 6m52s, 9m51s, 8m37s, 5m2s — the same shape the bead recorded for
`refinery-reconcile` 36 hours earlier, and the same the deacon measured from
`pass.log` mtimes 12 hours before that.

## Ruled out, with evidence

Beyond the three the bead already eliminated:

1. **FS-pressure tick shedding** (`shouldSkipTickForFSPressure`, which gates the
   tick *before* dispatch and can skip 5 consecutive ticks). Not it:
   `supervisor.fs_pressure.skipped_tick` has zero events in 6h and
   `/proc/pressure/io` reads `some avg60=0.00`.

2. **The per-tick dispatch budget** (`defaultMaxOrderDispatchesPerTick = 4`,
   round-robin over ~45 registered orders — the arithmetic is seductive:
   45/4 × 30s ≈ 6 min, which matches the median). Not it. Budget 4 at 2
   ticks/min imposes a hard ceiling of 8 dispatches/minute, and the observed
   per-minute counts break it: 11 at 04:18 and 17 at 04:23, with many minutes
   at zero. That is a blocked loop draining a backlog, not a rationed one.
   The round-robin itself is correct — `nextDispatchStart` advances only on an
   actual dispatch, and non-due orders are skipped without spending budget.

3. **A CPU-bound controller.** Not it: sampled over 7 minutes the controller
   held 7–12% of one core with all 26 threads in `S`. It is waiting, not
   computing.

## Why the mitigations already in the tree could not fix it

The codebase had met this three times and each time treated it as an ordering
problem:

- `tick()` put dispatch first, "so due formulas are not starved by slow
  startup/config drift work."
- `run()` dispatched once before the startup reconcile, because "a cold-start
  reconcile can take minutes."
- `runOrderTrackingSweepWatchdog` exists because "when slow reconciler cycles
  keep order-tracking-sweep from firing, every order's tracking jams and no
  order fires (#2168)" — the same root cause, caught downstream.

The first two order dispatch *ahead of* the slow work. That controls where
dispatch sits inside a tick; it cannot change **how often a tick begins**,
which is what actually sets the dispatch rate. The third is a recovery valve
for damage the lag had already done. All three are consistent with the real
constraint and none of them relieves it.

## Fix

Give order dispatch its own goroutine and its own ticker
(`cmd/gc/order_dispatch_loop.go`), started before the startup reconcile and
torn down before shutdown drains. Dispatch stops sharing a clock with
reconcile work, so `every = "60s"` means 60 seconds regardless of what the
reconciler is doing.

The old design got mutual exclusion between dispatch and
reload/rescan/shutdown-drain for free, from "reload runs on the same goroutine
as tick". That is now stated explicitly as `CityRuntime.orderMu`, held across a
whole dispatch pass, which preserves `drain`'s guarantee that no dispatch can
create a new in-flight signal on a dispatcher while drain observes it. A reload
waits out at most one pass.

`cr.cfg` is owned by the reconciler and swapped on reload, so the dispatch path
no longer reads the field directly: `dispatchOrders` takes one snapshot per
pass via `currentConfig()` (under `serviceStateMu`) and threads it through the
rescan and the two tracking watchdogs. That also stops a single pass from
straddling a reload and mixing two configs.

## Lock ordering, and what a reload now waits for

There is exactly one edge between the two locks involved: `dispatchOrders`
holds `orderMu` and then takes `serviceStateMu.RLock` via `currentConfig()`.
Nothing takes `orderMu` while holding `serviceStateMu` — `reloadConfigTraced`
releases `orderMu` before its `serviceStateMu.Lock()`, and neither
`rescanOrderDispatcher` nor `drain` touches `serviceStateMu`. So there is no
cycle.

A reload can now wait on a dispatch pass that is already running, which the old
design never did because the two were strictly sequential on one goroutine. The
serialized total is unchanged: the reconciler used to pay the whole pass inline
in every tick. A pass is bounded by the per-order gate (`orderGateTimeout = 8s`,
with a 24s backoff that stops an order timing out its gate every cycle), and
dispatch launches its work in goroutines rather than waiting on it. The reverse
coupling — a slow pass delaying reconcile — is therefore bounded and strictly
rarer than what it replaces.

## What this does not cover

- **Why the reconciler blocks for minutes at a time** is a separate question.
  It is I/O-bound, its select loop also serves the control-dispatcher and
  nudge-wake handlers (each of which runs its own desired-state build and
  session-bead sync), and one tick loads a session snapshot six or seven times.
  Worth its own bead; this fix makes order cadence independent of the answer
  rather than waiting for it.
- **Trace coverage.** The `orders.dispatch` phase was recorded on the
  reconciler's per-tick trace cycle. Dispatch no longer runs there, and
  emitting cycles from a second goroutine is not safe with the current tracer,
  so that record is gone. `TraceSiteOrderDispatch` is now unreferenced.
- The `liveness-sweep` `trigger=condition` precheck failure the bead flags as
  separately-observed is untouched and still wants its own bead. It is not
  explained by this: a starved dispatch delays a check, it does not make one
  exit 1.
