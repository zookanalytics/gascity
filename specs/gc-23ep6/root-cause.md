---
name: The usage sink did not "stop at 19:05" and v1.4.1 did not cause it (gc-23ep6)
description: Root-cause record for the usage-telemetry outage. Proves the v1.4.1 binary swap is coincidental (the supervisor still runs the pre-swap inode), that the sink was healthy throughout, that the emitter had been intermittent for a day before the reported stop, and that the real defect is the usage lane reading only OPEN session beads while the reconciler closes terminal sessions in the same pass.
---

# gc-23ep6: why usage.jsonl went silent

## Verdict

The reported cause is wrong, and so is the reported shape of the failure.

- **v1.4.1 did not cause it.** The controller process never ran v1.4.1.
- **The sink was never broken.** It was a healthy `LocalSink` the whole time.
- **It was not a clean stop at 19:05:16.** The emitter had been mostly dead for
  ~19 hours before that, and degraded for a day and a half.

The real defect: the usage lane can only see **open** session beads, and the
reconciler stamps a session's terminal state and closes its bead in the **same
pass** — so almost no session is ever observed open *and* terminal, and its
interval is never accounted.

## Evidence

### 1. The v1.4.1 swap is coincidental — ruled out by process identity

The running controller is PID 3204367, `gc supervisor run`, started
**2026-08-14 02:21:23** — 17 hours *before* the 19:12:38 rebuild:

```
$ ls -l /proc/3204367/exe
/proc/3204367/exe -> /home/zook/go/bin/gc (deleted)
```

Replacing a binary's path leaves a running process on its original inode. The
supervisor has therefore been executing **pre-rebase** code continuously across
the swap, and v1.4.1's usage-emission path has never run in this city. The
rebuild changed the binary that newly-exec'd `gc` children use (the
control-dispatchers recycled at 19:16:45), not the process that owns the
model-usage sweep.

This also disposes of the bead's "unexplained 7-minute gap" puzzle: there is no
causal edge to explain, in either direction.

`gascity-supervisor.service` is separately stuck in an `activating (auto-restart)`
crash loop (restart counter ~28.5k, i.e. ~2 days at 5s), because PID 3204367
already holds the city. That is a real but **pre-existing and unrelated**
condition — it long predates 19:05 — and is not filed here.

### 2. The sink was healthy throughout

```
GET /v0/city/loomington/usage
{"available":true,"recording":true,"source":"local_estimate", ...}
```

`source=local_estimate` is only returned when the live sink passes
`usage.IsLocalSink`. Not `Discard`, not an `exec:` provider, not a config flip.
`city.toml` carries no `[usage]` block, so the default local sink is correct.

### 3. It was not a clean stop — per-hour record counts over the whole file

`Fact.At` is stamped at write time, so these buckets are write times.

| window | compute/hr | note |
| --- | --- | --- |
| 08-13 07:00–17:00 | ~1200 | steady |
| 08-14 02:21 | — | supervisor PID 3204367 starts |
| 08-14 02:00–07:00 | 1–11 | collapse |
| 08-14 07:00 → 14:00 | 0 | 7-hour gap |
| 08-14 23:00 → 08-15 18:00 | 0 | **19-hour gap** |
| 08-15 18:00–19:05 | 2, then 1 | final trickle |

19:05:16 is the tail of an already-sporadic trickle. The mayor's 20:35 note
correctly refuted the "city is quiet" hypothesis, but its flush-cadence timeline
assumed a healthy writer that stopped; the writer had been mostly dead since
08-14 02:21.

### 4. The defect: the lane only sees open beads

`emitDueComputeFacts` (`cmd/gc/usage_compute.go`) is fed
`sessionBeadSnapshot.OpenInfos()`, and `loadSessionBeadSnapshot` says so
explicitly:

> Closed history is intentionally not loaded here — the reconciler calls this
> several times per tick and closed history grows without bound. Callers that
> need a closed record must fetch that one ID explicitly.

So an interval is accounted only if some pass observes the session while it is
**both** open **and** terminal. But `closeSessionBeadIfReachableStoreUnassigned`
(the drain path, `cmd/gc/session_reconciler.go`) stamps the terminal state and
closes the bead in one pass: the pass before the drain sees an awake session, the
pass after sees nothing.

Measured on the live store for 2026-08-15:

```
status  state            n   with usage_compute_emitted_at
closed  drained         70                              2
open    awake           20                              1
closed  failed-create    1                              0
closed  stranded-repair  1                              0
closed  gc_swept         1                              1
```

**68 of 70 ended intervals were dropped.** Every recently-closed session
(`lx-2onya`, `lx-win1u`, `lx-5ktd6`, `lx-63jes`, `lx-hhzm9`, `lx-803mk`, …) has
both `usage_compute_emitted_at` and `usage_model_swept_at` unset.

Both fact kinds died together because the terminal model sweep
(`SweepSessionModelUsage`) runs in the same per-session branch as the compute
fact.

### 5. The old ~1200/hr was the same bug's other face

The compute lane re-emits an interval on every tick until its marker is
committed. When ticks were slower and terminal beads lingered open, the same code
**over**-emitted (collapsed at read time by `IdempotencyKey`). The lane's output
has always been a function of how long a terminal bead happens to linger before
being closed — a race, not an accounting rule. That is why it inverted from
over-counting to near-total loss without any code change.

## Fix

Track the set of session ids owing an unaccounted interval and diff it across
passes. A session that leaves the open snapshot costs exactly one `Get` by id —
the closed-record read the snapshot loader sanctions — and is routed by the
**same** `processSessionBead` the open lane uses, so the decision is made on the
fresh bead: a session that merely dropped out of a *partial* snapshot is re-read
and left alone rather than mis-billed for an interval that has not ended.

Two properties worth keeping in mind when reading it:

- `processSessionBead` reports whether anything is still **owed**, which is
  deliberately not "did this call write a fact". `emitComputeFactForBead` returns
  false both for a failed write and for a no-op (no interval, or a marker an
  earlier pass already stamped); reading the no-op cases as failures retains a
  settled session forever and re-`Get`s it every pass — an unbounded leak on the
  synchronous reconcile tick. Guarded by
  `TestEmitDueComputeFactsDropsSettledVanishedSession`.
- The tracking set is bounded by the fleet, and accounting that does not settle
  stays in it and is retried.

## What this does NOT fix

- **The live lane** — incremental billing while a session is awake — records
  nothing: every open session has `invocation_usage_cursor` unset, including ones
  awake 4h+. Awake sessions *are* present in `OpenInfos()`, so this change does
  not touch that path. Filed as **gc-cj68c**. After this fix, facts are recovered
  at session close; gc-cj68c still owes billing *during* a long-lived session.
- **Non-compute-terminal close states.** A session whose close writes
  `gc_swept`, `failed-create`, or `stranded-repair` still records nothing,
  because `isComputeTerminalState` rejects it. That is now the remaining
  under-count, and it is a deliberate scope boundary: whether those intervals
  should bill is a product question, not a bug.
- **Unpriced facts.** `claude-opus-5` / `claude-sonnet-5` carry
  `cost_usd_estimate=0` with `unpriced=true`. Token counts are correct; dollar
  figures understate. Separate from this outage, as the bead already notes.
