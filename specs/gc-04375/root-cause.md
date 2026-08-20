---
name: Root cause of the TestCityRuntimeForceShutdownTearsDownAfterLateAsyncSweep flake (gc-04375)
description: The flake was a real product race, not a test defect — the stop path sent mid-create sessions to Suspend, which refuses them, so a force shutdown's late sweep leaked exactly the sessions it exists to catch. Records the mechanism, why three prior triage rounds could not reach it, why the first fix was rejected in review, and what generalizes.
---

# gc-04375: the flake was a leak, not a test defect

## Verdict

`TestCityRuntimeForceShutdownTearsDownAfterLateAsyncSweep` was failing
because the product it tests was wrong. The stop path sent mid-create
sessions to `Manager.Suspend`, which rejects `start-pending` and `creating`
with an illegal-transition error **before** reaching the provider `Stop`, so
a force shutdown that enumerated a running session whose create had not yet
committed left that runtime alive. The test asserted the guarantee the late
sweep is supposed to provide, and the guarantee did not hold.

Fixed by sending mid-create stop targets to the teardown-only lever instead
of the lifecycle one: `stopTargetThroughWorkerBoundary`
(`cmd/gc/session_lifecycle_parallel.go`) now routes `start-pending` and
`creating` to `Kill`, which already accepts both states. `Suspend` keeps
refusing them, and the refusal is now correct rather than merely
inconvenient — see *Scope of the fix*.

## The mechanism

Force shutdown runs two stop passes (`CityRuntime.shutdown`,
`cmd/gc/city_runtime.go`). The second exists specifically to catch sessions
created too late for the first, which are by definition the ones whose
async start commit has not landed. Which stop verb that second pass used
came down to a race:

1. `markCityStopSessionSleepReason` (`cmd/gc/cmd_stop.go`) marks a session
   with `sleep_reason=city_stop` **only if its state is already `active`**.
2. `stopTargetThroughWorkerBoundary` (`cmd/gc/session_lifecycle_parallel.go`)
   reads that marker to choose its verb: marked → `Kill`, unmarked → `Stop`.
3. `Kill` → `Manager.Kill`, which explicitly accepts `StateCreating` and
   `StateStartPending` and calls `sp.Stop`. The session dies.
4. `Stop` → `SessionHandle.Stop` → `Manager.Suspend`, which ran
   `Transition(creating, CmdSuspend)`. That pair is absent from the
   transition table, so it returned `IllegalTransitionError` and **never
   reached `sp.Stop`**. The session survived.

Step 2 now has a third branch — mid-create → `Kill` — which is the fix; step
4 is unreachable for those states. The rest of the mechanism is unchanged.

So if the async start's `commitAsyncStartResultWithContext` won the race to
flip the bead `creating → active`, the shutdown killed the session and the
test passed. If it lost, the bead was still `creating`, suspend refused, and
`sp.IsRunning("worker")` was still true at the assertion. Nothing else about
the test was non-deterministic — that one race decided every run.

Both interleavings now stop the session, so the test is deterministic for
the same reason the leak is closed: not because the test was made to wait
for the race to settle, but because both outcomes of the race are now
correct.

## Why three triage rounds did not reach it

Three beads (gc-04375, gc-drm7k, gc-gvcal) and several independent
measurement rounds landed on "flaky, pre-existing, load-dependent" without
finding the cause. Two properties of the failure are why, and both
generalize.

**The error was written to a discarded stderr.** The test builds its
`CityRuntime` with `stdout: ioDiscard{}, stderr: ioDiscard{}`, and every
stop-path failure is best-effort logged rather than returned. The
illegal-transition error was printed on every failing run and thrown away,
which is why the failure looked like a bare assertion with "no useful
detail." Swapping the two `ioDiscard{}`s for a locking buffer and dumping it
in the `t.Fatalf` turns an undebuggable flake into a named error in one run.
When a test fails with no detail, check whether the code under test is
writing the detail to a sink the test discards.

**`0.00s` was the strongest available clue and pointed away from timing.**
Every recorded failure was `(0.00s)`. That rules out every timeout in the
fixture — `hangBudget` is `6 × testutil.GoroutineRaceTimeout` = 60s, so a
budget that actually elapsed would show up as seconds. An assertion that
fails instantly under load is a *scheduling-order* race, not a *duration*
race: something did not happen, rather than took too long. Tuning timeouts
could never have fixed this one.

The standalone-vs-shard asymmetry follows from the same fact. Standalone the
test passed 60/60 even at load average 17, because the async commit
goroutine had a free core and always won. Inside the shard it competed with
~1,485 sibling tests for 8 cores and lost often enough to fail 1-in-3, or
2-in-2 on a busy host. The bead's own note that "N=3 per side is not enough"
and that a 1-vs-1 comparison "produces a confident-looking and completely
wrong attribution" is the right lesson from those rounds and is preserved
here — but the reproduction that mattered in the end was not statistical at
all. It was a four-line unit test at the layer where the refusal happened.

## Scope of the fix

Step 4 of the mechanism above has two sides, and the first attempt fixed the
wrong one. That version carved `start-pending` and `creating` into
`Manager.Suspend` on the same pattern as the adjacent `failed-create`
carve-out: tear the runtime down, leave the bead where it is, return
success. The pre-open review (`gc-vrou8`) rejected it, correctly.

`Suspend` is a lifecycle operation. Its contract with the caller is that the
session is now durably paused, and a mid-create bead cannot honor that.
`StateStartPending` means the controller has reserved an identity and still
intends to start it, and the reconciler reads raw `start-pending` — and
`pending_create_claim` — as a start request
(`sessionStartRequestedInfo`, `cmd/gc/session_reconcile.go`). A bead left in
that state is relaunched on the next controller tick. Since
`POST /v0/session/{id}/suspend` calls `Manager.Suspend` directly, that
carve-out handed an operator `200 OK` for suspending a session that was
still queued to start, and the session came back.

Writing a durable cancellation inside `Suspend` instead — clearing
`pending_create_*` and moving the state — is not available either.
`creating` means a provider `Start` call is in flight, so clearing the lease
underneath it races that create's own commit and rollback, both of which the
reconciler owns.

What force shutdown actually needs from a mid-create session is a teardown,
not a suspension, and that lever already exists. `Manager.Kill` explicitly
accepts both mid-create states and stops the runtime without touching the
persisted lifecycle — which is exactly the "leave the bead for the
reconciler to reap" property the first attempt wanted, obtained without
lying to anyone about a lifecycle. So the routing moved rather than the
semantics:

- The transition table is unchanged: `creating` and `start-pending` still do
  not accept `suspend`, and `Suspend` still returns `IllegalTransitionError`
  for them, which the API maps to 409 Conflict. A rejected transition now
  provably leaves no trace — no `sp.Stop`, no metadata write.
- `stopTargetThroughWorkerBoundary` gained a mid-create branch ahead of its
  suspend fallback, routing those targets to `Kill`. The branch keys on the
  bead's state, not on whether the shutdown was forced. A session can be
  mid-create during any stop, and the bead's state — not the operator's
  urgency — is what decides which lever is legal for it; gating on
  `forceStopRequested` would leave the identical leak on the graceful path
  and would have to thread that signal through the stop-target layer to do
  it.
- That branch tolerates an already-gone runtime, by the same rule
  `Manager.Suspend` applies on its own active path: judge by whether the
  provider reported a live process *before* the teardown, not by the shape
  of the error. A `start-pending` bead routinely has no runtime at all, and
  `gc stop` reaches every session bead with no state pre-filter; without the
  tolerance, every session that had not yet reached its provider start would
  report a stop failure. A runtime that was live and refused to die still
  surfaces — swallowing that would report a clean teardown to the stop
  sweep, which then tears the provider server down believing the fleet is
  drained.

The `failed-create` carve-out inside `Suspend` is untouched. It stays
because that state is terminal — the create already rolled back, so there is
no start request left to contradict and nothing in flight to race.

## Blast radius

`workerStopSessionTargetWithConfig` — and therefore `Manager.Suspend` on the
stop path — has exactly one non-test caller,
`stopTargetThroughWorkerBoundary`, so every `gc stop`, `gc suspend`,
supervisor shutdown and restart funnels through the single site that
changed. Sessions in any other state are unaffected: they take the same
suspend path as before, pinned by
`TestStopTargetThroughWorkerBoundaryStillSuspendsActive`.

`Manager.Suspend`'s two non-test callers are `SessionHandle.Stop`
(`internal/worker/handle_lifecycle.go`, reached only through the funnel
above) and the `POST /v0/session/{id}/suspend` handler
(`internal/api/huma_handlers_sessions_command.go`). The API behavior for
mid-create sessions is unchanged from `origin/main`: still a 409, and still
the truthful answer.

## Upstream

The test file blob is identical across the fork's merge-base, `origin/main`
and `upstream/main`, and so is the defect — this is a pure upstream bug on
files neither side has diverged on. Fixed fork-side first per the standing
lens; the bead carries `upstream_pr_candidate`.
