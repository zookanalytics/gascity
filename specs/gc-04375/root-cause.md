---
name: Root cause of the TestCityRuntimeForceShutdownTearsDownAfterLateAsyncSweep flake (gc-04375)
description: The flake was a real product race, not a test defect — Manager.Suspend refused the mid-create states, so a force shutdown's late sweep leaked exactly the sessions it exists to catch. Records the mechanism, why three prior triage rounds could not reach it, and what generalizes.
---

# gc-04375: the flake was a leak, not a test defect

## Verdict

`TestCityRuntimeForceShutdownTearsDownAfterLateAsyncSweep` was failing
because the product it tests was wrong. `Manager.Suspend` rejected the
`start-pending` and `creating` states with an illegal-transition error
**before** it reached the provider `Stop`, so a force shutdown that
enumerated a running session whose create had not yet committed left that
runtime alive. The test asserted the guarantee the late sweep is supposed
to provide, and the guarantee did not hold.

Fixed by giving `Suspend` the same stance `Kill` already documents — both
mid-create states mean "a runtime process could plausibly exist" — in
`internal/session/manager.go`.

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

The transition table is deliberately unchanged: `creating` still does not
accept `suspend` as a lifecycle transition. `Suspend` short-circuits before
consulting it and leaves the bead where it is, exactly as the adjacent
`failed-create` carve-out does — an in-flight create may still be running,
and the reconciler owns reaping a create that never completed. Marking the
bead `suspended` would invent a lifecycle the session never had.

The carve-out reports a teardown failure when the runtime was live and stays
quiet when it was not, rather than copying `failed-create`'s unconditional
discard. Discarding it would reproduce this same bug one layer up: a live
runtime that refused to die would be reported to the stop sweep as a clean
teardown.

## Blast radius

`Manager.Suspend` has two non-test callers: `SessionHandle.Stop`
(`internal/worker/handle_lifecycle.go`) and the
`POST /v0/session/{id}/suspend` handler
(`internal/api/huma_handlers_sessions_command.go`). Neither branches on the
error, so neither changes behavior beyond the intended one: suspending a
mid-create session now tears its runtime down and reports success instead of
failing and leaving it alive.

## Upstream

The test file blob is identical across the fork's merge-base, `origin/main`
and `upstream/main`, and so is the defect in `internal/session/manager.go` —
this is a pure upstream bug on files neither side has diverged on. Fixed
fork-side first per the standing lens; the bead carries
`upstream_pr_candidate`.
