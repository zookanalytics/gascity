---
name: Triage of the ten "load-sensitive" push-gate flake beads (gc-8jrtx)
description: The ten beads parked behind gc-8jrtx are not one load-isolation problem. They are at least six distinct mechanisms, only three of which share a fix; two are not load-sensitive at all, one was already given the class fix and still fails, and one filed root cause was wrong in a way that would have shipped a no-op. Records the per-bead verdict, the evidence for each, and what generalizes about batch-filing flakes.
---

# gc-8jrtx: the class is not a class

## Verdict

**N independent problems, not one load-isolation problem.**

Ten beads, at least six distinct mechanisms. A single "these are load noise"
disposition would have buried four real defects, one open product question, and
one wrong root cause. The subset that genuinely shares a fix is three beads.

The bead's own framing named the risk exactly — gc-04375 sat in this bead's
table of flakes, was fixed per-test, and turned out to be a **product** race
(`Manager.Suspend` refusing mid-create sessions, landed as #153), not load
noise. That counter-example is the rule here, not the exception.

## The per-bead verdict

| Bead | Mechanism | Load-sensitive? | Disposition |
|---|---|---|---|
| gc-b3g52 | 50ms whole-watch deadline on calls that return early | Yes | **Fixed** — class fix |
| gc-nnl64 | 25ms deadline vs 4 × 1ms polls | Yes | **Fixed** — class fix |
| gc-cr6lj | 25ms must separate "instant" from "never" | Yes | **Fixed** — class fix |
| gc-8ors6 | Canonical path length vs `$TMPDIR` | **No** — deterministic | **Fixed** — and filed cause corrected |
| gc-i344n | Unguarded `home` TempDir vs exiting bd/dolt child | Widens window only | **Fixed** — both sites |
| gc-t9zf7 | 2s **production** grace before SIGKILL | Yes, but not test-side | Un-park — product question |
| gc-var24 | Live Dolt latency probe as a precondition | Yes, by construction | Un-park — visit-6 ruling engages |
| gc-ek4pu | `> file` truncates before `printf` writes | Widens window only | Un-park — own fix, already specified |
| gc-o95fg | Unknown; test has no timing at all | **Not demonstrated** | Un-park — needs investigation |

Plus this bead's own third test, `TestDisableAndPurgeExactTokenConflictAndPeerCleanRecovery`,
which has the same shape as gc-o95fg and is covered under it below.

## The three that do share a fix

These are the only genuine class: **a test-side wall-clock margin too small for
an assertion that is not about time.** In each, the deadline exists to bound a
failure, the assertion is about a returned value, and the call returns as soon
as the expected thing happens — so a generous deadline costs nothing and a
small one is a bet on the host being idle.

- **gc-b3g52** — `doEventsWatch`'s argument is a whole-watch context deadline.
  Seven call sites expect an early return (a buffered-replay match, or a
  rejected scope) and carried a 50ms literal that had to cover goroutine
  scheduling, an httptest handler and a loopback hop. They now share
  `eventsWatchTestDeadline`. The eighth site is
  `TestDoEventsWatchTimesOutWithoutMatch`, where expiry **is** the behaviour
  under test; it keeps a short value under `eventsWatchTestExpiryDeadline`.
  Naming both is the point — the 50ms had already been copied eight times.
- **gc-nnl64** — the success-path test pinned `supervisorReadyTimeout` to 25ms
  while its hook reports ready on the 4th poll of a 1ms ticker: four scheduler
  turns. `waitForSupervisorPID` returns the moment the hook answers, so the
  deadline is never reached on this path. It never proved the timeout hook was
  used anyway — the 15s default would pass identically — and the timeout's own
  behaviour stays covered by the zero-timeout sibling test.
- **gc-cr6lj** — one 25ms value had to separate an initial `Run` that returns
  immediately from a `Fix` that never returns. `Doctor.Run` races the check
  goroutine's *first scheduling* against `time.After(CheckTimeout)`; losing
  that race marks the fast initial failure "timed out" and skips the fix path,
  which is the reported `fixCalls = 0`. Widened to 2s, ~80× the margin.

A clock seam was considered for gc-cr6lj and rejected: injecting a clock into
`Doctor` is a production change made only for a test, and the test does not
need to observe time — only to not be defeated by it.

**Honest limit on this evidence.** These three are justified by mechanism, not
by an on-demand reproduction. Adding CPU hogs to take the host from load 20 to
load 41 did **not** reproduce the old failures — a tight-loop hog is not the
gate's profile of parallel `go test` processes doing I/O, allocation and
process spawning. The changes are strictly margin-widening on assertions that
do not test timing, so their risk is near zero, but they are not proven fixes.

## The two that are not load problems at all

### gc-8ors6 — the filed root cause was wrong

The bead diagnosed the failure as `len(base) mod 8`: the loop grows `aliasName`
in 8-character `-segment` steps against a hard `<= 100` assertion. Its fix
candidates followed from that — overshoot by one character, or compute the
alias length arithmetically.

**Both would have been no-ops.** The asserted canonical path is
`filepath.Join(normalizePathForCompare(aliasCityPath), ".gc", "controller.sock")`,
which resolves the symlink away — so it is `base + "/city/.gc/controller.sock"`
and **never contains `aliasName` at all**. Growing `aliasName` cannot move it.

The real precondition is a hard threshold on `base` alone: the suffix is 25
bytes, so the fixture is constructible only when `len(base) <= 75`. The
recorded failing path reproduces to exactly the reported 102 bytes from that
arithmetic. `shortSocketTempDir` guarantees a short root only on macOS; on
Linux it inherits `$TMPDIR`, and the gate sets a long per-agent, per-run one.

Fixed with `shortSocketTempDirWithinLimit`, which takes the headroom the caller
needs and falls back to `/tmp` — the short root that both the macOS branch of
`testutil.ShortTempDir` and production's own `controllerSocketPath` fallback
already use. Verified both directions: reverting only this call site fails at
0.00s under the recorded `TMPDIR` (123 > 100); with the fix the test **passes**
(confirmed `--- PASS`, not `SKIP`) 5/5 there and 5/5 under a short `TMPDIR`.

The sibling `TestControllerSocketFallbackUsesShortPathForLongCityPath` was
checked and is safe: it grows a path until it *exceeds* the limit, so a long
`$TMPDIR` only makes its loop terminate sooner.

### gc-i344n — a cleanup race with physical evidence

`home := t.TempDir()` lacked the `retryRemoveAllForTest` guard its sibling
`dir` already had. `home` is where bd writes `~/.beads`, so `t.TempDir()`'s own
`RemoveAll` raced a still-exiting bd/dolt child and reported "directory not
empty" — which `testing` counts as a **failure even for a test that skipped**.

Confirmed physically rather than by reading: `/var/tmp/rp` holds 20 leaked
fixture directories, and every one of them is `.../001/.beads` (that is `home`)
— never `.../002`, the dir that already had the guard. The guarded one always
cleans up; the unguarded one does not. Both tests in the file have the shape
and both leak, so both were fixed. Cleanups run LIFO, so the retry is
registered immediately after `t.TempDir()` to sit just ahead of the removal it
drains for.

Not reproducible on this host — `bd` here is a `CGO_ENABLED=0` build, so both
drift tests skip. The evidence is the asymmetry plus the leaked-directory
census.

## The four handed back

### gc-t9zf7 — the class fix was already applied here, and did not work

This is the most important negative result. The test was **already given** the
load-tolerant treatment: commit `dc683ffa8` (#125, 2026-08-11, bead gc-o7nj1)
replaced a single read of the interrupt marker with a 5s poll loop, for
precisely the reason the class fix exists.

That fix was present at both later observations — verified with
`merge-base --is-ancestor` against `c239b74dd` (this bead's 08-14 run) and
`a4ceef5bb` (gc-t9zf7's 08-20 run) — and the test failed anyway, burning the
full 5s poll (`7.02s` total in the retained log).

The reason is that the binding deadline is not in the test. `runWithContext`
calls `execgrace.Apply(cmd, 2*time.Second)`, which sets `cmd.WaitDelay = 2s`:
the child gets **2 seconds of wall clock** to wake from SIGINT, run its trap
and write the marker before Go force-kills it. A test-side poll of any length
cannot rescue a marker that was never written.

So the open question is a product one, and it is the same question the grace
period exists to answer: under load the cooperative rollback silently loses to
the forced kill, which is exactly the resource leak the comment on
`runWithContext` says the grace was added to prevent. Whether 2s is the right
policy is a judgment for the owner of that path, not a test fix.

### gc-var24 — the visit-6 ruling engages

`TestDoctorScriptSteadyHealthySkipsSweep` decides "steady healthy" from a live
Dolt latency probe against a fixed 1000ms threshold; the gate's own concurrency
measured 1291ms, so the tick was classified unhealthy and the sweep ran. The
assertion is then correct about what happened and wrong about what it meant to
test.

This is load-sensitive **by construction**, but the standing disposition for
this rig says a self-correcting resource sawtooth is accepted as steady state,
and a gate that refuses under it is correct behaviour to preserve rather than
throughput to recover. The test is measuring the sawtooth. The defensible
change is to *establish* the precondition (inject the probe or the threshold)
rather than hope for it — the bead's own candidate 1 — which is a real change
to a health-classification path and wants its own review.

### gc-ek4pu — a real race, already correctly specified

The spy child publishes its env snapshot with `printf ... > $SPY`, and the
shell creates and truncates `$SPY` **before** `printf` writes. The reader
breaks on the first successful `os.ReadFile`, so it can read the empty file.
The filed proof is sound and worth preserving: the observed value was a
1-element `[""]`, not the 3-element `["","",""]` an actually-empty env would
produce — so it read before any line was written, not after three empty ones.

Load only widens the window; this fails on a fast machine too. The fix is
specified on the bead (atomic `os.Rename` into place, or poll until three lines
parse). Independent of everything else here.

Worth noting the same `>`-truncates-before-write mechanism is called out in the
comment `dc683ffa8` added to the exec test — the same defect, found twice
independently, in two packages.

### gc-o95fg — the "load-sensitive" label is unsupported

`TestRecordOnceFreshQuotaBootstrapNeverReplacesDestinationRace/conflicting_destination_appears_before_install`
has **no clock, no sleep, no goroutine, no `t.Parallel`, no real concurrency**.
It drives the race entirely through an injected `storageHooks.beforeStep`
counter. There is nothing in it for load to stretch.

The retained log shows it failed in **0.06s** with `spool_unix_test.go:263:
load quota: absent` — the immediate-assertion signature the bead itself names
as the gc-04375 tell for a real race rather than load noise. It was batch-filed
in the same minute as gc-nnl64 and gc-t9zf7 from one run, with shared
boilerplate asserting all three were "the same shape as gc-b3g52 and gc-cr6lj".
They are three different mechanisms, and this one has no timing dependence at
all.

Two concrete leads, neither yet proven:

1. **The assertion cannot distinguish success from failure.** `RecordDropped`
   is returned from at least eleven distinct sites in `spool.go`, and its own
   doc says "ineligible **or could not be** [stored]". So
   `got != RecordDropped` passes both for the behaviour under test (refused to
   replace a racing quota) and for roughly ten unrelated storage failures. On
   any of those the first assertion passes vacuously, the hook never reaches
   rename #2, `competingData` is never written, and the test fails at the
   second assertion with exactly "absent" and no diagnostic. Making the test
   assert *why* it dropped would convert this from a mystery into a message.
2. **The fixture is rooted at `/tmp`.** `inspectStorageTestHome` deliberately
   forces `TMPDIR` and `GOTMPDIR` to `/tmp` for a documented trust-boundary
   reason (it needs a root-owned sticky ancestor, since the shared workspace
   lives below a group-writable `/data`). `/tmp` here is a 16G tmpfs shared by
   the whole fleet and documented to be exhausted by build waves. That is a
   plausible source of the storage failures lead 1 would surface — and it is
   confined to `internal/productmetrics` (3 files) plus one `cmd/gc` file.

This also covers **this bead's own third test**,
`TestDisableAndPurgeExactTokenConflictAndPeerCleanRecovery`: same package, same
`/tmp`-rooted fixture, and 99 lines containing no timing or concurrency
primitive whatsoever. Two deterministic tests in one package failing only under
a loaded gate points at shared fixture state or host resource pressure, not at
timing.

The trust-boundary choice is upstream-owned and has a stated rationale, so it
should not be changed fork-locally on suspicion. Lead 1 is the cheap, safe,
diagnostic-improving next step and does not depend on lead 2 being right.

## What generalizes

- **A batch filing is not a diagnosis.** gc-nnl64, gc-o95fg and gc-t9zf7 were
  filed in the same minute from one run with identical prose calling all three
  load-sensitive. One is a 25ms test deadline, one has no timing at all, and
  one is a 2s production grace. The shared boilerplate was the only thing they
  shared.
- **Check whether the class fix already landed.** gc-t9zf7 had received it nine
  days before the observation used to file it. Verifying that first would have
  reframed the bead from "flaky test" to "the cooperative-cancellation grace
  loses under load", which is a different and more interesting bug.
- **A wrong root cause is more expensive than none.** gc-8ors6's filed
  mechanism was specific, plausible and wrong, and both of its fix candidates
  would have shipped a no-op that left the gate failing exactly as before.
- **Duration is evidence.** 0.00s and 0.06s failures are immediate assertions;
  7.02s is an exhausted poll. The retained logs distinguished the three
  mechanisms faster than any amount of re-running.
- **`--- FAIL` on a test that skipped is a cleanup failure, not a capability
  gap** — and leaked temp directories on disk are physical evidence you can
  count.
