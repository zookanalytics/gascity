# Upstream sync 2026-10-07: carried-commit ledger

Rebase of the fork's carried commits from upstream base `7072b91ea`
(2026-09-29, upstream #6849) onto `upstream/main` `e39772dd1` (2026-10-07,
upstream #7326). 296 upstream commits came in; 60 fork commits were evaluated
one by one.

Verdict key: **KEEP** = still needed, upstream has no equivalent and the
patched code path still exists. **DROP-SUPPLANTED** = upstream carries an
equivalent fix. **DROP-OBSOLETE** = the mechanism the fix guarded was removed.
**REWORK** = intent kept, re-expressed on upstream's new shape. **REGENERATE**
= generated artifact re-derived on the rebased tree.

Every DROP cites the upstream evidence so a later "did we lose behaviour"
audit (see the gc-64f42 incident, where a shed fix recurred in production)
can check the claim instead of re-deriving it.

## Dropped

| Fork commit | Subject | Verdict | Evidence |
|---|---|---|---|
| d45e7f258 | test(productmetrics): neutralize ambient opt-out env in real-getenv tests (gc-1mtj) | DROP-SUPPLANTED | upstream #4809 `a699601f0` (already in the merge base): `internal/testenv/testenv.go` lists `DO_NOT_TRACK` / `GC_DISABLE_USAGE_METRICS` in `LeakVectorVars` and `init()` unsets them; `productmetrics/testenv_import_test.go` imports testenv |
| 502066c9e | fix(productmetrics): sync peer-successor injection to the uploader barrier (gc-i8xf9) | DROP-SUPPLANTED | upstream `071346188` (#6973) adds `startDisableAndPurgeAtUploaderBarrier` at the same site in `control_unix_test.go` |
| 92344d46d hunk (c) | closed-status early return in `work_assignment.go` `ReleaseWorkBead` (gc-7j2nz) | DROP-SUPPLANTED | upstream `042193bad` / `b8c5fb7d5` made `ReleaseWorkBead` a fenced two-tier write (snapshot status+assignee fence). The fork's regression test `TestReleaseWorkBead_DoesNotReopenClosedStep` was kept and passes against the fenced path. Hunks (a)+(b) of the same commit (cherry-picks of upstream PRs #6168 / #6410, both still OPEN upstream) were kept. |
| f336f4cc6 (orphan-sweep half) | SIGPIPE drain in `orphan-sweep.sh` | DROP-SUPPLANTED | upstream `f9efab910` (#6728) rewrote the same pipelines as here-strings; the other script sites in that commit were still defective upstream and were kept |
| 6d033a3d9 | build(bazel): sync BUILD files | REGENERATE | `make bazel-sync` on the rebased tree |
| 4ed047609 | chore(census): re-derive the resource census | REGENERATE | census recipe in TESTING.md on the rebased tree |

## Reworked

| Fork commit | Subject | What changed on upstream | How the fork intent was re-expressed |
|---|---|---|---|
| abdf9bf99 | defer idle-timeout stop while a human terminal is attached (gc-rjtk1) | upstream `f2a2cf5ba` (#6900): an attach-probe error HOLDS (fail closed); new `pendingInteraction` hold rung in the timer trace | the reconciler feeds `TimerFacts.Attached` through upstream's `attachmentHolds()` so a probe error defers the stop; the trace test enumerates the attachments dimension inside upstream's pending-hold wrapper; added `TestReconcileSessionBeads_IdleTimeoutHoldsOnAttachProbeError` |
| 7220a18d4 | release work in every reachable store when a session bead closes (gc-d9qnh) | upstream `042193bad` (#7021): `closeBead` takes `expected session.Info`, `ReleaseWorkBead` fenced; `workAssignmentStores` retired for the storeref resolver (`assigned_work_scope.go`) + residency ratchet | re-implemented as `closeReleaseScope` (sweep / reachable) whose walk delegates to upstream's `sweepAssignedWorkLegs` / `assignedWorkPlanForSessionInfo`; per-leg writes stay on the fenced `ReleaseWorkBead`; the fork's store-list helpers and 10 of 11 `residency:allow` markers were not ported; the fork's `releaseStores[0]` current-claim bug was not ported. Upstream's next-tick `releaseOrphanedPoolAssignments` does NOT cover the leak (skips unrouted / non-pool work), so the fix is still needed |
| d5eb98586 | let a claim holder heartbeat a lease `gc hook --claim` stamped (gc-ox80c) | upstream `9d1f0159f` added `cmd/gc/bd_close_actor.go` (`closeActorForOwnClaim` / `ownClaimCloseEnv`), same mechanism but only for close / `update --status closed`; identity policy excludes GC_SESSION_NAME/GC_ALIAS (#6324) | rebased verbatim, then superseded by a follow-up commit: `heartbeat <id>` joins upstream's own-claim actor targets; the fork's three helpers were deleted. **Behaviour change:** alias- or session-name-stamped claims can no longer be heartbeated via actor rewrite (upstream #6324 policy) |
| 42ff30f88 | supervisor crash-loops against a stale controller holder (gc-x1a87) | upstream `4f518b62a` (#7046) is a different fix (different-install mismatch, fail-open) and introduced `readSupervisorExePathHook` | kept: the two renderers are distinct diagnoses at disjoint call sites (duplicate-supervisor refusal with terminal exit vs invoked-binary mismatch). Follow-up commit folded the duplicated build-identity plumbing onto upstream's `knownGCBuildID` / `supervisorHealthStatusHook` |
| f336f4cc6 | drain `grep -q` of its pipefail SIGPIPE false-negative (gc-d760o) | orphan-sweep half supplanted (above); `scripts/test-docker-session` deleted upstream (`4ba80e52d`); upstream's new `cross-rig-deps.sh` has the same pattern | took upstream's orphan-sweep.sh, dropped `_list-helpers.sh` (wisp-compact uses a here-string), kept the other script fixes, fixed the two new `cross-rig-deps.sh` pipelines, tightened the detector regex so `\|\| grep -q` is not flagged |
| 11ce08e00 | pre-push gate silently skipped its test fan-out on a repeat push (gc-01o2l) | pre-push now execs `.githooks/lib/push-suite.sh` (`e902ed5a9`) instead of make | kept upstream's exec; fork's SIGPIPE-safe change probe and skip announcements layered on top; `push-suite.sh:212` pipeline fixed; contract test rewritten on upstream's `prePushFixture` (drives the real hook + push-suite.sh); the detector now walks `.githooks/lib/` too |
| a8b720970 | test(runtime/exec): poll for the interrupt marker (gc-o7nj1) | upstream `968e8754b` (SIGINT→SIGTERM, renamed test/vars) and `3c859f768` (child writes marker itself) rewrote the test | poll loop re-applied over upstream's renamed `terminateFile` / "terminated" marker |
| 95553ee02 | reinstall golangci-lint when the installed one is not the pin (gc-c9upz) | upstream `5a953bc0a` moved lint/vet to nogo; `$(GOLANGCI_LINT)` prerequisite still used by fmt/lint targets | guard target kept verbatim; consumers narrowed from eight to five (`lint-golangci`, `fmt-check`, `fmt-check-changed`, `fmt`, `install-tools`); the nogo lint block is upstream's |
| 9216373c4 | native dependency guard baseline (docs) | upstream moved to a go.sum-based module count, 190M binary cap, `--gc-binary` (`fb0761285`, `50ae04971`) | docs refreshed to the new mechanism |

## Kept as-is (rebased; any conflicts were mechanical)

SHAs are the rebased ones on this branch.

| Commit | Subject |
|---|---|
| 4d9fb3c88 | feat(doctor): warn on malformed .beads/config.yaml (gc-0kuep) |
| 34093c43d | fix(doctor): exclude slash-bearing strings from session bead ID heuristic (gc-119r) |
| 260891720 | fix(start): pass SSH_AUTH_SOCK through to agent sessions (gc-4kq5vc) |
| b6f9aaa53 | fix(doctor): floor order-firing staleness so short-cadence orders don't false-overdue (gc-9i9k9x) |
| 0b2998e94 | fix(convoy): resolve gc convoy create rig scope like gc bd (gc-nm4d2h) |
| efec969ee | fix(dolt): guard gc dolt sync against concurrent runs with flock (gc-s7jgz) |
| daf2e5b8d | gc-unpyk: wake-budget has no per-session isolation — one spinning session starves all starts |
| 9be08ce2f | feat(config): resolve "<pack>//<subpath>" agent path refs against the imported-pack closure (gc-wvjrm) |
| 53edd3668 | test(doctor): skip the embedded-Dolt drift tests on a CGO_ENABLED=0 bd (gc-t4pi) |
| 1f30a7c5d | fix(doctor): stop surfacing the benign idle-timeout precedence warning (gc-qmr9) |
| bee651a92 | fix(hooks): normalize managed Codex hooks after provider overlay staging (gc-beez) |
| 318c3c111 | feat(nudge): add `gc nudge show` to report whether a queued nudge was delivered or dropped (gc-1kz4) |
| 459bb8bed | Rework branch polecat/gc-ddvrx: address pre-open signoff findings (gc-2s6oz) |
| 875bea2ff | test(tmux): synchronize activity-window tests on output, not the wall clock (gc-wq7t4) |
| 83210669d | fix(orders): refuse unbound city registration of a rig-scoped order (gc-g3pgp) |
| 55cacb521 | test(cmd/gc): stop the reload accept-refresh test deadlocking the binary (gc-8o8un) |
| 0bd90536b | test(cmd/gc): gate drift NFR wall-clock budgets behind an opt-in (gc-3fs0v) |
| 30f19697f | tickDebouncer.cancelPending races an in-flight AfterFunc callback, so a cancelled tick can still fire (gc-l2c6v) |
| ef44079c8 | Rework branch polecat/gc-rfxju: address pre-open signoff findings (gc-kdmqv) |
| a741aa25f | gc doctor: v2-routed-to-namespace — gc-vbkys has short-form gc.routed_to="mayor" (gc-yzxra) |
| 1f1abc1f9 | Rework branch polecat/gc-kawr5: address pre-open signoff findings (gc-uuhqa) |
| 4e9f5ef94 | fix(sling): a molecule poured over a convoy leaves the tracked member independently pool-routed — two polecats dispatched onto one branch (gc-p64nt) |
| f47092fbd | Rework branch polecat/gc-23ep6: address pre-open signoff findings (gc-xeufv) |
| 3967e5dda | gc session: a CLOSED session bead permanently reserves its alias — no CLI lever releases it, so an on_demand agent can never exist again (gc-5fdrr) |
| 4dc54c9c2 | supervisor crash-loops unboundedly against a stale controller holder; no guard, no alarm (gc-x1a87) |
| 993633905 | compact integrity gate: same-count hash drift has no writer-race defer path, so an ordinary bd update hard-quarantines the db and blocks reclaim (gc-800l fix gap) (gc-i52hj) |
| c90204eb1 | feat(doctor): report when the running gc build is behind origin/main (gc-0qbf5) |
| 1e38b859e | gc scope watchdog never restarts dolt: a mid-lifetime crash leaves the scope with a dead server until some later gc invocation notices — a transient ENOSPC panic became a 6h58m city-wide data-plane outage (2026-08-19) (gc-zl5ta) |
| baaf44080 | flaky: TestCityRuntimeForceShutdownTearsDownAfterLateAsyncSweep fails ~1-in-3 in cmd/gc unit shard 1 (pre-existing) (gc-04375) |
| 78030e3f0 | fix(order): make a bounded order-history read store-complete (gc-6a6vz) |
| fcf3ea926 | pre-push gate (make test-fast-parallel) flakes on 3 unrelated timing tests under fleet load; blocks polecat handoff (gc-8jrtx) |
| 4955adffd | fix(dispatch): close scope-check control bead before converging its scope (gc-gfoc7) |
| ae453fec4 | gc CLI prints the always+fresh named_session advisory on every command's stderr — 7.3% of all tool-result text city-wide (gc-dqn8l) |
| a02476d72 | fix(doctor): name an empty GIT_CONFIG_GLOBAL as the beads.role write cause (gc-lgq4p) |
| 2ef48dfec | detached-orphan lane has no merge_result guard: every parked merge anchor matches isDetachedHandoffOrphanCandidate and is re-stamped back into pool demand (gc-gf1l6) |
| 940a3c7aa | one stale [[orders.overrides]] entry silently disables EVERY override in city.toml: ApplyOverrides returns at the first unmatched entry (gc-ndns7) |
| e9ce775dc | city.toml config-edit rewrite strips every comment and silently drops orders.overrides stanzas, changing live behaviour (gc-4q9z5) |
| 8cef98203 | gc bd show silently omits cross-store dependency edges that bd list renders (gc-klgt8) |
| 898e20769 | doctor duration-range flags agent idle_timeout=0, which the runtime treats as disabled (gc-ygqk7) |
| c64671f3b | fix: gate worktree pruning on live-session liveness in gc doctor and the reconciler (gc-9hy4l) |
| ab63546ba | gc events lies to a machine reader three ways: --since discards a timed-out page walk, a comma --type exits 0 with no records and no stderr, and the --json deprecation notice corrupts stdout JSONL (gc-378x4) |
| 9bfc135aa | test(doctor): bound the fix-timeout test so a missed CheckTimeout can't hang 20m (gc-0dzmj) |
| 023348ddb | fix: cascade-nudge-on-blocker-close fires on partial unblock, and its session-source nudge re-injects every turn (queue never dedups/withdraws it) (gc-f3k8d) |
| 5c96f3945 | chore: Re-diagnose model-usage zero-emission gap: tk-md98fc premise refuted (resume/compaction do not rotate the session id) (gc-o9x7m) |
| 66f5398ea | fix(doctor): exempt reserved "human" route target from stale-routed-config (gc-k40o8) |
| d3e75df57 | fix(orders): resolve rig paths for order stores without mutating the shared config slice (gc-4jard) |
| 0eadbbbcd | fix(bd): let a claim holder heartbeat a lease gc hook --claim stamped (gc-ox80c) |

## Regenerated artifacts

Bazel BUILD files were hand-synced (bazel is not installed on the machine that did the rebase); run `make bazel-sync` on a bazel-equipped host before merging and commit any delta. The resource census, CLI reference, doctor check-name golden, config schema docs and metrics command census were regenerated with their owning generators (see the `chore(census)` / `chore(gen)` commits on this branch).

## Upstream-PR candidates (generic fixes the fork should flush upstream)

Nothing in the carried set depends on T3 Code or DoltLite; every commit is a
generic fix. The evaluators ranked these highest for an upstream PR because the
unpatched upstream code still misbehaves for every adopter: gc-9hy4l (worktree
prune without liveness force-removes a live agent's worktree), gc-gfoc7
(scope-check close ordering deadlock), gc-d9qnh (multi-store release on close),
gc-01o2l / gc-d760o (pre-push and script SIGPIPE false negatives), gc-c9upz
(golangci pin guard), gc-119r / gc-k40o8 (doctor false positives), gc-7j2nz's
regression test, and the wake-budget fairness floor (gc-unpyk). The outbox bead
for this is gc-hqcfm.

## Follow-up commits made during the sync

These sit after the rebased carried commits and are part of the sync, not new
features:

- `gc bd heartbeat` rework onto upstream's own-claim actor path (supersedes
  the carried gc-ox80c commit; see Reworked above).
- supervisor stale-image doctor check shares upstream's build-identity helpers
  (gc-x1a87).
- the duplicate-dispatch gate in `mol-polecat-base` / `mol-scoped-work`
  (gc-p64nt) now parks its own step as `status=blocked` and leaves through the
  worker's `gc hook --claim --drain-ack` loop instead of running
  `gc runtime drain-ack` on an open step. Upstream #6992 forbids drain-acking a
  still-claimed step (it is released and re-pooled, so workflow-finalize never
  fires); `blocked` is the one state the hook claim, dependency resolution and
  the drain-ack release all refuse to advance. The parked step keeps its stale
  assignee by design; the documented resolution is to cancel the duplicate run.
- store-op phase expectations in `city_runtime_phases_test.go` gained the 8
  live `List` probes the carried #6168/#6410 cherry-pick issues per
  pool-managed seat (upstream added those goldens in #6952 after the fork
  commit was written).
- the fork's scope-watchdog signaled-server test waits for the fake dolt's
  start record before killing it (a macOS startup race that predates the
  rebase).
- bazel BUILD hand-sync, census re-derivation, this ledger.

## Known pre-existing failures on this host

`make test-fast-parallel` on the macOS host that did the rebase fails on
tests that fail identically on untouched upstream/main there, so they are not
caused by the carried commits:

- `examples/bd/dolt` `TestBackupOrderEscalatesOffsiteFailureWithConfiguredBound`
  and `TestBackupOrderRejectsUnusableOffsiteTimeout` (host `timeout`
  behaviour).
- `scripts` `TestHermeticTagsSyncsLedgerTagsIntoGoTests` and
  `TestHermeticTagsRejectsTagsItCannotRewrite` (the tool needs Python 3.11
  `tomllib`; the host has 3.9).
- `cmd/gc` `TestStopManagedCityBoundsForcedShutdownWhenRuntimeHangs` (timing).
- `cmd/gc` `TestInitRunDoltConfigGetRetriesSlowProbeOnLoadedHost` is
  load-sensitive (10s probe budget) and passes in isolation.

Linux CI is the authority for these.
