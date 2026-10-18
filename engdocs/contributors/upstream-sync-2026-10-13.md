# Upstream sync 2026-10-13: carried-commit ledger

Rebase of the fork's carried commits from upstream base `e39772dd1`
(2026-10-07, upstream #7326) onto `upstream/main` `755d36931` (2026-10-13,
upstream #7818). 398 upstream commits came in (1922 files, 660 added, 19
deleted); 84 fork commits were evaluated one by one. 79 remain, plus three
sync-derived commits (a ported test, the census, the Bazel sync) and this
ledger: 83 over `upstream/main`.

Verdict key: **KEEP** = still needed, upstream has no equivalent and the
patched code path still exists. **DROP-SUPPLANTED** = upstream carries an
equivalent fix. **DROP-OBSOLETE** = the mechanism the fix guarded was removed.
**REWORK** = intent kept, re-expressed on upstream's new shape. **REGENERATE**
= generated artifact re-derived on the rebased tree.

Every DROP cites the upstream evidence so a later "did we lose behaviour"
audit can check the claim instead of re-deriving it. The verification that
settled each drop is recorded too.

## What changed upstream (the parts that touched the carry)

- **The session Manager's lifecycle verbs take an Actor** (`76a4ec738`, R10):
  `Suspend(ctx, by, id, soft)`, `Kill(ctx, by, id)`, and a Manager started
  without an absolute city path refuses. Every fork Manager caller and test
  was re-plumbed (`newTestManager`, `testActor`).
- **Seat work is a value** (`SeatWork` over `WorkLegs`, R9): the reconciler
  passes one `*SeatWork` per tick where it passed `rigStores`; the close
  cascade releases claims over every leg (`ReleaseClaims`); the old
  `cmd/gc/session_work_guard.go` is gone (`e11c512e1`).
- **The reconciler compat entry point is `reconcileSessionBeadsAtPath`**
  (`eb49a81cd`): fork tests that called `reconcileSessionBeads` moved to it,
  with the env's city path and start options.
- **Native Dolt store reads go through beads' issueops read roles**
  (`ad78b1a46`, `3c445b9f6`, `493103a67`), and **batched children reads are a
  store capability** (`beads.ChildrenBatchLister` / `ChildrenOfAny`,
  `52b95a13a`).
- **Supervisor**: a `supervisor-binary` doctor check (`3298e7bac`) and an
  `alreadyRunning` closure that also waits out a lock holder (`6e06fb9fe`).
- **Pre-push hook** reads the whole diff once and refuses an unfetched remote
  tip (`b05fe44a6`); `scripts/check-eventexport-isolation.sh` got the
  here-string fix (`0218a1e40`).
- **Pricing table** gained the Claude 5 family (`d361d4191`, `35fc9bb2e`).
- **Ratchets**: command census `next_id` 210 → 211 upstream; resource census
  rebased to upstream's point-in-time baseline; go 1.26.6 → 1.26.9; beads pin
  unchanged (v1.3.1).

## Dropped

| Fork commit | Subject | Verdict | Evidence |
|---|---|---|---|
| b40a373e4 | fix(session): release work in every reachable store when a session bead closes (gc-d9qnh) | DROP-SUPPLANTED | upstream `60d6f0f85` (#7487, close cascade and tick releases use the gates' legs), `b18086138` (#7525, late re-read of a closing seat's claims), `aeb86c845` (#7561, `ReleaseClaims` over `sw.Legs()`; `WorkLegs` = city work store + serving rigs + every relocated binding, "Every seat reads every leg, whatever its agent's scope"), `e11c512e1` (#7628) deleted `session_work_guard.go`; the cross-store close gate is `closeAllowed`. **Verified before dropping:** the fork's three cross-store `TestCloseBead*` tests were ported onto upstream's `closeBead(store, sw, …)` in a scratch worktree on untouched `upstream/main` and passed (rig-store in_progress work released with its pool route restored; fan-out to every serving rig; the same bead id in two stores released in both). They are kept as `a73196f53` because upstream has no such regression test. Accepted delta: a claim whose route no configured agent serves is kept and alerted (`session.stranded_claims`, owner ruling O5) where the fork released it. |
| 3dd7ad20a | fix(session): correct misspelling in session_beads.go comments (gc-rsfvx4) | DROP (falls with b40a373e4) | every "behaviour" line it fixed was fork text added by b40a373e4; upstream's `session_beads.go` has none |
| bd44229f9 | fix(dispatch): close scope-check control bead before converging its scope (gc-gfoc7) | DROP-OBSOLETE | the self-blocking shape (a scope body with a `blocks` edge on the scope-check that closes it) stopped being minted in `ba85a40e2` (#5202, `formula.RewriteRecipeDepsToControls`, in the merge base); upstream pins the legacy shape as refused (`TestProcessScopeCheckAbortRefusesLegacyBodyBlockedOnControl`) and `3bc41481b` (#7444) requires controls to close LAST for crash safety, the opposite of the fork's reorder. **Verified 2026-10-13 by live-store query** (`gc bd list --metadata-field gc.kind=scope-check --include-ephemeral --include-infra --include-gates --include-templates --limit 0 --json`): 0 open scope-check controls in the gascity, gc-toolkit and HQ stores (7 / 35 / 0 exist, all closed); no formula names a scope-check step as a dependency. Dropped with its two tests and `specs/gc-gfoc7/`. |
| 4d092abaa | build(bazel): sync BUILD files (upstream-sync 2026-10-07) | REGENERATE | re-derived as `336eac7cd` (hand sync, see below) |
| 1a3bda7a6 | chore(census): re-derive the resource census (upstream-sync 2026-10-07) | REGENERATE | re-derived as `4f90f7053` |

## Reworked

| Fork commit | Subject | What changed on upstream | How the fork intent was re-expressed |
|---|---|---|---|
| a1214ce4b → `ea077c154` | price the Claude 5 family and name unpriced invocations (gc-kawr5) | `d361d4191` (#6384) and `35fc9bb2e` (#7713) price opus-5, sonnet-5, fable-5, fable-5-1, opus-5-5 | pricing table is upstream's; the fork's unused rows (opus-4-6, haiku-4-5 alias, mythos-5; 0 live usage rows) dropped; **added `claude-sonnet-5-5`** ($2/$10, cache read $0.20, the fleet's most-reported model string, unpriced upstream: an upstream PR candidate); kept the `gc.agent.invocation.unpriced` counter, the `agent-token-telemetry` doctor check and the production-coverage test |
| c8300088a → `45812a5a5` | supervisor crash-loops against a stale controller holder (gc-x1a87) | `3298e7bac` (#6724) `supervisor-binary` doctor check (ExeDeleted + BuildDiffers) at the same registration point, plus `readSupervisorExeLink(pid) (string, bool, error)` which clashed with the fork's helper; `6e06fb9fe` (#7526) `alreadyRunning` closure + `awaitSupervisorLockOrInstance` | the fork's `supervisor-stale-image` doctor check, its tests and golden line dropped; `classifySupervisorHolder` reads the raw procfs link through upstream's `readProcExeLinkHook`; the stale-holder message sits inside upstream's `alreadyRunning` closure; unchanged and still unique: terminal exit code 4 in `runSupervisor` (upstream still `return 1`), the `RestartPreventExitStatus` entry, the message renderer |
| 1e03ea5f1 → `fc34bcb27` | tickDebouncer.cancelPending races an in-flight AfterFunc (gc-l2c6v) | `2b461e733` (#7610) made `cancelPending() bool` (a patrol fire runs a pending poke's tick); the race is unfixed upstream | upstream's bool signature and body, with the fork's generation counter (`gen`, `d.gen++`, `fire(gen)`) merged in |
| e0bad5ed0 → `6ae55425e` | pre-push gate silently skipped its fan-out (gc-01o2l) | `b05fe44a6` (#7806) reads the whole diff once, fails closed on a diff error, refuses an unfetched remote tip | hook body is upstream's; the fork's own change capture dropped; kept the ref counter and the three skip announcements (`TestPrePushAnnouncesEverySkip`), the `push-suite.sh` pipeline fix, the `internal/testpolicy/shellpipeline` detector and `scripts/prepush_gate_contract_test.go` |
| 3e76a2964 → `cec16df7b` | drain `grep -q` of its pipefail SIGPIPE false-negative (gc-d760o) | `0218a1e40` (#7732) fixed `scripts/check-eventexport-isolation.sh` identically | took upstream's version of that file; the other sites (cross-rig-deps.sh, spawn-storm-detect.sh, wisp-compact.sh, lib/test-slice.sh, rebase-resolve-lib.sh, .githooks/lib/push-suite.sh) are still `| grep -q` upstream and stay fixed here |
| 254144dec → `96b90d276` and fbc3aeefe → `89c920b24` (with 65490dcf7 → `c9681bd90`) | convoy member reads through parent-child edges and keyed lookups (gc-5k8udo); push ListQuery.ParentIDs/IDs down in BdStore (gc-d43dp4); convoy list cost (gc-cvl9w) | native store reads rebuilt over issueops read roles (`493103a67` #7563 et al.); `beads.ChildrenOfAny` capability (`52b95a13a` #7456: BdStore reads children of many parents with one `bd dep list … --type=parent-child` plus one exact batched show; other stores read one exact `Children` per parent) | re-expressed in upstream's capability shape. `NativeDoltStore.ChildrenOfAny` reads the inbound parent-child edges of every parent through the Relations role, then the children's rows by exact id (`List(IDFilter)` first, per-id `Get` where a reader refuses IDFilter, edges via `EdgeReader` in DepListBatch's chunks); `NativeDoltStore.GetExactBatch` is the same keyed read (`native_dolt_store_children_batch.go`, tests assert every reader List names its ids and never lists a tier). The fork's raw-storage natives (the SearchIssues ParentIDs edge read and IDs filter, their spies and integration tests) were dropped: they had no home on the issueops read roles. `convoy.MembersBatch` and the API graph BFS (`handler_beads.go`) read legacy children through `beads.ChildrenOfAny`; on a bare BdStore that is upstream's two-call edge read, on the native store the keyed read above, and every wrapper the CLI and controller hand out forwards the capability (`beadPolicyStore`, `emittingClassStore`, `ProxiedStore` to its read leaf, `CachingStore` to its backing store in one batch, since a ParentID listing never reads the cache). Tier narrowing on a bd-backed store: `BdStore.ChildrenOfAny` ignores the tier option and returns durable children only, so the API graph BFS and convoy list no longer see an ephemeral child that `List(ParentIDs, TierBoth)` read through the wisp leg; no class stores steps in the ephemeral tier today. Kept: the BdStore pushdown (`bdstore_list_pushdown.go`, the ListQuery contract, the integration tests; upstream's BdStore still cuts IDs/ParentIDs Go-side), the convoy half, the CachingStore / ProxiedStore / emitter / policy-store `GetExactBatch` forwarders. The emitter's over-claims are pinned per engine (`TestEmittingClassStoreOverClaimsArePinnedPerEngine`: SQLite lacks both batch reads). |

## Kept, with a non-trivial resolution

| Commit | Subject | Resolution |
|---|---|---|
| `29618ba04` | `gc nudge show` (gc-1kz4) | `writeQueuedSessionNudgeResult` takes the fork's nudge id and upstream's undelivered reason; upstream's new self-queued caller passes no id, and the summary line omits the "(nudge …)" suffix then |
| `e6f798875` | wake-budget per-template round-robin (gc-unpyk) | its two tests call `executePlannedStartsTraced` with a city path and a nil trace (upstream renamed the entry point) |
| `7703aa06b` | `gc session release-name` (gc-5fdrr) | command census re-minted on upstream's `next_id`: `gc nudge show` = 211, `gc session release-name` = 212, `next_id` = 213 (upstream took 210 for `runtime workdir-export`); generated files re-run through `gen-command-census` and checked |
| `566b5bf3b` | scope watchdog restarts dolt (gc-zl5ta) | the fork's restart loop calls upstream's `finishManagedDoltScopeWatchdogTermination` at both termination tails (`f4196f0b5`: a failed terminate returns 1 instead of waiting forever) |
| `85f4ff4d3` | route mid-create stop targets to Kill (gc-04375) | the mid-create arm kills through `workerKillSessionTargetCtx` with the sweep actor; the three conformance tests use `newTestManager` and the Actor verbs; the merge resurrected upstream's deleted `TestConformance_QuarantineReactivation`, which was removed again |
| `49aa8e8a8` | `gc bd show` cross-store dependency edges (gc-klgt8) | upstream's `bdShowMissError` not-found path kept, then `completeShownDependencies` after decode |
| `7db757707` | worktree pruning gated on liveness (gc-9hy4l) | the pool-slot retirement threads the session snapshot beside upstream's `SeatWork`; **the v2 close effect's prune (`reconcile_effect_close.go`, `pruneWorktree`) now receives liveness inputs too**, gathered per cascade from the process walk and the open session rows' recorded dirs; without it v2 would prune a live worktree. The legacy pass gathers the liveness inputs lazily (`sync.OnceValue`, on the first prune a pass reaches) instead of every tick: the walk is an `lsof` subprocess on a host without `/proc`, and per tick it took `TestSessionTimelineDifferential` from 1.4 s to 180 s on macOS |
| `482905c18` | `gc events` machine-reader fixes (gc-378x4) | docs-only conflict: upstream's rotation bullet in `event-bus.md`, plus one sentence on the list paths reading the retained archives |
| `af893a487` | cherry-pick of upstream #6168 + #6410 (gc-7kea9) | both PRs still OPEN upstream (checked 2026-10-13); upstream calls `sessionHasOpenAssignedWorkForReachableStore` on the config-drift drain, the max-age gather and a drained (dead) pool seat, not on the no-wake-reason drain of a LIVE pool seat, and has no `currently_processing_bead_id` ownership, so both halves stay. The drain guard calls the helper in upstream's `SeatWork` form, which loads the tick's legs on that first question: the phase goldens (`city_runtime_phases_test.go`) record two live Lists (one per status) instead of the carried eight, sorted as a run because the load is concurrent; the timeline differential's "a live quarantined row" fixtures no longer account a begin-then-vetoed drain (the guard retains the seat before the drain begins), their goldens were regenerated and admitted by a `gc-7kea9` ruling |
| `aff76b890` | defer idle-timeout stop while a terminal is attached (gc-rjtk1) | `lifecycle_timers.go` unchanged upstream; `b16b82944` fixes the sibling sleep-suppression layer; the two tests re-plumbed onto `reconcileSessionBeadsAtPath` |
| `d89793086` | killed test runs leak GOTMPDIR work dirs (gc-68bao) | both sides' additions to `harness_reap_test.go` kept |
| `e435768f9` | fail a backup sync that exits 0 without committing (gc-8g3jj) | upstream `2796da291` (#6363) first-attempt detail reporting kept in the retry loop; the torn-backup escalation therefore names the first torn chunk file, and the test follows |
| `67dac104c` | `ListConvoys` follows `next_cursor` (gc-i7ujo8) | doc-comment collision above `routeMailCheck`, both paragraphs kept |
| `3d3fba8b4` | stop re-reading the whole session history (gc-k0n2xi) | `ClosedNamed` added to upstream's rewritten `gatherEnv`; upstream's new test caller of `supervisorBuildAgentsFnWithSessionBeads` passes a nil cache |

Everything else in the carry applied cleanly and was verified against upstream
by symbol and file (the three audit tables that fed this ledger are summarized
in the commit messages; the previous ledger's evidence standard applies).

## Not consolidated (deliberately)

- `059bae6f2` (gc-0dzmj) and `be466313f` (gc-8jrtx) both raise doctor test
  timing headroom but fix distinct tests for distinct beads.
- `cd6a4bb33` (gc-ewcrp6) and `d30710061` (gc-c9upz) both edit the Makefile
  lint target but fix distinct defects (a bazel-less pre-commit vs a stale
  pinned linter).

## Regenerated artifacts

- **Command census**: ids 211/212, `next_id` 213; `go run ./cmd/gen-command-census -check` clean.
- **Resource census** (`7a90f7870`): scope=all subprocess 749/227 → 753/228,
  scope=all fixed_sleep 495/184 → 494/185, untagged subprocess 500/152 →
  499/152 (debt) and 471/137 → 470/137 (small), untagged fixed_sleep
  318/123 → 316/123 (debt and small); TESTING.md block regenerated with
  `-update` (12 lines).
- **Goldens**: `cmd/gc/testdata/doctor_check_names.golden` (union: upstream's
  `supervisor-binary`, the fork's `binary-freshness`, `bd-config-parse`,
  `agent-token-telemetry`, …; the fork's `supervisor-stale-image` dropped) and
  `internal/testenv/testdata/gc_env_read_baseline.golden` (union) both pass
  their tests.
- **Bazel BUILD files**: hand-synced, bazel is not installed on the rebase
  host. Every fork-added `_test.go` is listed in its package's `go_test` srcs;
  a fork-added library file is listed only where the `go_library` has an
  explicit srcs list (`internal/beads` globs its library sources, so nothing was
  added there); the two fork-only packages got a BUILD.bazel modelled on
  `internal/testpolicy/resourcecensus`. The non-bazel half of `make bazel-sync`
  (`tools/bazel/repo_tree.py`, `hermetic_tags.py`, `integration_suite.py`) was
  run on the host: `repo_tree.py` added the two new packages to the root
  `BUILD.bazel` repo-tree filegroups, which the hand sync had missed; the other
  two reported no drift. The gazelle half has not run (no bazel host), and the
  fork's "BUILD files in sync" job is pinned to a Blacksmith runner the fork
  does not have, so a per-package `go_test` srcs omission would only surface
  when a commit is sent upstream. Run `make bazel-sync` on a bazel host when
  one is available and commit any delta.
- **Native dependency guard**: modules=737 (cap 737), binary 179,081,954 bytes
  (cap 190,000,000); passes.
- **cipolicy goldens**: unchanged (the fork touches neither hashed workflow).

## Gates run on the rebase host (macOS arm64, go 1.27.2 system toolchain, no bazel)

- `go build ./...` and `go vet ./...` clean over the final tree.
- `go vet -tags integration ./internal/beads/ ./test/integration/` clean.
- Targeted `go test` for every package a conflicted or reworked commit
  touched: `cmd/gc` (nudge, debouncer, telemetry doctor, supervisor holder,
  watchdog, drain guard, idle-timeout attach, prune/close effect, convoy,
  class-store emitter, census and golden tests), `internal/session`,
  `internal/beads`, `internal/convoy`, `internal/api`, `internal/pricing`,
  `internal/productmetrics`, `internal/mail/beadmail`, `internal/testenv`,
  `internal/testpolicy/resourcecensus`, `scripts` (hook contract, policy,
  pin, hermetic), `examples/bd/dolt` (backup order).
- Not run here: lint (the host's golangci-lint is built against go 1.26.6 and
  the repo now needs 1.26.9), `make test-fast-parallel`, bazel, the
  acceptance tiers. Linux CI is the authority.

## Known pre-existing failures on this host

Fail identically on untouched `upstream/main` in a scratch worktree, so they
are not caused by the carry:

- `scripts` `TestHarnessSignalDuringLaunchStillTearsDownTheJustForkedJob`
  (the watchdog fork timing under bash 5.3 on macOS; 60 s timeout).
- `internal/api` `TestAPIBeadCloseAsksTheOwningRigsCheckoutForABindingResidentBead`
  fails with `git commit: exit status 128` under the host's `~/.gitconfig`
  (`commit.gpgsign = true`, `gpg.format = ssh`, and the agent socket is
  stale): the test commits in a fixture repo and inherits that config. It
  fails the same way on untouched `upstream/main`, and passes on both trees
  with `commit.gpgsign=false` in the environment (`GIT_CONFIG_COUNT`), so it
  is the host, not the carry.
- `cmd/gc` `TestCmdWorktree*` (9 tests) fail the same way (`git commit:
  Couldn't get agent socket?`) for the same gitconfig reason, identically on
  the pre-fix and final tips; the carry touches no `cmd_worktree` file.
- `scripts` `TestRRCWriterCredential` (the RBE credential mint returns a
  certificate and `curl` cannot connect from this host), and
  `test/acceptance/helpers` `TestBdLatestSchemaVersion*` (the host `bd` is a
  CGO-free build, so embedded Dolt is unavailable): red on both trees.
- `examples/gastown` never runs here: `TestMain` hangs in the orphan Dolt
  store sweep over the host `$TMPDIR` (2,680 entries) on both trees.

## Open fork pull requests, re-evaluated against this sync

Read-only audit of the 20 PRs open on `zookanalytics/gascity` on 2026-10-13
(no PR was closed or commented on by the sync):

- **Close, supplanted:** #222 (gc-7zhki2) — upstream `7814dc135` / `74fa35a93`
  thread the configured search paths through every stale-resume probe site.
  The first commit of #219 is a byte-identical carry of upstream `8a52426d6`.
- **Rework after the sync lands:** #234 (fork CI runners: upstream's
  `bazel.yml` now hardcodes nine blacksmith runners, one unguarded), #228
  (doctor cancel polls must move onto upstream's memoized read path), #224
  (the Codex hook override must also feed upstream's provider-template launch
  command, or async-start drift fires), #219 (only the wake-scope filter,
  `--brief`, and the full-page re-read remain; the one-read-per-scan half is
  upstream `5fa137b28`), #200 (fold into gc-9hy4l and cover the v2 prune
  site), #195 (upstream's v2 `launchWorkDir` bypasses the gate; add it there).
- **Keep, rebase after the sync:** #232, #230, #229, #227, #217, #215, #214,
  #202, #199, #196, #194, #193 (several conflict only with fork `main`;
  #232's tests need the actor-taking Manager verbs).
- **Unrelated:** #191 (pack formulas only).

## Upstream-PR candidates

Unchanged from the previous ledger, plus: `claude-sonnet-5-5` pricing (unpriced
upstream, the fleet's most-reported model); the duplicate-supervisor terminal
exit code (gc-x1a87, upstream `runSupervisor` still returns a bare 1); the
tickDebouncer generation counter (gc-l2c6v, race unfixed upstream); the
pre-push skip announcements and shellpipeline detector (gc-01o2l / gc-d760o);
the v2 close-effect prune liveness (gc-9hy4l, upstream's v2 path prunes
without liveness inputs); the BdStore ParentIDs/IDs pushdown (gc-d43dp4);
`claude-haiku-5-5` pricing ($0.10/$0.50, unpriced on both sides); upstream's
`nativeListLimitPushdown` sends `query.Limit` down for a `SortCreatedAsc` +
`IncludeClosed` + `TierBoth` listing even when `ParentIDs` or `IDs` are set and
cut Go-side, so such a limited query can truncate before its residual filter
(no current caller sets a Limit on one); upstream's `BdStore.ChildrenOfAny`
ignores tier options and drops ephemeral children, where `Children(id,
WithBothTiers)` reads them. The convoy/native perf work should go
upstream only in the `ChildrenOfAny` shape, which `NativeDoltStore.ChildrenOfAny`
/ `GetExactBatch` now take.
