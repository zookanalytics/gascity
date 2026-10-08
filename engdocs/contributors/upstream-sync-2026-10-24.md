# Upstream sync 2026-10-24: carried-commit ledger

Rebase of the fork's carried commits from upstream base `3c4cf446b`
(2026-10-16, upstream #8016) onto `upstream/main` `c05f1d782` (2026-10-24,
upstream #8249). 134 upstream commits came in (1,477 files, +235.5k/-31.6k
lines; most of it the opjournal shadow-op lanes, the conversation-lane host,
the worker turn boundary, `connect` worker-site mode, the work ladder, and the
reconciler v2 default). 107 fork commits were evaluated one by one against
the new upstream range by three independent audits plus a consolidation map,
then replayed linearly and consolidated. The result is 98 carried commits
plus two regenerated artifacts (the resource census and the Bazel BUILD sync)
and this ledger: 101 over `upstream/main`.

Commits are cited by bead id and fork pull-request number, not by SHA: the
fork's SHAs are rewritten on every sync, and the earlier ledgers' SHA tables
already point at commits that no longer exist.

Verdict key as in the earlier ledgers: **KEEP**, **DROP-ABSORBED** (upstream
carries an equivalent fix), **DROP-SUPERSEDED** (upstream solved the issue
another way), **DROP-OBSOLETE** (the patched code is gone), **REWORK** (intent
kept, re-expressed on upstream's shape), **REGENERATE** (generated artifact
re-derived on the rebased tree), **SQUASH** (a later fork commit that patched
an earlier one is now part of it).

## What changed upstream (the parts that touched the carry)

- **Nudge delivery through ops** (`c11ffbc52` OJ-5, `5b75e237f` OJ-13): `gc
  session nudge` grew `--key` (op idempotency), a `sessionNudgeResult`, and
  `writeSessionNudgeResult`. The carried `gc nudge show` (gc-1kz4) and
  `--reference-bead` (gc-f3k8d) sit beside them; `--key` is op replay, not
  queue supersession, so the cascade-nudge dedup is still needed.
- **Order `requires_backend` and timeout causes** (`1a004d506`, `dbc3acd5d`,
  `2230eea0a`): `dispatchOne` now calls `withOrderTimeout`, and order history
  says when a run timed out. The carried watchdog fix (gc-7s8mo, gc-f1si5v)
  feeds its recorded run timeout into `withOrderTimeout`; the budget accrual
  (gc-q1ttz) sits over the new `requires_backend` skip.
- **BdStore reads** (`2fe2752df` multi-assignee fan-out, `03a398e13` wisp-leg
  quoting and `len(IDs)==0`, `94c682128` min bd 1.3.0, `b44d3097e` Live "open"
  is bd's raw open, `0a276f56d` row revision on native lists, `3afca7612`
  `ReadyOrdered`/`readyPage`): the carried ParentIDs/IDs pushdown
  (gc-d43dp4), metadata-clause pushdown (gc-wtquzx), brief/field projection
  (gc-due3fr, gc-7eevlv) and status pushdown (#244) were re-expressed over
  these; see Reworked.
- **Doctor check contexts** (`729ca22c7`): upstream landed `CheckContext.Done`
  and `Canceled()` and rewrote the pool-idle-routed-work check for large
  cities. The carried abandonment fix (gc-hvk4ub) keeps only what upstream
  lacks: `onAbandon`, `ErrCheckAbandoned`, `stoppableDemandReads`, and the
  Canceled gates in the other checks.
- **Session start** (`8760f7d0e` MintIncarnation, `4668395f4` per-field
  fingerprint policy, `f3d85ea3d` prepare budget, `a85d80d73`,
  `171ad5877`): `preparedStart`/`startCommitPatch` were rewritten and
  `ensureRunningOn` split into `ensureRunningOn` + `ensureLive`. The carried
  foreign-checkout guard (gc-2ddu7) now lives in `ensureLive`; its requirement
  row is SESSION-START-021 (upstream took 018–020).
- **Hook claim** (W1 `8d113cd48`, `a3b38d550`, `a85eeac1b`, `b7d47f68e`,
  `f8f323a70`, `c03611be2`, `f27e0ac4d`): `cmd_hook_claim.go` was rewritten
  around the started-cell fences. The carried StepReminder on
  `existing_assignment` claims (gc-ycww6) sits on top without touching them.
- **Core pack formula lint** (`b84c9fd8e`): a new test refuses any `bd update`
  line in a core formula that mentions a graph-bead variable. The carried
  duplicate-dispatch gate (gc-p64nt, gc-8azum, gc-r0q1kd) builds its refusal
  note in a `REFUSAL_NOTE` variable first; the note text is unchanged. The
  gc-toolkit twins of these formulas need the same shape.
- **Dispatch holds** (`53410be45`): `beadmeta.HasDispatchHold` for
  `hold:mayor`/`hold:external`. It does not cover the carried detached-orphan
  guard's shapes (merge_result-parked anchors without a hold label, step
  beads, claim-time branch keys), so gc-gf1l6 is carried beside it.
- **Codex hooks staging** (`45e46d550`): the tick now stages the raw Codex
  hooks file for the fork's agent shape without normalizing it, so the carried
  normalization (gc-beez) is appended after upstream's staging loops.
- **go 1.26.10** (`b941bd330`); minimum bd 1.3.0 (`94c682128`); the complexity
  baseline refreshed (`ba3ed611d`); dashboard/contrib dependency bumps.

## Dropped

No carried commit was dropped whole. Four carried hunks that upstream landed
were dropped from their commits:

| Carried commit | Dropped hunk | Verdict | Evidence |
|---|---|---|---|
| gc-wtquzx (#226) | `quoteBdQueryString`/`bdQueryStringEscaper`; the beadmail test assertion | DROP-ABSORBED | upstream `03a398e13` adds `quoteBdQueryValue` and the stronger assertion; the metadata-clause pushdown itself is still absent upstream (`listEphemeral` sends no metadata clause) and is kept |
| gc-k0n2xi (#223) | the private `sessionStateFilterAdmitsClosed` helper | DROP-ABSORBED | upstream `cff13efe0` adds `session.StateFilterIncludesClosed`; `Store.List` now uses it |
| gc-d43dp4 (#231) | the `len(query.IDs) == 0` guard in `canApplyWispsServerLimit` | DROP-ABSORBED | upstream `03a398e13` has the identical guard |
| gc-hvk4ub (#228) | `CheckContext.Done`/`Canceled()`/`boundedRun` and the pool-idle check rewrite (with `TestPoolIdleRoutedWorkCheckStopsListingOnceAbandoned`, which pinned the old `collectStoreFindings` shape) | DROP-ABSORBED | upstream `729ca22c7` landed identical Done/Canceled and a deeper pool-idle rewrite |

Near misses, kept: gc-kawr5 (the merge base already priced the Claude 5
family, but the carried hunk is `claude-sonnet-5-5` only); gc-beez (see
above); gc-7kea9's cherry-picks of upstream #6168/#6410 (both still open
upstream).

## Reworked

| Carried commit | What upstream changed | Resolution |
|---|---|---|
| gc-1kz4 `gc nudge show` | `sessionNudgeResult`/`writeSessionNudgeResult` (OJ-5) | `nudgeID` carried on the result; `writeQueuedSessionNudgeResult(target, mode, nudgeID, json, undelivered, op, …)`; `NudgeID` beside `OpID/Key/Replayed` in the JSON; the two fork tests call `deliverSessionNudgeKeyed` (the fork's `deliverSessionNudgeWithWorker` name is now an upstream test helper). Census id 212. |
| gc-5fdrr `gc session release-name` | upstream minted census ids up to 211 | id 213, `next_id` 214 |
| gc-f3k8d `--reference-bead` | `--key` and the op request | both flags on `cmdSessionNudge`; reference set after `resolveSessionNudgeTarget`; cli.md regenerated |
| gc-klgt8 cross-store deps in `gc bd show` | `bdSplitSafeUpdate` rewritten, `ErrDepListBatchUnsupported` | `DepListBatch` uses the fork's edge-record args; `depListDown` falls back to the one-id issues read when the backing refuses edge records (upstream's Postgres-refusal test) |
| gc-7s8mo / gc-f1si5v watchdog sweep | `withOrderTimeout`, `orderTimeoutError` | the recorded run timeout is what `withOrderTimeout` gets in `dispatchOne` and `doOrderRunExecResult` |
| gc-q1ttz budget accrual | `requires_backend` skip, timeout-cause helpers | accrual re-applied over them; the fork's test takes the new `stubReloadBeadsLifecycle` arity |
| gc-wtquzx metadata pushdown | `quoteBdQueryValue`; `Sort == SortDefault` documented as the only valid wisp-limit prefix | kept upstream's sort condition (the fork had also allowed `SortCreatedDesc`) |
| gc-5k8udo convoy members | `ReadyOrdered` in the over-claim list; cache-installing and tier-aware `ChildrenOfAny` wrappers | the fork's simple `CachingStore.ChildrenOfAny` and `beadPolicyStore.ChildrenOfAny` forwards dropped for upstream's; `CachingStore.GetExactBatch` registered as a read in the local-write census; `distinctNonEmpty` defined here |
| gc-d43dp4 ParentIDs/IDs pushdown | `listViaBDList` split with a per-assignee fan-out | `readBDList` (id-chunked) is the core, `fetchViaBDList` a thin wrapper; a 2+-assignee non-wisp leg goes through upstream's fan-out |
| gc-due3fr / gc-7eevlv brief and field projection | `MetadataEquals`, `TestReadyQueryIsZeroConsultsEveryField` | `Fields` after `MetadataEquals`; the IsZero test exempts projection fields (`readyQueryProjectionFields`); `CachingStore.Ready` gates on `!q.IsZero()`; the native ready request sets `Brief` from `Fields.omitsText()` and `project` clears Description only (Revision kept) |
| gc-x647oo config load extraction | `validateOpsModes`, `[ops]`, `WorkLadder` | `validateOpsModes` lives in the fork's `validateDeploymentLayer` |
| gc-a75mwa query indexes | `startTestDoltServer(t testing.TB, opts…)` | signature merged |
| gc-ri5p1e `gc prime` ambiguity | hook fast path (`b30c09996`), `stepLookup` | fast path kept; `primeAgentFromMatches`/`primeAmbiguityError` on its lookup |
| gc-dnzugu native control store | `StampOpenedStore(..., defaulted, ...)` | `openNativeControlStore` passes `ConditionalWritesDefaulted` |
| gc-ycww6 StepReminder | `hookClaimStoppedAdoption`, `(verdict, landedAt)` | StepReminder set from the canonical bead when owned |
| gc-2ddu7 foreign checkout | `ensureLive`, MintIncarnation, prepare budget | guard in `ensureLive` before the hold-consume block; SESSION-START-021 |
| gc-hvk4ub doctor abandonment | `729ca22c7` | the abandonment plumbing is a superset of upstream's Done; pool-idle check is upstream's |
| #244 status pushdown | `nativeListPushesRawOpen` | the raw-"open" arm precedes the folded-"open" arm |
| gc-p64nt / gc-8azum / gc-r0q1kd formulas | refused-shape lint | `REFUSAL_NOTE` shape, text unchanged |
| gc-gf1l6 detached-orphan guard | `HasDispatchHold` | both predicates; upstream's new fixtures use `BranchMetadataKey` |
| gc-t4pi embedded-Dolt skip | metadata.json dolt-dir lookup | upstream's lookup plus the fork's skip |
| gc-23ep6 usage intervals | `workerFactoryWithAttribution` | both; the fork's `getFailingStore` test type renamed `usageGetFailingStore` |
| gc-l6zf4t temp-root sweeps | `doltorphan.Sweep` needs `.dolt/noms` (`585150bc5`) | fixture creates it |

## Consolidated (SQUASH)

Each issue is one commit. These later fork commits patched an earlier one and
are now part of it:

| Folded into | Folded commit | Why |
|---|---|---|
| gc-8azum (#191) duplicate-dispatch gate | gc-r0q1kd (#240) atomic claim | pure rework of #191's gate (review finding gc-yna6va) |
| gc-7s8mo (#209) watchdog sweep | gc-f1si5v (#229) run timeouts | #209's body names gc-f1si5v as the residual exposure of the same defect |
| gc-due3fr (#219) brief ready reads | gc-7eevlv (#238) field projection | #238 replaces #219's `Brief` API with `Fields` |
| gc-cvl9w (#187) convoy list cost | gc-5k8udo (#213) members via edges, and the `ChildrenOfAny` closed-children hunk of gc-dnzugu (#239) | #213 rewrites #187's `MembersBatch`; the #239 hunk fixes #213's `ChildrenOfAny` |
| gc-23ep6 usage intervals | the usage-lane half of gc-oh4e2p (#232): retry an unsettled closed session every 10m for 24h | bounds #232's catch-up loop in the lane gc-23ep6 introduced; the session half stays its own commit |
| gc-c9upz (#164) golangci-lint pin | the `golangci-lint-pinned` guard rewrite of gc-ewcrp6 (#212) (gc-upuqvc: build the linter with go.mod's Go) | rewrites #164's guard; the pre-commit fallback stays its own commit |
| ledger 2026-10-07 | ledgers 2026-10-13, 2026-10-15, and this one | one docs commit for the sync records |

Deliberately not consolidated: gc-8azum into gc-p64nt (distinct root causes:
sling leaving a tracked member pool-routed vs the gate reading the assignee
alone); gc-01o2l into gc-d760o (two incidents of the `grep -q` SIGPIPE class);
the once-only unbound-rig-scope warning of gc-q1ttz into gc-g3pgp (a
sub-feature of the starvation fix, not a patch).

The squash-merge bodies of the PRs merged since 2026-10-15 embedded the whole
fork history (4.8k–7.5k lines each); each was trimmed to its own sections.

## Regenerated artifacts

- Command census: `gc nudge show` 212, `gc session release-name` 213,
  `next_id` 214 (`gen-command-census -check` clean); cli.md and the city
  schema via `genschema` (no drift).
- Resource census (`test/test-resource-counts.toml`): the previous sync's rows
  plus `internal/pidutil/vanish_darwin_test.go` subprocess 1 (#237); every
  changed row belongs to a carried test.
- Bazel BUILD files: hand-synced (no bazel on the rebase host) from the
  previous sync's BUILD commit, the three python halves of `make bazel-sync`,
  and a gazelle-emulating srcs/deps checker that reports 0 findings on pure
  upstream and on this tree (it found four test deps the previous sync
  lacked). `internal/beads` `shard_count` 13 -> 9 with the budget test's pin:
  upstream runs 11, which co-shards the two heavy beads tests once the fork's
  test files are added (`TestBeadsShardsKeepHeavyTestsApart`), and 13 makes
  87 integration-packages actions, over upstream's pinned two-wave cap of 85
  that `TestIntegrationPackagesFitsTheJobsBudget` refuses to widen; 9 keeps
  them apart at 83. gazelle's embedsrcs placement, select() branches and nogo
  stay unverified until CI's "BUILD files in sync" runs.

## Gates run on the rebase host (macOS arm64, system go 1.27.2 selecting go1.26.10, no bazel)

- `go build ./...` and `go vet ./...` over the final tree: clean. Every one of
  the carried commits compiles on its own (`go build ./...` per commit, 100
  of 100 ok).
- golangci-lint 2.12.0 (`GOTOOLCHAIN=go1.26.10`) over the 46 packages the
  fork changes: 0 issues. With `--build-tags integration` it reports 50
  findings, none in a file the fork changes (upstream's own integration
  tests; CI's golangci config sets no build tags). gofmt: clean.
- `go test` over every package except `cmd/gc`, `session/tmux` and
  `beadstest`: 208 ok. The reds are pre-existing on pure upstream on this
  host: `scripts` `TestWorkerEnvNotifyBeads*` (BSD sed/wc in the script's
  stub) and `test/acceptance/helpers` `TestBDInitWrapper*`,
  `TestBdLatestSchemaVersion*` (a `/private` temp-path mismatch and the
  CGO-less bd's embedded-Dolt refusal).
- cmd/gc: the 545 tests the audits and conflicts name (combined `-run`
  regex) pass; the package-level dolt leak guard trips on upstream's
  `TestSessionNudgeLaneRoutesUnservedSessionsLegacy` (a leaked `dolt
  sql-server`), reproduced on pure upstream. The whole cmd/gc suite was not
  run here; CI runs it.
- `scripts` BUILD-shape tests (`*ShardsKeepHeavyTestsApart`,
  `TestIntegrationPackagesFitsTheJobsBudget`, bazel_*): pass with
  `shard_count = 9`.
- A fresh reviewer compared the fork's delta before (`3c4cf446b..` old main)
  and after (`upstream/main..` this branch) hunk by hunk over every
  fork-touched file and found no behavior lost; its one finding (a comment
  naming a helper that was never renamed) is folded into gc-klgt8.

## Open fork pull requests, re-evaluated against this sync

- #227 (gc-wye0uw, keep a running pool session's work_dir across a cap
  raise): **KEEP**. `bindPoolSessionTriggerBead` is byte-identical upstream;
  upstream `2dec01c71` and `4668395f4` make a live session across a cap raise
  the supported path. Only conflict: its REQUIREMENTS row becomes
  SESSION-RECON-022 (upstream took 020–021).
- #224 (gc-c54dxs, Codex hooks on the launch command): **REWORK-AFTER-SYNC**.
  The core issue (Codex reads no `.codex/hooks.json` in a linked worktree) is
  not fixed upstream, but `45e46d550` rewrote `InstallWithResolver` and the
  tick's staging (`workDirOverlayProvider`, `StageProviderOverlayDirSkippingInstalledMergeable`);
  re-add a codex strip branch there, append the strip after upstream's staging
  loops, and invert upstream's new `TestMaterializeProviderOverlays_StagesUninstalledMergeableCodexHook`
  for codex. Decide whether a pack-supplied codex hooks file staged at start
  and stripped on the tick re-creates the drift cycle #7944 fixed.
