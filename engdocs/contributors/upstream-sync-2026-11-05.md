# Upstream sync 2026-11-05: carried-commit ledger

Rebase of the fork's carried commits from upstream base `c05f1d782`
(2026-10-24, upstream #8249) onto `upstream/main` `9f807760c` (2026-11-05,
upstream #7100). 367 upstream commits came in (2,653 files, +428.8k/-31.1k
lines; most of it the typed-store programme and its `storeclass` lint, the
v2 session reconciler's hardening, API read routing for supervisor-hosted
cities, the claims-stall guard, the work-GC lane, and Dolt 2.6 with beads
v1.3.4). The old fork tip was `e03752646`: 102 commits over the old base,
of which 99 were carried commits and three were the regenerated census, the
BUILD sync and the ledger. Each carried commit was evaluated against the
new upstream range by four independent audits plus a consolidation map,
replayed linearly in three audited segments, consolidated, and then read by
two independent reviewers. The result on branch `sync/upstream-2026-11-05`
is 92 carried commits plus three regenerated artifacts (the storeclass
baseline, the resource census and the Bazel BUILD sync) and this ledger: 96
over `upstream/main`.

Of the 99: four were dropped whole because upstream covers them, two
(gc-p64nt and gc-8azum, the core pack's load-context duplicate-dispatch
gate) were shed, and one (gc-i7ujo8) was folded into gc-cvl9w after upstream
absorbed the rest of it. Of the 92 that remain, 51 replayed clean, 34 were
re-expressed on upstream's new shape and 7 lost hunks upstream now carries.

Commits are cited by bead id and fork pull-request number, not by SHA: the
fork's SHAs are rewritten on every sync. Upstream commits are cited by SHA
and pull-request number.

Verdict key as in the earlier ledgers: **KEEP**, **DROP-ABSORBED** (upstream
carries an equivalent fix), **DROP-SUPERSEDED** (upstream solved the issue
another way), **DROP-OBSOLETE** (the patched code is gone), **REWORK** (intent
kept, re-expressed on upstream's shape), **REGENERATE** (generated artifact
re-derived on the rebased tree), **SQUASH** (a later fork commit that patched
an earlier one is now part of it). This ledger adds **SHED**: a carried piece
upstream does not cover, dropped because it is not load-bearing in this city,
with the evidence and the residual risk recorded.

## What changed upstream (the parts that touched the carry)

- **Typed-store programme and the `storeclass` lint** (`8eb722b90` #8435,
  `8a85384bd` #8548, `7cc627b9f` #8722, `3f6a5647c` #8730, `72a8d5279` #8782,
  `0af2214da` #8806). `internal/session`, `graphroute`, `convoy`,
  `sourceworkflow` and `molecule` take typed handles (`beads.SessionStore`,
  `beads.WorkStore`, `beads.GraphStore`, `beads.MoleculeTarget`); an untyped
  `nil` no longer satisfies a store parameter; `convoy.Members` and
  `convoy.TrackItem` are gone (`MembersIn` / `TrackItemIn` with
  `convoy.MemberClasses`). A nogo analyzer counts every raw `beads.Store`
  parameter, field, result and variable, every class-handle mint, unwrap and
  store type assertion in production code against a baseline that may only
  shrink. Thirteen packages are sealed and have no baseline at all:
  `internal/convoy`, `executionevent`, `extmsg`, `graphroute`, `graphv2`,
  `mail`, `mail/beadmail`, `molecule`, `nudgequeue`, `opjournal`, `session`,
  `sourceworkflow`, `workladder`. Carried production code in a sealed package
  must use typed handles. See Regenerated artifacts.
- **Graph-takeover routing** (`ddb7576a1` #7846): a workflow attached to a
  bead restamps it with `gc.execution_routed_to` and clears both
  `gc.routed_to` and `gc.run_target`; route recovery and the detached-orphan
  sweep then leave it alone. This is the opposite answer to the one gc-p64nt
  carried (keep `gc.run_target`, teach route recovery to skip a driven bead).
  See Dropped.
- **Session start and rollback** (`9481d0a38` #8420, `997378712` #8488,
  `d6bf3d664` #8110, `45941642f` #8405): a rollback of a never-finished start
  defers instead of stopping a live incarnation; a warm-claim trigger
  resolver is threaded through the prepared launch; interrupted closes finish
  through `finishInterruptedClose`. #8420 is what the v2 reconciler needs and
  is byte-identical to upstream after this sync.
- **BdStore and caching reads** (`5c28b43e2` #8284 concurrent `listTierPair`,
  `79c81b043` #8360 `GetExactBatch`, `DepListBatch` over edge records and
  read-tier widening in `CachingStore.Ready`, `49f1387ee` #8447 graph-binding
  relocation).
- **Orders** (`b1ec5ff73` #8471 `staleTrackingRead` on the tracking sweeps,
  `221b1476d` #8341 work-GC lane and `moleculeClassStore`, `b66d5254e` #8674
  claims-stall guard and event floors, `691283db4` #8690 `ApplyOverrides`
  continues past an unmatched entry).
- **`gc session list`** (`2a7681066` #8712, `a061170bd` #8727): `--state all`
  and `--state closed` return closed history; a supervisor-hosted city lists
  through the supervisor API whether or not the controller probe answers, and
  both routes print one document. The routing gc-65iln3 was written against
  no longer exists.
- **Core pack** (`c2f5a09e4` #8342 outcome-contract paragraph on every
  retry-managed step, `e2cb3038b` #8630 `wisp-compact.sh` deleted,
  `d68ad2fc8` #8494 a scan that refuses a bare `bd` call in an embedded pack).
- **CI** (`6f488617b` #8563): upstream's `bazel.yml` selects runners by
  `github.repository_owner_id` itself, and three new workflows
  (`advisory-probes`, `security-scan`, `storeclass-baseline`) name Blacksmith
  runners.
- **Toolchain and pins**: go 1.27.3 and Dolt >= 2.6.0 (`f78779e41` #8291);
  golangci-lint 2.14.0 (`db0629225` #8321); beads v1.3.4 with upstream's own
  `beads_migrations.patch`.

## Carried commits

In replay order. "clean" means the patch replayed unchanged apart from
context.

| # | Bead / PR | Purpose | Status this sync |
|---|---|---|---|
| 1 | gc-0kuep | doctor warns on a malformed `.beads/config.yaml` | clean |
| 2 | gc-119r | doctor's session-bead-id heuristic excludes slash-bearing strings | clean |
| 3 | gc-4kq5vc | pass `SSH_AUTH_SOCK` through to agent sessions | reworked: doc comment merged with upstream's `DemandSpawnEnvKeys` wording (`75b59b537` #8468) |
| 4 | gc-9i9k9x | floor order-firing staleness for short-cadence orders | clean |
| 5 | gc-nm4d2h (#32) | `gc convoy create` resolves rig scope like `gc bd` | clean |
| 6 | gc-s7jgz (#39) | flock guard on `gc dolt sync` | clean |
| 7 | gc-unpyk (#57) | per-template fairness in the wake budget | clean |
| 8 | gc-wvjrm (#62) | `<pack>//<subpath>` agent path refs | clean |
| 9 | gc-t4pi | skip embedded-Dolt drift tests on a CGO-less bd | clean |
| 10 | gc-qmr9 (#103) | doctor drops the benign idle-timeout precedence warning | clean |
| 11 | gc-beez (#104) | normalize managed Codex hooks after overlay staging | clean |
| 12 | gc-1kz4 | `gc nudge show` | reworked: `nudgeID` carried on upstream's `queueSessionNudgeWithNote` / `writeQueuedSessionNudgeNote` split (`f7995bae7` #8449); census id 212 -> 215 |
| 13 | gc-ddvrx | census-owner-liveness scoped to this city's namespaces | clean |
| 14 | gc-wq7t4 (#111) | tmux activity-window tests synchronize on output | partly absorbed: the streaming-test rewrite dropped (`e9c57416f` #8497); the ceiling-test hunk kept |
| 15 | gc-g3pgp | refuse unbound city registration of a rig-scoped order | reworked: test file is upstream's plus the six carried tests; now holds gc-ndns7's two comment rewordings |
| 16 | gc-4jard | order-store rig paths resolved without mutating the shared config slice | clean |
| 17 | gc-8o8un (#118) | reload accept-refresh test no longer deadlocks the binary | clean |
| 18 | gc-3fs0v (#119) | drift NFR wall-clock budgets behind an opt-in | clean |
| 19 | gc-l2c6v (#122) | `tickDebouncer.cancelPending` race | clean |
| 20 | gc-rfxju | reject nameless pool routes | reworked: the carried test passes `beads.SessionStore{}` where graphroute now takes a typed store (`3f6a5647c` #8730, `72a8d5279` #8782) |
| 21 | gc-yzxra (#141) | doctor `v2-routed-to-namespace` short-form routes | clean |
| 22 | gc-kawr5 | price `claude-sonnet-5-5`; name unpriced invocations; `agent-token-telemetry` check | reworked: store-check count 25 and skip message 31 over upstream's five new doctor checks |
| 23 | gc-5fdrr (#144) | `gc session release-name` | reworked: `ReleaseSessionNameClaim` takes `beads.SessionStore` (`internal/session` sealed by `3f6a5647c` #8730); census id 213 -> 216 |
| 24 | gc-x1a87 (#149) | supervisor crash-loop guard against a stale controller holder | reworked: upstream's `ExtraEnv` literal (`9e4420b9d` #8466) plus `AlreadyRunningExitCode` |
| 25 | gc-i52hj (#147) | compact integrity gate defers on a writer race | clean |
| 26 | gc-0qbf5 (#152) | doctor reports a gc build behind origin/main | clean |
| 27 | gc-zl5ta (#151) | scope watchdog restarts a dead Dolt server | clean |
| 28 | gc-6a6vz (#155) | bounded order-history read is store-complete | clean |
| 29 | gc-8jrtx (#156) | three timing tests hardened for fleet load | reworked: hang-deadline lint line map 435 / 875 / 921 / 1434 (`31950ae05` #8187 moved the lines) |
| 30 | gc-dqn8l (#157) | named-session advisory no longer printed on every command | clean |
| 31 | gc-lgq4p | doctor names an empty `GIT_CONFIG_GLOBAL` | clean |
| 32 | gc-na313 (#159) | native dependency guard module baseline | clean (docs only; `go.mod`, `go.sum` and the guard script equal upstream's) |
| 33 | gc-gf1l6 (#162) | detached-orphan lane `merge_result` and step guards | reworked: sits after upstream's graph-takeover exclusion (`ddb7576a1` #7846); three upstream fixtures switched to `BranchMetadataKey` |
| 34 | gc-4q9z5 (#168) | city.toml edits keep comments and `orders.overrides` (`internal/tomledit`) | reworked: splice head, then upstream's `snapshotCityEdit` (`822dd74ec` #7955) |
| 35 | gc-klgt8 (#169) | `gc bd show` renders cross-store dependency edges | partly absorbed: the `DepListBatch` half dropped (`79c81b043` #8360); `DepList` down reads through upstream's batch; the `Get` repair kept |
| 36 | gc-ygqk7 (#171) | doctor accepts `idle_timeout=0` | clean |
| 37 | gc-9hy4l | worktree pruning gated on live-session liveness | reworked: gate threaded into upstream's `finishInterruptedClose` (`45941642f` #8405); the stash probe upstream removed is not restored (`b244d69f5` #4732); upstream's configured-root test given a liveness probe (`2454a5303` #6732) |
| 38 | gc-378x4 (#174) | `gc events` machine-reader fixes | reworked: test file append beside upstream's new test |
| 39 | gc-0dzmj (#178) | bound the doctor fix-timeout test | clean |
| 40 | gc-f3k8d (#183) | cascade nudge: queue delivery with `--reference-bead` | partly absorbed: the partial-unblock gate dropped for upstream's `still_held` (`d6bf3d664` #8110); queue delivery and the reference on the assignee arm only |
| 41 | gc-o9x7m (#185) | model-usage zero-emission re-diagnosis | clean |
| 42 | gc-k40o8 (#186) | doctor exempts the reserved `human` route target | clean |
| 43 | gc-7kea9 (#188) | cherry-pick of upstream #6168 + #6410 (live-seat timer-drain protection) | reworked: one comment; both upstream PRs are still open |
| 44 | gc-rjtk1 (#126) | defer the idle-timeout stop while a terminal is attached | reworked: one additive trace constant after upstream's `TraceReasonInterruptedClose` (`45941642f` #8405); legacy path only, v2 has its own attach fence |
| 45 | gc-ox80c (#177) | claim holder may heartbeat a `gc hook --claim` lease | clean |
| 46 | gc-d760o | `grep -q` pipefail SIGPIPE class across shipped scripts | partly absorbed: the `wisp-compact.sh` hunk dropped (script deleted by `e2cb3038b` #8630); upstream's new `notify-claims-stall.sh` given the fix; now holds the `shellpipeline` detector |
| 47 | gc-01o2l (#148) | pre-push gate no longer skips its fan-out on a repeat push | clean; its detector extraction moved to gc-d760o |
| 48 | gc-o7nj1 (#125) | runtime/exec interrupt-marker test polls | clean |
| 49 | gc-c9upz (#164) | reinstall golangci-lint when it is not the pin | reworked: `.PHONY` context; now holds the `LINT_GOTOOLCHAIN` / `LINT_ENV` definition its guard uses |
| 50 | gc-68bao (#176) | reap leaked GOTMPDIR work dirs | clean |
| 51 | gc-cvl9w (#187) | convoy list cost: batched members and `ChildrenOfAny` | partly absorbed: three duplicate `GetExactBatch` methods dropped (`79c81b043` #8360); batch re-landed on typed handles (`72a8d5279` #8782); now holds gc-i7ujo8's progress fan-out and the `ChildrenOfAny` conformance case |
| 52 | gc-ofr2iz (#207) | specs: why bead queries are slow on Dolt | clean |
| 53 | gc-ymv5j4 (#208) | open-work guard and spec-sidecar close list only open members | reworked: on upstream's typed `CloseSpecSidecarsForRoot(beads.MoleculeTarget, ...)` (`72a8d5279` #8782) |
| 54 | gc-7s8mo (#209) | watchdog sweep honors a run's recorded timeout | reworked: `honorRunTimeouts` appended after upstream's `staleTrackingRead` on the five sweep functions (`b1ec5ff73` #8471) |
| 55 | gc-0mue2i (#210) | converge closed workflow roots that list no steps | reworked: on upstream's `sweepStores` / `sameStoreFan` (`7cc627b9f` #8722) |
| 56 | gc-2st54j (#204) | share each store's in-flight gate index read across the pass | clean |
| 57 | gc-5cq3k3 (#216) | controller watches only what the config revision hashes | clean |
| 58 | gc-a8fkt6 (#206) | scrub the test temp root before matching store ids | clean |
| 59 | gc-stxs1f (#198) | isolate HOME in the legacy-transcript API test | clean |
| 60 | gc-8g3jj (#192) | fail a backup sync that exits 0 without committing its manifest | reworked: three failure messages reworded so upstream's bd-routing scan does not read them as bare `bd` calls (`d68ad2fc8` #8494) |
| 61 | gc-92bf3j (#205) | cmd/gc test temp dirs stay under the per-run root | clean |
| 62 | gc-dd0bdr (#221) | workdir test keeps the caller's city path out of assertions | clean |
| 63 | gc-wtquzx (#226) | ephemeral-tier bd query pushes metadata filters down | clean |
| 64 | gc-dz7j6r (#203) | gitcred fixture group | clean |
| 65 | gc-1ghh2b (#220) | script fixtures stay off Homebrew's network | clean |
| 66 | gc-vf5swy (#201) | `test-local-parallel` jobs run in a non-login shell | clean |
| 67 | gc-k0n2xi (#223) | closed named-session index cached across demand passes | reworked: cache rides upstream's `desiredStateOptions` (`b781da8c7` #8259) after `.withCrossStoreGate`; `ClosedNamed` added to the v2 env |
| 68 | gc-ibhb76 (#225) | `gc wait list` reports a capped read on both API routes | reworked: cap probe over upstream's `CachedRead` rows; `Capped` returned beside `Partial` / `Refresh` (`ad82aa6e1` #8699, `0b202116b` #8756) |
| 69 | gc-ewcrp6 (#212) | pre-commit hook passes on the bazel-less macOS host | reworked: `.PHONY` and `scripts/AGENTS.md` placement; its `LINT_GOTOOLCHAIN` hunk moved to gc-c9upz |
| 70 | gc-d43dp4 (#231) | BdStore pushes `ListQuery.ParentIDs` / `IDs` down | reworked: upstream's concurrent `listTierPair` when the query names no ids (`5c28b43e2` #8284), the carried serial path when it does |
| 71 | gc-d9qnh | cross-store claim-release tests | clean |
| 72 | #237 | darwin zombie state from `kinfo_proc` | reworked: upstream's new session-leader test kept, the moved table deleted (`e4782ddff` #8637) |
| 73 | gc-q1ttz (#194) | order dispatch budget accrues per patrol interval | reworked: `replaceOrderDispatcher` keeps upstream's `carryEventFloorsFrom` (#8674) and the carried `carryDispatchRotationFrom` |
| 74 | gc-5pexef (#235) | native Dolt opens keep `BEADS_` / `BD_` env slots | clean |
| 75 | gc-nh4eqz (#234) | fork CI on GitHub-hosted runners | partly absorbed: the `bazel.yml` half dropped (`6f488617b` #8563); runner selection re-keyed to upstream's owner-id predicate and added to three new workflows; CI hash re-derived |
| 76 | gc-due3fr (#219) | control dispatcher wake scope and brief ready reads | reworked: `CachingStore.Ready` merged with upstream's read-tier widening (`79c81b043` #8360); wake filter off on a relocated graph binding (`49f1387ee` #8447) |
| 77 | gc-x647oo (#236) | `gc bd` passthrough fast path | reworked: `ValidateRigFederation` (#8811) in `validateDeploymentLayer`; new `loadBdConfig` loads the full composition for a read on a split city (#8333) |
| 78 | gc-a75mwa (#217) | query indexes on managed bead stores | reworked: `run()` keeps upstream's work-GC lane (`221b1476d` #8341) and the index lane; `OpenDedicated` beside upstream's `OpenPrivate` (`faefebb03` #8728) |
| 79 | gc-ri5p1e (#196) | `gc prime` refuses an ambiguous bare agent name | partly absorbed: the inline render dropped for upstream's `primeAgentPrompt` / `primeStrictTemplatesRender` (`234bd7aa7` #8316); the ambiguity lookup re-landed on them |
| 80 | gc-dnzugu (#239) | control dispatchers take the native store | clean; its conformance case moved to gc-cvl9w |
| 81 | gc-elbshw (#241) | forked supervisor starts in its own session | clean |
| 82 | gc-163b32 (#242) | refuse a supervisor fork outside its service manager | reworked: the carried refusal first, then upstream's tmux-socket refusal (`9e4420b9d` #8466) |
| 83 | #243 | native listings skip count aggregates | clean |
| 84 | gc-2c1aw (#202) | order root carries its labels from creation | reworked: `RootLabels` on upstream's `moleculeClassStore` / `MoleculeTarget` shape (`221b1476d` #8341, `72a8d5279` #8782); its event test records after the dispatcher is built (#8674) |
| 85 | gc-ycww6 (#193) | `gc hook --claim` StepReminder on a held claim | clean |
| 86 | gc-oh4e2p (#232) | transcript ambiguity check keyed on `work_dir` | clean |
| 87 | gc-l6zf4t (#230) | cmd/gc test startup sweeps use the inherited temp roots | reworked: guard constructor takes `testscript.TestingM` (`827236558` #8335, `d167500f4` #8292) and keeps upstream's `diag` (`a67b01f77` #8280) |
| 88 | gc-2ddu7 (#195) | keep a starting session out of another session's checkout | reworked: `preparedLaunchWorkDir` forwards upstream's trigger resolver (`997378712` #8488); requirement row SESSION-START-027; `9481d0a38` #8420 untouched |
| 89 | gc-ysf8tb (#215) | keyed Claude transcript lookup outside the `work_dir` folder | clean |
| 90 | gc-hvk4ub (#228) | abandoned doctor checks stop reading the store | reworked: keeps upstream's `.withSessionsStore` (`997378712` #8488) and `s.assigned` (`2ec66cb49` #8819); an abandoned leg now grades as faulted |
| 91 | #244 | native listings push the not-closed statuses down | reworked: upstream's sweep test double models a status list (`b1ec5ff73` #8471) |
| 92 | #245 | two-phase list hydration as a beads source patch | clean; applies unchanged to beads v1.3.4 |

## Dropped

| Carried commit | Verdict | Upstream | Evidence |
|---|---|---|---|
| gc-ndns7 (#167) one stale `[[orders.overrides]]` entry disables every override | DROP-ABSORBED | `691283db4` #8690 | the same `ApplyOverrides` loop: it continues past an unmatched entry and reports it (the error is multi-line now). Its two comment rewordings moved into gc-g3pgp |
| gc-23ep6 compute intervals for sessions closed between passes | DROP-SUPERSEDED | `4ebacb166` #8485 | close markers are stamped in `session.ClosePatch` and settled in `settleClosedInterval`; the PR body rejects the open-set-diff design the fork carried. Residuals: a raw `bd close` is unaccounted, the settle gives up after 15 minutes, and the replacement lists closed session rows with a metadata filter every 30 s (see Open notes) |
| gc-04375 (#153) force-shutdown race on a `creating` session | DROP-SUPERSEDED | `ed1abd7ce` #8369 | a sweep-only `creating` arm in `Manager.Suspend`, at the spot where the carried commit held a "do not add a carve-out here" comment. Residual: a `start-pending` row still logs a stop error on `gc stop` |
| gc-t7jhlt (#197) backup-order timeout fake installed as `gtimeout` too | DROP-ABSORBED | `ac4a392e9` #8281 | the same change to `writeRecordingTimeout` |
| gc-p64nt (#143) a molecule poured over a convoy leaves the tracked member pool-routed | DROP-SUPERSEDED for the attached bead; SHED for the rest, the gate included | `ddb7576a1` #7846 | see below |
| gc-8azum (#191) load-context's duplicate-dispatch gate misses an unclaimed live peer holding the branch worktree | SHED (operator ruling, 2026-11-06) | none | see below |

**gc-p64nt in detail.** Three of the four incidents on the bead were
bare-bead pours, which `ddb7576a1` fixes with its own semantics; the fourth
is unverifiable (its molecule is purged). What the fork carried beyond that,
and what happened to each piece:

- The route-recovery liveness gate (`liveGraphWorkflowDrivesBead`, the
  `graphStore` parameter through the recovery passes, `relocatedGraphStore`)
  and `internal/sourceworkflow/input_convoy.go`: dropped. Upstream's
  restamp clears `gc.run_target`, so recovery has nothing to promote, and
  upstream's `TestOnFormulaGraphV2RawRoutedThenWrappedIsSingleDemand` pins it.
- The convoy-member restamp (`retireInputConvoyClaimRoutes`): SHED. Upstream
  does not cover it: on a convoy target it restamps only the convoy bead.
  It is not load-bearing here. Across all six rigs, 4 of 8,778 workflow
  roots were poured over an explicit convoy: two multi-member with every
  member already closed, two single-member in September. No formula in this
  city works a multi-member convoy, and under upstream's semantics the
  restamp would strand open members of one. Residual risk: a hand-slung
  single-member convoy whose member is still raw-routed; gc-toolkit's
  `mol-polecat-work` load-context gate backstops it.
- The both-tiers read in `gc formula cook --attach`: SHED. Nothing in the
  live packs or the city config calls `cook --attach`, and none of the 8,778
  roots (21 live) is ephemeral. Residual: a hand-run against an explicit
  convoy whose root was materialized as a wisp.
- The load-context duplicate-dispatch gate in `mol-polecat-base.toml` and
  `mol-scoped-work.toml`, with gc-8azum on top of it (`work-holder.sh`,
  `gc core scripts-dir`, the `mol-polecat-commit.toml` and `pack.toml`
  hunks, two test files; about 2,400 lines): SHED on the operator's ruling
  of 2026-11-06 ("if it breaks again, we figure out another approach").
  Upstream has no equivalent: its load-context reads the bead and goes on
  to workspace-setup, and it still allows a second dispatch through a
  different formula over the same live bead, `--force`, or a plain
  re-sling. The four core formulas are byte-identical to upstream again.
  The city runs this binary's embedded core pack, but the pools' default
  `mol-polecat-work` overrides load-context with gc-toolkit's own gate
  (2,264 runs since 2026-08-15), and both gc-8azum incidents were
  `mol-polecat-work` runs fixed there (tk-r4n51xq). Residual risk: the
  formulas that inherit the core load-context are now ungated, as upstream
  ships them. Those are `mol-polecat-report` (12 runs since 2026-08-15; no
  checkout, so a duplicate costs a second report), `mol-scoped-work` (5
  runs, last 2026-09-07), `mol-polecat-commit` and the four gascity-keeper
  `mol-upstream-gc-*` formulas (no runs since 2026-08-14). No blocked
  load-context step was found in any rig, so the gate has no recorded save.
  If a duplicate dispatch shows up on one of these, the cheaper fix is
  gc-toolkit's `work-gate.sh` at that formula's load-context, or an
  upstream pull request; no upstream pull request for the gate exists.

One-time hazard from the semantic change: a workflow started by the old
binary retired only `gc.routed_to` and kept `gc.run_target`. With the gate
gone, route recovery on the new binary can re-route such a bead once. See
Operational notes.

## Partly absorbed

Carried hunks that upstream landed, dropped from their commits:

| Carried commit | Dropped hunk | Upstream | Kept |
|---|---|---|---|
| gc-wq7t4 (#111) | the streaming-test rewrite | `e9c57416f` #8497 moved the test to `setup_activity_unix_test.go` on an injected clock | the ceiling-test hunk |
| gc-klgt8 (#169) | the `DepListBatch` rewrite, its edge-record helpers and four tests | `79c81b043` #8360 | the `DepList` down read (through upstream's batch, falling back to the rows shape on `ErrDepListBatchUnsupported`) and the `Get` repair with its six tests |
| gc-f3k8d (#183) | the partial-unblock gate and its test file | `d6bf3d664` #8110 `still_held`; upstream tests pin nudging on a failed blocker read | `--reference-bead`, and `--delivery=queue --reference-bead` as trailing flags on the assignee arm only; the routed arm is upstream's |
| gc-d760o | the `wisp-compact.sh` hunk | `e2cb3038b` #8630 deleted the script | every other script, plus the same fix on upstream's new `notify-claims-stall.sh` |
| gc-cvl9w (#187) | `GetExactBatch` on the native, proxied and emitting stores, and a duplicate test | `79c81b043` #8360 | `ChildrenOfAny`, `CachingStore.GetExactBatch`, the batched convoy members |
| gc-i7ujo8 (#211) | the keyset-walk refactor, its guard test, and the `internal/api` and `cmd_mail` hunks | `be73316e3` #8701, `91eaef218` #8700 | only the 8-way `fetchConvoyProgress` fan-out, folded into gc-cvl9w |
| gc-nh4eqz (#234) | the `bazel.yml` runner hunks and the `bazel_multilane_test.go` hunk | `6f488617b` #8563 | the six runner-policy jobs, the review-formulas shard, `runner_policy.py`, the guard test |
| gc-ri5p1e (#196) | the inline prime render and builtin-prompt block | `234bd7aa7` #8316 `primeAgentPrompt` | the ambiguity lookup and refusal |

## Consolidated (SQUASH and moves)

| Into | From | Why |
|---|---|---|
| gc-cvl9w (#187) | gc-i7ujo8 (#211), whole | what survives of it is the progress fan-out in the code gc-cvl9w owns |
| gc-cvl9w (#187) | the conformance case `ChildrenOfAnyIncludesClosedChildren` from gc-dnzugu (#239) | the test for a fix the previous sync already folded into gc-cvl9w |
| gc-c9upz (#164) | the Makefile hunk defining `LINT_GOTOOLCHAIN` and extending `LINT_ENV` from gc-ewcrp6 (#212) | the guard in gc-c9upz uses the variable; the previous split left it undefined between the two commits |
| gc-d760o | the `internal/testpolicy/shellpipeline` detector and its `pack_assets_test.go` hunk from gc-01o2l (#148) | upstream now appends to the same test file, so the two conflicted in turn; the hook half stays in gc-01o2l |
| ledger commit | this ledger | one docs commit for the sync records |

Eight replay fixups were autosquashed into their owners (gc-rfxju, gc-378x4,
gc-0mue2i, gc-x647oo, gc-8g3jj, gc-2c1aw, gc-9hy4l, #244). Thirty-six
commit messages gained one "Upstream sync 2026-11-05" paragraph, and text
describing code no longer in the diff was removed from gc-due3fr (upstream
#7404's message), gc-x647oo (a pidutil change and its revert) and gc-dnzugu
(the `ChildrenOfAny` section).

Still deliberately apart: gc-q1ttz's unbound-rig-scope warning and gc-g3pgp.

## Regenerated artifacts

- **Command census**: upstream minted 212, 213 and 214, which collided with
  the fork's two. `gc nudge show` 215, `gc session release-name` 216,
  `next_id` 217 (`gen-command-census -check` clean). `genschema`, `genspec`
  and the client generator produce no drift.
- **Storeclass baseline (new this sync)**:
  `tools/nogo/analyzers/storeclass/baseline.txt` and `ceiling.txt`. Regenerate
  from `tools/nogo` with
  `go run ./analyzers/storeclass/cmd/storeclass-baseline -write`. It refuses
  growth, so raise `ceiling.txt` by hand to the totals it reports and rerun;
  the same run rewrites `allowed.txt`. Without `-write` it checks. Six
  `cmd/gc` ceiling rows, upstream to this tree: cap-assert 106 to 108, mint
  167 to 169, raw-field 178 to 184, raw-param 1093 to 1120, raw-result 269 to
  281, unwrap 145 to 146. `baseline.txt` gains 46 rows and loses 2. Every
  other package equals upstream; no row is in a sealed package
  (`sealed_floor.txt` and `allowed.txt` unchanged). `go test
  ./analyzers/storeclass/...` passes. Upstream's `storeclass-baseline`
  workflow fails a pull request that raises the baseline unless it carries
  the `storeclass/baseline-raise` label, so a fork pull request that adds a
  raw-store site needs the label or a workflow decision. Nothing on a
  bazel-less host shows this ratchet: nogo is the only caller, and its test
  lives in the separate `tools/nogo` module.
- **Resource census** (`test/test-resource-counts.toml`): new rows for
  `cmd/gc/cmd_convoy_progress_fanout_test.go` (http_test_server 1),
  `examples/bd/dolt/sync_concurrency_test.go` (fixed_sleep 1, subprocess 4)
  and `internal/pidutil/vanish_darwin_test.go` (subprocess 1); changed rows
  `scripts/harness_reap_test.go` subprocess 4 to 5,
  `test/integration/bdstore_test.go` subprocess 6 to 7,
  `cmd/gc/dolt_scope_watchdog_test.go` fixed_sleep 6 to 4 and subprocess 5 to
  3. Every path is fork-added or fork-edited. Upstream's new `test_target`
  kind (it counts `go test` lines in the Makefile and workflows) shows no
  fork delta.
- **cipolicy**: `expectedCIExecutionHash` is
  `671499394b5be779728c862772227f9910d19b23ea447273c51f144a8108d399`, read
  from the failing test. The fork's only delta in `ci.yml` is the
  runner-policy job's `runs-on` line; `nightly.yml` equals upstream's, so the
  nightly digests are upstream's.
- **Bazel BUILD files**: hand-synced again (no bazel on the rebase host), 14
  files, from the previous sync's BUILD commit, the three python halves of
  `make bazel-sync`, and a gazelle-emulating srcs/deps checker that reports 0
  findings on pure upstream and on this tree (44 before the sync). Two new
  BUILD files (`internal/tomledit`, `internal/testpolicy/shellpipeline`).
  `internal/beads` `shard_count` 11 to 9 with the budget test's pinned row:
  upstream's 11 co-shards the two heavy beads tests once the fork's test
  files are added (`TestBeadsShardsKeepHeavyTestsApart`). Upstream's
  integration-packages total is 86 at a cap of 86; the fork sits at 84.
  gazelle's embedsrcs placement, `select()` branches, runtime `data` inputs
  and nogo stay unverified until CI runs.
- **Beads patch**: `third_party/patches/beads_list_two_phase_hydration.patch`
  (#245) applies unchanged to beads v1.3.4 and is registered beside
  upstream's `beads_migrations.patch` in `MODULE.bazel` and
  `third_party/patches/BUILD.bazel`. The list-path files are identical
  between v1.3.1 and v1.3.4; beads has not absorbed two-phase hydration, the
  count-aggregate skip (#243) or the status push-down (#244).
- **Native dependency guard**: unchanged. `go.mod`, `go.sum` and the script
  equal upstream's; modules 737 against a cap of 737. See Known reds for the
  binary cap.

## Review outcome

Two independent reviewers read the replayed tree against the originals and
against upstream, one over the first 75 inventory commits and one over the
rest, without the drivers' reasoning. Neither found a defect in a
re-expression. Confirmed specifically: #8420 is intact on the legacy and v2
start paths; the list-revision contract holds (`FieldsFull` stays the zero
value and a projected row is never installed in the cache); the three
order-dispatch carries compose without weakening an upstream gate; no raw
`beads.Store` was added to a sealed package; no upstream assertion was
loosened; the four whole drops are covered upstream. Upstream tests edited
by the replay were each judged legitimate: the `DepListBatch` control in
gc-klgt8, the `prepareChosenRow` helper in gc-2ddu7, the status-list test
double in #244.

Their must-fix items were the ratchets above (storeclass baseline, CI hash,
BUILD sync). Should-fix items applied:

- gc-8azum: the load-context gate failed open when `gc session list --json`
  printed its error document. Fixed at the time, then moot: the commit was
  shed afterwards (see Dropped).
- gc-hvk4ub: an abandoned leg in the v2 session-migration check now counts
  as faulted as well as not-checked, so under v2 it grades as upstream's
  check does.
- gc-cvl9w: `emittingClassStore.ChildrenOfAny` no longer sits inside
  `DepListBatch`'s doc comment.

Open notes, not changed:

- `CachingStore.GetExactBatch` (gc-cvl9w) leaves dirty ids unresolved, which
  upstream's `GetMany` reads as absent. No path reaches it today.
- Upstream's replacement for gc-23ep6 lists closed session history every 30
  s, the scan gc-23ep6 was designed to avoid and that gc-ymv5j4 and
  gc-k0n2xi remove elsewhere. Measure it after the cutover.
- gc-2ddu7 against #8420 on the legacy path: the carry records
  `launch_task_work_dir` before the launch, and #8420 now defers instead of
  rolling back when another incarnation holds the name, so the row can
  record the unfinished start's directory while the live incarnation runs
  elsewhere. Narrow, v2 is not exposed, and a fix means new code in #8420's
  two deferral arms; left unfixed by ruling.
- gc-x647oo: on a split city a `gc bd` read prints each config warning
  twice. No effect on an unsplit city.
- gc-due3fr: the wake filter is off on a relocated graph binding, which
  means "wake on everything", as upstream does; no wake is missed.
- gc-nh4eqz: two schedule-only workflows still name Blacksmith runners, as
  before this sync. Fork CI now runs bazel with no remote cache (#8261);
  whether cold lanes fit the 90-minute budget is unknown.
- gc-a75mwa: the index self-test was only measured on Dolt 2.2 to 2.4.2. On
  2.6 it may pass, and metadata index builds would then start on live stores
  for the first time.
- #244: a row holding a custom bd status is not in the pushed not-closed
  list, while upstream's sweeps (#8471) say the read must list every
  not-closed status the gate counts. This city defines no custom status.
- gc-s7jgz: the flock-held `exit 0` runs before upstream's
  `write_order_outcome` (#8459), so a tick skipped because another sync is
  in flight records as completed, not skipped.
- gc-f3k8d now nudges when the blocker read fails (upstream's behavior);
  `ListMailInbox` is a first-page read again with gc-i7ujo8's walk gone.

## Known reds that are not the fork's

- `internal/worker/workertest`
  `TestProgramStatusConformance/grok/tmux-cli/WC-PSTAT-002` times out at
  bare upstream `9f807760c` on this host, in `sessionlog.FindGrokSessionFile`.
  The fork's diff is empty for every package on that path.
- `internal/dispatch` `TestProcessRalphControlInfraRetry*` (the exit_75
  case) fails at bare upstream on this host; timing-based. The fork has no
  diff in that package.
- `test/acceptance/helpers` `TestBdLatestSchemaVersionIsolatesHOMEFromSharedServerConfig`
  and `TestBdLatestSchemaVersionLeavesNothingInItsTempRoot` fail at bare
  upstream on this host: the host `bd` is built with `CGO_ENABLED=0` and
  cannot open the embedded Dolt the probe asks for.
- `cmd/gc` `TestInitFromPinsHostedDoltEndpoint` and
  `TestInitFromHostedPreservesRigSiteBindings` fail at bare upstream until
  the host has Dolt 2.6.0 or newer.
- `cmd/gc` `TestBuildParamsDefaultBranch*`,
  `TestStorageMigrateFailsLoudlyWhenTheServedNoteCannotBeWritten` and
  `TestRigImportResumesAfterACrashAtEveryStep` are load-sensitive on this
  host; each passes alone on this tree.
- Native dependency guard, binary cap: on this host (darwin/arm64) the guard
  reports 192,759,906 bytes against a cap of 190,000,000. Built for
  linux/amd64 with the guard's own build line, upstream is 199,100,541 bytes
  and this tree 199,881,603 (the fork adds 781,062). Upstream alone is over
  the cap when built here. The 190,000,000 is the script's default for the
  Go-native `make check-native-dependency-surface` (set 2026-10-05, #7074),
  which no workflow runs. CI runs the Bazel target
  `//scripts:check_native_dependency_surface_test`, which measures the cgo
  binary against its own cap of 230,000,000 (re-baselined 2026-11-03,
  #8724, when upstream measured 218,018,960). The fork's Bazel-built size
  was not measured here. No cap raise is carried.

## Gate results

Run on the finished tree, one job at a time at `nice 19` with capped
parallelism (the operator asked for the host to stay usable), Go-native
because this host has no bazel. The full runs were on the tree just before
the three test fixes below; the fixes were then re-run on the final tree.

- `TestListRevisionFidelityAdmitsRealNativeDolt` (`-tags integration`,
  `internal/beads`): `--- PASS`.
- golangci-lint 2.14.0 over every package the fork changes against
  `9f807760c`: 0 issues. Re-run over `cmd/gc` and
  `internal/beads/beadstest` after the test fixes: 0 issues.
- `go test` over every package except `cmd/gc` and
  `internal/worker/workertest`: 217 packages ok, 2 red (`internal/dispatch`
  and `test/acceptance/helpers`), both red at bare upstream on this host
  (see "Known reds").
- `go test ./cmd/gc`, the whole package (42 minutes): 17 tests red. Each was
  re-run alone at bare upstream `9f807760c` and on this tree:
  - 8 were the carry breaking tests upstream added, through test doubles
    upstream added. Fixed in the owning commits, and all 8 pass:
    - gc-wtquzx: `beadstest.FakeBd`'s `bd query` now takes the
      `metadata.<key>` clauses the carry pushes into the wisp-tier query
      (`TestRunCancelBdCallsDoNotGrowPerMember`,
      `TestRunCancelCloseOrderHonorsBlockersWhenBdRefusesTheBatch`,
      `TestClosureLevelWalkMatchesThePerNodeWalk`,
      `TestPurgeRefusesANestedRootWithAnOpenMember`,
      `TestPurgeRefusesAMemberReopenedBehindTheCache`,
      `TestWispGCGraphArmHolderGuardBackingCallsPerTick`).
    - gc-cvl9w: `GetExactBatch` left the cache's omitted-capability list,
      because the carry puts it on `CachingStore`
      (`TestControllerBindingCacheForwardsEveryEngineCapability`).
    - gc-klgt8: `TestDanglingBlockingEdgesReadsALoneBdRowsRawEdges` checks
      its bd double's one-row answer by asking the double, and expects
      `BdStore.DepList` to show the raw edge, because the carry reads the
      "down" axis from bd's dependency records.
  - 2 need Dolt 2.6.0 or newer and fail at bare upstream too (host has
    2.4.2): `TestInitFromPinsHostedDoltEndpoint`,
    `TestInitFromHostedPreservesRigSiteBindings`.
  - 7 are host load: `TestBuildParamsDefaultBranch*` (5) and
    `TestStorageMigrateFailsLoudlyWhenTheServedNoteCannotBeWritten` fail
    alone at bare upstream and pass alone on this tree;
    `TestRigImportResumesAfterACrashAtEveryStep` passes alone on both.
- `go test ./test/docsync` on the final tree: ok.
- `go vet -tags integration ./cmd/gc ./internal/beads/...` on the final
  tree: clean.
- After those runs gc-8azum was shed (see Dropped). Re-run on the tree
  without it: `go build ./...`; `go vet` and golangci-lint over
  `internal/bootstrap/...` (0 issues); `go test` over
  `internal/bootstrap/...`, `internal/config/...`,
  `internal/testpolicy/...`, `scripts/...` and `test/docsync` (all ok); a
  `cmd/gc` selection on formulas, packs and scoped work (ok);
  `gen-command-census -check` clean; the resource census unchanged; the
  BUILD checker at 0 findings after the core pack's BUILD entries for the
  removed files came out. The full `cmd/gc` suite was not run again.
- Not run: `internal/worker/workertest` beyond the known red, the
  integration and acceptance tiers, and anything under bazel (nogo, the
  `BUILD files in sync` gate). Fork CI on the pull request is the first
  bazel evidence for this tree.

Run during the replay, before the gates: `go build ./...` and `go vet ./...`
clean on the final tree; `go vet -tags integration` clean over the packages
each segment touched; golangci-lint 2.14.0 at 0 issues over the small
reworked packages (not `cmd/gc`); `go test` over the small packages each
segment touched (25, 20 and 38 packages, including `internal/beads`,
`internal/api`, `internal/session`, `internal/config` and `internal/doctor`;
the two that failed in the second segment were upstream tests the carry
broke, fixed in gc-9hy4l and gc-8g3jj); and named `cmd/gc` selections of 260, 166, 928 and 155
tests with no failure. Per-commit buildability was not checked.

## Not carried, on purpose

- **gc-65iln3** (keep `gc session list` local when the controller probes
  miss): not written. `apiRouteLocalWhenSupervisorHosted`,
  `localWhenSupervisorHostedAPIClient` and the `supervisor-hosted` fallback
  reason no longer exist (`2a7681066` #8712); the open contract issue #8453
  closed with `a061170bd` #8727. API-served rows never carry `session_key`,
  `command` or `resume_*`.
- **gc-pdgws8** (codex `--no-daemon`): two commits on a local branch that
  was never pushed, with one staged, uncommitted fix to
  `cmd/gc/cmd_session_test.go` in its worktree. Not covered upstream. The
  live city already launches with `--no-daemon` through `city.toml`. On the
  new base it needs that staged hunk and upstream's new golden row in
  `internal/config/provider_test.go`; only CHANGELOG conflicts.
- **#224** (gc-c54dxs, gc-h6ic7b: Codex hooks on the launch command): open,
  no review, left to go stale. Not covered upstream (upstream #4996 is
  open). It depends on gc-beez being carried unchanged, which it is; on the
  new base it hits one import-block conflict in `cmd_session_test.go`, and
  upstream's new `acpCommand` in `resolveTemplate` gets no hook args, which
  the rework has to decide.
- **#227** (gc-wye0uw, gc-8tb9lh: keep a running pool session's `work_dir`
  across a cap raise): open, no review, left to go stale. Partly covered by
  `94eda8f2e` #8681. Upstream rewrote `bindPoolSessionTriggerBead`, and
  SESSION-RECON-022 is taken, so its row becomes 023. Run the fresh-mode
  case of `TestPoolCapRaiseKeepsLiveSessionWorkDirUntilItStops` on the new
  base first: if it passes without the production hunk, the pull request is
  supplanted.

The reworks for #224 and #227 and both gc-pdgws8 commits exist only on the
rebase host. No other pull request is open on the fork.

## Operational notes for the cutover build

- **Dolt >= 2.6.0** is required (`f78779e41` #8291). `gc start`, `gc init`
  and `gc doctor` enforce it; the supervisor's own boot does not. Upstream
  calls the upgrade one-way and says to back up the data directory first.
- **Town pin**: the city's `build-pins.env` pinned `BD_VERSION=v1.3.2` ahead
  of the fork's old v1.3.1. The build script builds `bd` and `gc` from that
  one beads tree, so the pin must not stay behind `deps.env`'s v1.3.4.
- **`gc session list --state all`** now lists closed sessions; a closed row
  carries `"state": "closed"` and `"closed": true`. gc-toolkit handles it
  (its #1404 counts only open sessions as live); the core pack no longer
  carries a gate that reads the listing.
- **`GC_INSTANCE_TOKEN`** must be in the calling shell for a self
  `gc handoff` and for `gc runtime request-restart` (`a86d05bd7` #8397);
  both exit 1 without it. Codex can strip variables with TOKEN in the name.
- **API reads** (`320b482fc` #8331): in a supervisor-hosted city about
  sixteen read commands go through the supervisor API and its bead cache
  (`session list` under open filters, `session peek`, `rig list`,
  `rig status`, `beads list`, `beads show`, `convoy list`, `convoy status`,
  `mail count`, `mail check`, `mail peek`, `order history`, `wait list`,
  `wait inspect`, `status`, `ready`). A read just after a write can show the
  old state. `GC_NO_API=1` forces the direct read. `gc bd` is not routed.
- **Legacy reconciler behavior that changes on upgrade**: a claims-stall
  guard withholds a template's demand and mails the escalation recipient,
  with no config knob (`b66d5254e` #8674); `gc runtime drain-ack` exits 1
  when the ack did not persist (`7a04b34d9` #8458); rig-scoped named
  sessions restart on config drift (`fdafc8ee0` #8694); the repo-wide stash
  gate on worktree reaping is gone, so expect a catch-up wave of reaps
  (`b244d69f5` #4732); legacy reuse no longer re-points a running pool
  session at work in another directory (`94eda8f2e` #8681).
- **Session shells** lose `GC_TRIGGER_BEAD_ID` and its three siblings
  (`75b59b537` #8468).
- **`[[orders.overrides]]`**: an entry naming a missing order is now a
  config error for `gc order` commands. Run `gc order list` on the new
  binary before the cutover.
- **Restarts**: this city runs no `gc nudge poll` sidecars; restart the
  supervisor and the two `gc convoy control --serve` processes instead.
- **Re-route hazard, one time**: before swapping binaries, list open and
  in-progress beads that carry `gc.run_target` in every rig store, and look
  for any with neither `gc.routed_to` nor `gc.execution_routed_to`:

  ```bash
  gc bd list --status open,in_progress --has-metadata-key gc.run_target --brief --json -n 0
  ```

  On 2026-11-06 all seven stores (HQ, gc-toolkit, signal-loom, gascity,
  shutupandlisten, sprintshow, growth-factor) held none; the same filter
  finds 97 closed beads with the key in gc-toolkit, so it matches. Run it
  again right before the swap, because a workflow started in between can
  add one.
- **v2 session reconciler**: the default logic is unchanged for an unsplit
  city (unset still resolves to legacy). `work_ladder` defaults to `on` and
  is v2 only; upstream #8132 was closed unmerged and landed as #8616, #8586,
  #8620, #8624 and #8673. Run upstream's
  `TestListRevisionFidelityAdmitsRealNativeDolt` (`internal/beads`,
  integration tag) on the synced build before an explicit v2 switch, because
  #243, #244, #245 and gc-due3fr sit under that read.

## For the next sync

- **Storeclass is a sixth regenerable ratchet**, beside the command census,
  the resource census, the cipolicy hashes, the native dependency guard and
  the BUILD sync. On a bazel-less host the gate must run the generator in
  check mode and `go test ./scripts/cipolicy/`, or both stay invisible until
  CI. Type sealed-package code during the replay; it cannot be baselined.
- **Segment the replay.** Put a `break` in the rebase todo after each
  audited stretch (here after inventory commits 040 and 075), give each
  stretch its own driver and its own audit, and require build and vet at
  each break. Fixes for an earlier commit go in as `fixup!` commits at a
  break and are autosquashed once at the end.
- **Log as you go.** Two drivers were cut off mid-stretch when the session
  process exited. Git state survived; unwritten logs and verification did
  not. Append the per-commit record every few commits, and set a backup ref
  after every pass.
- **Verify a stretch in its own worktree** at an explicit SHA while the next
  driver continues; hand findings over as patch files with the exact
  `fixup!` subject.
- **Upstream tests that the carry breaks** were the main source of late
  fixes (five this time: a doctor check built by struct literal, the
  bd-routing scan, an order event recorded before the dispatcher, a tmux
  socket refusal, a status-equality test double). Run each touched package's
  whole test set at the break, not only the carried tests.
- **Check a "clean" replay for compile breaks** against upstream-added
  callers and retyped parameters; gc-rfxju and gc-0mue2i replayed without a
  textual conflict and did not compile alone.
