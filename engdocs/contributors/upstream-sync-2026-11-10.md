# Upstream sync 2026-11-10: carried-commit ledger

Rebase of the fork's carried commits from upstream base `9f807760c`
(2026-11-05, upstream #7100) onto `upstream/main` `f14384642` (2026-11-10,
upstream #9049). 121 upstream commits came in (1,254 files, +86.5k/-12.0k
lines). The old fork tip was `cb80c5354`: 96 commits over the old base, of
which 92 were carried commits and four were the storeclass baseline, the
resource census, the BUILD sync and the ledger.

The operator's instruction for this sync was to carry less: drop whatever
upstream covers, shed what this city does not use, and fold a commit that
fixes another carried commit into it. Each carried commit was evaluated
against the whole of the new upstream by four independent audits, plus a
consolidation map, an upstream briefing and an open-pull-request audit. The
operator then ruled on every shed candidate. The result on branch
`sync/upstream-2026-11-10` is 71 carried commits, this ledger and three
regenerated artifacts (storeclass baseline, resource census, Bazel BUILD
sync): 75 over `upstream/main`. The carry against upstream fell from 45.5k
added lines to 31.1k (ledgers excluded), and from 536 changed files to 406.

Of the 92: three were dropped because upstream covers them, eighteen were
shed whole, four were trimmed, and one hunk group moved between two carried
commits. Of the 71 that remain, 12 changed in this sync and 59 replayed
unchanged apart from context.

Commits are cited by bead id and fork pull-request number, not by SHA: the
fork's SHAs are rewritten on every sync. Upstream commits are cited by SHA
and pull-request number. Verdict key as in the earlier ledgers: **KEEP**,
**DROP-ABSORBED**, **DROP-SUPERSEDED**, **DROP-OBSOLETE**, **REWORK**,
**REGENERATE**, **SQUASH**, and **SHED** (not covered upstream, dropped
because it is not load-bearing in this city, with the residual risk
recorded).

## What changed upstream (the parts that touched the carry)

- **Typed stores, T6 and T7** (`b68da94ca` #8856 through `f14384642` #9049):
  API handlers and sling take class handles; `State` lost its per-class
  accessors (`70583dada` #8926); convoy membership is walked by
  `membersFrom`, shared by `MembersIn` and the new `MembersOn` (#9049). The
  fork called none of the removed accessors.
- **Storeclass `cache-peel` rule** (`1c78c0b9f` #8969, `35be9d0ec` #8997):
  a new enforced analyzer rule; the baseline shrank upstream. The fork adds
  no cache-peel site.
- **Unix build tags** (`7e2431b2b` #8887, `dfcf6cb1a` #8888) and the
  **Windows lane** (`0629c3fce` #8977): Unix-only tests moved to
  `*_unix_test.go`; gazelle now emits per-platform `select()` lists; `ci.yml`
  gained two jobs, which changes the CI policy hash.
- **Doctor and the order dispatch floor** (`83d6398a5` #8951, `9ba215cb5`
  #9002): `order-firing-current` is judged against a computed dispatch
  floor, and a new `order-dispatch-floor` check warns when intervals fall
  below it. This replaces gc-9i9k9x.
- **`internal/platform`** (`fdfec4c19` #8889, `34485296e` #9035): `filelock`,
  `livefile` and `localsocket`, adopted at about twenty call sites. No carried
  commit that remains takes a file lock of its own.
- **Session lifecycle** (`64ba02c02` #9011): `gc session attach` asks the
  controller to wake a sleeping session and took requirement id
  SESSION-START-027.
- **Storage split S5** (`2e5cfe241` #8868 and siblings): `gc storage
  snapshot` and `snapshot restore` took command census ids 215 and 216.
- **Config** (`5d166766b` #8746): `[daemon] drift_deferral_escalate_after`,
  validated as a positive duration.
- **Pins**: unchanged. go 1.27.3, Dolt 2.6.0, beads v1.3.4, golangci-lint
  2.14.0; `deps.env` and `MODULE.bazel` are byte-identical to the old base.

## Carried commits

In replay order. "unchanged" means the patch replayed with no change beyond
context. BUILD.bazel hunks and `specs/` records were removed from every
carried commit (see Consolidated and Shed); that is not repeated per row.

| # | Bead / PR | Purpose | Status this sync |
|---|---|---|---|
| 1 | gc-119r | doctor's session-bead-id heuristic excludes slash-bearing strings | unchanged |
| 2 | gc-4kq5vc | pass `SSH_AUTH_SOCK` through to agent sessions | unchanged |
| 3 | gc-nm4d2h (#32) | `gc convoy create` resolves rig scope like `gc bd` | unchanged |
| 4 | gc-t4pi | skip embedded-Dolt drift tests on a CGO-less bd | unchanged |
| 5 | gc-beez (#104) | normalize managed Codex hooks after overlay staging | unchanged |
| 6 | gc-ddvrx | census-owner-liveness scoped to this city's namespaces | unchanged |
| 7 | gc-wq7t4 (#111) | tmux activity-window ceiling test synchronizes on output | unchanged |
| 8 | gc-g3pgp | refuse unbound city registration of a rig-scoped order | now holds the deduplicating unbound-rig-scope warning log from gc-q1ttz |
| 9 | gc-4jard | order-store rig paths resolved without mutating the shared config slice | unchanged |
| 10 | gc-8o8un (#118) | reload accept-refresh test no longer deadlocks the binary | unchanged |
| 11 | gc-3fs0v (#119) | drift NFR wall-clock budgets behind an opt-in | unchanged |
| 12 | gc-l2c6v (#122) | `tickDebouncer.cancelPending` race | unchanged |
| 13 | gc-rfxju | reject nameless pool routes | unchanged |
| 14 | gc-yzxra (#141) | doctor `v2-routed-to-namespace` short-form routes | unchanged |
| 15 | gc-kawr5 | price `claude-sonnet-5-5`; name unpriced invocations; `agent-token-telemetry` check | reworked: store-check count 26 and skip message 32 over upstream's `storage-split` check (the hunk replayed without a conflict at the wrong value) |
| 16 | gc-5fdrr (#144) | `gc session release-name` | reworked: census id 216 -> 217 |
| 17 | gc-i52hj (#147) | compact integrity gate defers on a writer race | unchanged |
| 18 | gc-zl5ta (#151) | scope watchdog restarts a dead Dolt server | unchanged |
| 19 | gc-6a6vz (#155) | bounded order-history read is store-complete | unchanged |
| 20 | gc-8jrtx (#156) | four timing tests hardened for fleet load | unchanged |
| 21 | gc-dqn8l (#157) | named-session advisory no longer printed on every command | unchanged |
| 22 | gc-lgq4p | doctor names an empty `GIT_CONFIG_GLOBAL` | unchanged |
| 23 | gc-gf1l6 (#162) | detached-orphan lane `merge_result` and step guards | unchanged |
| 24 | gc-4q9z5 (#168) | city.toml edits keep comments and `orders.overrides` (`internal/tomledit`) | unchanged |
| 25 | gc-klgt8 (#169) | `gc bd show` renders cross-store dependency edges | unchanged |
| 26 | gc-ygqk7 (#171) | doctor accepts `idle_timeout=0` | unchanged |
| 27 | gc-9hy4l | worktree pruning gated on live-session liveness | unchanged |
| 28 | gc-378x4 (#174) | `gc events` rejects a comma `--type`; `--json` notice off stdout | unchanged |
| 29 | gc-0dzmj (#178) | bound the doctor fix-timeout test | unchanged |
| 30 | gc-f3k8d (#183) | cascade nudge: queue delivery with `--reference-bead` | unchanged |
| 31 | gc-k40o8 (#186) | doctor exempts the reserved `human` route target | unchanged |
| 32 | gc-7kea9 (#188) | a live session's `currently_processing` bead counts as owned (from upstream #6410, still open) | trimmed: see Trimmed |
| 33 | gc-ox80c (#177) | claim holder may heartbeat a `gc hook --claim` lease | unchanged |
| 34 | gc-d760o | `grep -q` pipefail SIGPIPE class across shipped scripts, with the `shellpipeline` detector | partly absorbed: the orphan-sweep test case dropped; its test re-landed on upstream's `pack_assets_test.go` (`dfcf6cb1a` #8888) |
| 35 | gc-c9upz (#164) | reinstall golangci-lint when it is not the pin | reworked: its tests sit after upstream's new oapi-codegen retry test (`b32baea29` #8992) |
| 36 | gc-cvl9w (#187) | convoy list cost: batched members and `ChildrenOfAny` | reworked: on upstream's `membersFrom` (#9049), which now takes a resolver factory; holds gc-d43dp4's `convoySet` cleanup |
| 37 | gc-ymv5j4 (#208) | open-work guard and spec-sidecar close list only open members | unchanged |
| 38 | gc-7s8mo (#209) | watchdog sweep honors a run's recorded timeout | unchanged |
| 39 | gc-0mue2i (#210) | converge closed workflow roots that list no steps | unchanged |
| 40 | gc-2st54j (#204) | share each store's in-flight gate index read across the pass | unchanged |
| 41 | gc-5cq3k3 (#216) | controller watches only what the config revision hashes | unchanged |
| 42 | gc-a8fkt6 (#206) | scrub the test temp root before matching store ids | unchanged |
| 43 | gc-stxs1f (#198) | isolate HOME in the legacy-transcript API test | unchanged |
| 44 | gc-8g3jj (#192) | fail a backup sync that exits 0 without committing its manifest | unchanged |
| 45 | gc-92bf3j (#205) | cmd/gc test temp dirs stay under the per-run root | reworked: import block beside upstream's `localsocket` import (#8889) |
| 46 | gc-dd0bdr (#221) | workdir test keeps the caller's city path out of assertions | unchanged |
| 47 | gc-wtquzx (#226) | ephemeral-tier bd query pushes metadata filters down | unchanged |
| 48 | gc-dz7j6r (#203) | gitcred fixture group | unchanged |
| 49 | gc-1ghh2b (#220) | script fixtures stay off Homebrew's network | unchanged |
| 50 | gc-vf5swy (#201) | `test-local-parallel` jobs run in a non-login shell | unchanged |
| 51 | gc-k0n2xi (#223) | closed named-session index cached across demand passes | unchanged |
| 52 | gc-ewcrp6 (#212) | pre-commit hook passes on the bazel-less macOS host | reworked: `scripts/AGENTS.md` paragraph after upstream's `run_suite` paragraph (`7fd0a8142` #7826); its root `AGENTS.md` sentence dropped, because it put the file 32 bytes over upstream's 20,000-byte budget (`TestRootAgentsFileWithinBudget`) |
| 53 | gc-d43dp4 (#231) | BdStore pushes `ListQuery.IDs` down | trimmed: see Trimmed |
| 54 | #237 | darwin zombie state from `kinfo_proc` | unchanged |
| 55 | gc-q1ttz (#194) | order dispatch budget accrues per patrol interval | its warning log moved to gc-g3pgp |
| 56 | gc-5pexef (#235) | native Dolt opens keep `BEADS_` / `BD_` env slots | unchanged |
| 57 | gc-nh4eqz (#234) | fork CI on GitHub-hosted runners | reworked: CI hash re-derived over upstream's Windows-lane `ci.yml` |
| 58 | gc-due3fr (#219) | control dispatcher wake scope and brief ready reads | unchanged |
| 59 | gc-x647oo (#236) | `gc bd` passthrough fast path | reworked: upstream's `drift_deferral_escalate_after` check re-landed in the split `validateDeploymentDurations` |
| 60 | gc-dnzugu (#239) | control dispatchers take the native store | unchanged |
| 61 | gc-elbshw (#241) | forked supervisor starts in its own session | unchanged |
| 62 | gc-163b32 (#242) | refuse a supervisor fork outside its service manager | unchanged |
| 63 | #243 | native listings skip count aggregates | unchanged |
| 64 | gc-2c1aw (#202) | order root carries its labels from creation | unchanged |
| 65 | gc-ycww6 (#193) | `gc hook --claim` StepReminder on a held claim | unchanged |
| 66 | gc-oh4e2p (#232) | transcript ambiguity check keyed on `work_dir` | unchanged |
| 67 | gc-2ddu7 (#195) | keep a starting session out of another session's checkout | reworked: requirement row SESSION-START-028; `relaunchCall` keeps upstream's `queueStartupNudgeHandback` (`310eabd54` #8971) and the launch-dir record |
| 68 | gc-ysf8tb (#215) | keyed Claude transcript lookup outside the `work_dir` folder | unchanged |
| 69 | gc-hvk4ub (#228) | stop an abandoned pack doctor script and its process group | trimmed: see Trimmed |
| 70 | #244 | native listings push the not-closed statuses down | unchanged |
| 71 | #245 | two-phase list hydration as a beads source patch | unchanged; applies to beads v1.3.4 |

## Dropped because upstream covers it

| Carried commit | Verdict | Upstream | Evidence |
|---|---|---|---|
| gc-9i9k9x floor order-firing staleness for short-cadence orders | DROP-SUPERSEDED | `83d6398a5` #8951, `9ba215cb5` #9002 | `order-firing-current` is judged against the computed dispatch floor, at least six minutes in this city against the carry's fixed five |
| gc-o7nj1 (#125) runtime/exec interrupt-marker test polls | DROP-OBSOLETE | none needed | `Start` blocks in `cmd.Run()` until the trap shell exits, so the marker exists before `Start` returns. Decided by reading; if wrong, one test flake returns |
| gc-d9qnh cross-store claim-release tests | DROP-ABSORBED | upstream's `closeBead` releases over every leg | tests only, for a fix upstream owns. Not confirmed: that an upstream test pins the same-id-in-two-stores case |

## Shed on the operator's ruling (2026-11-10)

None of these is covered upstream. Each was presented with its evidence
and the operator chose to stop carrying it.

| Carried commit | Why it was not load-bearing here | Residual risk |
|---|---|---|
| gc-unpyk (#57) wake-budget fairness | legacy reconciler start path only; the city runs v2 since 2026-11-06 | a revert to `session_reconciler = "legacy"` loses it |
| gc-rjtk1 (#126) no idle-stop while a terminal is attached | legacy loop only; v2's `armIdleTimeout` refuses to idle-stop an attached or uncertain session | same |
| gc-0kuep doctor warns on a malformed `.beads/config.yaml` | detection only | the detection is gone unless redone as a pack doctor script |
| gc-0qbf5 (#152) doctor reports a gc build behind origin/main | detection only, fork operations | same |
| gc-s7jgz (#39) flock on `gc dolt sync` | the order's open-tracking gate and 300 s timeout already single-flight it | a hand-run sync beside the order's. Not checked: whether the city has a Dolt remote |
| gc-wvjrm (#62) `<pack>//<subpath>` agent path refs | no such reference in the city config or any pack | none today |
| gc-qmr9 (#103) hide the idle-timeout precedence warning | the warning cannot fire: gastown is not imported and no agent sets both keys | the warning returns if such a config appears |
| gc-1kz4 `gc nudge show` | no pack or prompt calls it; its census id collided with upstream on every sync. Transcripts were not scanned for ad-hoc use | no CLI answer to "was this queued nudge delivered" |
| gc-x1a87 (#149) supervisor crash-loop guard against a stale controller holder | both effective halves need systemd or procfs; the host is launchd | matters again if the fleet returns to Linux |
| gc-68bao (#176) reap leaked GOTMPDIR work dirs | the sweep returns at once without `/proc` | same |
| gc-01o2l (#148) pre-push repeat-push skip | the skip is fixed upstream (`b05fe44a6` #7806) and `7fd0a8142` #7826 fails a 141 closed | skip announcements are gone; `.githooks/lib/push-suite.sh` line 191 is upstream's `printf ... \| grep -q` under pipefail again, and no fork test walks `.githooks/` |
| gc-l6zf4t (#230) cmd/gc test startup sweeps at the inherited temp roots | adds 6 to 8 s to every cmd/gc test binary start; one recorded save | leaked test Dolt stores under the host TMPDIR are not swept by the tests |
| gc-ibhb76 (#225) capped-read notice on the API routes of `gc wait list` | past 1000 open waits only; the city has no `[api]` section | an API-routed listing past the cap is silently short |
| gc-a75mwa (#217) query indexes on managed bead stores | the metadata-index half is inert (the live Dolt fails the self-test); the two wisp column indexes it built persist | new rigs and rebuilt tables do not get the indexes. Open PR #247 appends to this commit's test file |
| gc-ri5p1e (#196) `gc prime` refuses an ambiguous bare agent name | reached only by a hand-typed bare `gc prime <name>` and the `gc sling` error text | first-match render for a shared bare name |
| gc-na313 (#159) beads version bump anchors page | docs only, for a task the fork no longer does | none |
| gc-o9x7m (#185) model-usage zero-emission re-diagnosis | a record, referenced by nothing | none |
| gc-ofr2iz (#207) why bead queries are slow on Dolt | a record | none |

Also shed: all eight `specs/gc-*` analysis records (1,439 lines) that sat
inside carried commits (gc-kawr5, gc-8jrtx, gc-ymv5j4, gc-k0n2xi, gc-due3fr,
gc-ysf8tb, gc-7eevlv, gc-xmah9h). The ruling named the records inside
gc-kawr5, gc-8jrtx and gc-ymv5j4; the other five are the same class and
went with them.

## Trimmed

| Carried commit | Removed | Kept |
|---|---|---|
| gc-7kea9 (#188) | SHED: the held-work keep-alive on the legacy no-wake drain arm, its test file and three golden edits (v2 does not run that loop). DROP-ABSORBED: `TestReleaseWorkBead_DoesNotReopenClosedStep`; upstream's fenced release covers it and pins it with `TestReleaseWorkBead_Tier2DoesNotTrustACachedRead` | the `currently_processing` ownership guard in `releaseOrphanedPoolAssignments` and its tests, live under v2 |
| gc-d43dp4 (#231) | SHED: the `ListQuery.ParentIDs` pushdown (`listChildren`, `parentChildIDs`, the Doltlite `IN`, 20 tests). `ParentIDs` is upstream's field and behaves as upstream's again; nothing in production sets it once the API graph walk reads through `ChildrenOfAny` | the IDs pushdown, which gc-due3fr builds on, and the `ChildrenOfAny` graph walk |
| gc-hvk4ub (#228) | SHED: the per-check `Canceled()` polls and their parameter threading across 30 `cmd/gc` files and three doctor checks. The abandonment signal itself (`CheckContext.Done`, `boundedRun`) turned out to be upstream's | stopping an abandoned pack doctor script and its process group; 1,559 lines over 39 files became about 450 over 5 |
| gc-d760o | DROP-ABSORBED: the `rig-live-session-beyond-pipe-buffer` test case; upstream's `orphan-sweep.sh` uses here-strings | everything else |

Not taken: gc-hvk4ub could shrink to about 50 lines if the runner did not
send the SIGTERM itself, at the cost of a race where `gc doctor` exits
before the script is signalled.

## Consolidated (SQUASH and moves)

| Into | From | Why |
|---|---|---|
| gc-g3pgp | gc-q1ttz's unbound-rig-scope warning log (`unboundRigScopedLog`, `scanOrderSetSnapshotFSWith`, the start, reload and lane wiring, three tests) | it replaced the three handler lines gc-g3pgp added |
| gc-cvl9w (#187) | gc-d43dp4's two `internal/convoy/membership.go` hunks | they only delete a filter gc-cvl9w added |
| BUILD sync commit | eighteen BUILD.bazel hunks that sat in ten carried commits | BUILD entries are regenerated, one commit |

Eleven replay and review fixups were autosquashed into their owners. All 71
messages were rewritten: 39 subjects that were bead titles are Conventional
Commit subjects now, squash-merge bodies and earlier syncs' paragraphs were
removed, and text describing code no longer in a diff was cut. Twelve
messages end with an "Upstream sync 2026-11-10" paragraph.

## Regenerated artifacts

- **Command census**: upstream minted 215 and 216. `gc session
  release-name` is 217, `next_id` 218 (`gen-command-census -check` clean).
- **Storeclass baseline**: five `cmd/gc` ceiling rows, upstream to this
  tree: cap-assert 100 to 102, mint 166 to 168, raw-field 178 to 184,
  raw-param 1092 to 1114, raw-result 269 to 281. No sealed-package row, no
  cache-peel site. `go test ./analyzers/storeclass/...` passes.
- **Resource census**: new rows for
  `cmd/gc/cmd_convoy_progress_fanout_test.go` (http_test_server 1) and
  `internal/pidutil/vanish_darwin_test.go` (subprocess 1); changed rows
  `cmd/gc/dolt_scope_watchdog_test.go` (fixed_sleep 6 to 4, subprocess 5 to
  3) and `test/integration/bdstore_test.go` (subprocess 6 to 7).
- **cipolicy**: `expectedCIExecutionHash` is
  `847f97e3f0c24d09483f1eccefe611775ffab4d5b498992fbde5a23c07ae62ed`. The
  fork's only delta in `ci.yml` is the runner-policy job's `runs-on` line;
  the nightly hashes are upstream's.
- **Bazel BUILD files**: hand-synced (no bazel on the rebase host), 16
  files, with a gazelle-emulating checker that models build constraints and
  per-platform `select()` lists and reports 0 findings on pure upstream and
  on this tree. `internal/beads` `shard_count` 11 to 9 with the budget
  test's pinned row, as last sync. gazelle's exact formatting, nogo and
  sandbox `data` inputs stay unverified until CI runs. The checker is kept
  at `~/.claude/gascity-sync-tools/buildsync` on the rebase host.
- **Beads patch** (#245): applies unchanged to beads v1.3.4.

## Review outcome

One independent reviewer read the rebased tree against the old carried
commits and against upstream without the replay's reasoning. No must-fix.
Confirmed: `MembersOn` is upstream's behavior inside the factory wrapper
and the fork's `internal/convoy` delta equals upstream's own; all 38
duration checks run on both validation paths; 26 registered city store
checks; census ids 1 to 217 with no gap; no production code sets
`ListQuery.ParentIDs`; the doctor runner closes upstream's channel through
the `abandonment` wrapper; no symbol, flag, key, schema or census row of a
dropped commit survives. `GOOS=windows` builds of upstream's Windows-lane
lists and a `GOOS=linux` vet of the Linux-tagged files the fork edits are
clean. Three small fixes went into their owners (a comment naming a shed
test file, a stale failure message, a blank line).

Open notes, not changed:

- gc-q1ttz against upstream's `order-dispatch-floor`: the check's text and
  `docs/tutorials/07-orders.md` describe a per-pass budget; the carry
  accrues per patrol interval. The floor arithmetic still holds.
- `internal/api/handler_beads.go` carries a comment saying the bd batch
  ignores the tier option; `BdStore.ChildrenOfAny` honors it. Wrong since
  before this sync.
- `internal/beads/query_parent_ids_test.go` now holds only the
  `distinctNonEmpty` test; renaming it needs a BUILD edit.
- gc-d43dp4 lost two unit pins of the both-tiers id read with the ParentIDs
  tests; an integration case covers it.

## Gate results

Run on the rebased tree as one sequential job at `nice 19` with capped
parallelism, Go-native because this host has no bazel. The full runs were
on the tree before the BUILD sync commit, the message rewrite and four
small fixes (three review fixes and the `AGENTS.md` budget fix); the
affected tests were re-run on the final tree.

- `go build ./...`, `go vet ./...` and `go vet -tags integration` over
  `cmd/gc`, `internal/...` and `test/...`: clean.
- golangci-lint 2.14.0 over every package the fork changes against
  `f14384642`: 0 issues.
- `gen-command-census -check`, `go test ./scripts/cipolicy/`, the
  storeclass generator in check mode and the resource census test: clean.
- `go test` over every package except `cmd/gc` and
  `internal/worker/workertest`: 217 packages ok, 6 red.
  - `test/docsync` `TestRootAgentsFileWithinBudget`: the carry's (root
    `AGENTS.md` at 20,032 bytes against a budget of 20,000). Fixed in
    gc-ewcrp6; passes on the final tree.
  - `scripts` `TestBeadsShardsKeepHeavyTestsApart`: expected on a tree
    without the BUILD sync; passes on the final tree.
  - `internal/testenv` `TestInitScrubsLeakVectors`,
    `TestInitPassthroughPreservesNamed`, `TestInitRefusesProdDoltPort`,
    `TestInitRefusesAmbientCityDoltPort`: fail alone at bare upstream on
    this host too.
  - `examples/gastown` `TestJsonlExportArchive*` (2): pass alone at bare
    upstream and on this tree; the fork has no diff in the package.
  - `internal/dispatch` `TestProcessRalphControlInfraRetryBudgetExhaustionBurns`
    and `test/acceptance/helpers` `TestBdLatestSchemaVersion*` (2): the host
    reds the previous ledger records; the fork has no diff in either
    package.
- `go test ./cmd/gc`, the whole package (28.5 minutes): 6 tests red.
  - `TestInternalBeadsCredentialUsesExactNoninteractiveRequest`,
    `TestInternalBeadsCredentialDoesNotLeakProviderMessage`,
    `TestRemoteContextProviderCachesAndForceRefreshes`: fail alone at bare
    upstream on this host too.
  - `TestBuildParamsDefaultBranch*` (3): load. Re-run alone three times at
    bare upstream and three times on this tree, all pass.
  - The two Dolt-version reds of the previous sync
    (`TestInitFrom*Hosted*`) pass now.
- Trim verification, each on its own tree before the squash: gc-7kea9 (119
  `cmd/gc` tests around pool release, phases and the timeline goldens),
  gc-d43dp4 (`internal/beads`, `internal/convoy`, `internal/api` whole, and
  the two edited integration tests against real bd and Dolt), gc-hvk4ub
  (`internal/doctor` whole and the `cmd/gc` doctor selection): all pass.
- Not run: `internal/worker/workertest`, the integration and acceptance
  tiers as suites, and anything under bazel (nogo, the `BUILD files in
  sync` gate). Fork CI on the pull request is the first bazel evidence for
  this tree. Per-commit buildability was checked only for gc-g3pgp after
  the fold.

## Not carried, on purpose

- **#247** (gc-7b6msw, doltpool keeps every connection a burst returns):
  open; not covered upstream. Its test appended after gc-a75mwa's test
  code, which is shed, so it needs a rebase.
- **#227** (gc-wye0uw, keep a live pool session's `work_dir` across a cap
  raise): open; not supplanted, but its own body says the fix is legacy
  reconciler only. A candidate to close under the same ruling as the legacy
  sheds.
- **#224** (gc-c54dxs, Codex hooks on the launch command): open; not
  covered upstream (upstream #4996 is open). Conflicts with upstream's kiro
  branch in `internal/hooks/hooks.go` (`5eb5e794d` #8972). Depends on
  gc-beez, which is carried unchanged.
- **gc-pdgws8** (codex `--no-daemon`): closed not-needed on 2026-11-10; the
  city sets it in `city.toml`.

## Operational notes for the cutover build

- **Shed commands and checks**: `gc nudge show` no longer exists; the
  doctor checks for a malformed `.beads/config.yaml` and for a build behind
  origin/main are gone; `gc prime <bare-name>` renders the first match
  again. `supervisor.toml`'s `require_service_manager` (gc-163b32) is still
  honored.
- **`pack//subpath` refs** are no longer resolved. The city uses none.
- **Push-gate reaper** (`7fd0a8142` #7826, `ef7cb5f71` #8991): it signals
  processes holding a slot under a dead stamped PID. Every agent on this
  host is one uid, so it can reach another agent's leaked descendants.
- **First boot**: `gc start` pins `dolt.shared-server: false` in each
  managed scope's `.beads/config.yaml` (`8e75c50aa` #8895); a new core
  order `binding-snapshot` joins the ring (inert without a binding);
  `spawn-storm-detect` runs `gc ready --unassigned` across all stores every
  five minutes (`19bfa91c2` #8953); `orphan-sweep` releases claims of closed
  pool sessions (`82b991b46` #8717).
- **`gc session attach`** asks the controller to wake a sleeping session
  and gains `--no-start` (#9011).
- **New doctor checks**: `order-dispatch-floor` and `storage-split`. With
  gc-q1ttz carried, read the floor warning as an estimate.
- **Index builds**: with gc-a75mwa shed, the supervisor stops running the
  query-index lane. Existing indexes stay.

## For the next sync

- The gazelle-emulating BUILD checker and its Go source dump are kept on
  the rebase host; calibrate on bare upstream first.
- A hunk that upstream changes identically replays without a conflict at
  the wrong value (gc-kawr5's count this time). Audits should name every
  counter a carry shares with upstream.
- `git log --grep=<bead>` matches bodies as well as subjects; build a
  `fixup!` subject from a subject-only match.
- A carried test file that upstream also owns is restored from upstream,
  not deleted, when the carried part is dropped.
- A late whole-commit shed stayed cheap: eighteen commits came out in one
  rebase with two conflicts (the census id and the ledger index).
