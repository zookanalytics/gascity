# Upstream sync 2026-11-22: carried-commit ledger

Rebase of the fork's carried commits from upstream base `f14384642`
(2026-11-10, upstream #9049) onto `upstream/main` `c377651cb` (2026-11-23,
upstream #9437). 192 upstream commits came in (1,763 files, +168.1k/-11.2k
lines). The old fork tip was `96179ad1f`: 80 commits over the old base, of
which 76 were carried commits and four were the storeclass baseline, the
resource census, the BUILD sync and the ledger. The sync was prepared
against `1f7330b74`; gc-buhh8z (#255) merged while it ran and was added to
the branch before the push, unchanged.

The operator's instruction for this sync was the standing one: carry less.
Drop whatever upstream covers, shed what this city does not use, and fold a
commit that fixes another carried commit into it. Each carried commit was
evaluated against the whole of the new upstream by four independent audits,
plus a consolidation map, an upstream briefing and an open-pull-request
audit. The operator then ruled on every shed candidate. The result on branch
`sync/upstream-2026-11-22` is 63 carried commits, three regenerated artifacts
(storeclass baseline, resource census, Bazel BUILD sync) and this ledger: 67
over `upstream/main`. The carry against upstream fell from 31,962 added lines
to 27,428 (ledgers excluded), and from 418 changed files to 361.

Of the 76: three were dropped because upstream covers them, one because a
later carried commit reverses it, nine were shed whole, and five were
trimmed. Of the 63 that remain, 21 changed in this sync and 42 replayed
unchanged apart from context.

Commits are cited by bead id and fork pull-request number, not by SHA: the
fork's SHAs are rewritten on every sync. Upstream commits are cited by SHA
and pull-request number. Verdict key as in the earlier ledgers: **KEEP**,
**DROP-ABSORBED**, **DROP-SUPERSEDED**, **DROP-OBSOLETE**, **REWORK**,
**REGENERATE**, **SQUASH**, and **SHED** (not covered upstream, dropped
because it is not load-bearing in this city, with the residual risk
recorded).

## What changed upstream (the parts that touched the carry)

- **Typed stores, T7b.2 through T7g** (`eb5f0b042` #9058, `b24da46ef`
  #9095, `294079d53` #9155, `2cc2bcebd`, `73d47a3bc`, `28f93a238`): reads go
  through `Stores().Plan(storeref.<Intent>)` legs. Two things bound the
  carry. A new shrink-only `TestStoreIdentityGuard` in `cmd/gc` forbids `==`
  on stores and store-keyed maps; two carried comparisons moved to
  `beads.SameStore` (gc-k0n2xi, gc-dnzugu). And T7e added a frozen-reference
  equivalence test, `TestBeadOwnerReadersMatchTheFrozenReaders`, that pins
  which reads `collectBeadGraph` makes; see gc-d43dp4 under Carried commits.
- **Windows port** (`3ff6849ef` #9124, `d2254b579` #9194, `918f45f71` #9200,
  `1feaf7eeb` #9215, `35c20c956` #9086 and siblings): process groups go
  through `internal/processgroup`, sockets through `localsocket`, shell
  commands through a `runtime` seam, and `internal/pidutil` is split by
  platform (`8e7d91b58` #9047). #237 was re-landed on the split.
- **Order scanning** (`3ee9c9658` #9115): `orders.ScanRoots` returns a
  disabled order with `Enabled: false`, and `orders.FilterEnabled` decides
  the active set. gc-zppbzq's tests and comments follow it.
- **Config composition** (`1f0c10495` #9050, `1f5457b2c` #9091): upstream
  extracted `mergeIncludeFragments` (the same helper gc-x647oo had extracted,
  with the two booleans in the other order) and added `ComposeFragments`;
  the pinned-bd resolver reads the root `[workspace.env]` table through
  `workspaceEnvTable`. `gc init` writes a born-split city by default.
- **Worktree pruning** (`420b7bb65` #9163, `4a98bc80e` #8112): the doctor
  check lost its stash gate, and the drain-ack close prunes the session's
  `worker_dir`. gc-9hy4l's liveness gate covers both.
- **Native-open environment** (`bc6bc2871` #9235): the open-environment
  mutex became an `execenv` projection window with snapshot readers. The
  slot growth gc-5pexef fixes is still there.
- **Hook claims** (`b9393bcf4` #9082, `d1c45f12f` #8127): `gc hook --claim
  --worktree` and `gc.exclude_claimants` on every claim tier.
- **Session lifecycle** (`03f6fe825` #8763): detached user services survive
  orphan cleanup; it took requirement id SESSION-START-028.
- **Storage split** (`11a3589ab` #9136): `gc storage split` took command
  census id 217.
- **Upstream landings of fork carries**: `c57246cbc` #7836 (gc-wtquzx),
  `208a43158` #9106 (gc-yzxra), `86032f586` #8374 (gc-kawr5's pricing row),
  `dd979674d` #8711 (gc-nm4d2h's explicit `--rig`), `6cf1cccd6` #7917 (two
  of gc-d760o's scripts).
- **Pins**: Go 1.27.3 to 1.27.4 (`4fb3582b8` #9107), Node 24.22.1 to
  24.22.3. Dolt 2.6.0, beads v1.3.4 and golangci-lint 2.14.0 unchanged.
  `ci.yml`, `nightly.yml` and `scripts/cipolicy/policy.go` did not change.

## Carried commits

In replay order. "unchanged" means the patch replayed with no change beyond
context.

| # | Bead / PR | Purpose | Status this sync |
|---|---|---|---|
| 1 | gc-119r | doctor's session-bead-id heuristic excludes slash-bearing strings | unchanged |
| 2 | gc-4kq5vc | pass `SSH_AUTH_SOCK` through to agent sessions | unchanged |
| 3 | gc-t4pi | skip embedded-Dolt drift tests on a CGO-less bd | its classifier test sits beside upstream's new ready-timeout test (`9039b3a61` #9253) |
| 4 | gc-beez (#104) | normalize managed Codex hooks after overlay staging | unchanged |
| 5 | gc-ddvrx | census-owner-liveness scoped to this city's namespaces | unchanged |
| 6 | gc-wq7t4 (#111) | tmux activity-window ceiling test synchronizes on output | unchanged |
| 7 | gc-zppbzq (#250) | a city-level `scope = "rig"` order is expanded per rig | reworked: see Consolidated and the note below the table |
| 8 | gc-8o8un (#118) | reload accept-refresh test no longer deadlocks the binary | unchanged |
| 9 | gc-3fs0v (#119) | drift NFR wall-clock budgets behind an opt-in | unchanged |
| 10 | gc-l2c6v (#122) | `tickDebouncer.cancelPending` race | unchanged |
| 11 | gc-rfxju | reject nameless pool routes | unchanged |
| 12 | gc-5fdrr (#144) | a closed legacy holder's session name is released to its configured identity | trimmed: see Trimmed |
| 13 | gc-i52hj (#147) | compact integrity gate defers on a writer race | unchanged |
| 14 | gc-6a6vz (#155) | bounded order-history read is store-complete | holds the eight comment lines gc-zppbzq had edited in its files |
| 15 | gc-8jrtx (#156) | timing tests hardened for fleet load | partly superseded: see Trimmed |
| 16 | gc-dqn8l (#157) | named-session advisory no longer printed on every command | trimmed: see Trimmed |
| 17 | gc-gf1l6 (#162) | detached-orphan lane `merge_result` and step guards | unchanged |
| 18 | gc-4q9z5 (#168) | city.toml edits keep comments and `orders.overrides` (`internal/tomledit`) | unchanged |
| 19 | gc-klgt8 (#169) | `gc bd show` renders cross-store dependency edges | reworked: the bd double of upstream's new close-fallback test (`189612d9a` #9150) answers with a dependency record; see Gate results |
| 20 | gc-ygqk7 (#171) | doctor accepts `idle_timeout=0` | unchanged |
| 21 | gc-9hy4l | worktree pruning gated on live-session liveness | reworked: on upstream's stash-gate removal (#9163); upstream's new drain-ack prune (#8112) takes its liveness inputs from a new `storePruneLiveness`; upstream's `TestFinalizeDrainAckStoppedSession_PrunesWorkerDir` stubs a clean scan and a fail-closed sibling test was added; schema docs regenerated |
| 22 | gc-0dzmj (#178) | bound the doctor fix-timeout test | unchanged |
| 23 | gc-f3k8d (#183) | cascade nudge: queue delivery with `--reference-bead` | unchanged |
| 24 | gc-7kea9 (#188) | a live session's `currently_processing` bead counts as owned (from upstream #6410, still open) | unchanged |
| 25 | gc-d760o | `grep -q` pipefail SIGPIPE class in shipped scripts | trimmed and partly absorbed: see Trimmed |
| 26 | gc-c9upz (#164) | reinstall golangci-lint when it is not the pin | unchanged |
| 27 | gc-cvl9w (#187) | convoy list cost: batched members and `ChildrenOfAny` | reworked: `"ChildrenOfAny"` added to upstream's reshaped SQLite capability list |
| 28 | gc-ymv5j4 (#208) | open-work guard and spec-sidecar close list only open members | unchanged |
| 29 | gc-7s8mo (#209) | watchdog sweep honors a run's recorded timeout | unchanged |
| 30 | gc-0mue2i (#210) | converge closed workflow roots that list no steps | unchanged |
| 31 | gc-2st54j (#204) | share each store's in-flight gate index read across the pass | unchanged |
| 32 | gc-5cq3k3 (#216) | controller watches only what the config revision hashes | unchanged |
| 33 | gc-stxs1f (#198) | isolate HOME in the legacy-transcript API test | unchanged |
| 34 | gc-8g3jj (#192) | fail a backup sync that exits 0 without committing its manifest | unchanged |
| 35 | gc-92bf3j (#205) | cmd/gc test temp dirs stay under the per-run root | unchanged |
| 36 | gc-dd0bdr (#221) | workdir test keeps the caller's city path out of assertions | unchanged |
| 37 | gc-dz7j6r (#203) | gitcred fixture group | unchanged |
| 38 | gc-1ghh2b (#220) | script fixtures stay off Homebrew's network | unchanged |
| 39 | gc-vf5swy (#201) | `test-local-parallel` jobs run in a non-login shell | unchanged |
| 40 | gc-k0n2xi (#223) | closed named-session index cached across demand passes | reworked: its cache compares stores with `beads.SameStore` (`TestStoreIdentityGuard`) |
| 41 | gc-ewcrp6 (#212) | pre-commit hook passes on the bazel-less macOS host | unchanged |
| 42 | gc-d43dp4 (#231) | BdStore pushes `ListQuery.IDs` down; the API graph walk reads through `ChildrenOfAny` | reworked: see the note below the table |
| 43 | #237 | darwin zombie state from `kinfo_proc` | reworked: the one-line change moved to `pidutil_unix.go` `platformAlive` (#9047); `zombie_other.go` tagged `!darwin && !windows` |
| 44 | gc-q1ttz (#194) | order dispatch budget accrues per patrol interval | unchanged |
| 45 | gc-5pexef (#235) | native Dolt opens keep `BEADS_` / `BD_` env slots | reworked: on upstream's `execenv` projection window (#9235) |
| 46 | gc-nh4eqz (#234) | fork CI on GitHub-hosted runners | unchanged; the CI hash stands |
| 47 | gc-due3fr (#219) | control dispatcher wake scope and brief ready reads | reworked: two tests keep the fork's form over upstream's `installFakeBdOnPath` rewrite (#9215); the wake scope resolves a rig path inline (the helper was gc-4jard's) |
| 48 | gc-x647oo (#236) | `gc bd` passthrough fast path | reworked: `LoadDeployment` calls upstream's `mergeIncludeFragments` (#9050); the two pinned-bd resolver hunks dropped for upstream's `workspaceEnvTable` (#9091) |
| 49 | gc-dnzugu (#239) | control dispatchers take the native store | reworked: `controlStoreRegistry.keepsLocked` uses `beads.SameStore` |
| 50 | gc-163b32 (#242) | refuse a supervisor fork outside its service manager | reworked: `supervisorDirectRestartRefusal` re-inserted in upstream's reshaped `cmd_start_drift.go` |
| 51 | #243 | native listings skip count aggregates | unchanged |
| 52 | gc-2c1aw (#202) | order root carries its labels from creation | unchanged |
| 53 | gc-ycww6 (#193) | `gc hook --claim` StepReminder on a held claim | reworked: upstream's exclude-claimants refusal (#8127) returns under the fork's `certifyHookAdoption` signature; `StepReminder` sits after upstream's `WorkDir` and `WorkBranch` |
| 54 | gc-oh4e2p (#232) | transcript ambiguity check keyed on `work_dir` | unchanged |
| 55 | gc-2ddu7 (#195) | keep a starting session out of another session's checkout | reworked: requirement row SESSION-START-029 |
| 56 | gc-ysf8tb (#215) | keyed Claude transcript lookup outside the `work_dir` folder | unchanged |
| 57 | gc-hvk4ub (#228) | stop an abandoned pack doctor script and its process group | unchanged |
| 58 | #244 | native listings push the not-closed statuses down | unchanged |
| 59 | #245 | two-phase list hydration as a beads source patch | unchanged; applies to beads v1.3.4 |
| 60 | gc-ciew5u (#249) | dashboardbff starts no run tailer on a run cancel | new since the last sync; unchanged |
| 61 | gc-7b6msw (#247) | doltpool keeps every connection a burst returns | new since the last sync; unchanged |
| 62 | gc-6dntzs (#251) | the repository architecture document | new since the last sync; holds gc-zppbzq's six-line edit of it |
| 63 | gc-buhh8z (#255) | the core gc-mail skill says how `--notify` wakes a reader | merged during the sync; unchanged. Upstream did not touch the file in range. Not in the audits, the review or the full gate run: a skill text edit, checked with `test/docsync`, `test/packlint` and the core pack's tests on the final tree |

**gc-zppbzq (#250).** Its squash began by reverting gc-g3pgp, and the rest
replaces that guard: a city-level order that declares `scope = "rig"` is
copied to each rig unless the rig's own layers define the name. With
gc-g3pgp dropped from the replay, the commit is the change alone (13 files,
about +1,170 lines) and sits in gc-g3pgp's old slot. For #9115, a disabled
definition is now the rig's instance of the order, off unless an
`[[orders.overrides]]` entry for that rig enables it: three tests assert on
`orders.FilterEnabled` or on the disabled instance, and two comments say so.
Upstream #6232 (open) covers part of the same change.

**gc-d43dp4 (#231) and the frozen reference.** Upstream's `collectBeadGraph`
reads a level's children with `List(ParentIDs, AllowScan)`. The native store
does not push `ParentIDs` down, so on this city that read scans the store;
the fork's walk reads through `beads.ChildrenOfAny`. Upstream's new
`TestBeadOwnerReadersMatchTheFrozenReaders` compares the live reader's reads
with a frozen copy, and failed on the difference. The fork's walk was kept
and `frozenCollectBeadGraph` in `internal/api/bead_owner_legs_reference_test.go`
was changed to make the same read, with a header note. For that one read the
test still proves which handle answers it; it no longer proves the fork's
read equal to upstream's. The alternative, upstream's read, would have
dropped the hunk.

## Dropped because upstream covers it

| Carried commit | Verdict | Upstream | Evidence |
|---|---|---|---|
| gc-wtquzx (#226) ephemeral-tier bd query pushes metadata filters down | DROP-ABSORBED | `c57246cbc` #7836 | the same change with the same key rule and its own unit and real-bd tests. Upstream pushes JSON-number and `null` filter values, which the fork held back; no caller that can hit the difference was found |
| gc-yzxra (#141) doctor `hold-label-routed-to` against `v2-routed-to-namespace` | DROP-ABSORBED | `208a43158` #9106 | the same fix with its own two-pass convergence test |
| gc-nm4d2h (#32) `gc convoy create` resolves rig scope like `gc bd` | DROP-SUPERSEDED | `dd979674d` #8711 | upstream honors an explicit `--rig` and pins the opposite of the rest: `TestConvoyCreateWithoutRigFlagKeepsDefaultPlacement` asserts that `GC_RIG` and the working directory do not choose the store. Gone with it: the cwd and `GC_RIG` fallback, `--city-scope`, and co-locating on a `--rig`/issue mismatch (upstream refuses). No pack calls `gc convoy create` |
| gc-g3pgp refuse unbound city registration of a rig-scoped order | DROP-OBSOLETE | none needed | gc-zppbzq (#250) reverts it whole; no symbol of it existed at the old tip |

## Shed on the operator's ruling (2026-11-22)

None of these is covered upstream. Each was presented with its evidence
and the operator chose to stop carrying it.

| Carried commit | Why it was not load-bearing here | Residual risk |
|---|---|---|
| gc-zl5ta (#151) scope watchdog restarts a crashed managed Dolt | never fired in the live `dolt.log` (2026-10-06 on: 18 supervised starts, no crash restart); the running Dolt has no watchdog above it; upstream's controller-side recover lifts its ENOSPC skip once disk space returns (`e8547dc57`) | a slower restart after a Dolt crash, and only while a controller runs. Inferred, not proven: that the stuck ENOSPC skip was what held the 2026-08-19 outage open |
| gc-ox80c (#177) claim holder may heartbeat a `gc hook --claim` lease | nothing gc-toolkit ships calls `gc bd heartbeat` | a hand-run heartbeat on a hook-claimed lease is refused again |
| gc-378x4 (#174) `gc events` rejects a comma `--type`; `--json` notice off stdout | no shipped script passes either | a comma list silently matches nothing; the notice is on stdout |
| gc-lgq4p doctor names an empty `GIT_CONFIG_GLOBAL` | error wording only; nothing shipped sets it empty | the clearer message |
| gc-k40o8 (#186) doctor exempts the reserved `human` route target | gc-toolkit's `lifecycle.sh` refuses a bare `human` route and has `doctor/accepted.toml` | not checked: existing beads routed to `human`, which would warn again |
| gc-4jard order-store rig paths resolved without mutating the shared config slice | the race only writes for a relative rig path; every live rig path is absolute | a `-race` test with a relative rig path reports it again; `resolveRigPaths` writes through the shared slice in `cmd/gc/order_store.go` |
| gc-elbshw (#241) forked supervisor starts in its own session | `supervisor.toml` sets `require_service_manager = true`, so with gc-163b32 no fork can happen here | a host without that key: a forked supervisor keeps the caller's terminal |
| gc-a8fkt6 (#206) scrub the test temp root before matching store ids | test-only; left sibling tests uncovered; the fleet's temp-root naming rule covers the class | `cmd_wait_test.go` and `infra_class_migrate_test.go` can false-red under a temp root containing `gc-<n>` |
| gc-kawr5 price `claude-sonnet-5-5`; name unpriced invocations; `agent-token-telemetry` check | the pricing row is upstream's (`86032f586` #8374, cache read at $0.10 where the fork had $0.20); the rest is detection only and no OTel exporter is configured | doctor no longer names unpriced models. Registered city store checks are upstream's 25 again, "skipped 31" |

Offered and kept (the operator selected none of them): the BdStore fast
paths (gc-d43dp4's IDs pushdown and gc-due3fr's `--brief` arm, about 1,500
lines, live only in a native-preflight fallback), gc-q1ttz, gc-6dntzs, and
replacing gc-beez with `install_agent_hooks` config.

## Trimmed

| Carried commit | Removed | Kept |
|---|---|---|
| gc-5fdrr (#144) | SHED: the `gc session release-name` command, `internal/session/name_release.go`, its schema, CLI docs and census entry. No pack calls it; its census id collided with upstream on three syncs running. The `names.go` error no longer points at the command | the automatic release of a closed legacy holder's session name, with its tests. The command census equals upstream's again |
| gc-d760o | SHED: the `internal/testpolicy/shellpipeline` detector and the walker test. DROP-ABSORBED: the `cross-rig-deps.sh` and `spawn-storm-detect.sh` fixes (`6cf1cccd6` #7917) | here-string fixes in `escalate.sh`, `notify-claims-stall.sh`, `scripts/lib/test-slice.sh`, `scripts/test-integration-shard`, `scripts/rebase-resolve-lib.sh`, and `TestRebaseResolveLibNoPipefailGrepQ` |
| gc-dqn8l (#157) | SHED: the `gc github pr backfill` hunk and its test; no script runs the command and the city has no GitHub monitor | the advisory filter in `shouldEmitLoadCityConfigWarning` |
| gc-8jrtx (#156) | DROP-SUPERSEDED: the hang-deadline lint-map hunk (upstream keys the map by function, `1feaf7eeb` #9215) and the socket-path helper (`controllerSocketPathLimit` is gone, `35c20c956` #9086, and `testutil.ShortTempDir` falls back to `/var/tmp` on every Unix) | the `doEventsWatch`, supervisor-ready and doctor fix-timeout hardening |
| gc-x647oo (#236) | DROP-SUPERSEDED: its two pinned-bd resolver hunks; upstream's `workspaceEnvTable` is cheaper and takes no repo-cache lock (#9091) | everything else |

With the detector gone, two piped `grep -q` under pipefail are unguarded
and identical to upstream's: `examples/bd/dolt/assets/scripts/mol-dog-doctor.sh`
line 240 and `.githooks/lib/push-suite.sh` line 191.

## Consolidated (SQUASH and moves)

| Into | From | Why |
|---|---|---|
| gc-zppbzq (#250) | gc-g3pgp, as its absence | #250's first half was the exact inverse of gc-g3pgp; the pair is one commit that never adds the guard |
| gc-6a6vz (#155) | gc-zppbzq's comment edits in `cmd/gc/cmd_order.go` and `cmd_order_history_store_completeness_test.go` | the lines are gc-6a6vz's |
| gc-6dntzs (#251) | gc-zppbzq's six-line edit of `engdocs/architecture.md` | the document is gc-6dntzs's; gc-zppbzq moved ahead of it |
| BUILD sync commit | gc-zppbzq's `internal/orderdiscovery/BUILD.bazel` hunk | BUILD entries are regenerated, one commit |

Replay, shed and review fixups were autosquashed into their owners.

## Regenerated artifacts

- **Command census**: upstream minted 217 (`gc storage split`). With the
  `release-name` command shed the fork mints none: the ledger and its
  mirrors equal upstream's, `next_id` 218 (`gen-command-census -check`
  clean).
- **Storeclass baseline**: five `cmd/gc` ceiling rows, upstream to this
  tree: cap-assert 98 to 100, mint 164 to 165, raw-field 177 to 182,
  raw-param 1079 to 1100, raw-result 268 to 280. No sealed-package row, no
  cache-peel or read-marker growth. `go test ./analyzers/storeclass/...`
  passes.
- **Resource census**: new rows for
  `cmd/gc/cmd_convoy_progress_fanout_test.go` (http_test_server 1) and
  `internal/pidutil/vanish_darwin_test.go` (subprocess 1); changed row
  `test/integration/bdstore_test.go` (subprocess 5 to 6).
- **cipolicy**: unchanged. `expectedCIExecutionHash` is still
  `847f97e3f0c24d09483f1eccefe611775ffab4d5b498992fbde5a23c07ae62ed`;
  `ci.yml` and `nightly.yml` did not move upstream.
- **Requirement ids**: gc-2ddu7's row is SESSION-START-029.
- **Bazel BUILD files**: hand-synced (no bazel on the rebase host), 14
  files, with the gazelle-emulating checker at 0 findings on pure upstream
  and on this tree; `internal/tomledit` gets a new BUILD file and
  `repo_tree.py` its three root filegroup entries. `internal/beads`
  `shard_count` 11 to 9 with the budget test's pinned row, as on the last
  three syncs: at 11 the fork's test files put
  `TestRealNativeDoltStoreGuardedBatchConformance` and
  `TestNativeDoltLabelCASRace` in one shard, and 9 is the only passing count
  inside the lane's 87 pinned test actions. gazelle's exact formatting,
  nogo and sandbox `data` inputs stay unverified until CI runs.
- **Beads patch** (#245): applies unchanged to beads v1.3.4.

## Review outcome

One independent reviewer read the rebased tree against the old carried
commits and against upstream without the replay's reasoning. No must-fix.
Confirmed by reading both sides: all six prune call sites pass liveness
inputs and the drain-ack path fails closed; `readBDList` is safe under
upstream's concurrent fan-out and still passes the sort flag; both
`mergeIncludeFragments` call sites have the booleans in upstream's order,
`ValidateStorageConfig` runs once on the full load, and `migrate_command`
and the import-edge exclusion still run; gc-zppbzq behaves correctly for a
disabled city-level order, a disabled rig definition and an override;
`SameStore` is pointer identity for `*NativeDoltStore`; the service-manager
refusal precedes the kill-and-fork; no symbol, flag, key, golden row, schema
or CLI doc line of a dropped commit survives; the command census, doctor
golden, store-check counts and env-read baseline are byte-identical to
upstream's; SESSION-START ids 001 to 029 are unique and in order. A
`GOOS=linux` vet of `cmd/gc` is clean.

Fixed after the review, in their owners: four statements that still said
the pinned-bd resolver reads the deployment layer (gc-x647oo); a header note
on the edited frozen reference and a wrong comment about the bd batch and
tiers in `handler_beads.go` (gc-d43dp4); five commit messages describing
hunks no longer in their diffs. The gate then found one test double the
review had not: see gc-klgt8 under Gate results.

Open notes, not changed:

- `ValidateStorageConfig` runs later in the full load than upstream's
  (inside `validateDeploymentLayer`, after pack expansion), as before this
  sync. It changes error precedence only.
- gc-q1ttz against upstream's `order-dispatch-floor`, as in the last ledger.
- Per-commit buildability was checked only for gc-zppbzq at its new slot.

## Gate results

Run on the rebased tree as one sequential detached job at `nice 19` with
capped parallelism, Go-native because this host has no bazel. The full runs
were on the tree before the BUILD sync commit, the message rewrite, the
review's comment fixes and one test-double fix; the affected tests were
re-run on the final tree.

- `go build ./...`, `go vet ./...` and `go vet -tags integration` over
  `cmd/gc`, `internal/...` and `test/...`: clean.
- golangci-lint 2.14.0 over every package the fork changes against
  `c377651cb`: 0 issues.
- `gen-command-census -check`, `go test ./scripts/cipolicy/ ./test/docsync`,
  the storeclass generator in check mode and the resource census test:
  clean.
- `go test` over every package except `cmd/gc` and
  `internal/worker/workertest`: 220 packages ok, 5 red. Every red test was
  then run alone at bare upstream and on this tree.
  - `scripts` `TestBeadsShardsKeepHeavyTestsApart`: expected on a tree
    without the BUILD sync; passes on the final tree.
  - `examples/gastown` `TestJsonlExportArchive*` (2),
    `internal/runtime/herdr`
    `TestDeliverStartupTurnGivesUpWhenTheNotReadyBudgetRunsOut`,
    `test/acceptance/helpers` `TestBdLatestSchemaVersion*` (2) and
    `internal/dispatch`
    `TestProcessRalphControlInfraRetryBudgetExhaustionBurns`: fail alone at
    bare upstream on this host too.
  - `internal/dispatch`
    `TestProcessRalphControlInfraRetryThenPassClosesSameIteration`: load. It
    failed in the full run and once alone on this tree, then passed five
    times alone on this tree and three times at bare upstream; the fork has
    no diff in the package.
- `go test ./cmd/gc`, the whole package (64 minutes): 10 tests red.
  - `TestGcBdCloseFallsBackWhenTheSpeculativeEdgeReadFails`: the carry's.
    Upstream's new test (`189612d9a` #9150) answers a one-anchor `bd dep
    list` with joined rows; gc-klgt8 reads dependency records. Its double
    now answers with a record, in gc-klgt8; the test passes on the final
    tree.
  - `TestCheckoutRootForTestSourceRejectsUnexpectedLayout`: fails alone at
    bare upstream too (a stray `go.mod` under the host temp root).
  - `TestReverse*` (5 in the full run): upstream's reverse-migration tests
    (`a511fa4f8` #8958). Run alone, 14 of them fail at bare upstream and 3
    on this tree, different ones each time, on the controller-lock refusal;
    the fork has no diff in their code. Host or load, not the carry.
  - `TestBuildParamsDefaultBranch*` (3): fail alone at bare upstream too
    this time (6 there, 4 on this tree); the last ledger recorded them as
    load.
- Trim verification, each on its own tree before the squash: gc-5fdrr
  (`internal/session`, `internal/productmetrics`, `test/docsync` and a
  `cmd/gc` census and schema selection), gc-d760o
  (`internal/bootstrap/packs/core`, `internal/testpolicy/...`, the `scripts`
  rebase-resolve tests), gc-dqn8l (the advisory and `TestGitHubPRBackfill*`
  tests): all pass.
- Not run: `internal/worker/workertest`, the integration and acceptance
  tiers as suites, and anything under bazel (nogo, the `BUILD files in
  sync` gate, the split-strict lane). Fork CI on the pull request is the
  first bazel evidence for this tree.

## Not carried, on purpose

- **#253** (gc-3znzo8, never pour bead by bead after a batch error): open;
  not covered upstream; its diff applies to the new upstream with no
  conflict. Upstream #6334 (open) edits the same retry logic.
- **#252** (gc-qdpbqb, `notify_on_mail`): closed unmerged on 2026-11-23,
  retired; its skill text landed as gc-buhh8z (#255).
- **#254, #256, #257, #258, #259, #260, #261, #262**: opened while the sync
  ran, on the old main. Not audited against the new upstream. Each needs a
  rebase onto the new main; read "What changed upstream" and the Carried
  commits table first for the files it touches.
- **#227** and **#224**: closed unmerged on 2026-11-11 and 2026-11-13.
- **Open upstream pull requests on the ground of a carry**, none in this
  upstream: #6410 and #7913 (gc-7kea9), #7901 (gc-l2c6v), #7921 (gc-8o8un),
  #8794 (#243), #6232 (gc-zppbzq), #9338 (gc-7s8mo), #9347 (gc-hvk4ub),
  #9069 and #8800 (gc-dqn8l), #9311 (the `notify-claims-stall.sh` line of
  gc-d760o), #6373 (gc-ciew5u's `runtailer.go`). Each becomes a drop or a
  rework when it merges.

## Operational notes for the cutover build

- **Shed commands and checks**: `gc session release-name` no longer exists
  (`rigs/gc-toolkit/docs/gascity/agent-types.md` line 458 still mentions
  it); the `agent-token-telemetry` doctor check is gone; `gc convoy create`
  without `--rig` places the convoy as upstream does (first issue's store,
  else the city store) and has no `--city-scope`; `gc events --type a,b`
  matches nothing again; doctor warns about a bare `human` route.
- **Sonnet 5.5 cache reads** are priced at $0.10 per million tokens
  (upstream's row), half the fork's old figure.
- **Dolt**: the scope watchdog no longer restarts a crashed managed Dolt;
  the controller's recover does. `gc dolt compact` skips `DOLT_GC` while any
  session is connected (`5e60a76af` #9204), and the `mol-dog-compactor`
  order passes no `--force`, so with an always-connected controller the
  full GC may never run: watch store size.
- **bd schema**: gc no longer runs `bd migrate schema` on an existing
  database (`754b6990d` #9273). It prints `bd schema:` lines and adds a
  `bd-schema` doctor check and `[beads] migrate_command`. Run `gc doctor
  --check bd-schema` once after the cutover.
- **`gc bd ready`** hides session and order-tracking beads (`6bd609f5b`
  #7409); `gc ready` and raw `bd ready` do not.
- **First boot**: `nudge-on-route` has no saved mark on its first run and
  looks back one hour, so it may burst once (`764db10e8` #6197). The reaper
  holds a burst of more than 25 stale issues instead of closing it
  (`af55872b6` #9144). Cold-start failures back off from 30 s to 30 min with
  a `session.startup_health_alert` event (`297772b4c` #6164).
- **`gc stop`** stops a registered city's sessions itself when no
  controller serves it (`30eb6ae46` #9271).
- **`GET /beads`** answers 422 on an unknown query parameter (`59500dd89`
  #8428).
- **`gc init`** writes a born-split city by default (#9050): scratch and
  e2e cities change unless created with `--storage=single`.
- **OTEL environment names** changed (`187bf9a65` #8183). Not checked:
  whether they are in the session fingerprint, which would cause one drift
  restart per session.
- **New doctor checks**: `city-suspension` and `bd-schema`.
- **Build**: Go 1.27.4.

## For the next sync

- A frozen-reference or equivalence test upstream adds over a function a
  carry changes fails at runtime, not at merge: the audits should list such
  tests for every carried production hunk (`bead_owner_legs_*` and
  `work_federation_legs_*` in `internal/api` this time). The same goes for
  a new upstream test double that answers bd in a shape a carry no longer
  asks for (gc-klgt8's dependency records).
- A shed can break a kept commit that calls its helper (gc-due3fr called
  gc-4jard's `resolvedRigPath`). The consolidation map's buildability chains
  name these; read them before the shed.
- A later fork commit that reverts an earlier one: drop the earlier one from
  the todo and replay the later one as a reduced patch
  (`git diff <parent with the revert cherry-picked> <commit>`).
- `git apply --3way --index` of such a patch at a conflict stop, followed by
  `git rebase --continue`, keeps the original message.
- gc-d43dp4 and gc-due3fr carry about 1,500 lines that act only on a bd
  fallback; the operator kept them this time.
