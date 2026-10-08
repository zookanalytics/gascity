# Upstream sync 2026-10-15: carried-commit ledger

Rebase of the fork's carried commits from upstream base `755d36931`
(2026-10-13, upstream #7818) onto `upstream/main` `5ae611c4f` (2026-10-15,
upstream #7972). 65 upstream commits came in (556 files, +60.9k/-3.1k lines,
most of it new libraries with no production caller yet: `internal/connect`,
`connect/tokenrender`, `eventfact`, `opjournal/legacyop`, `runtime/guard`).
83 fork commits were evaluated one by one against the new upstream range; the
2026-10-13 ledger's verdicts were the starting point. 80 remain, plus two
sync-derived commits (the census and the Bazel sync) and this ledger: 83 over
`upstream/main`. The carry was then rolled forward once more, onto
`3c4cf446b` (see [Roll-forward 2026-10-16](#roll-forward-2026-10-16-onto-3c4cf446b)).

Verdict key as in the [2026-10-13 ledger](upstream-sync-2026-10-13.md):
**KEEP**, **DROP-SUPPLANTED**, **DROP-OBSOLETE**, **REWORK**, **REGENERATE**.

## Roll-forward 2026-10-16: onto `3c4cf446b`

Before landing, the carry was replayed again onto `upstream/main` `3c4cf446b`
(2026-10-16, upstream #8016): 23 more upstream commits (290 files,
+43.2k/-1.7k lines). Most is new surface (opjournal conversation lane, turn
observation, the incarnation fence, `GuardedBatcher`, compact-state health);
the parts that matter here are the `gc bd` per-call cuts (`903c683f5` #7983,
`2538e2e5f` #7986, `0167220f8` #7985, `bf1cfc9ca` #7990, `7b7899ebb` #7988)
and the nudge-queue seam (`4a0bc8ac8` #7959, `d82f5c7bf` #7958). Nothing new
was dropped: none of the 23 fixes a carried bug or removes a guarded path.
Still 83 commits over `upstream/main`.

- **Nudge queue seam** (`4a0bc8ac8`): every `cmd/gc` reach into the queue
  goes through `openNudgeQueue`, ratcheted by
  `TestNudgeQueueIsReachedOnlyThroughTheSeam`. `gc nudge show` (gc-1kz4) now
  reads the queue with `openNudgeQueue(...).Queue.Snapshot()` and shadows with
  `nudgeShadows(store)` instead of `nudgequeue.LoadState` / `nudgeFrontDoor`.
  The "Queued nudge … in <location> (nudge <id>)" line takes upstream's
  `Location`; upstream's characterization test passes the new `nudgeID`
  argument as "" (no id note, golden unchanged). The cascade-nudge
  `reference` field (gc-f3k8d) sits beside upstream's new `fromClaim` /
  `claimKeys` fields in `nudgeTarget`.
- **`gc bd` write guard skips its read where bd resolves ids exactly**
  (`0167220f8`): on such a scope (HTTP backend, prefixed ids) a non-close
  write no longer reads its bead. Upstream keeps the read for closes because
  its claim-identity close consumes the bead; the carried claim-identity
  *heartbeat* (gc-ox80c) consumes it too, and would silently run under the
  ambient actor there. Resolution, folded into gc-ox80c:
  once the child's env is built, `doBd` reads the bead (one `bd show` through
  `bdWriteGuardStore`) for a `heartbeat <id>` the guard skipped, only when
  `GC_SESSION_ID` is set and the effective `BEADS_ACTOR` is not already it
  (`ownClaimHeartbeatNeedsRead`). Outside a session, or when the child already
  acts as its session id, nothing would change the actor, so upstream's saving
  stands. Pinned by
  `TestGcBdSessionHeartbeatReadsItsClaimWhereBdResolvesIDsExactly`; upstream's
  `TestGcBdWriteGuardSkipsTheReadWhereBdResolvesIDsExactly` pins
  `GC_SESSION_ID=""` so it stays hermetic under a session.
- **`internal/api/AGENTS.md`**: union of upstream's "Nudge queue through
  State" invariant and the carried "Keyset list reads walk every page" rule
  (gc-i7ujo8).
- **No conflict, checked by meaning:** gc-klgt8 (`gc bd show` cross-store
  deps) is BdStore-level and untouched by the by-id routing; no carried store
  wrapper needs `GuardedBatcher` forwarding (the only fork wrapper type is a
  test fake); the compact drift defer (gc-i52hj) applies cleanly beside the
  compact-state health and pinned-endpoint skip.
- **Ratchets:** the command census (`next_id` 214), resource census v3, the
  native dependency guard (737 modules), cipolicy goldens and the three Bazel
  python tools all pass unchanged; the carried census and BUILD commits
  replayed cleanly. Gazelle is still unverified (no bazel on this host).
- **Gates on `3c4cf446b`** (same host and toolchain as below): `go build ./...`;
  `go vet ./...` plus the `integration` and `acceptance_a` tags; `make
  fmt-check`; `make lint-changed-go LINT_CHANGED_SCOPE=tracked
  LINT_CHANGED_REF=upstream/main` (0 issues); 35 targeted packages
  (`internal/beads`, `nudgequeue`, `storebinding`, `session`, `worker`, `api`,
  `doctor`, `orders`, `convoy`, `bootstrap/packs/core`, `examples/bd`,
  `productmetrics`, `test/docsync`) ok; the full `cmd/gc` package ok (969 s).

## What changed upstream (the parts that touched the carry)

- **Nudge outcomes are typed** (`a949e456b`, #7865):
  `nudgequeue.TerminalOutcome` / `ClassifyTerminalState` is the single list of
  terminal nudge states, and `injected_unobserved` counts as delivered.
- **Orders count only real runs** (`cef7ec76a`, #7766): `orders.IsRunRecord`
  filters what holds an order's cooldown, cursor and single-flight; the
  generic bead API refuses coordination rows (mail, waits, nudges, order runs)
  and `POST /mail` sends only as a configured identity.
- **Git hooks fall back to plain Go** (`afff5f04f` #7941, `d8adec76e` #7945):
  `.githooks/lib/local-runtime.sh` decides whether the host can run
  Bazel-built tests (bazel present, pinned ICU loadable);
  `GC_PREPUSH_SUITE` and the new `GC_PRECOMMIT_DOCS=auto|bazel|go` pick the
  engine.
- **Resource census policy v3** (`8ed726532`, #7957): counts live in the
  generated `test/test-resource-counts.toml`; `policy.go`,
  `test/test-resources.toml` and TESTING.md hold policy only.
- **Ready cache serves `GET /beads/ready`** (`8e920748b`, #7863), with a
  work-outcome veto for evicted closed blockers.
- **Reconciler v2 contract steps** (v5.10j/p/q/w/x), the orphan-sweep
  foreign-claim rule with `gc agent is-foreign` (`81928b7a9`), a capped
  managed-Dolt preflight wait whose health probe takes a context (`73c37c8fc`),
  and event authorship registration (`30b8ac715`).
- **Ratchets**: command census `next_id` 211 → 212 upstream (`gc agent
  is-foreign` = 211); go.mod (go 1.26.9), go.sum, the native dependency guard
  and cipolicy goldens unchanged.

## Dropped

| Fork commit | Subject | Verdict | Evidence |
|---|---|---|---|
| `faa0aa030` | test(supervisor): check the forced-stop deadline by outcome (gc-7lc4j) (#218) | DROP-SUPPLANTED | upstream `db64daa2a` (#7857) fixes the same flake in the same test, `TestStopManagedCityBoundsForcedShutdownWhenRuntimeHangs`: it times only the forced phase against a sibling timer armed on the grace warning, so the teardown exec and host timer lateness are outside the measurement. **Verified:** upstream's test ran `-count=5` on this macOS host and passed every time (`ok cmd/gc 3.688s`). |
| `4f90f7053` | chore(census): re-derive the resource census (upstream-sync 2026-10-13) | REGENERATE | re-derived under policy v3, see below |
| `336eac7cd` | build(bazel): hand-sync BUILD files (upstream-sync 2026-10-13) | REGENERATE | re-derived, see below |

## Reworked

| Fork commit | Subject | What changed on upstream | How the fork intent was re-expressed |
|---|---|---|---|
| `29618ba04` | `gc nudge show` (gc-1kz4) | `a949e456b` added `TerminalOutcome` / `ClassifyTerminalState` at the spot where the fork added string constants `OutcomeDelivered`/`OutcomeDropped`/`OutcomeUnknown` and `OutcomeForState` (a redeclaration that does not compile) | the fork no longer touches `internal/nudgequeue`; `cmd_nudge_outcome.go` keeps its labels locally and `nudgeOutcomeForShadowState` maps `ClassifyTerminalState` onto them (delivered; failed or superseded → dropped; not-terminal → unknown). Behaviour change: `injected_unobserved` now reports delivered where it reported unknown. New table test `TestNudgeOutcomeForShadowStateFollowsClassifyTerminalState`. Command id re-minted 212. |
| `afaf918cd` | pre-commit passes without bazel; golangci-lint pin includes the builder Go (gc-ewcrp6, gc-upuqvc) | `afff5f04f` / `d8adec76e` gave the docs step its own fallback (`GC_PRECOMMIT_DOCS`, `local-runtime.sh`) and documented "Both hooks fall back…" in CONTRIBUTING and TESTING | upstream's `.githooks/pre-commit` and docs block taken verbatim; the fork's docs fallback (`bazel_on_path` / `announce_go_twin`) dropped. The lint step is re-expressed in upstream's idiom: `GC_PRECOMMIT_LINT=auto\|bazel\|go`, where `auto` runs `make lint-changed-go` behind a banner only when `NOGO_BAZEL` (default `bazel`) is not on PATH. It does not consult `local-runtime.sh`, because nogo analyzes at build time and never loads ICU. Explicit values are never replaced; an unknown value exits 2. Kept unchanged: `make lint-changed-go`, `LINT_ENV`/`LINT_GOTOOLCHAIN`, the builder-Go pin in `golangci-lint-pinned`, the format script's pinned binary, `CGO_ENABLED=0` codegen. `githooks_pre_commit_bazel_fallback_test.go` now covers only the lint step (six cases plus the unknown-mode refusal); upstream's local-runtime tests own the docs step. CONTRIBUTING folds the lint twin into upstream's paragraph ("Three knobs…"); TESTING's lint table names the knob. |

## Kept, with a non-trivial resolution

| Commit | Subject | Resolution |
|---|---|---|
| gc-5fdrr | `gc session release-name` | command census re-minted on upstream's `next_id`: `gc nudge show` = 212, `gc session release-name` = 213, `next_id` = 214; generated files re-run through `gen-command-census`, `-check` clean |
| gc-p64nt | a molecule poured over a convoy leaves the member pool-routed | **import cycle:** upstream's new `internal/orders/run_record_test.go` imports `internal/sourceworkflow`, and the fork's `sourceworkflow/input_convoy.go` imported `internal/convoy`, which reaches `internal/orders` through `internal/config`, so `go vet ./internal/orders` failed. `ListLiveInputConvoyRootsForItem` became `ListLiveInputConvoyRootsForConvoys(rootStore, convoys, allowedRootKey)`; the caller (`cmd/gc/route_recovery.go`) walks the item's tracking convoys with `convoy.TrackingConvoysForItem` and wraps its error as before. Folded into the owning commit |
| gc-k0n2xi | stop re-reading the whole session history per demand pass | `managedDoltHealth` takes a context upstream (`73c37c8fc`); the fork's `allocator_demand_reads_test.go` fixture updated, folded into the owning commit. `ClosedNamed` rides in upstream's shared `k1EnvWith` |
| gc-d43dp4 | push ListQuery.ParentIDs and IDs down to bd in BdStore | upstream's new `TestCachedReadyOverBdBackingHonorsAnEvictedBlockersWorkOutcome` pins the cost of the cache's closed work-outcome read as upstream's whole-store `bd list --all` plus `bd query --all`. With the fork's pushdown that read is one keyed `bd list --all --id=…` (the wisp leg is skipped because the durable read returned every id); the test's every behavioural assertion passed, and its cost assertion now names the keyed read. Folded into the owning commit |
| gc-cvl9w | convoy list cost | its message was a bad squash carrying the full messages of most earlier fork commits (13,653 lines); trimmed to its own gc-cvl9w parts. Its native-store `filter.IDs` hunk, which gc-d43dp4 removed again, is dropped from both commits (tree-neutral) |
| gc-2st54j | shared in-flight gate index read | upstream's `orders.IsRunRecord` filter in `entriesForStore` survives the fork's index rewrite |
| gc-d760o | `grep -q` pipefail fixes | `07c913007` rewrote `cross-rig-deps.sh`; the here-string fix sits at its new lines 82 and 100 |
| gc-9hy4l | worktree prune gated on liveness | upstream's new config-drift row in `reconcile_arms_close_test.go` kept beside the fork's signature change |

Considered and left unchanged: **gc-rjtk1** (idle stop deferred while a human
is attached). Upstream `808b69f1c` (#7858) stopped a dead tmux server from
reading as attached in the v2 rollback guard. The fork's idle rung goes
through upstream's shared `attachmentHolds`, which upstream left unchanged
for its own destructive gates, and the idle ladder only runs for a session
the reconciler saw `alive`, so a confirmed-dead server never reaches it. No
gap to follow.

Event authorship (`30b8ac715`): no fork commit adds an event type.

## Not consolidated (deliberately)

- The gc-upuqvc half of gc-ewcrp6 (the builder-Go pin) edits lines gc-c9upz
  introduced, but it depends on `LINT_GOTOOLCHAIN`, which gc-ewcrp6 defines;
  splitting it would move that definition too. Kept whole.
- Pairs that edit each other's lines as separate changes (gc-ndns7 on
  gc-g3pgp, gc-01o2l on gc-d760o, gc-5k8udo on gc-cvl9w, gc-d43dp4 on
  gc-klgt8 / gc-wtquzx) stay separate commits.

## Regenerated artifacts

- **Command census**: ids 212/213, `next_id` 214;
  `go run ./cmd/gen-command-census -check` clean.
- **Resource census** (policy v3): upstream's `policy.go`,
  `test/test-resources.toml` and TESTING.md block unchanged (the fork adds no
  `[[medium]]` or `[[reviewed_hermetic_body]]` rows); `test/test-resource-counts.toml`
  regenerated with `-update`. Every changed record is a fork-carried test
  file: `cmd/gc/dolt_scope_watchdog_test.go` (fixed_sleep 6 → 4, subprocess
  5 → 3), `examples/bd/dolt/sync_concurrency_test.go` (new: fixed_sleep 1,
  subprocess 4), `scripts/harness_reap_test.go` (subprocess 4 → 5),
  `test/integration/bdstore_test.go` (subprocess 6 → 7).
- **Bazel BUILD files**: hand-synced (no bazel on the rebase host) by
  re-applying the previous sync's BUILD commit onto upstream's files. Four
  deps the earlier hand sync had missed were found by comparing each rule's
  gascity imports with its deps and added: `//internal/tomledit` (config),
  `//internal/testpolicy/shellpipeline` (core_test, scripts_test),
  `//internal/convoy` (sourceworkflow_test). `repo_tree.py`,
  `hermetic_tags.py` and `integration_suite.py` report no drift. Gazelle has
  not run: run `make bazel-sync` on a bazel host and commit any delta.
- **Goldens / generated docs**: `genspec`, `gen-client` and `genschema`
  (with `CGO_ENABLED=0`) regenerate byte-identical files.

## Gates run on the rebase host (macOS arm64, go 1.26.9 toolchain via GOTOOLCHAIN=local, no bazel)

- `go build ./...`, `go vet ./...`, `go vet -tags integration ./...` and
  `go vet -tags acceptance_a ./test/acceptance/...` clean over the final tree.
- `make lint-changed-go LINT_CHANGED_SCOPE=tracked LINT_CHANGED_REF=upstream/main`
  (golangci-lint 2.12.0 built with go1.26.9, and go vet, over every package
  the carry touches): 0 issues. `make fmt-check`: clean.
- `go test ./cmd/gc` (whole package) and `go test` of every other package
  the carry touches, plus `internal/nudgequeue`, `internal/events`,
  `internal/testpolicy/resourcecensus`, `test/docsync` and
  `scripts/cipolicy`: green except the pre-existing reds below. Three reds
  were the sync's own and are fixed in their owning commits: the
  `internal/orders` test import cycle (gc-p64nt), the doctor city store-check
  count 19 → 20 and its skip-count test 25 → 26 (gc-kawr5; upstream's
  `ownerless-runtimes` took the 19th slot), and the ready-cache cost
  assertion (gc-d43dp4).
- `go test -tags integration ./test/integration -run 'BdStore|ListQuery|Pushdown|Wisp'`
  against the host bd 1.3.1: pass (one skip).
- `go run ./cmd/gen-command-census -check`, the resource census tests,
  `scripts/check-native-dependency-surface.sh` (modules=737, cap 737; binary
  181,815,234 bytes) and the cipolicy goldens: pass.
- Not run here: bazel (`bazel test //...`, nogo, gazelle), the acceptance
  tiers, `make test-fast-parallel`. Linux CI is the authority.

## Known pre-existing failures on this host

Two reds are deterministic on this host, not load flakes. They fail
identically on untouched `upstream/main` in a scratch worktree, and on every
rerun:

- `scripts` `TestRRCWriterCredential` (the RBE credential mint returns a
  certificate and `curl` cannot connect from this host).
- `examples/gastown` `TestMain` hangs in the orphan Dolt store sweep over
  the host `$TMPDIR` (killed at the timeout on both trees). With an empty
  `TMPDIR` the package passes on the fork tree (`ok examples/gastown 214s`).

## Open fork pull requests, re-evaluated against this sync

Read-only audit of the 17 PRs open on `zookanalytics/gascity` on 2026-10-15
(none was closed or commented on):

- **Close, supplanted:** none.
- **Rework after the sync lands:** #202 (gc-2c1aw): its tests that seed bare
  `order-run:` beads must use `orders.IsRunRecord` shapes, and its
  molecule_failed partial-root rule needs a re-check (`cef7ec76a`).
- **Keep, re-verify after the rebase:** #229 (sweep-tracking run timeouts vs
  `IsRunRecord`) and #219 (the ready-projection work-outcome veto read cost).
  #224 must redo its census edit in the v3 per-file shape.
- **Keep:** the rest. 16 of the 17 sit on the 2026-10-07 base and 12 already
  conflict with fork `main` from the 2026-10-13 rewrite; all need a rebase.
- Upstream open PRs worth comparing first: #6908 (#232), #4996 (#224), #6434
  (#229), #6942 (#202).

## Upstream open PRs that would supplant carried commits

Check these at the next sync; if one has merged, the fork commit becomes a
DROP or REWORK candidate:

| Upstream PR | Fork commit |
|---|---|
| #7901 (tickDebouncer race) | gc-l2c6v |
| #7917 (here-string `grep -q` in cross-rig-deps, spawn-storm-detect, wisp-compact) | those hunks of gc-d760o |
| #7836 (wisp-leg metadata pushdown) | gc-wtquzx |
| #7605 (convoy members in one batch per store) | gc-5k8udo's convoy half |
| #7934 (BdStore forwards ListQuery IDs and ParentIDs) | gc-d43dp4 |
| #6168, #6410 (still open; cherry-picked) | gc-7kea9 |
| #5888 (release closed configured name holders) | gc-5fdrr |
