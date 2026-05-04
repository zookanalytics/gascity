# Design Review Legs — dolt-compact-cli

Round: design-exploration (mol-idea-to-plan.design-exploration, bead `gc-oick69`)
Date: 2026-05-04
Coordinator: gascity/gastown.nux

Six lens-focused analyses of the PRD draft + PRD review + human
clarifications. Per the formula's "parity of outcome, not verbatim
port" allowance, executed inline rather than via `gc sling` fan-out
(matching the prd-review predecessor). Each leg consumed:

- `.prd-reviews/dolt-compact-cli/prd-draft.md`
- `.prd-reviews/dolt-compact-cli/prd-review.md`
- `.plan-reviews/dolt-compact-cli/human-clarifications.md`
- `examples/dolt/formulas/mol-dog-compactor.toml`
- `examples/dolt/orders/mol-dog-compactor.toml`
- selected `cmd/gc/cmd_dolt_cleanup*.go` and `cmd/gc/dolt_cleanup*.go`
- `examples/dolt/commands/cleanup/{command.toml,run.sh}`
- `internal/formula/types.go` (current `Step` schema)
- `cmd/gc/dolt_lifecycle_lock.go`

---

## Leg 1 — `api`: CLI / API shape, ergonomics, discoverability

### Summary

The CLI surface is locked along three axes by the human-clarification
round: dual surface (`gc dolt-compact` Go top-level + `gc dolt compact`
pack delegate, Q3), single final JSON envelope `gc.dolt.compact.v1`
(Q1), auto-discovery via `gc rig list --json` only (Q4). The remaining
API decisions are flag taxonomy, error-class taxonomy (PRD Q5),
`--threshold` override semantics on explicit databases (PRD Q14), and
the output mode (default human-readable + `--json` opt-in vs `--json`
default for daemon callers).

### Key Considerations

- **Symmetric flag set with the formula vars.** The formula contract
  (`commit_threshold`, `databases`, `mode`, `keep_recent`) must map
  1:1 to flags so a dog can pour the formula and shell out without
  string surgery: `--threshold`, `--databases`, `--mode`,
  `--keep-recent`. Anything else is a layering violation.
- **Top-level vs `dolt-cleanup` symmetry.** `gc dolt-cleanup` lives at
  top level (not under `dolt`) because the pack already owns
  `gc dolt …`. Same constraint here: `gc dolt-compact` at top level,
  `gc dolt compact` is the pack delegate. (This is also Q3's binding
  decision; reaffirmed by reading the cleanup source comment at
  `cmd/gc/cmd_dolt_cleanup.go:719`.)
- **Default output: human-readable.** Operator stories 1, 3, 4 in the
  PRD all show human output with progress lines. `--json` opts into
  the envelope. Dog scripts must pass `--json`. This mirrors
  `gc dolt-cleanup` exactly.
- **Discovery path.** Empty `--databases` triggers `gc rig list --json`
  expansion. **Never** `SHOW DATABASES` (Q4). Operators wanting an
  unregistered DB must pass it positionally. Unregistered positional
  args are accepted (operator override) but flagged in the envelope
  as `discovery_source: "explicit"`.
- **Error classes (PRD Q5).** Eight classes covering the observed
  failure modes: `ok`, `below-threshold`, `concurrent-write-collision`
  (surgical, retried internally), `concurrent-write-fatal` (post-retry),
  `integrity-mismatch`, `database-locked`, `database-unreachable`,
  `invalid-identifier`, `internal-error` (catch-all for unexpected SQL
  errors so the dog can escalate). Map each to a stable string in the
  envelope.
- **Threshold override on positional args (PRD Q14).** When a database
  is named explicitly (positional or `--databases` list), `--threshold`
  remains effective: a user passing `gc dolt-compact hq --threshold=10000`
  is asking *"compact only if hq exceeds 10000 commits"*. Skip with
  `below-threshold` if not. Operators wanting unconditional compaction
  can pass `--threshold=0`. This is consistent with the formula's
  threshold semantics — the threshold gates *every* candidate, named
  or auto-discovered.
- **`--inspect-only` and `--dry-run`.** Defer to v2 (PRD review §
  "Important But Non-Blocking"). MVP needs only the four formula vars
  + `--json` + `--port`. Adding two preview modes is gold-plating
  before a single end-to-end cycle has run.
- **`--port` flag.** Mirror cleanup's port resolution chain exactly
  (AD-04: `--port > city dolt.port > <rigRoot>/.beads/dolt-server.port
  > 3307`). Reuse `dolt_cleanup_port.go` if it's exported, otherwise
  factor out the chain into a shared helper this PR can reuse.

### Options Explored

**Option A — Single command, sub-flags only.** `gc dolt-compact
[databases…] --mode --threshold --keep-recent --databases --port
--json`. Flat surface; matches cleanup. **Chosen.**

**Option B — Subcommand-per-mode.** `gc dolt-compact flatten` and
`gc dolt-compact surgical`. Rejected: the formula treats mode as a
variable (`{{mode}}`), and mapping mode → subcommand introduces a
shape mismatch the dog would have to translate.

**Option C — Streaming NDJSON.** Rejected by Q1.

**Option D — Default `--json` when stdout is not a TTY.** Tempting
(automatic dog-friendly behavior) but introduces a hidden mode
switch operators may not realize is happening. Reject; require
explicit `--json`. Match cleanup.

### Recommendation

- Cobra command name `dolt-compact` at top level.
- Pack delegate `examples/dolt/commands/compact/{command.toml,run.sh}`
  shells out to `gc dolt-compact "$@"` with the env-passing pattern
  cleanup already uses (`PACK_DIR/assets/scripts/runtime.sh`).
- Flags: `--mode`, `--threshold`, `--keep-recent`, `--databases`,
  `--port`, `--json`. Positional args = explicit DB list.
- Default human output; `--json` opt-in.
- Error taxonomy: 9 stable classes (see Key Considerations), surfaced
  per database in the envelope.
- `--threshold=0` is the documented operator override for "compact
  this DB regardless."

### Constraints Identified

- Pack delegate must shell out via `runtime.sh` (matches cleanup,
  ensures `GC_DOLT_PORT`/`GC_DOLT_HOST`/`GC_DOLT_USER` propagate).
- Cobra command lives in `cmd/gc/cmd_dolt_compact.go`. Logic
  extracted to `cmd/gc/dolt_compact_*.go` for testability.
- `examples/dolt/commands/compact/run.sh` is shell, not Go. It
  forwards `"$@"` verbatim — same trap as the `gc dolt sql` regression
  in `examples/dolt/sql_test.go:TestSQLScriptForwardsQueryArgs`. Add a
  parallel test for compact's run.sh so the same gap can't recur.

### Open Questions

- **OQ-API-1.** Should the envelope schema include the *requested*
  parameters (mode, threshold, keep_recent, databases) so a downstream
  reader can audit the actual invocation without `ps`? Recommend yes,
  under an `invocation` key — cheap and high-value for the dog's
  per-step bead notes.
- **OQ-API-2.** Exit code mapping: `0` = all success; `1` = at least
  one DB hit `integrity-mismatch` or `concurrent-write-fatal`
  (escalate-class); `2` = invocation error (bad flag, port unresolved,
  no DBs to compact when explicit list given). The dog interprets the
  exit code first, then drills into the envelope. Matches cleanup.

### Integration Points

- `cmd/gc/main.go` — register the new command on the root.
- `cmd/gc/dolt_cleanup_port.go` — shared port-resolution helper
  (extract if necessary).
- `examples/dolt/commands/compact/` — new pack-delegate directory.
- `examples/dolt/pack.toml` — register the new pack command if the
  pack manifest enumerates commands.

---

## Leg 2 — `data`: data model, storage, migrations, schema evolution

### Summary

This work has two distinct data models: **(a)** the JSON envelope
schema (`gc.dolt.compact.v1`) the CLI emits, and **(b)** the formula
TOML schema additions (`executor`, `zfc_exempt`) introduced by PR1.
Storage migration is concentrated in PR1 — every embedded formula
gets walked, and steps with intended exemptions get the `zfc_exempt`
tag. PR2 changes only `mol-dog-compactor` (drops the exemption,
adds `executor`).

### Key Considerations

- **Envelope schema is forwards-compatible by construction.** Mirror
  `CleanupReport` in `cmd/gc/cmd_dolt_cleanup.go:32`: stable shape
  from day 1, additive evolution, empty arrays render as `[]` so
  callers survive zero-result cycles. Pin `schema = "gc.dolt.compact.v1"`
  in the top-level field; bump only on incompatible changes.
- **Per-database result.** The envelope's `databases` array has one
  entry per database considered (auto-discovered or explicit). Each
  entry: `name`, `discovery_source` (`rig-registry|explicit`),
  `pre_commits`, `post_commits` (zero if skipped/failed),
  `outcome` (one of the 9 classes from Leg 1), `mode` (echoed),
  `attempts` (1 or 2 for surgical retry), `pre_row_counts` and
  `post_row_counts` (omitempty), `gc_reclaimed_bytes` (omitempty),
  `error_message` (omitempty), `duration_ms`.
- **Top-level summary.** `summary.databases_inspected`, `compacted`,
  `skipped`, `failed`, `total_duration_ms`, `gc_reclaimed_bytes`.
  Mirrors cleanup's `Summary` block.
- **Errors block.** Top-level `errors` array for invocation-level
  problems (port unresolved, rig-list failure). Per-database errors
  ride on the database entry. Same split as cleanup.
- **PR1 schema additions to `internal/formula/types.go:Step`:**
  - `Executor string \`toml:"executor,omitempty" json:"executor,omitempty"\``
  - `ZfcExempt bool \`toml:"zfc_exempt,omitempty" json:"zfc_exempt,omitempty"\``
  Both are optional and default to zero. Steps without either field
  are interpreted as "must resolve to a Cobra command or pack script
  via convention" — the regression test does the lookup.
- **No migration for existing TOMLs in PR1 except `mol-dog-compactor`.**
  The regression test passes today only if either the formula step
  resolves to an existing command OR carries the new exemption tags.
  Walking embedded formulas at PR1 time will surface the audit list:
  expect `mol-dog-compactor` to be the only existing case that needs
  `zfc_exempt = true` (other formulas should already have agent-resolvable
  steps). Other ZFC-exempt steps that surface during the audit get
  tagged in PR1; that's the migration scope.
- **No data migrations on the wire.** Beads, mail, dolt commit graph
  — none change. Only on-disk TOML files mutate (PR1) and a new
  envelope shape begins emitting (PR2).
- **Schema evolution path.** Future field additions (e.g.,
  `compaction_method_versioned` for new modes) are additive. A
  `compactor.v2` schema bump happens only if a field becomes incompatible
  (rare). The regression test pins `gc.dolt.compact.v1`.

### Options Explored

**Option A — Inline structs in cmd_dolt_compact.go.** Simple, mirrors
cleanup. **Chosen.**

**Option B — Shared package `internal/dolt/envelope`.** Tempting if
several commands eventually emit similar envelopes, but premature.
Cleanup didn't extract; compact shouldn't either until a third
consumer arrives.

**Option C — Embed pre/post row counts as a top-level summary.**
Rejected: per-database row counts can be large (dozens of user tables
× many DBs); they belong on the per-database record so callers can
discard them when summarizing.

### Recommendation

Adopt the cleanup envelope shape verbatim, scaled to compact's per-DB
result model. Pin the schema string. Do **not** extract a shared
envelope package yet.

For PR1: add two optional fields to `formula.Step`, parse them in the
recipe loader, and ensure the cooked metadata propagates them on the
emitted bead (so the dog's bead-walking can see the exemption without
re-parsing the TOML).

### Constraints Identified

- The envelope must be valid JSON even when the CLI exits non-zero.
  This is a cleanup-pattern requirement: callers parse on stdout and
  use the `errors` array + per-DB `outcome` to drive escalation.
  Achieved by deferring writes until the end-of-run encoder block.
- The TOML schema additions must be **omitempty** so existing
  formulas serialize identically. Reading: zero-value `bool` is
  falsy → no behavior change for unannotated steps.
- `internal/formula/types.go:Step` is hot — many tests reference it.
  Adding fields is safe (Go zero values), but each cooking path
  that copies steps must propagate the new fields. Audit cooking
  helpers in `internal/formula/` for `Step{}` literals during PR1.

### Open Questions

- **OQ-DATA-1.** Should `executor` be a free-form string (e.g.,
  `"gc dolt-compact"`) or a structured tag (e.g.,
  `{ kind = "cobra", command = "dolt-compact" }`)? Free-form is
  simpler; structured supports future binding kinds (HTTP, container)
  but is YAGNI. Recommend free-form string in PR1; revisit only if
  a non-CLI binding emerges.
- **OQ-DATA-2.** Envelope-schema field ordering — does `databases`
  come before or after `summary`? Cleanup puts `summary` last; mirror
  for consistency.
- **OQ-DATA-3.** Should the envelope record the human-readable
  formula contract version (`graph.v2`) or the formula path? Useful
  for forensic correlation but not required for MVP. Defer.

### Integration Points

- `internal/formula/types.go` — add 2 fields (PR1).
- `internal/formula/recipe.go` and `compile.go` — propagate new
  fields through cooking (PR1).
- `cmd/gc/cmd_dolt_compact.go` — define the envelope structs (PR2).
- Reserved metadata keys under `gc.formula.executor` /
  `gc.formula.zfc_exempt` if the cooked bead needs to surface them
  (PR1, optional).

---

## Leg 3 — `ux`: mental model, workflow fit, error experience, docs examples

### Summary

The CLI is the executor for a 24h-cooldown silent-failure recovery,
so the dominant UX is **the dog's**, not the human's. The human-side
UX is the "operator running ad-hoc compaction" path (PRD Story 1),
which must produce confidence-inducing progress lines, not a blank
4-minute pause followed by JSON. The most important error UX is
**the integrity-mismatch escalation message** — the operator must
be able to act on it without re-reading the formula prose.

### Key Considerations

- **Default mode is the operator-facing one.** Human-readable progress:
  one line per phase per database (`inspect:`, `compact:`, `verify:`,
  `gc:`, `report:`). Story 1 in the PRD already sketches this. Do
  not surprise the operator with JSON unless they pass `--json`.
- **Progress visibility for long compactions.** `dolt_gc` can take
  minutes-to-hours on large DBs (PRD review § Important But
  Non-Blocking). For MVP, emit a single line on entry (`gc: <db> in
  progress…`) and a single line on exit with elapsed + reclaimed
  bytes. Don't introduce a tick-based heartbeat — that's gold-plating.
  An operator running interactively can hit Ctrl-C; a dog runs it
  via `gc bd` and the heartbeat is the dog itself, not the CLI.
- **Errors must explain the failure class AND the recovery path.**
  - `integrity-mismatch` → "Database left in pre-compaction state.
    Re-run `gc dolt compact <db>` after investigation. Report:
    `gc mail send mayor/ -s 'ESCALATION: Compaction integrity check
    failed [CRITICAL]' -m '...'`"
  - `concurrent-write-fatal` (surgical, post-retry) → "Surgical
    rebase collided with live writes twice. Wait for write activity
    to settle or use `--mode=flatten` (concurrent-write safe)."
  - `database-locked` → "Another `gc dolt-compact` is already running
    against <db>. Wait or kill it."
  - `database-unreachable` → "Dolt server at port <p> not reachable.
    Check `gc dolt status`."
- **Mental model: this is `gc dolt-cleanup` for commit history.**
  Operators already know cleanup. The compact CLI shares the port
  chain, the `--json` opt-in, the rig-registry discovery, the
  identifier validation. Documenting compact as "cleanup's sibling
  for commit graphs" anchors the new tool in known terrain.
- **Workflow fit: dog cycle vs operator.** Dog: `gc bd dispatch` →
  `gc dolt-compact --json` (parsed) → per-step bead closure → mail
  to mayor on failure. Operator: `gc dolt-compact hq --mode=flatten`,
  read progress, done. Both paths through the same Cobra command.
- **Confused-deputy hazard.** A dog mis-pouring the formula could in
  theory pass `--threshold=0` and force-compact every DB. Mitigations:
  (a) explicit `--threshold` flag default mirrors the formula default
  (500), (b) the dog reads the formula vars and forwards them — no
  hidden defaults. The CLI's defaults match the formula's defaults.
  No magic.
- **Docs examples.** Add a section in `engdocs/contributors/` (or
  wherever cleanup lives) covering: (1) the operator one-liner,
  (2) the dog cycle end-to-end, (3) recovery procedures for the four
  escalation classes above, (4) what to do if a previous run left
  `compact-base` / `compact-work` branches (surgical mode crash
  recovery — the answer is "next run cleans them up before locking",
  per Q5 ordering).

### Options Explored

**Option A — Verbose-by-default progress.** `inspect:` line per DB,
`compact:` with phase progress (pre-flight counts, reset, commit,
post-flight counts), `verify:`, `gc:`, `report:`. **Chosen for the
default human output.** Mirrors PRD Story 1.

**Option B — Quiet-by-default, `--verbose` for phase lines.** Rejected:
operators want to see what's happening during a 4-minute compaction
on HQ. Quiet-by-default trains a habit of "if it hangs I'll just
Ctrl-C" — bad for trust.

**Option C — Emit progress events to the gc event bus.** Deferred to
v2 per PRD review § Observations. Adding the event-bus dependency for
no current consumer is YAGNI.

### Recommendation

- Default human output mirrors PRD Story 1 line-for-line.
- Errors include both class and recovery path inline.
- Document the CLI in `engdocs/contributors/dolt-compact.md`
  (parallel to whatever cleanup has) covering operator + dog flows.
- Update `mol-dog-compactor` step descriptions in PR2 to reference
  the CLI's flags exactly (`--mode={{mode}} --threshold={{commit_threshold}}`,
  etc.) so a human reading the formula sees the same surface.

### Constraints Identified

- Per the PRD, the order of operations is **Reaper → Compactor →
  Doctor (gc)**. Documentation must NOT instruct an operator to run
  compact before reaper has cleaned up — wrong row counts in the
  pre-flight baseline. Surface this in the operator guide.
- Recovery doc must explain the lock-then-cleanup ordering (Q5):
  if you see a stale `compact-base` branch, run `gc dolt-compact
  <db> --mode=surgical` and the next run will clean it up. Do NOT
  manually drop the branches while a compaction is running.

### Open Questions

- **OQ-UX-1.** Should the CLI emit a TTY-detection-based color hint
  (red errors, green successes) like cleanup does? Cleanup uses
  ✓/⚠/✖ glyphs not ANSI color. Mirror the glyph approach (terminal-
  agnostic, log-friendly).
- **OQ-UX-2.** When `--inspect-only` ships in v2, should it surface
  predicted reclaim bytes? Useful but speculative. Defer with the
  rest of v2.

### Integration Points

- `engdocs/contributors/` — new `dolt-compact.md` (or extend the
  existing dolt cleanup doc).
- `examples/dolt/commands/compact/command.toml` — operator-facing
  description (single sentence).
- `mol-dog-compactor.toml` (PR2) — step descriptions reference CLI
  flags.

---

## Leg 4 — `scale`: bottlenecks, scale limits, degradation modes, caching

### Summary

The scale dimension is dominated by two facts: **(1)** `dolt_gc` is
expensive (minutes-to-hours on large DBs), and **(2)** the dog runs
this at 24h cooldown, not on demand. The CLI itself doesn't need to
scale — one process, one or a few databases, one cycle. The
bottleneck is wall-clock time per database, not throughput. The
degradation modes are concentrated in the surgical rebase path
(graph-change collisions) and in the post-compaction `dolt_gc` step.

### Key Considerations

- **Worst-case wall time.** HQ at 1611 commits today flattens in
  seconds; a 100k-commit database in surgical mode with `dolt_gc`
  could take an hour or more. The 24h-cooldown order tolerates this,
  but: a multi-DB cycle (4 DBs in loomington) needs to handle each
  DB serially without one stuck DB blocking the rest forever.
- **Per-DB time budget.** Recommend a per-database soft budget
  (configurable, default `1h`) after which the CLI reports
  `database-deadline-exceeded` for that DB and moves on. The dog can
  pour subsequent step beads regardless. Rationale: a `dolt_gc`
  hung on a wedged DB shouldn't lock the entire cycle. Implementation:
  `context.WithTimeout` on the SQL session, check between phases.
- **Sequential vs parallel DB processing.** Sequential. Parallelism
  multiplies disk I/O and SQL connection pressure on a single Dolt
  server. Each DB's compaction is heavy; queueing is fine.
- **`dolt_gc` cost (PRD Q10).** Run once **per database** post-compact,
  not aggregated. Aggregating means a single failure during gc rolls
  back the visibility of *all* DBs' compaction. Per-DB gc keeps the
  blast radius small. Document this as a decision in the design doc.
- **Caching.** None needed. The pre-flight row counts are the only
  per-database state, and they live in CLI memory for the duration
  of one DB's compaction. No cross-invocation cache.
- **Scaling auto-discovery.** `gc rig list --json` is bounded by city
  size (small N — typically <20 rigs even in busy cities). Discovery
  cost is negligible.
- **Concurrent invocations.** Two `gc dolt-compact <db>` invocations
  against the same DB must not race. Lifecycle lock (PRD constraint
  reuses `dolt_lifecycle_lock.go`). **But:** the existing
  `dolt_lifecycle_lock.go` is a *managed-Dolt-lifecycle* lock (single
  flock under `<city>/.gc/runtime/packs/dolt/`); it's not per-database.
  Reusing it serializes ALL compaction across all DBs in a city — fine
  for MVP (compactor cycles run rarely; a 24h-cooldown order is the
  primary caller). A per-database advisory lock can be added later
  if the global serialization causes pain.
- **Failure modes:**
  - Pre-flight row count read times out → mark DB as
    `database-unreachable`, skip.
  - DOLT_RESET / DOLT_COMMIT fails → mark DB as `internal-error`,
    leave in pre-compaction state (no recovery needed because
    nothing committed).
  - Post-flight count mismatch → `integrity-mismatch`, escalate.
    Database is left at the failed-post-compaction state — operator
    must manually verify before re-running. Document the recovery.
  - Surgical retry-then-fail → `concurrent-write-fatal`. Branch
    cleanup runs on next invocation per Q5.
  - `dolt_gc` failure → `internal-error`, but compaction itself is
    already committed — do NOT roll back. Note in the envelope
    that gc didn't run.

### Options Explored

**Option A — Per-DB advisory lock now.** Adds a SQL-level lock
(GET_LOCK(<db>)). Rejected for MVP — global serialization is fine
at current scale. Revisit if a city grows to dozens of compactor-
target DBs.

**Option B — Aggregated `dolt_gc` after all DBs.** Considered, rejected
above (blast radius too large, fragile error path).

**Option C — Per-DB hard timeout via `SET STATEMENT_TIMEOUT`.**
Rejected: cannot reliably timeout `CALL dolt_gc()` from inside a
transaction. Use Go-side context cancellation on the connection
instead.

### Recommendation

- Sequential per-DB processing.
- Per-DB soft timeout (`--per-db-timeout`, default `1h`) implemented
  via `context.WithTimeout` on the SQL session for that DB.
- Reuse `dolt_lifecycle_lock.go` as a global advisory lock for MVP
  (acknowledged limitation: serializes across DBs).
- Document the limitation and the path to per-DB locking.

### Constraints Identified

- The existing `dolt_lifecycle_lock.go` lock is a city-wide
  managed-Dolt-lifecycle lock, not a per-database compaction lock.
  The PRD constraint to "reuse `dolt_lifecycle_lock.go`" needs a
  design-level disambiguation: either (a) reuse it as a coarse
  city-wide compaction lock, or (b) introduce a separate
  `dolt_compact_lock.go` for per-DB locking. Recommend (a) for
  MVP. PR2 design doc must state this explicitly.
- `dolt_gc` lock contention with other consumers is observable but
  not testable in the unit suite. Integration test should at minimum
  verify a `dolt_gc` call returns successfully on a populated test DB.

### Open Questions

- **OQ-SCALE-1.** Should the per-DB timeout default be exposed in the
  formula vars, or fixed in the CLI? Current formula has no timeout
  var. Add `--per-db-timeout` as a CLI flag with a default; do NOT
  expose in the formula until a city actually hits the limit.
- **OQ-SCALE-2.** What's the upper bound on commit count where surgical
  mode remains feasible? At some scale, the rebase plan itself becomes
  unmanageable. Document an empirical guidance ("surgical practical to
  ~10k commits; flatten preferred above") in the operator guide once
  measured.

### Integration Points

- `cmd/gc/dolt_lifecycle_lock.go` — reused as the advisory lock.
- `cmd/gc/dolt_compact.go` (new) — per-DB context budgeting.
- Future: `cmd/gc/dolt_compact_db_lock.go` if per-DB locking is added.

---

## Leg 5 — `security`: trust boundaries, attack surface, validation, permissions

### Summary

The CLI runs as the operator (or the dog) against the local Dolt
server. The trust boundary is the Dolt server itself — once a SQL
connection is open, the CLI can do anything the connection's user
can. The attack surface is **input validation on database names** and
**control of which databases the CLI will touch**. The dog is a
trusted caller; the human operator is also trusted. The realistic
threat model is operator error, not adversarial actors.

### Key Considerations

- **SQL injection on database identifiers.** Database names flow
  through `USE \`<name>\``, `CALL DOLT_RESET(...)`,
  `CALL DOLT_COMMIT(...)`, `CALL DOLT_BRANCH(...)`, etc. Name strings
  must be validated before being interpolated. Mirror cleanup's
  charset: `[A-Za-z0-9_-]`, leading char `[A-Za-z0-9_]`. Reject
  anything outside on entry. **Never** rely on backtick-quoting alone
  — Dolt's quoted-identifier handling has historically had escape-
  related rough edges.
- **Auto-discovery is the safety net.** Q4 binds discovery to
  `gc rig list --json`. This means the CLI cannot accidentally
  compact a non-rig database (test/orphan DBs) on auto-mode.
  Operator override via positional args is explicit and auditable
  — they typed the name. Both paths re-validate the charset.
- **No `SHOW DATABASES`.** Q4 forbids it. Even if it were added
  later, the result must be filtered through the rig registry.
  Worth a regression test: assert the CLI never runs `SHOW
  DATABASES` (negative-grep over recorded SQL in integration test).
- **Privilege escalation surface.** None — CLI inherits the operator's
  Dolt credentials via the standard env (`GC_DOLT_USER`,
  `GC_DOLT_PASSWORD`). Compaction does not require any new privilege
  beyond what the existing dolt-cleanup needs. (It does need
  `DROP TABLE` rights for nothing, but `DOLT_RESET`/`DOLT_COMMIT`
  permissions on the target DB.)
- **Branch leakage.** Surgical mode creates `compact-base` and
  `compact-work` branches. If the CLI crashes between branch creation
  and cleanup, those branches persist with intermediate compaction
  state. The next invocation cleans them up (after acquiring the
  lock — Q5). Operator-visible: `dolt branch -a` shows the leftover.
  Document this so an operator who finds them doesn't panic-delete.
- **Lock-file safety.** The advisory flock is a file under
  `<city>/.gc/runtime/packs/dolt/`. If the CLI crashes, the file is
  closed by the kernel and the flock released — standard flock
  semantics. No stale-lock recovery code needed.
- **`dolt_gc` is destructive.** It reclaims unreferenced chunks
  permanently. There is no undo. Recovery is from JSONL backups + Dolt
  filesystem backups (formula's safety section). The CLI should NOT
  add an opt-out for `dolt_gc` — the formula contract says it runs.
  An operator wanting to skip gc can manually flatten/squash without
  using this CLI.
- **Read-only operator probes.** `gc dolt-compact --inspect-only` (v2)
  must NOT acquire the lifecycle lock — it's read-only and should be
  parallel-safe with running compactions. Note for future PR.
- **Argv leakage.** The CLI accepts no secrets on the command line.
  Credentials come from env (`GC_DOLT_PASSWORD`). Confirmed safe.
- **JSON envelope leakage.** The envelope contains database names,
  row counts, and reclaimed bytes. None of this is sensitive in the
  Gas City threat model (DB names are public via the rig registry,
  row counts are operationally observable). No sanitization needed.

### Options Explored

**Option A — Identifier charset matches cleanup exactly.** Single
source of truth, fewer surprises. **Chosen.**

**Option B — Allow Unicode-ish names via `IDN` normalization.**
Rejected: complicates SQL identifier handling, opens injection
edges, and the rig registry doesn't support such names anyway.

**Option C — Lock per-database via `GET_LOCK` instead of file lock.**
Considered for Leg 4. Both have security tradeoffs (file lock survives
restarts; SQL lock dies with the connection). Sticking with file lock
for MVP per Leg 4.

### Recommendation

- Reject database names that fail the charset check before any SQL
  is run. Surface the rejection as `invalid-identifier` in the
  envelope.
- Pin `gc rig list --json` as the auto-discovery source; never
  `SHOW DATABASES`.
- Document the surgical-mode branch artifacts so operators don't
  manually delete them mid-compaction.
- No new privileges, no new secrets, no new attack surface vs cleanup.

### Constraints Identified

- Test must verify the charset validator rejects: `;DROP DATABASE x`,
  trailing space, leading dash, empty string, control characters,
  Unicode. Mirror cleanup's identifier-validation tests.
- Integration test must verify the full SQL path uses the validated
  name only — no string concatenation of unvalidated input downstream.

### Open Questions

- **OQ-SEC-1.** Should the regression test for executor-binding (PR1)
  also verify that no embedded formula step's `executor` field
  contains shell metacharacters? Since `executor` is a contract
  marker, not an exec path, this is overkill. Document the field as
  *not* an exec path; future tooling that resolves `executor` to a
  binary must re-validate.
- **OQ-SEC-2.** Is there a privileged "destructive operation"
  confirmation gate for ad-hoc operator runs? Cleanup has `--force`
  to gate destruction. Compact has no equivalent — every run is
  destructive. Recommend documenting that `gc dolt-compact <db>`
  is destructive; no `--force` flag (would just be ceremony).

### Integration Points

- `cmd/gc/cmd_dolt_cleanup.go` (already has a charset validator —
  factor it into a shared helper for compact to reuse, or copy).
- `cmd/gc/dolt_compact_validate.go` (new) — identifier validation,
  rig-registry filter.

---

## Leg 6 — `integration`: code placement, rollout path, compatibility, testing strategy

### Summary

The PR1/PR2 split is the central structural decision and is locked.
PR1 introduces formula-schema fields and the executor-binding
regression test; PR2 introduces the CLI, the envelope, the pack
delegate, and the formula update. Integration is mostly mechanical
because cleanup provides the template. Testing strategy mirrors
cleanup's: unit tests for algorithm logic and validators,
integration tests against a real Dolt sql-server. The novel test is
the executor-binding regression test in PR1.

### Key Considerations

- **PR1 scope (ships first):**
  1. Add `Executor` and `ZfcExempt` fields to
     `internal/formula/types.go:Step`.
  2. Wire them through `internal/formula/recipe.go` and
     `compile.go` (cooked metadata propagation).
  3. Migration: walk every embedded formula in the repo, identify
     steps that need exemption tags, apply them. Expected scope is
     1 file (`mol-dog-compactor.toml` getting `zfc_exempt = true`),
     but the audit may surface others.
  4. Regression test: walk all embedded formulas (helper that lists
     them via `internal/formula/` registry or `examples/*/formulas/`
     glob); for each step assert it either resolves to a real Cobra
     command (look up in the root command tree) or pack script (look
     up in `examples/*/commands/`) **or** carries `executor` +
     `zfc_exempt = true`. Test failure messages name the offending
     formula + step.
  5. No CLI changes. No `gc dolt-compact` references — it doesn't
     exist yet.
- **PR2 scope (ships after PR1 lands):**
  1. `cmd/gc/cmd_dolt_compact.go` — Cobra command.
  2. `cmd/gc/dolt_compact*.go` — algorithm core, discovery,
     verification, envelope.
  3. `cmd/gc/cmd_dolt_compact_test.go` — unit tests.
  4. `cmd/gc/cmd_dolt_compact_integration_test.go` —
     `//go:build integration`, real Dolt sql-server.
  5. `examples/dolt/commands/compact/{command.toml,run.sh}` — pack
     delegate.
  6. `examples/dolt/commands/compact/sql_test.go` (or equivalent) —
     arg-forwarding regression like `examples/dolt/sql_test.go`.
  7. `examples/dolt/formulas/mol-dog-compactor.toml` update: drop
     `zfc_exempt = true` from each step (or from the whole formula,
     depending on how PR1 defines the field), set
     `executor = "gc dolt-compact"` on each executable step. Update
     step descriptions to reference CLI flags.
- **Compatibility:**
  - Existing formulas without the new fields work unchanged
    (zero-value `bool`/empty string).
  - Existing dog cycles continue to safely-skip until PR2 lands and
    the formula is updated. No interregnum.
  - Test DBs (orphan namespaces) are unaffected — auto-discovery
    excludes them by design.
- **Test strategy:**
  - **Unit (PR1):** schema parse round-trip, regression test core
    (with fixture formulas including a missing executor → fails;
    well-formed → passes).
  - **Unit (PR2):** envelope shape stability, charset validator,
    error-class mapping, surgical retry semantics (mocked SQL),
    integrity-mismatch detection.
  - **Integration (PR2):** run against a tmp Dolt sql-server
    populated with N commits; verify post-flatten commit count = 1,
    verify post-surgical commit count = `keep_recent + 1`, verify
    `dolt_gc` reduces disk size, verify retry behavior under
    injected concurrent writes.
  - **End-to-end smoke (manual, post-deploy):** trigger
    `mol-dog-compactor` cycle on the loomington test rig, verify it
    actually compacts (counter to today's safely-skip).
- **Rollout path:**
  - PR1 lands → contract enforced; nothing else changes user-visibly.
  - PR2 lands → next 24h cycle (or operator-triggered cycle) runs
    real compaction. Watch the deacon report and the patrol cycle
    bead notes.
  - Post-deploy: confirm the `gastown__polecat-lx-cpzqmh` /
    `lx-w466x4` / `lx-ot7tih` pattern is replaced by
    "compacted N databases" in the next cycle's report.
- **Backward-compat removal:** the old "safely-skip when ZFC-exempt"
  branch in dog-side code should remain in PR2 — but the formula
  no longer triggers it because `zfc_exempt` is dropped from the
  compactor formula in PR2. The skip path stays for any future
  daemon-only formulas (they'd carry `zfc_exempt = true` and an
  `executor` pointing at the daemon entry).
- **Where the regression test lives:** prefer
  `internal/formula/embedded_executor_binding_test.go` so it can use
  the formula registry directly. Acceptable alternative:
  `cmd/gc/embedded_formula_executor_test.go` if the test needs the
  Cobra command tree at hand. The latter is more pragmatic.

### Options Explored

**Option A — Bundle PR1 + PR2.** Rejected by Q2 — explicit human
binding decision was the split.

**Option B — Land the formula update with PR1 (turn `mol-dog-compactor`
into `zfc_exempt = true` in PR1 itself).** This is Q2's binding decision
exactly, since the migration sweep is part of PR1 scope. Confirm.

**Option C — Land regression test with PR2 instead of PR1.** Rejected
by Q2: the regression test ships in PR1 specifically to enforce the
contract before any new executor lands.

**Option D — Skip the pack delegate (`gc dolt compact` shell)
entirely.** Considered. Rejected: operator UX consistency requires
both surfaces. The dog can shell out to either, but operators who
type `gc dolt cleanup` will type `gc dolt compact`.

### Recommendation

- Adopt the PR1/PR2 split exactly as the human-clarification round
  binds it.
- Place the regression test in `cmd/gc/embedded_formula_executor_test.go`
  for ergonomic access to the Cobra command tree. (Alternate:
  `internal/formula/` if formula-package access ergonomics win out.)
- Mirror cleanup's directory and file structure, scaled to compact's
  responsibilities.
- Add an arg-forwarding regression test for `commands/compact/run.sh`
  parallel to the cleanup pattern.

### Constraints Identified

- The regression test in PR1 will fail today if any embedded formula
  step has no executor binding and no exemption marker. The migration
  sweep (PR1 step 3) must catch all such steps before the test can
  pass. This is a "find-then-tag" exercise — expect to discover a
  small number of additional formula steps that need annotating.
- PR1's regression test must enumerate "embedded formulas" precisely.
  Recommend: every `*.toml` under `examples/*/formulas/` (and any
  other registered formula directories — `internal/formula/` should
  expose a registry helper). Document the boundary.
- PR2 must update `mol-dog-compactor` to reference the CLI's flags
  using the formula's variable substitution syntax (`{{mode}}`,
  `{{commit_threshold}}`, etc.). The dog's pour fills these in before
  invoking the CLI.

### Open Questions

- **OQ-INTEG-1.** Q15 / PRD Open Question 15: confirms the CLI flips
  `mol-dog-compactor` from `zfc_exempt = true` to
  `executor = "gc dolt-compact"`. The default assumption is the flip
  (daemon path is rejected as a Non-Goal). The design doc should
  state this binding plainly so PR2 doesn't introduce both an exemption
  and an executor by accident.
- **OQ-INTEG-2.** Q16 / PRD Open Question 16: confirm "ZFC" expansion.
  Should be "Zero Framework Cognition" per `AGENTS.md`'s "Key design
  principles" section. The design doc should fix this expansion in
  the PR1 schema doc-comment.

### Integration Points

- `cmd/gc/main.go` — register the new command.
- `cmd/gc/cmd_dolt_cleanup.go` — port chain helper, identifier
  validator (factor or copy).
- `internal/formula/types.go`, `recipe.go`, `compile.go` — schema
  additions.
- `examples/dolt/commands/compact/` — new pack-delegate dir.
- `examples/dolt/formulas/mol-dog-compactor.toml` — formula update
  (PR2).
- `engdocs/contributors/` — new operator/dev guide.

---

## Synthesis Notes (for design-doc.md)

Cross-leg threads converging on a single design:

1. **All six legs ratify the human-clarification bindings.** No leg
   contests Q1-Q5 or the PR1/PR2 split. Confidence is uniformly high.

2. **The cleanup pattern is the design.** Legs 1, 2, 3, 5, 6 all reach
   for cleanup as the template. Diverging is a per-divergence
   decision that the design doc must justify.

3. **Three design-level details emerge that the PRD did not pin:**
   - **Per-DB soft timeout** (Leg 4). New flag `--per-db-timeout`,
     default 1h, enforced via `context.WithTimeout`.
   - **Lock granularity** (Leg 4 + Leg 5). MVP reuses
     `dolt_lifecycle_lock.go` as a city-wide compaction lock,
     accepting the global serialization for simplicity.
   - **Error class taxonomy** (Leg 1 + Leg 4). 9 stable classes
     covering observed and predicted failure modes.

4. **PRD Open Questions resolved by the design exploration:**
   - **PRD Q5 (error classification taxonomy)** → 9 classes pinned
     (Leg 1).
   - **PRD Q10 (`dolt_gc` cost)** → per-DB invocation, accept the
     cost; `--per-db-timeout` provides the safety valve (Leg 4).
   - **PRD Q12 (event-bus emission)** → defer to v2; no consumer
     today (Leg 3).
   - **PRD Q13 (`--inspect-only` / `--dry-run`)** → defer to v2; not
     needed for the silent-failure recovery (Leg 1).
   - **PRD Q14 (`--threshold` override on positional)** → threshold
     gates every candidate; explicit `--threshold=0` is the operator
     override for unconditional compaction (Leg 1).
   - **PRD Q15 (CLI flips `zfc_exempt` → `executor`)** → confirmed
     flip; daemon path is a rejected Non-Goal (Leg 6).
   - **PRD Q16 (ZFC expansion)** → "Zero Framework Cognition" per
     AGENTS.md (Leg 6).

5. **Design-level open questions remaining for prd-align/plan-review
   rounds:**
   - OQ-DATA-1: free-form vs structured `executor` field shape.
   - OQ-INTEG-1/2: confirm the binding wording in the design doc.
   - OQ-SCALE-1: where to expose `--per-db-timeout` (CLI flag only
     or also a formula var).
   - OQ-API-2: precise exit-code mapping.

6. **No leg flagged a fundamental redesign or a missing PRD section.**
   Build it.
