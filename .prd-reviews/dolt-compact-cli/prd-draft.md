# PRD: `gc dolt compact` CLI subcommand (executor for mol-dog-compactor)

## Problem Statement

The `mol-dog-compactor` formula declares itself ZFC-exempt because compaction
requires SQL connections via `database/sql`, multi-query transactional state,
branch-create/delete cleanup paths, concurrent-write retry with error
classification, and pre/post integrity verification — none of which an agent
running shell pipelines can reliably perform.

The formula was nevertheless dispatched to the `gastown.dog` pool every 24h.
Dogs read the exemption and **safely-skip** the work. No compaction ever runs.
The formula closes green at the bead level while doing nothing.

Local operational evidence (loomington city, deacon patrol 2026-05-03) confirms
the silent-failure mode:

- `lx-cpzqmh` — compact step safely-skipped
- `lx-w466x4` — verify vacuous, 0 dbs compacted
- `lx-ot7tih` — report: 4 inspected, 0 compacted

Behavior is worse than the upstream report (gastownhall/gascity#1557) describes:
upstream calls it "Bead lifecycle stalls." Locally it's a green-signal silent
failure. HQ commit count grew 1455 → 1611 over ~2 days post-`dolt_ignore` fix —
moderate growth, but persistent and below the 50k deacon alert threshold so
nothing pages.

The decided fix is option 3 from the upstream issue: a Go-implemented
`gc dolt compact` CLI subcommand that the dog can shell out to, lifting the ZFC
exemption.

## Goals

1. **Replace the safely-skip with real work.** The mol-dog-compactor cycle
   should compact actual databases when commit counts exceed the threshold.
2. **Single source of truth.** The formula remains the executable contract;
   the CLI is the executor. No hardcoded values that could drift from the
   formula.
3. **Operator UX.** `gc dolt compact <db> --mode=flatten` is shell-debuggable
   without standing up a dog.
4. **Algorithm parity.** Both flatten and surgical algorithms behave exactly
   as documented in the existing formula.
5. **Integrity guaranteed.** Pre/post row counts, post-compaction `dolt_gc`,
   and concurrent-write error classification all match the formula contract.
6. **Per-step observability.** The dog must be able to read structured CLI
   output and close `inspect`, `compact`, `verify`, and `report` step beads
   independently.
7. **Regression-proof.** An executor-binding test prevents future formulas
   from re-introducing the orphan-formula condition. **Ships in PR1 ahead of
   the CLI** so the contract is enforced before any new executor lands.
8. **Test coverage.** Unit tests cover algorithm logic and error classification;
   integration tests exercise both modes against a real Dolt server.

### Shipping Order (per human clarification)

This work splits into **two PRs**, with PR1 a hard prerequisite of PR2:

- **PR1 — Executor-binding contract (broad scope):**
  - Add `executor` and `zfc_exempt` TOML fields to formula step schema.
  - Update the formula parser to read them.
  - Migrate existing embedded formulas: tag steps that already have
    executors / intended exemptions. `mol-dog-compactor` gets
    `zfc_exempt = true` matching its current prose. **No forward refs to
    `gc dolt-compact`** (it doesn't exist yet).
  - Land the regression test that asserts every embedded-formula step
    either resolves to a real Cobra command / pack script or carries an
    `executor` reference plus the exemption marker.
- **PR2 — CLI + dog-formula update (narrow scope):**
  - `gc dolt-compact` Go command + `gc dolt compact` pack delegate.
  - `gc.dolt.compact.v1` JSON envelope.
  - Update `mol-dog-compactor` to drop `zfc_exempt = true` and set
    `executor = "gc dolt-compact"` (resolves the existing ZFC contract
    mismatch — to be confirmed during design; see Open Question 15).
  - Lands against the contract already enforced by PR1.

This ordering is reflected in the bead DAG: the PR1 work bead is a
`blocks` prerequisite of the PR2 work bead, not bundled into one bead.

### Acceptance Criteria

- `gc dolt compact hq --mode=flatten` runs end-to-end from a shell against a
  populated test Dolt server.
- `mol-dog-compactor` cycle (24h cooldown order) runs end-to-end: dog claims,
  shells out to the CLI, compaction actually occurs, integrity verified,
  `dolt_gc` runs, report sent.
- A new test (in `cmd/gc` or `examples/dolt`) verifies every executable step in
  every embedded formula has either an executor binding (resolves to a real
  Cobra command or pack script) or carries the documented exemption marker
  (`zfc_exempt = true` plus an `executor` reference) — no orphan formula refs.
  **This test ships in PR1**; PR2 lands against an already-enforced contract.

## Non-Goals

- **No compactor daemon.** Option 1 from the upstream issue is rejected.
  Daemon lifecycle, supervision, error recovery, and event-driven scheduling
  are unjustified for a 24h-cooldown task.
- **No formula split** (option 2). Splitting compact/verify into a daemon and
  inspect into the dog still needs the daemon.
- **No event-driven or commit-rate-based scheduling.** 24h cooldown stays.
- **No new compaction targets** beyond what the formula already covers.
- **No upstream PR submission.** The gascity origin/upstream model treats
  upstream PR submission as operator-gated. This run produces plan + beads +
  (later) commits on origin/main only.
- **No backwards-compatibility shim** preserving the old "safely-skip" path.
  Once the CLI lands and the formula is updated, the old skip behavior is
  removed.
- **No changes to the underlying compaction algorithms.** Flatten and surgical
  semantics are ported verbatim; no new modes, no parameter tuning.

## User Stories / Scenarios

### Story 1: Operator running ad-hoc compaction

> "HQ commit count is climbing. I want to flatten it now without waiting 24h."

```
$ gc dolt compact hq --mode=flatten
inspect: hq @ 1611 commits (threshold 500) → candidate
compact: hq → flatten in progress…
  pre-flight row counts: issues=12834, deps=4221, mail=8190, …
  reset to root, commit "compaction: flatten history"
  post row counts: issues=12834, deps=4221, mail=8190, … OK
verify: hq integrity OK
gc:     reclaimed 1.2 GB
report: 1 db inspected, 1 compacted, 0 skipped, 0 failed
```

### Story 2: Compactor dog running the formula

> Dog claims a step bead, shells out to the CLI with structured output, and
> closes per-step beads from the parsed result.

The CLI emits structured output (NDJSON or JSON-final-line) the dog can read.
Each phase corresponds to a formula step. Failures bubble up so the dog can
escalate as the formula prescribes.

### Story 3: Operator debugging a failed cycle

> "Last night's compactor failed. I want to inspect without compacting."

```
$ gc dolt compact --inspect-only
hq:       1611 commits → candidate
gascity:   234 commits → below threshold, skip
loomington: 87 commits → below threshold, skip
```

### Story 4: Concurrent-write hazard during surgical compaction

> "Surgical mode hit a graph-change error. I expect one retry then a clean
> failure."

The CLI retries once with a 2s pause (matching the formula). On second failure,
exits non-zero with an error class the dog can map to a step failure.

### Story 5: CI/test verification

> "I'm a test author. I want a focused integration test that exercises both
> modes against a real Dolt server."

Existing patterns in `examples/dolt/*_test.go` provide the scaffolding.

## Constraints

- **Existing formula variables must be honored exactly:**
  - `--mode` (flatten | surgical, default flatten)
  - `--threshold` (commit count, default 500)
  - `--keep-recent` (surgical only, default 50)
  - `--databases` (comma list, empty = auto-discover)
- **CLI naming convention — locked.** Per `cmd/gc/cmd_dolt_cleanup.go:719`
  comment: pack scripts use `gc dolt <subcommand>` (e.g., `gc dolt cleanup`);
  Go-side commands are top-level `gc dolt-<subcommand>`. Mirror cleanup's dual
  surface exactly:
  - `gc dolt-compact` — Go-side, invoked by both the dog formula and the pack
    delegate.
  - `gc dolt compact` — pack delegate at
    `examples/dolt/commands/compact/{command.toml,run.sh}` (operator-facing).
- **Auto-discovery — locked.** Empty `--databases` discovers exclusively via
  `gc rig list --json` (same path as `gc dolt-cleanup`). **Never `SHOW
  DATABASES`** against the live Dolt server — that scope includes test/orphan
  DBs and is a footgun for compaction. Operators wanting unregistered DBs must
  pass them as positional args.
- **Concurrent-write safety:**
  - flatten: safe (merge base shifts; data preserved)
  - surgical: not safe; retry once with 2s pause on graph-change error
- **Integrity verification:** pre/post row counts on all user tables
  (excluding `dolt_*` system tables); mismatch → error, leave db in
  pre-compaction state.
- **`dolt_gc` runs post-compaction** to reclaim unreferenced chunks.
- **Test coverage:** unit + integration. Integration mirrors patterns in
  `examples/dolt/*_test.go` (real Dolt sql-server).
- **No SQL injection.** Database identifiers must be validated (charset
  identical to existing pack scripts: `[A-Za-z0-9_-]`, leading
  `[A-Za-z0-9_]`).
- **No agent-driven judgment in Go.** ZFC applies: the CLI's behavior is
  determined by flags + reality, not heuristics.
- **Origin/upstream model.** This work lands on origin/main only. No upstream
  PR.
- **Surgical-mode startup ordering — locked.** Surgical mode acquires the
  advisory lifecycle lock **first**, then drops leftover `compact-base` /
  `compact-work` branches. Cleaning branches before locking risks pulling
  branches out from under a still-running prior compaction. Reuse
  `dolt_lifecycle_lock.go`; crash recovery is implicit ("next run cleans up
  its own orphans" — but only after acquiring the lock).
- **Default principle.** Mirror `gc dolt-cleanup` patterns wherever possible —
  same envelope shape, same dual CLI surface, same database-discovery path,
  same identifier validation, same lifecycle locking. Diverge only with
  stated cause documented in the design doc.

## Open Questions

> Q1, Q2, Q3, Q4, Q6, Q7, Q8, Q9, Q11 were resolved in the human-clarification
> round; see the **Clarifications from Human Review** section below for the
> binding decisions. The unresolved questions still in scope for the
> design-exploration phase are 5, 10, 12, 13, 14, plus the two new follow-ups
> (15, 16) introduced by the clarification round.

1. ~~**Naming.**~~ **Resolved (Q3):** build both `gc dolt-compact` (Go) and
   `gc dolt compact` (pack delegate). See Constraints.

2. ~~**Per-step output format.**~~ **Resolved (Q1):** single final JSON
   envelope on stdout, schema id `gc.dolt.compact.v1`, mirroring
   `gc.dolt.cleanup.v1`.

3. ~~**Streaming vs final-envelope parsing.**~~ **Resolved (Q1):** dog parses
   the final envelope after the CLI exits. No streaming.

4. ~~**Auto-discovery scope.**~~ **Resolved (Q4):** `gc rig list --json` only.
   Never `SHOW DATABASES`. See Constraints.

5. **Error classification taxonomy.** What error classes does the CLI expose,
   and how do they map to dog-side actions (skip, escalate, retry-step)?
   - `concurrent-write-collision` (surgical, retried internally)
   - `integrity-mismatch` (escalate to mayor)
   - `database-locked` / `database-unreachable` (skip)
   - others?

6. ~~**Locking and re-entrancy.**~~ **Resolved (Q5):** advisory lock via
   `dolt_lifecycle_lock.go`, acquired before any branch cleanup. See
   Constraints.

7. ~~**Auto-discover for HQ.**~~ **Resolved (Q4):** rig registry returns HQ
   like any other registered rig DB. CLI does not read the city layout
   directly nor query Dolt for the database list.

8. ~~**JSON envelope schema versioning.**~~ **Resolved (Q1):** mint
   `gc.dolt.compact.v1` and pin the schema from day 1, mirroring how
   `gc.dolt.cleanup.v1` is treated.

9. ~~**Executor-binding regression-test scope.**~~ **Resolved (Q2):** test
   walks all embedded formulas, asserts every step either resolves to a real
   Cobra command / pack script or carries `executor = "..."` plus
   `zfc_exempt = true`. **Ships in PR1** (independent of the CLI). Test
   location TBD during design — recommend `cmd/gc/` for pack-script visibility.

10. **`dolt_gc` cost.** `dolt_gc` can be expensive on large databases. Should
    it be on by default per-database, or aggregated to one call after all
    compactions? Formula currently says "post-compaction" per database.

11. ~~**Surgical-mode crash recovery.**~~ **Resolved (Q5):** advisory lock
    first, then drop leftover `compact-*` branches. Crash recovery is implicit
    in the next run's lock-then-cleanup ordering. Document the manual recovery
    procedure for operators in the design doc.

12. **Observability budget.** Should the CLI emit events to the gc event bus
    (`gc events emit`)? Useful for dashboard / patrol alerts, but couples
    the CLI to event-bus infrastructure. (Per PRD review: defer to v2 unless
    a concrete consumer exists.)

13. **Read-only / dry-run mode.** Should `gc dolt compact --inspect-only`
    or `--dry-run` be supported for operator preflight? Useful for
    pre-cycle audits. (Per PRD review: defer to v2.)

14. **How does the CLI handle databases that don't meet the commit threshold
    when explicitly named (`gc dolt compact hq --threshold=10000`)?** Does
    `--threshold` override per-database, or skip with a message?

15. **(New, design-exploration follow-up — from Q2 clarification.)** Does
    shipping the CLI flip `mol-dog-compactor` from `zfc_exempt = true` to
    `executor = "gc dolt-compact"` (resolving the existing ZFC contract
    mismatch), or do daemon and CLI coexist? Default assumption is **flip**
    (the daemon path was rejected as a Non-Goal). Confirm in the design doc.

16. **(New, design-exploration follow-up — from Q2 clarification.)** Confirm
    "ZFC" expansion (Zero Framework Cognition) for unambiguous developer-facing
    docs covering the new `zfc_exempt` schema field.

## Rough Approach

### CLI surface (provisional)

```
gc dolt-compact [databases...] [flags]

Flags:
  --mode flatten|surgical    Compaction algorithm (default: flatten)
  --threshold N              Commit count threshold (default: 500)
  --keep-recent N            Surgical mode: recent commits to preserve (default: 50)
  --databases CSV            Database list (alt to positional args; empty = auto-discover)
  --inspect-only             Report candidates without compacting
  --json                     Emit gc.dolt.compact.v1 JSON envelope on stdout
  --port N                   Override Dolt port resolution (mirrors dolt-cleanup)
```

Pack delegate: `examples/dolt/commands/compact/{command.toml,run.sh}` shells
out to the Go CLI, exposing `gc dolt compact` with the same flags.

### Module layout (provisional)

- `cmd/gc/cmd_dolt_compact.go` — Cobra command, flag wiring
- `cmd/gc/dolt_compact.go` — algorithm core (flatten + surgical), no I/O on
  Cobra (testable)
- `cmd/gc/dolt_compact_inspect.go` — database discovery + commit-count probe
- `cmd/gc/dolt_compact_verify.go` — pre/post row count integrity check
- `cmd/gc/dolt_compact_envelope.go` — JSON envelope (`gc.dolt.compact.v1`)
- `cmd/gc/cmd_dolt_compact_test.go` — unit tests
- `cmd/gc/cmd_dolt_compact_integration_test.go` — `//go:build integration`,
  real Dolt server

### Reuse strategy

- Port resolution: reuse `dolt_cleanup_port.go` chain (`--port` > city
  `dolt.port` > `<rigRoot>/.beads/dolt-server.port` > 3307).
- Lifecycle lock: probably reuse `dolt_lifecycle_lock.go`.
- SQL execution: reuse the existing `database/sql` plumbing pattern from
  `gc dolt-cleanup`.
- Identifier validation: identical charset to cleanup's `[A-Za-z0-9_-]`.

### Formula + order updates

- `examples/dolt/formulas/mol-dog-compactor.toml`:
  - Remove ZFC-exemption block (executor now exists)
  - Each step's description updates to reference the CLI (`gc dolt compact …`)
- `examples/dolt/orders/mol-dog-compactor.toml`: unchanged (still 24h cooldown
  to dog pool).
- `examples/dolt/commands/compact/`: new pack-script binding.

### Tests

- Unit:
  - flatten algorithm with mocked SQL responses
  - surgical algorithm with mocked rebase plan
  - retry-on-collision behavior
  - integrity mismatch detection
  - identifier charset rejection
  - JSON envelope shape
- Integration (`//go:build integration`):
  - bring up a Dolt sql-server on a temp port
  - populate with N commits
  - run `gc dolt-compact` flatten end-to-end, verify commit count drops
  - run surgical mode, verify only `keep-recent` commits remain
  - inject concurrent write between pre-flight and rebase, verify retry
  - verify `dolt_gc` ran (chunk reclamation observable via disk size)
- Regression test (location TBD):
  - parse all embedded formulas, ensure every step has an executor binding
    or a recognized exemption marker

### Open implementation questions tracked above; will be resolved during
the design-exploration phase.

## Clarifications from Human Review

Recorded 2026-05-04 from the live conversation that followed PRD review
synthesis (`prd-review.md`). The five "Critical Questions" Q1-Q5 were
presented; each is bound below. The answers also introduce the PR1/PR2
shipping order (now in **Goals → Shipping Order**) and the firm
auto-discovery / lock-ordering rules (now in **Constraints**).

### Default principle (overarching)

Mirror `gc dolt-cleanup` patterns wherever possible. Same envelope shape,
same dual CLI surface, same database-discovery path, same identifier
validation, same lifecycle locking. Diverge only with stated cause
documented in the design doc.

### Q1 — JSON envelope shape

**A: (b)** — single final JSON envelope on stdout, schema id
`gc.dolt.compact.v1`, mirroring `gc.dolt.cleanup.v1`. Pin from day 1. No
streaming NDJSON, no sidecar file. Dog parses the envelope after the CLI
exits.

### Q2 — Executor-binding contract + regression test

**A: confirmed** with explicit PR split.

- Schema: introduce explicit TOML fields on formula steps:
  - `executor = "<command-or-path>"` — names the binding (Cobra command,
    pack script, etc.).
  - `zfc_exempt = true` — declares the step as Zero-Framework-Cognition
    exempt (pairs with `executor` to point at the human/external executor
    when an agent cannot execute the step alone).
- Regression test scope: **all embedded formulas only.** Every step must
  resolve to either (1) a real Cobra command / pack script, or (2) carry
  `executor = "..."` plus `zfc_exempt = true`.

**PR split:**
- **PR1** (broad scope, prerequisite): schema fields + parser + migration
  sweep across all existing embedded formulas + regression test. Migration
  tags only steps that already have intended executors / exemptions —
  e.g., `mol-dog-compactor` gets `zfc_exempt = true` matching its current
  prose. **No forward refs to `gc dolt-compact`** (it doesn't exist yet).
- **PR2** (narrow scope): `gc dolt-compact` CLI + `gc.dolt.compact.v1`
  envelope + `mol-dog-compactor` formula update. Lands against the
  already-enforced PR1 contract.

**Two design-exploration follow-ups (now Open Questions 15 & 16):**
- (a) Does shipping the CLI flip `mol-dog-compactor` from `zfc_exempt =
  true` to `executor = "gc dolt-compact"` (resolving the existing ZFC
  contract mismatch), or do daemon and CLI coexist? Default assumption:
  flip (the daemon path was rejected as a Non-Goal).
- (b) Confirm "ZFC" expansion (Zero Framework Cognition) for unambiguous
  docs covering the new `zfc_exempt` schema field.

### Q3 — CLI naming convention

**A: (c) — both, same dual surface as cleanup.**

- `gc dolt-compact` (Go top-level) — implementation; invoked by both the
  dog formula and the pack delegate.
- `gc dolt compact` (pack delegate) at
  `examples/dolt/commands/compact/{command.toml,run.sh}` — operator-facing
  ergonomic surface.

### Q4 — Auto-discover scope

**A: (a), firm.** `gc rig list --json` only — production DBs, identical
to `gc dolt cleanup`. **Never `SHOW DATABASES`** against the live Dolt
server (test/orphan DBs are a footgun). Operators wanting unregistered
DBs must pass them as positional args.

### Q5 — Surgical-mode lock + cleanup ordering

**A: confirmed** with explicit ordering: **acquire advisory lifecycle
lock first, THEN drop leftover `compact-base` / `compact-work` branches.**
Otherwise a still-running prior compaction could have its branches pulled
out from under it. Reuse `dolt_lifecycle_lock.go`. Crash recovery is
implicit in the "next run cleans up its own orphans" model — but only
after acquiring the lock. Document the manual recovery procedure for
operators in the design doc.

### Material scope changes recorded above

- **Goals section** now includes a "Shipping Order" subsection describing
  the PR1/PR2 split and DAG dependency.
- **Acceptance Criteria** now binds the regression test to PR1 explicitly.
- **Constraints** now lock CLI naming, auto-discovery (with the "never
  `SHOW DATABASES`" rule), surgical-mode startup ordering, and the default
  "mirror cleanup" principle.
- **Open Questions** are pruned (Q1, Q2, Q3, Q4, Q6, Q7, Q8, Q9, Q11
  resolved) and extended with the two new design-exploration follow-ups
  (Q15 & Q16).
