# Design: `gc dolt compact` CLI subcommand (executor for `mol-dog-compactor`)

The compactor formula declares itself ZFC-exempt (daemon-only executor)
but is dispatched to the `gastown.dog` pool every 24h. Dogs read the
exemption and safely-skip — no compaction occurs. Upstream issue
`gastownhall/gascity#1557` enumerates three fix options; option 3
(CLI subcommand) is the decided long-term answer. Implement it.

Scope: a new `gc dolt compact [databases...]` subcommand that:

- Honors existing formula variables: `--mode` (flatten|surgical),
  `--threshold` (commit count, default 500), `--keep-recent`
  (surgical, default 50), `--databases` (comma list, empty =
  auto-discover)
- Implements both flatten and surgical algorithms exactly as
  described in the existing formula
- Performs integrity verification (pre/post row counts, dolt_gc
  post-step, error classification)
- Emits structured output the dog can read for per-step closure
  (inspect / compact / verify / report)
- Has unit + integration tests
- Updates `mol-dog-compactor` formula and order to invoke the CLI
  and lift the ZFC exemption
- Adds an executor-binding test so this orphan-formula regression
  cannot recur

Acceptance:

- `gc dolt compact hq --mode=flatten` runs end-to-end from a shell
- `mol-dog-compactor` cycle runs end-to-end: dog claims, shells out,
  compaction actually occurs, integrity verified, `dolt_gc` runs,
  report sent
- Test verifies the formula's executor binding (no orphan formula
  refs)

## Executive Summary

This design proposes building `gc dolt-compact` as a Go-side
sibling of `gc dolt-cleanup`. The two share a port-resolution
chain, a JSON-envelope shape, a rig-registry-only auto-discovery
path, identifier-charset validation, and a flock-based advisory
lock. The compactor's algorithmic core (flatten and surgical) is
ported verbatim from the existing `mol-dog-compactor` formula —
no new modes, no parameter tuning. Integrity verification (pre/post
row counts) and `dolt_gc` are part of the algorithm, not optional
phases.

The work ships in two PRs, with PR1 a hard prerequisite of PR2:

- **PR1** introduces formula-schema fields (`executor`, `zfc_exempt`)
  and the executor-binding regression test that walks every embedded
  formula and asserts each step is either resolvable to a concrete
  command or carries the exemption marker. `mol-dog-compactor` is
  migrated to `zfc_exempt = true` matching its current prose.
  No new CLI is introduced.
- **PR2** introduces `gc dolt-compact` (Go top-level), `gc dolt
  compact` (pack delegate), the `gc.dolt.compact.v1` JSON envelope,
  and the `mol-dog-compactor` formula update that drops `zfc_exempt
  = true` and sets `executor = "gc dolt-compact"`.

The design exploration yielded three PRD-level resolutions: a 9-class
error taxonomy, a per-DB soft timeout to bound `dolt_gc` cost, and
a binding decision on lock granularity (city-wide flock for MVP,
per-DB lock as a future enhancement). It also pins the resolutions
of PRD Open Questions 5, 10, 12, 13, 14, 15, and 16.

PRD-alignment round 1 added three contract-discharge mechanisms:
(1) the rollback path on integrity-mismatch (flatten captures
`pre_hash` and `DOLT_RESET --hard`s; surgical halts before the
branch swap) discharges the PRD constraint "leave db in
pre-compaction state"; (2) the `phases` block on the JSON envelope
plus the per-DB `pre_hash` field discharge Goal 6's "close
inspect/compact/verify/report step beads independently" without
forcing the dog to derive aggregate status from the per-DB array;
(3) a formula/CLI defaults consistency unit test plus a
formula-driven integration test discharge Goal 2's "no hardcoded
values that could drift from the formula" and Acceptance 2's
end-to-end formula→CLI→envelope→step-close path.

PRD-alignment round 2 closed three remaining alignment gaps:
(1) a §Operations / Manual Recovery subsection consolidates the
operator-facing recovery procedures (flatten failed rollback,
surgical mid-rebase crash, stuck advisory lock), discharging
PRD Q5/Q11's "document the manual recovery procedure for operators
in the design doc"; (2) Component 6 (Validator) was added to Key
Components alongside its R-6 drift mitigation, surfacing the
SQL-injection / charset-validation discharge that was previously
only described in prose; (3) TD-5 was extended with an explicit
algorithm-vs-wrapper framing for `--per-db-timeout`, defending
against Non-Goal 7's "no parameter tuning" reading by anchoring
the deadline to the orchestration layer rather than flatten or
surgical internals, and the Phase 2 step 10 consistency-test
scope was clarified accordingly.

PRD-alignment round 3 closed the last cluster of remaining gaps,
all surfaced by walking the user stories end to end and the open
questions one by one: (1) a §Step-execution contract subsection
binds the per-cycle CLI invocation rule (CLI runs once per cycle,
the `compact` step is the trigger, the `inspect` / `verify` /
`report` steps are bookkeeping anchors closed from the cached
envelope), discharging PRD Story 2's dog-cycle path against the
formula's preserved 4-step structure; (2) OQ-1 is bound to a
free-form `executor` string for PR1 (structured-tag form deferred
to v2); (3) OQ-4 is bound to CLI-only `--per-db-timeout` for v1
with an explicit promotion trigger; (4) OQ-5 is bound to a 3-value
exit-code table with a no-op-as-`0` rule per cleanup parity; (5)
OQ-7's ZFC expansion is moved from "open" to "resolved" since it is
already pinned in TD-9, the PR1 step 1 spec, and the `ZfcExempt`
doc-comment.

Confidence is high. The cleanup pattern provides a tested template;
the algorithmic content is already specified in the formula. The
durable win is the regression test in PR1 — it makes the orphan-
formula failure mode unrepeatable.

## Problem Statement

The `mol-dog-compactor` formula is dispatched to the `gastown.dog`
pool every 24 hours via a `cooldown` order. The formula declares
itself ZFC-exempt because compaction requires SQL connections via
`database/sql`, multi-query transactional state, branch
create/delete cleanup paths, concurrent-write retry with error
classification, and pre/post integrity verification — none of
which an agent running shell pipelines can reliably perform.

The dog reads the exemption and safely-skips the work. The formula
closes green at the bead level while doing nothing. Local
operational evidence (loomington city, deacon patrol 2026-05-03)
confirms the silent failure: cycle `lx-cpzqmh` (compact safely-skipped),
`lx-w466x4` (verify vacuous, 0 dbs compacted), `lx-ot7tih` (report,
4 inspected, 0 compacted). HQ commit count grew 1455 → 1611 over
~2 days post-`dolt_ignore` fix — moderate growth but persistent,
below the 50k deacon alert threshold so nothing pages.

The behavior is worse than upstream describes. `gastownhall/gascity#1557`
calls it "Bead lifecycle stalls." Locally it's a green-signal silent
failure. The fix is option 3 from the upstream issue: a Go-implemented
`gc dolt compact` CLI subcommand that the dog can shell out to,
lifting the ZFC exemption.

A second, equally important problem surfaced during planning: the
gap that produced the silent failure is structural, not local.
Nothing in the codebase prevents another formula from declaring
itself ZFC-exempt without naming an executor. The fix must therefore
include a regression test that pins the contract: every formula
step either resolves to a real Cobra command / pack script or
declares its exemption with both a `zfc_exempt` flag and an
`executor` reference.

## Proposed Design

### Shape

`gc dolt-compact` is a Go-side Cobra command at the root of the `gc`
binary, paired with a pack-delegate shell script at `examples/dolt/
commands/compact/run.sh` that exposes `gc dolt compact` as the
operator-facing surface. This dual-surface pattern matches
`gc dolt-cleanup` exactly. The dog formula invokes
`gc dolt-compact` directly with `--json` to consume the structured
envelope.

The CLI follows the formula's existing 4-step structure (inspect,
compact, verify, report) but does not literally run as four
processes — it runs as one process that emits a single final JSON
envelope describing all four phases. The dog parses the envelope
once after the CLI exits and closes its per-step beads accordingly.

The compactor algorithm has two modes (flatten and surgical) and is
ported verbatim from the formula's prose into Go. No semantic
divergence. The formula remains the executable contract; the CLI
is the executor.

### Step-execution contract

The CLI is invoked **exactly once per dispatched cycle**, not once
per formula step. Three forces require this:

1. **Lock contention.** All four phases share the city-wide advisory
   lock acquired inside the algorithm body (TD-4). A second
   invocation in the same cycle would either block on the held lock
   or, if the first invocation has already released, return
   `database-locked` once the next cycle's contender re-acquires.
   Either path turns the formula's 4-step structure into a serialised
   queue with no payoff.
2. **Algorithm re-entrancy.** Flatten and surgical are not re-entrant
   within a cycle. A second flatten invocation against an already-
   flattened DB would no-op (commit count below threshold) but still
   pay the lock acquire, the rig-list parse, and `dolt_gc` overhead;
   surgical's `compact-*` branch creation would race with leftover-
   branch cleanup.
3. **Envelope-driven step closure.** The `phases` block (Per-step
   bead mapping, below) is designed to drive all four step closures
   from one envelope. Multiple invocations would produce multiple
   envelopes, requiring a tiebreak rule the design has not specified.

**Designated trigger step.** The mol-dog-compactor formula's
`compact` step is the trigger. When the dog claims the `compact` step
bead, it invokes `gc dolt-compact --json` with the formula-bound
flags substituted from the formula vars; the dog parses the resulting
envelope and uses it to close `compact` immediately and the other
three step beads as they are claimed (or atomically, depending on the
dog's formula-execution model — the design constrains the outcome,
not the dispatch order).

**Bookkeeping steps.** The `inspect`, `verify`, and `report` steps
remain in the formula as bookkeeping anchors for the four phases of
the JSON envelope. They retain `executor = "gc dolt-compact"`
(declares the underlying binding for the executor-binding regression
test, Component 8) **and `zfc_exempt = true`** (declares that the dog
does not re-invoke the executor for these steps; closure happens via
dog-runtime logic that consumes the envelope produced by the
`compact` step). The `executor` + `zfc_exempt` pair on the
bookkeeping steps is the same shape the regression test accepts —
the contract is unchanged, only the per-step binding posture
differs.

The dog-runtime mechanism that propagates the envelope across step
beads in the same cycle is a small implementation concern (cache the
envelope keyed by molecule id; consume from cache when a non-`compact`
step bead is claimed). This is a PR2 implementation detail bounded by
the contract above; the design doc does not specify the cache shape.

### Two-PR Shipping Order

The work is split into two PRs. **PR1 is a hard prerequisite of
PR2.**

#### PR1 — Executor-binding contract (broad scope)

1. Add `Executor` and `ZfcExempt` fields to `internal/formula/
   types.go:Step`. Both optional, omitempty, with docstrings that
   spell out "ZFC" as "Zero Framework Cognition" and bind the
   semantics ("`executor` names the binding; `zfc_exempt = true`
   declares the step as Zero Framework Cognition exempt and pairs
   with `executor` to point at the human/external executor when an
   agent cannot execute the step alone"). The `Executor` field is
   typed as `string` (a free-form binding identifier, e.g.,
   `"gc dolt-compact"`) per OQ-1's resolution; the structured-tag
   form (`{ kind, command }`) is deferred to v2 and is additive
   when needed.
2. Wire the new fields through `internal/formula/recipe.go` and
   `compile.go` so cooked beads carry them in metadata under the
   reserved `gc.formula.executor` and `gc.formula.zfc_exempt` keys
   (or equivalent) when set.
3. Migrate every embedded formula in the repo. Walk
   `examples/*/formulas/*.toml` (and any other formula registry
   roots that the formula package exposes); for each step, decide
   whether it resolves to a Cobra command / pack script today, or
   if it's intended to be agent-executed (the bulk), or if it
   needs an exemption tag. Apply the tags as found.
4. Land the executor-binding regression test (see Component 8
   below). The test walks every embedded formula and asserts every
   step either resolves to a real Cobra command / pack script or
   carries `executor = "..."` plus `zfc_exempt = true`.
5. `mol-dog-compactor.toml` is migrated to `zfc_exempt = true`
   matching its current prose. **No forward references to
   `gc dolt-compact`** — the CLI doesn't exist yet.

PR1 lands on origin/main with no functional change to compaction
behavior (still safely-skips). The contract is enforced before any
new executor lands.

#### PR2 — CLI + dog-formula update (narrow scope)

1. `cmd/gc/cmd_dolt_compact.go` — Cobra command, flag wiring,
   orchestration.
2. `cmd/gc/dolt_compact_inspect.go` — database discovery and
   commit-count probe.
3. `cmd/gc/dolt_compact_flatten.go` — flatten algorithm.
4. `cmd/gc/dolt_compact_surgical.go` — surgical algorithm with
   retry-on-collision.
5. `cmd/gc/dolt_compact_verify.go` — pre/post row count integrity
   check.
6. `cmd/gc/dolt_compact_envelope.go` — `gc.dolt.compact.v1`
   envelope structs.
7. `cmd/gc/dolt_compact_validate.go` — identifier charset validation
   (factored shared helper if cleanup's validator hasn't been
   exported; otherwise reuse).
8. `cmd/gc/cmd_dolt_compact_test.go` — unit tests.
9. `cmd/gc/cmd_dolt_compact_integration_test.go` (`//go:build
   integration`) — real Dolt sql-server tests.
10. `examples/dolt/commands/compact/{command.toml,run.sh}` — pack
    delegate. `run.sh` shells out to `gc dolt-compact "$@"` via
    `runtime.sh` (matches cleanup's pattern). Add a parallel
    arg-forwarding regression test (`compact_test.go`) so the
    `gc dolt sql` regression that motivated #1485 cannot recur on
    the new wrapper.
11. `examples/dolt/formulas/mol-dog-compactor.toml`:
    - Set `executor = "gc dolt-compact"` on every step (declares the
      binding for the executor-binding regression test).
    - Set `zfc_exempt = true` on the `inspect`, `verify`, and
      `report` bookkeeping steps; **only the `compact` step drops
      `zfc_exempt` and runs the CLI** (per Step-execution contract).
    - Update the `compact` step's description to reference CLI flags
      using formula variable substitution
      (`gc dolt-compact --mode={{mode}} --threshold={{commit_threshold}}
      --keep-recent={{keep_recent}} --databases={{databases}} --json`).
    - Update `inspect` / `verify` / `report` step descriptions to
      reference the cached-envelope closure path produced by
      `compact` (the dog-runtime detail per Step-execution contract).
12. `examples/dolt/orders/mol-dog-compactor.toml` — unchanged
    (24h cooldown, dog pool).
13. `engdocs/contributors/dolt-compact.md` — operator + dog guide:
    flag reference, dog-cycle walkthrough, recovery procedures for
    each error class, the lock-then-cleanup ordering for surgical
    crash recovery.

PR2 lands against the contract PR1 enforced. The next 24h cycle
(or operator-triggered cycle) runs real compaction.

### Algorithms

Both algorithms are ports of the formula's existing prose.

#### Flatten

```
for each candidate database:
  acquire advisory lock (city-wide flock for MVP)
  USE <db>
  pre_hash := SELECT @@<db>_head     # captured for rollback path
  pre_row_counts := SELECT COUNT(*) for every user table
  root_hash := earliest commit hash from dolt_log
  CALL DOLT_RESET('--soft', root_hash)        # parent pointer moves
  CALL DOLT_COMMIT('-Am', 'compaction: flatten history')
  post_row_counts := SELECT COUNT(*) for every user table
  if pre != post:
    CALL DOLT_RESET('--hard', pre_hash)       # rollback to pre-compaction
    outcome = integrity-mismatch (escalate)
    skip dolt_gc; release lock; continue
  CALL dolt_gc()
  release lock
```

Flatten is concurrent-write safe — the merge base shifts but data
is preserved. The `pre_hash` capture-and-revert is the rollback path
that discharges the PRD constraint "mismatch → leave db in
pre-compaction state": the new flatten commit is reachable until
`dolt_gc` runs, so `DOLT_RESET --hard pre_hash` restores the original
HEAD without losing data. `dolt_gc` is intentionally skipped on
mismatch so an operator can re-inspect both states; the next
successful cycle reclaims chunks. The captured `pre_hash` is also
surfaced in the envelope (`pre_hash` field on each per-DB record)
so an operator can recover manually if the in-CLI rollback itself
fails.

#### Surgical

```
for each candidate database:
  acquire advisory lock
  USE <db>
  (Q5) drop leftover compact-base / compact-work branches
  pre_hash, pre_row_counts := pre-flight snapshot (HEAD on main + counts)
  root_hash := earliest commit
  CALL DOLT_BRANCH('compact-base', root_hash)
  CALL DOLT_BRANCH('compact-work', 'main')
  CALL DOLT_CHECKOUT('compact-work')
  CALL DOLT_REBASE('--interactive', 'compact-base')
  mark old commits as squash, keep recent {{keep_recent}}
  CALL DOLT_REBASE('--continue')
  if rebase failed with graph-change error:
    pause 2s; retry once
    if retry failed:
      outcome = concurrent-write-fatal (escalate)
      drop compact-* branches; release lock; continue   # main untouched
  post_row_counts := re-count tables on compact-work
  if pre != post:
    outcome = integrity-mismatch (escalate)
    drop compact-* branches; release lock; continue     # main untouched
  if HEAD on main moved since pre_hash:
    outcome = concurrent-write-fatal (escalate)
    drop compact-* branches; release lock; continue     # main untouched
  # only reached when verify and concurrency checks passed
  delete main, rename compact-work → main
  delete compact-base
  CALL dolt_gc()
  release lock
```

Surgical retries once with a 2s pause on graph-change errors. The
**halt-before-swap** rule discharges the PRD constraint "mismatch →
leave db in pre-compaction state": on any escalate-class outcome, the
working branches are dropped and `main` is never touched, so the
database remains exactly as it was on entry. The captured `pre_hash`
is also surfaced in the envelope for the same operator-recovery
reasons as flatten.

#### Per-DB soft timeout

Each database's compaction runs under `context.WithTimeout(ctx,
perDBTimeout)`. Default `1h`; configurable via `--per-db-timeout`.
On timeout, the per-DB outcome is `database-deadline-exceeded` and
the CLI moves to the next database. Rationale: a hung `dolt_gc` on
one wedged DB cannot block the rest of a multi-DB cycle.

### Error Class Taxonomy

The CLI exposes nine stable error classes per database in the
envelope:

| Class | Meaning | Dog action |
|-------|---------|------------|
| `ok` | compaction succeeded | close compact step `closed` |
| `below-threshold` | commit count under threshold | close compact step `closed` (skipped) |
| `concurrent-write-collision` | surgical retried internally | observability only; final outcome may be `ok` |
| `concurrent-write-fatal` | surgical failed after retry | escalate to mayor; close `escalated` |
| `integrity-mismatch` | pre/post row counts differ | escalate to mayor; close `escalated` |
| `database-locked` | advisory lock unavailable | close `skipped`; next cycle retries |
| `database-unreachable` | Dolt server unreachable | close `failed`; deacon nudge |
| `invalid-identifier` | DB name fails charset check | close `failed` (operator error) |
| `database-deadline-exceeded` | per-DB timeout hit | escalate to mayor (suspect hang) |
| `internal-error` | unexpected SQL or Go-side error | escalate to mayor |

The CLI exit code is bound at three values per OQ-5's resolution:

- `0` — full success **or** no-op (zero candidates from auto-discovery;
  every named DB below `--threshold`; every per-DB outcome in
  {`ok`, `below-threshold`, `database-locked` (skipped)}).
- `1` — at least one per-DB outcome in the escalate set
  ({`concurrent-write-fatal`, `integrity-mismatch`, `internal-error`,
  `database-deadline-exceeded`}).
- `2` — invocation error before per-DB execution begins (bad flag, port
  unresolved, identifier failed charset check, explicit DB list with
  zero matches in rig-registry).

Cleanup parity (cleanup's `0` covers no-op runs); the dog's
step-closure code reads the exit code only as a coarse signal
alongside the envelope's `phases` and per-DB outcomes (which carry
the precise breakdown), so finer-grained exit codes would duplicate
envelope fields without informing dog action.

### Threshold Semantics (resolves PRD Q14)

`--threshold` gates **every** candidate database, regardless of
discovery source. A database named explicitly (positional or
`--databases` list) that does not exceed the threshold is reported
with `outcome = below-threshold` and skipped, identical to an
auto-discovered DB below threshold. Operators wanting unconditional
compaction of a named database pass `--threshold=0`.

Rationale: the formula's `commit_threshold` variable is a single rule
that applies uniformly; per-DB exceptions would split the rule along
discovery-source lines and complicate dog-side per-step closure
(both an explicit-named and auto-discovered DB hitting `--threshold`
must close the `compact` step the same way, namely "skipped"). The
human output line is identical for both:

```
gascity: 234 commits → below threshold, skip
```

A future v2 may expose `--force-named` or per-DB threshold overrides
if a concrete operator workflow demands them; v1 keeps the rule
uniform.

### Trust and Discovery

Auto-discovery uses `gc rig list --json` exclusively. **Never
`SHOW DATABASES`** against the live server. Operators wanting an
unregistered DB pass it positionally; the CLI accepts it but flags
it in the envelope as `discovery_source: "explicit"`. The integration
test asserts no `SHOW DATABASES` is ever issued.

Database identifiers go through the cleanup-style charset validator
(`[A-Za-z0-9_-]`, leading char `[A-Za-z0-9_]`) before any SQL is
issued. Validation failures surface as `invalid-identifier`.

## Key Components

### 1. Cobra command — `cmd/gc/cmd_dolt_compact.go`

Top-level `dolt-compact` command. Flag wiring, port resolution
delegation, output mode selection (human vs JSON), top-level error
handling. Body delegates to per-database orchestration.

### 2. Inspector — `cmd/gc/dolt_compact_inspect.go`

Database discovery (rig registry parse + positional merge) + commit
count probe (`SELECT COUNT(*) FROM (SELECT 1 FROM dolt_log LIMIT
10000) AS t`). Returns the candidate list with per-DB commit counts
and threshold status.

### 3. Flatten executor — `cmd/gc/dolt_compact_flatten.go`

Pure algorithm core. Takes a `*sql.DB` and a database name. Runs
the flatten sequence end-to-end. Returns a per-DB result struct.
No I/O on Cobra; testable with a `*sql.DB` mock or a real
test-Dolt connection.

### 4. Surgical executor — `cmd/gc/dolt_compact_surgical.go`

Same shape as flatten. Implements the rebase plan, the retry-on-
collision (with `2s` pause), and the branch-cleanup ordering
(lock first, then drop leftovers).

### 5. Verifier — `cmd/gc/dolt_compact_verify.go`

Pre/post row count snapshot + comparison. Surfaces a typed result
that the executors consume to decide `ok` vs `integrity-mismatch`.
Excludes `dolt_*` system tables. On `integrity-mismatch`, returns
both the comparison detail and the `pre_hash` captured at entry so
the calling executor can issue `DOLT_RESET --hard pre_hash` (flatten)
or drop the working branches without swapping (surgical) to leave
the database in its pre-compaction state.

### 6. Validator — `cmd/gc/dolt_compact_validate.go`

Identifier charset validation discharging PRD Constraint 8 ("no SQL
injection"). Charset matches the existing pack-script rule:
`[A-Za-z0-9_-]` body, leading char `[A-Za-z0-9_]`. Runs before any
SQL is issued for a given database identifier; failures surface as
`outcome = invalid-identifier` (Error Class Taxonomy). Per R-6, the
validator is factored as a single shared helper with `gc dolt-cleanup`
to prevent drift; if cleanup's validator hasn't been exported the
factoring happens in PR2 against both call sites at once. Unit test
covers the boundary cases (empty, leading-digit, hyphen-leading,
non-ASCII).

### 7. Envelope — `cmd/gc/dolt_compact_envelope.go`

Defines `CompactReport`, `CompactDatabaseResult`,
`CompactRowCountSnapshot`, `CompactSummary`, `CompactError`. Pins
`schema = "gc.dolt.compact.v1"`. Marshals in the human path's
final-line print and in the `--json` path's `json.Marshal` call.

### 8. Executor-binding regression test (PR1) — `cmd/gc/embedded_formula_executor_test.go`

Walks every embedded formula. For each step:

1. If it has `executor != ""` and `zfc_exempt == true`, accept.
2. Otherwise, attempt to resolve the step's intended executor. The
   test inspects:
   - the root Cobra command tree for a command named after the
     step's `id` or matching a heuristic the test documents;
   - pack scripts under `examples/*/commands/<id>/run.sh`;
   - any other registered binding the formula package recognizes.
3. If neither resolves, fail with `formula <name> step <id>: no
   executor binding and no exemption marker`.

Failure messages include the path to the formula and the step ID
so operators can fix without grep. The test runs in the standard
unit-test set, not a separate tier.

The exact resolution heuristic is a PR1 decision; the test must be
strict enough to catch the orphan-formula failure mode but lenient
enough that an existing well-behaved step doesn't need annotation.
Recommended: a step's executor is "resolvable" if its
description or step ID names a `gc <command>` invocation that exists
in the Cobra tree, OR if a pack script exists at a documented path.
Steps without a resolvable executor must declare `zfc_exempt = true`
+ `executor = "..."` to pass.

### 9. Pack delegate — `examples/dolt/commands/compact/`

`command.toml` describes the operator-facing command. `run.sh`
sources `runtime.sh` and execs `gc dolt-compact "$@"`. Treats `$@`
verbatim to avoid the `gc dolt sql` arg-drop bug (#1485).

### 10. Formula update (PR2) — `examples/dolt/formulas/mol-dog-compactor.toml`

Drops `zfc_exempt`, adds `executor = "gc dolt-compact"`, updates
each step's description to reference the CLI flags using formula
variable substitution.

## Interface

### CLI surface

```
gc dolt-compact [databases...] [flags]

Auto-discovers production databases via `gc rig list --json` when no
positional databases and no --databases value is given. Compacts
each candidate whose commit count exceeds --threshold. Emits a
human-readable progress report by default; pass --json for the
gc.dolt.compact.v1 envelope on stdout.

Flags:
  --mode flatten|surgical    Compaction algorithm (default: flatten)
  --threshold N              Commit count threshold (default: 500)
  --keep-recent N            Surgical mode: recent commits to preserve (default: 50)
  --databases CSV            Database list (alt to positional args; empty = auto-discover)
  --port N                   Override Dolt port resolution (mirrors dolt-cleanup)
  --per-db-timeout DURATION  Per-DB soft timeout (default: 1h)
  --json                     Emit gc.dolt.compact.v1 JSON envelope on stdout
```

Pack delegate:

```
gc dolt compact [databases...] [flags]
```

Forwards `"$@"` to `gc dolt-compact`.

### JSON envelope (`gc.dolt.compact.v1`)

```json
{
  "schema": "gc.dolt.compact.v1",
  "invocation": {
    "mode": "flatten",
    "threshold": 500,
    "keep_recent": 50,
    "databases_arg": "",
    "per_db_timeout_ms": 3600000
  },
  "port": {
    "resolved": 3307,
    "source": "city-config",
    "fallback": false
  },
  "phases": {
    "inspect": "ok",
    "compact": "ok",
    "verify": "ok",
    "report": "ok"
  },
  "databases": [
    {
      "name": "hq",
      "discovery_source": "rig-registry",
      "pre_hash": "abc123…",
      "pre_commits": 1611,
      "post_commits": 1,
      "outcome": "ok",
      "mode": "flatten",
      "attempts": 1,
      "pre_row_counts": {"issues": 12834, "deps": 4221, ...},
      "post_row_counts": {"issues": 12834, "deps": 4221, ...},
      "gc_reclaimed_bytes": 1287340032,
      "duration_ms": 4521
    }
  ],
  "summary": {
    "databases_inspected": 4,
    "compacted": 1,
    "skipped": 3,
    "failed": 0,
    "total_duration_ms": 5102,
    "gc_reclaimed_bytes": 1287340032
  },
  "errors": []
}
```

The `errors` array carries invocation-level errors (port unresolved,
rig-list failure). Per-database errors live on the database record's
`outcome` and an optional `error_message` field. The `pre_hash` field
captures the DB's HEAD commit hash on entry so an operator can
manually recover (`DOLT_RESET --hard pre_hash`) if the in-CLI
rollback path ever fails.

#### Per-step bead mapping (resolves Goal 6)

The `phases` block exists so the dog can close
`inspect` / `compact` / `verify` / `report` step beads from a single
envelope read without re-deriving status from per-DB outcomes. Each
phase value is one of:

| Phase value | Meaning | Dog action |
|---|---|---|
| `ok` | every DB cleared this phase | close step `closed` |
| `partial` | some DBs cleared, others reported `below-threshold` / `database-locked` (skipped) but no escalations | close step `closed` (the per-DB array carries the detail) |
| `escalate` | at least one DB ended in `concurrent-write-fatal`, `integrity-mismatch`, `database-deadline-exceeded`, or `internal-error` | close step `escalated`; escalate to mayor with the offending DB names |
| `failed` | a phase-level prerequisite failed (e.g., rig-list error blocked `inspect`; invocation-level error blocked everything) | close step `failed`; deacon nudge |

Aggregation rules (CLI-side, deterministic):

- `inspect.value`: `ok` if every candidate's commit-count probe
  succeeded; `partial` if all DBs were `below-threshold` (no
  candidates); `failed` if rig-list / port resolution / the probe
  itself errored before any per-DB outcome could be recorded.
- `compact.value`: `ok` if every candidate compacted; `partial` if
  any was `below-threshold` / `database-locked` (skipped) but none
  escalated; `escalate` if any per-DB outcome is in the escalate
  set; `failed` if invocation-level errors prevented compaction.
- `verify.value`: `ok` if every compacted DB's verifier passed;
  `escalate` on any `integrity-mismatch`; `partial` only when no DB
  was eligible to verify (all skipped); `failed` mirrors `compact`.
- `report.value`: always `ok` if the envelope was emitted (the dog's
  ability to read the envelope is itself the report).

The dog's step-closure code reads `phases.<step>` and maps to the
table above. The escalate-class set used by the dog matches the
"escalate to mayor" rows in the Error Class Taxonomy table — so the
`phases` block is a faithful aggregate, not a new contract. The
envelope itself is produced by the `compact` step's CLI invocation
per Step-execution contract; the dog reads the cached envelope when
it claims the `inspect`, `verify`, and `report` bookkeeping step
beads.

### Default human output

Mirrors PRD Story 1:

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

Auto-discovery and explicit DBs receive the same threshold treatment
(see "Threshold Semantics" — Q14):

```
$ gc dolt compact hq gascity --threshold=500
inspect: hq @ 1611 commits (threshold 500) → candidate
inspect: gascity @ 234 commits → below threshold, skip
compact: hq → flatten in progress…
  …
report: 2 dbs inspected, 1 compacted, 1 skipped, 0 failed
```

Glyphs (`✓ ⚠ ✖`) for terminal-agnostic status indication, matching
cleanup.

## Data Model

### Formula schema additions (PR1)

`internal/formula/types.go:Step` gains two optional fields:

```go
type Step struct {
    // ... existing fields ...

    // Executor names the concrete binding that runs this step (e.g.,
    // "gc dolt-compact"). Used by the executor-binding regression
    // test to satisfy the contract for steps that cannot be resolved
    // by command-tree heuristics, and to declare the binding for
    // steps that pair with ZfcExempt.
    Executor string `json:"executor,omitempty" toml:"executor,omitempty"`

    // ZfcExempt declares this step as Zero Framework Cognition
    // (ZFC) exempt — i.e., an agent cannot execute it via shell
    // pipelines and a non-agent executor named in Executor runs
    // it instead. Pairs with Executor; either both are set or
    // neither. Setting ZfcExempt without Executor is a contract
    // violation surfaced by the executor-binding regression test.
    ZfcExempt bool `json:"zfc_exempt,omitempty" toml:"zfc_exempt,omitempty"`
}
```

Both fields are zero-value-safe so existing formulas serialize
identically.

### Envelope schema (PR2)

Defined in `cmd/gc/dolt_compact_envelope.go`. Pinned at
`gc.dolt.compact.v1` from day 1, additive evolution only. The shape
is forwards-compatible — empty arrays render as `[]`, omitempty
fields don't appear when zero. Field order mirrors cleanup's
ordering convention (`schema`, then `invocation` / `port`, then the
top-level `phases` aggregate, then per-database results, `summary`,
`errors`). The two PRD-discharge-driven fields are explicit:
`phases.{inspect,compact,verify,report}` (per-step closure aggregate
for the dog) and `databases[].pre_hash` (operator-recovery handle
when in-CLI rollback fails).

### Storage and migrations

- Beads, mail, the Dolt commit graph: unchanged.
- TOML formula files: 1 file mutated in PR1
  (`mol-dog-compactor.toml` adds `zfc_exempt = true`); the migration
  sweep may surface a small number of additional formulas needing
  the same treatment. PR2 mutates `mol-dog-compactor.toml` again
  (drops `zfc_exempt`, adds `executor`).
- No bead-store or Dolt-database schema changes.

## Trade-offs and Decisions

### TD-1. PR1/PR2 split (locked by Q2)

Splitting concentrates the broad-scope, low-risk schema work in
PR1 and the narrow-scope, behavioral work in PR2. PR1 enforces the
contract before any new executor lands; PR2 lands against an
already-enforced contract. Trade-off: two PRs, two reviews. Win:
the regression test cannot be defeated by the same PR introducing
the new executor.

### TD-2. Single final JSON envelope (locked by Q1)

Streamed NDJSON would carry per-phase events as they happen, useful
for very-long-running compactions. Trade-offs: no current consumer
needs streaming; sidecar files add I/O coupling. Decision: single
final envelope, mirroring `gc.dolt.cleanup.v1`. Future streaming
is additive (a new `--ndjson` flag could ship in v2 if a consumer
emerges).

### TD-3. Auto-discovery via `gc rig list --json` only (locked by Q4)

`SHOW DATABASES` would also work and would catch databases the rig
registry forgot. Trade-off: it would also catch test/orphan
databases — a footgun for compaction. Decision: rig registry only.
Operators who want an unregistered DB pass it positionally.
Integration test asserts no `SHOW DATABASES` is ever issued.

### TD-4. Lock granularity — city-wide flock (MVP)

A per-database advisory lock would allow concurrent compactions
against different DBs. The existing `dolt_lifecycle_lock.go` is a
city-wide managed-Dolt-lifecycle flock. Trade-off: city-wide
serializes all compaction across all DBs. At current city scale
(<5 compactor-target DBs) this cost is invisible — cycles run
sequentially anyway. Decision: reuse the city-wide lock for MVP;
introduce per-DB locking only when measured pain emerges.

### TD-5. Per-DB soft timeout

Compactor cycles can run for hours when `dolt_gc` is doing real
work. A hung DB shouldn't block the cycle indefinitely. Trade-off:
introducing a deadline means a DB that legitimately needs more time
than the default gets cut off and reported as
`database-deadline-exceeded`. Decision: 1h default, exposed as
`--per-db-timeout`. Documented as "tune up if your DB legitimately
needs longer." Not exposed as a formula var until a city actually
hits the limit.

`--per-db-timeout` is a **wrapper-level execution budget** enforced
by `context.WithTimeout` around each per-DB invocation; it is **not
a parameter of the flatten or surgical algorithms**, which continue
to consume the four formula-bound vars (`commit_threshold`,
`keep_recent`, `mode`, `databases`) verbatim. PRD Non-Goal 7 ("no
new modes, no parameter tuning") addresses algorithm internals; the
deadline is orthogonal — it bounds wall-clock cost in the
orchestration layer without changing flatten or surgical semantics.
If a city later wants the deadline to live in the formula (e.g., to
tune per-formula instead of per-binary), the addition is additive
(new formula var, new CLI flag default sourced from the var, no
algorithm change). This framing also explains why
`--per-db-timeout` is intentionally absent from the formula/CLI
defaults consistency test (Phase 2 step 10): it has no formula-bound
counterpart.

### TD-6. Error class taxonomy — 9 classes (resolves PRD Q5)

Fewer classes (3-4) lose useful operational distinctions; more
(15+) generate noise without action differentiation. Decision: 9
classes that map to four dog actions (close-ok, close-skipped,
escalate, deacon-nudge). Stable strings, additive evolution.

### TD-7. Default output is human-readable; `--json` opts in

Cleanup's pattern. Trade-off: dog scripts must remember to pass
`--json`. Mitigation: the formula's step-description prose
references `--json`, so a human reading the formula sees the dog's
exact invocation. Decision: don't auto-detect TTY for output mode
(hidden mode switches confuse).

### TD-8. Per-DB `dolt_gc` (resolves PRD Q10)

Aggregating `dolt_gc` to one call after all compactions reduces
total wall time but expands blast radius — one gc failure makes
all compaction invisible. Decision: per-DB gc, accept the cost,
let `--per-db-timeout` provide the safety valve.

### TD-9. CLI flips `mol-dog-compactor`'s exemption (resolves PRD Q15)

Default assumption confirmed. PR2 drops `zfc_exempt = true` on
compactor steps and sets `executor = "gc dolt-compact"`. Daemon
coexistence is rejected as a Non-Goal.

### TD-10. Defer `--inspect-only`, `--dry-run`, event-bus emission, NDJSON to v2

PRD review's Important-But-Non-Blocking section. None are needed
for the silent-failure recovery. Adding any of them now is gold-
plating. v2 backlog if and when consumers emerge.

PRD Story 3 (operator debugging via `gc dolt compact --inspect-only`)
is partially deferred along with this trade-off: v1 operators inspect
by running the full CLI with `--json` and reading the per-DB
`outcome`/`pre_commits` fields, or by running the human-readable mode
and reading the `inspect:` lines (which already preview `→ candidate`
vs `→ below threshold, skip` per DB without compacting yet at that
point in the pipeline). When a real `--inspect-only` consumer
emerges in v2, it composes onto the existing inspector phase.

## Risks and Mitigations

### R-1. Migration sweep (PR1) misses an exemption-needing formula

The executor-binding test would fail on first run. Mitigation:
PR1's audit is mechanical (walk every formula step, confirm
resolution). If the test surfaces an unanticipated case, fix in
PR1 before merging — the test failure tells the developer exactly
which formula+step needs annotating.

### R-2. Existing well-formed formulas cannot resolve cleanly via the regression test heuristic

The test's resolution heuristic must accept "this step's prose
describes shell commands an agent runs" as a valid binding. Risk:
the heuristic is too strict and demands annotation on legitimately
agent-resolvable steps. Mitigation: the heuristic is part of PR1
and can be tuned during review. Conservative default: a step's
description starting with a fenced shell block of `gc ...` /
standard-Unix invocations is "agent-resolvable." Anything else
must declare `executor` + `zfc_exempt = true`.

### R-3. Concurrent-write race during surgical compaction

Surgical mode is documented as not-safe with concurrent writes.
Mitigation: 2s retry on graph-change errors (matches formula).
On retry failure, escalate as `concurrent-write-fatal` **and halt
before the branch swap so `main` stays at `pre_hash`** — the
`compact-*` working branches are dropped and the database is left
exactly as it was on entry (pre-compaction state, per the PRD
constraint). Operators are told to use `--mode=flatten` if writes
are continuously busy.

### R-4. `dolt_gc` runs longer than `--per-db-timeout`

Soft timeout cuts off the gc; the CLI surfaces
`database-deadline-exceeded`. Compaction itself is already
committed at this point — the gc just didn't finish. Mitigation:
the DB's commit graph is correct; only the disk reclaim is
incomplete. Document that the next cycle's gc will reclaim what
this one missed.

### R-5. Pack delegate `run.sh` drops args (#1485 regression class)

The `gc dolt sql` regression where `run.sh` failed to forward `$@`
verbatim is a known footgun. Mitigation: copy the cleanup `run.sh`
pattern; add a parallel arg-forwarding regression test
(`compact_test.go`) so the same gap can't recur.

### R-6. Charset validator drift between cleanup and compact

If the validators diverge, an identifier accepted by one and
rejected by the other becomes operator confusion. Mitigation:
factor the validator into a single shared helper; cover with
unit tests in both packages.

### R-7. Lock-file leaks block all compaction

A bug in lock release would prevent any subsequent compaction.
Mitigation: standard flock semantics (kernel releases on process
exit). Add an integration test that runs two compact processes
back-to-back and asserts the second acquires the lock.

### R-8. Integration test flakes on CI

Spinning up a real Dolt sql-server in tests has been a source of
flakes. Mitigation: reuse `examples/dolt/sql_test.go` patterns
(temp ports, fixture data, deterministic teardown). Tag
integration tests with `//go:build integration` so the unit suite
stays fast.

### R-9. Cycle re-fails post-deploy

Post-PR2, the next cycle should compact something on a busy city.
If it doesn't (still safely-skipping) the formula update missed a
spot. Mitigation: the executor-binding test in PR1 would have
caught a disconnect; the **formula→CLI→envelope→step-close
integration test** in PR2 (Implementation Plan Phase 2 step 11)
exercises the executor binding without waiting on the 24h cooldown.
That same integration test loads `mol-dog-compactor.toml`, resolves
the `compact` step's executor, invokes the CLI exactly once, and
asserts all four `phases.<step>` values plus per-DB outcomes drive
correct step-closure decisions — directly exercising the per-cycle
invocation rule from Step-execution contract. Manual smoke after PR2
("trigger a cycle on loomington, watch the report bead") covers the
remaining 24h-cooldown surface that's operator-gated.

### R-10. In-CLI rollback fails on integrity-mismatch

The flatten path's `DOLT_RESET --hard pre_hash` could itself fail
(connection lost mid-rollback, Dolt server crash). The rolled-forward
flatten commit would then remain reachable until the next
`dolt_gc` runs. Mitigation: (1) the per-DB envelope record carries
`pre_hash` so an operator can manually re-issue
`DOLT_RESET --hard <pre_hash>` from `gc dolt sql`; (2) `dolt_gc` is
deliberately skipped on the mismatch path so the chunks remain on
disk; (3) the engdocs recovery section documents the manual
procedure. The surgical path has no equivalent risk: nothing is
swapped on mismatch, so there is no rollback to fail.

### R-11. Formula vars drift from CLI defaults

A future PR could change `commit_threshold` in the formula or the
`--threshold` default in the CLI without touching the other side,
silently breaking Goal 2's "single source of truth." Mitigation:
the formula/CLI defaults consistency unit test in Phase 2 step 10
loads the formula and asserts every default matches; CI fails on
drift. The test must be updated whenever a new formula var is
introduced; the test name and failure message tell the developer
exactly which side is stale.

## Operations

### Manual Recovery (resolves PRD Q5 / Q11 clarification)

The PRD Q5 clarification mandates that the design doc document the
manual recovery procedure for operators. Auto-recovery is implicit in
the lock-then-cleanup ordering (Algorithms / Surgical) and in flatten's
in-CLI rollback (Algorithms / Flatten). The four sub-cases below
consolidate operator-facing recovery for the failure modes the design
admits. The longer-form operator guide lives in
`engdocs/contributors/dolt-compact.md` (PR2 deliverable); this section
is the auditable summary that plan-review can verify against.

#### MR-1. Crash mid-flatten with successful in-CLI rollback

No operator action required. Symptom: the last envelope contains a
per-DB record with `outcome = integrity-mismatch`, `pre_hash` set,
and `gc_reclaimed_bytes = 0` (because `dolt_gc` is intentionally
skipped on the mismatch path). The DB's HEAD has been restored to
`pre_hash` by `DOLT_RESET --hard`. The next cycle re-inspects the DB
and, if commit count still exceeds threshold, retries; the next
successful cycle's `dolt_gc` reclaims chunks.

#### MR-2. Crash mid-flatten with failed in-CLI rollback (R-10)

The flatten path's `DOLT_RESET --hard pre_hash` itself failed
(connection lost mid-rollback, Dolt server crash). The new flatten
commit is still reachable until the next `dolt_gc` runs.

Operator procedure:

1. Read `pre_hash` from the most recent envelope's per-DB record (or
   from the run log).
2. Connect to the affected DB:
   `gc dolt sql --database <db>`.
3. Re-issue the rollback:
   `CALL DOLT_RESET('--hard', '<pre_hash>');`
4. Verify HEAD: `SELECT @@<db>_head;` should equal `pre_hash`.
5. Confirm row counts match the pre-compaction state.
6. The next compactor cycle will re-attempt and on success run
   `dolt_gc` to reclaim the abandoned flatten commit's chunks.

#### MR-3. Crash mid-surgical (any phase before swap)

`compact-base` and `compact-work` branches may be left behind; `main`
is untouched (per the halt-before-swap rule). The next CLI invocation
acquires the lock and drops the orphans automatically (Algorithms /
Surgical line "(Q5) drop leftover compact-base / compact-work
branches").

If a manual cleanup is preferred (e.g., the operator wants to inspect
the orphaned branches first), the procedure is:

1. Acquire the lifecycle lock by running the CLI with no DBs to
   compact (e.g., `gc dolt-compact --threshold=999999 <db>` to skip
   real work while still going through the lock-then-cleanup path),
   OR, for direct cleanup:
2. `gc dolt sql --database <db>` and `CALL
   DOLT_BRANCH('--delete', '--force', 'compact-base');` then the same
   for `compact-work`. Note that without the lifecycle lock, this is
   only safe when no other compactor is running on the same DB —
   prefer the CLI-driven path above when available.

#### MR-4. Stuck advisory lock (pathological)

Standard flock semantics release the lock on process exit (kernel-
enforced). If a stuck-lock case persists across compactor processes
exiting, this represents a bug in `dolt_lifecycle_lock.go` that
should be escalated, not worked around. The investigation step is:

1. `gc dolt sql -q "SELECT IS_FREE_LOCK('dolt_lifecycle')"`.
2. If `0` (held), identify the holding connection via
   `SHOW FULL PROCESSLIST`.
3. **Escalate to mayor before manually releasing.** The lock exists
   to serialize compaction across all DBs; releasing it under a
   running compactor is a recipe for the very integrity-mismatch the
   design's other recovery paths handle.

Operators should not need to release the lock manually under normal
operation. Documenting the path is for completeness; the standard
fix for an apparently stuck lock is to wait one cycle for the
holding process to exit, then verify.

## Implementation Plan

### Phase 1 — PR1: schema + regression test (broad scope)

1. **Add fields to `formula.Step`.** Doc-comment the field semantics
   inline; explicitly spell out "Zero Framework Cognition (ZFC)".
2. **Wire fields through cooking.** Audit every `Step{}` literal
   in `internal/formula/` for forgot-to-copy paths; update.
3. **Audit embedded formulas.** Walk `examples/*/formulas/*.toml`,
   identify steps that need `zfc_exempt = true`. Apply tags.
4. **Land the regression test.** Walk all embedded formulas; assert
   each step is either resolvable or annotated. Test failure
   names the exact formula+step.
5. **Migrate `mol-dog-compactor.toml`** to `zfc_exempt = true`
   matching its current prose. **No forward refs to
   `gc dolt-compact`**.
6. **Open PR1.** Quality gates: `make test-fast-parallel`,
   `go vet`, dashboard check (if API surface touched — likely
   not), `make test-cmd-gc-process-parallel`.

### Phase 2 — PR2: CLI + envelope + formula update (narrow scope)

1. **Skeleton.** New files in `cmd/gc/`. Cobra registration in
   `main.go`.
2. **Inspector.** Database discovery (rig-registry parse) +
   commit-count probe.
3. **Validator.** Identifier charset (factor or copy from cleanup).
4. **Flatten executor.** Algorithm port verbatim from formula
   prose. Per-DB context budgeting via `context.WithTimeout`.
5. **Surgical executor.** Algorithm port. Lock-then-cleanup
   ordering (Q5). Retry-on-collision semantics.
6. **Verifier.** Pre/post row count snapshot + comparison.
7. **Envelope.** Define structs; pin schema string.
8. **Cobra command body.** Parse flags, resolve port, run inspector,
   iterate candidates, dispatch to flatten/surgical, render output.
9. **Pack delegate.** `command.toml` + `run.sh`. Arg-forwarding
   regression test.
10. **Unit tests.** Charset validator, envelope shape (including the
    `phases` aggregation rules), mode selection, error-class mapping,
    retry semantics (mocked SQL), threshold-on-explicit-DB skip
    (Q14), and a **formula/CLI defaults consistency test** that
    loads `examples/dolt/formulas/mol-dog-compactor.toml`, extracts
    the `commit_threshold`, `keep_recent`, `mode`, and `databases`
    formula vars, and asserts the CLI flag defaults match.
    (`--per-db-timeout` is intentionally not in scope; per TD-5 it
    is a wrapper-level execution budget, not a formula-bound
    algorithm parameter — when a city later wires it into the
    formula, the test should be extended at the same time.) This
    test discharges Goal 2's "no hardcoded values that could drift
    from the formula" without relying on review vigilance.
11. **Integration tests.** Real Dolt sql-server. Populate, flatten,
    verify post-state. Populate, surgical, verify post-state. Inject
    concurrent write between pre-flight and rebase, verify retry.
    Run two CLIs back-to-back, verify lock works. Verify no `SHOW
    DATABASES` is issued. **Inject a row-count divergence between
    pre and post snapshots and assert flatten rolls back to
    `pre_hash` (HEAD restored, no orphaned commit visible) and
    surgical halts before swap (main untouched, `compact-*`
    branches dropped).** **Drive the executor binding via the
    formula:** load `mol-dog-compactor.toml`, resolve the step's
    `executor`, exec it with `--json`, and assert the parsed
    `phases` block plus per-DB outcomes drive correct step-closure
    decisions. This last test discharges Acceptance 2's
    formula→CLI→envelope→step-close path; the 24h-cooldown leg
    remains operator-gated post-deploy verification (R-9).
12. **Formula update.** `mol-dog-compactor.toml` adds
    `executor = "gc dolt-compact"` on every step; drops `zfc_exempt`
    on the `compact` step (the trigger) and retains `zfc_exempt = true`
    on the `inspect` / `verify` / `report` bookkeeping steps. Only the
    `compact` step's description carries the full CLI invocation
    (formula-var-substituted flags); the bookkeeping steps reference
    the cached-envelope closure path. Per Step-execution contract.
13. **Operator/dev guide.** `engdocs/contributors/dolt-compact.md`.
14. **Open PR2.** Quality gates as PR1 plus integration shard
    (`make test-integration-shards-parallel`).

### Phase 3 — Post-deploy verification

1. After PR2 lands, watch the next 24h cycle (or trigger via
   operator-side dispatch).
2. Verify the report bead shows non-zero compactions.
3. Verify HQ commit count drops.
4. Confirm the deacon report no longer flags compactor cycles as
   safely-skip.

## Open Questions

PRD Open Question Q14 ("threshold semantics on explicit DBs") was
resolved in prd-align round 1 — see "Threshold Semantics" above.
PRD-alignment round 3 promoted OQ-1, OQ-4, OQ-5, and OQ-7 from
recommendations to bound resolutions (consolidated under
"Resolutions" below). OQ-2, OQ-3, and OQ-6 remain appropriately
deferred to the next phase that resolves them naturally.

### Resolutions

#### OQ-1 (resolved). Free-form `executor` field for PR1

The `executor` field is a **free-form string** in PR1 (e.g.,
`"gc dolt-compact"`). Rationale: every binding the design
contemplates today (Cobra commands, pack scripts) is identifiable by
a single string token, and the executor-binding regression test
(Component 8) consumes the string directly. A future structured-tag
form (`{ kind, command }`) is additive — a new optional field on the
same `Step` struct can carry the structured form without breaking
PR1's schema. The structured form is deferred to v2 and will be
revisited only if a non-CLI binding emerges (e.g., a Go-internal
callback registry). PR1 step 1 in the Implementation Plan reflects
this binding in the field-type spec.

#### OQ-4 (resolved). `--per-db-timeout` lives on the CLI surface only for v1

`--per-db-timeout` defaults to `1h` and lives on the CLI surface
only; **the formula does not expose a `per_db_timeout` var in v1.**
Per TD-5, `--per-db-timeout` is a wrapper-level execution budget,
not a formula-bound algorithm parameter; binding it into the formula
requires either a corresponding consistency-test extension (Phase 2
step 10) or a deliberate exclusion. **Trigger for promotion to a
formula var:** at least one city's `database-deadline-exceeded`
outcomes become a recurring patrol signal (a single timeout is
operator-tunable from the CLI; persistent timeouts across cycles
indicate the default is wrong for that city). When promoted, the
consistency test (Phase 2 step 10) extends to cover the new var;
TD-5's "additive evolution" paragraph already describes the
migration path.

#### OQ-5 (resolved). Exit code precision

Exit codes are bound at three values (table inlined into the Error
Class Taxonomy paragraph above). Cleanup parity drives the no-op-as-`0`
choice (cleanup's `0` covers no-op runs). Story-2 corollary: when
auto-discovery returns empty (`--databases` empty and rig-registry
returns no candidates), the CLI emits a well-formed envelope with
`summary.databases_inspected = 0` and exits `0`; the dog closes all
four step beads `closed`. The dog's step-closure code reads the exit
code only as a coarse signal alongside the envelope's `phases` and
per-DB outcomes — finer-grained exit codes would duplicate envelope
fields without informing dog action.

#### OQ-7 (resolved). ZFC expansion is "Zero Framework Cognition"

"ZFC" expands to **Zero Framework Cognition** per AGENTS.md. The
expansion is pinned in three places: (1) the `ZfcExempt` field's
doc-comment in Data Model / Formula schema additions; (2) the PR1
step 1 implementation spec; (3) the engdocs operator guide
(`engdocs/contributors/dolt-compact.md`, Phase 2 step 13). No further
action required for the design contract.

### Remaining open questions

#### OQ-2. Regression-test resolution heuristic (Risk R-2)

The exact rule for "this step is resolvable to an executable
without annotation" is the most consequential PR1 detail. Drafted
above as "step description starts with a `gc <command>` invocation
or standard-Unix command." Plan-review-1 should pressure-test this
heuristic against the actual embedded formulas to ensure it
accepts well-formed steps and rejects orphans.

#### OQ-3. Where the regression test lives

`cmd/gc/embedded_formula_executor_test.go` (recommended for ergonomic
access to the Cobra command tree) vs `internal/formula/`
(formula-package access ergonomics). Chooseable in PR1.

#### OQ-6. Pack-delegate flag-passing for `--port`

Cleanup's `run.sh` reads `GC_DOLT_PORT` and forwards as a flag if
set. Compact should do the same. Confirm in PR2 review.
