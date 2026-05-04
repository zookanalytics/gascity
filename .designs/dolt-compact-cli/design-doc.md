# Design: Implement `gc dolt compact` CLI subcommand to provide an executor for mol-dog-compactor

The compactor formula declares itself ZFC-exempt (daemon-only executor) but
is dispatched to the gastown.dog pool every 24h. Dogs read the exemption
and safely-skip — no compaction occurs. Upstream issue
gastownhall/gascity#1557 enumerates three fix options; option 3 (CLI
subcommand) is the decided long-term answer. Implement it.

Scope: a new `gc dolt compact [databases...]` subcommand that:

- Honors existing formula variables: --mode (flatten|surgical),
  --threshold (commit count, default 500), --keep-recent (surgical, default 50),
  --databases (comma list, empty = auto-discover)
- Implements both flatten and surgical algorithms exactly as described in
  the existing formula
- Performs integrity verification (pre/post row counts, dolt_gc post-step,
  error classification)
- Emits structured output the dog can read for per-step closure
  (inspect/compact/verify/report)
- Has unit + integration tests
- Updates mol-dog-compactor formula and order to invoke the CLI and lift
  the ZFC exemption
- Adds an executor-binding test so this orphan-formula regression cannot recur

Acceptance:

- `gc dolt compact hq --mode=flatten` runs end-to-end from a shell
- mol-dog-compactor cycle runs end-to-end: dog claims, shells out,
  compaction actually occurs, integrity verified, dolt_gc runs, report sent
- Test verifies the formula's executor binding (no orphan formula refs)

---

## Executive Summary

`gc dolt compact` is built as a near-clone of `gc dolt-cleanup` to lift
the existing ZFC exemption on `mol-dog-compactor`. The work ships in **two
sequenced PRs**, locked by the human-clarification round:

- **PR1 — Executor-binding contract.** Adds `executor` and `zfc_exempt`
  fields to `formula.Step`, parser support, an exhaustive migration audit
  of all embedded formulas, an `engdocs/contributors/executor-binding.md`
  reference, and a regression test (`cmd/gc/executor_binding_test.go`)
  that walks every embedded formula's steps and asserts each is either
  agent-cookable or has a valid executor binding. PR1 ships before PR2 so
  the contract is enforced on day 1 — orphan-formula regressions cannot
  recur.
- **PR2 — CLI + dog-formula update.** Lands `gc dolt-compact` (Go) and
  the `gc dolt compact` pack delegate, the `gc.dolt.compact.v1` JSON
  envelope, integration tests against a real Dolt sql-server, operator
  docs, and the `mol-dog-compactor` formula update that flips
  `zfc_exempt = true` to `executor = "gc dolt compact"`. The cycle is
  live the next 24h after merge.

The CLI mirrors `gc dolt-cleanup` for every shared concern: port
resolution, identifier validation, lifecycle locking, dual CLI surface
(top-level Go + pack delegate), JSON envelope schema versioning, and the
auto-discovery path through `gc rig list --json`. Divergences are
documented inline. The largest novel piece is the executor-binding
contract itself (PR1) — small in code, large in long-term value: it
turns the orphan-formula failure mode into a build-time error.

Confidence: **high.** No fundamental blockers; clear precedent for every
load-bearing decision; the design-exploration round closed with no
re-opened questions from the human-clarification round.

---

## Problem Statement

`mol-dog-compactor` is a formula that defines four steps — inspect,
compact, verify, report — describing how to compact Dolt commit history.
Its prose declares the formula "ZFC-exempt" because compaction needs SQL
connections, multi-statement transactional state, branch creation/cleanup,
concurrent-write retry, and pre/post integrity verification. None of
these can be cooked into an agent prompt.

The formula is dispatched to the `gastown.dog` pool every 24h via the
order `examples/dolt/orders/mol-dog-compactor.toml`. The intent: a
hypothetical compactor daemon would see the molecule and execute the
steps for observability tracking.

**The bug:** no daemon exists. Dogs claim the molecule and have no
executor binding to run. The current code path produces a "safely-skip"
outcome that closes the cycle as green at the bead level while doing
nothing. Local evidence (loomington, deacon patrol 2026-05-03):

- `lx-cpzqmh` — compact step safely-skipped
- `lx-w466x4` — verify vacuous, 0 dbs compacted
- `lx-ot7tih` — report: 4 inspected, 0 compacted

HQ commit count grew 1455 → 1611 over ~2 days post-`dolt_ignore` fix.
Below the 50k deacon alert threshold, so nothing pages, but the trend is
unambiguous.

**The fix:** option 3 from upstream gascity#1557 — implement a
Go-side `gc dolt compact` CLI that the dog can shell out to. PR1 first
introduces the schema and regression test that prevents this exact
failure mode from recurring; PR2 fills it with the real executor.

**Underlying invariant being restored:** every executable formula step
must have an executor binding (Cobra command or pack script) OR carry an
explicit ZFC-exemption marker that points at a known-external executor.
Today there is no schema field for either, so the orphan condition
escapes review.

---

## Proposed Design

### Two-PR shipping order

**PR1 — Executor-binding contract** (broad scope, no new behaviour):

1. Add two fields to `formula.Step` in `internal/formula/types.go`:
   - `Executor string` (TOML `executor`, JSON `executor`).
   - `ZFCExempt bool` (TOML `zfc_exempt`, JSON `zfc_exempt`).
2. Update `stepTOMLAlias` and `toStep` to round-trip the new fields.
3. Add `cmd/gc/executor_binding_test.go` — walks every embedded formula
   in `examples/*/formulas/`, verifies each step's executor binding (see
   Trade-offs and Decisions for resolution rules).
4. Audit and migrate every embedded formula that should carry an
   exemption. Today only `mol-dog-compactor` qualifies; verify by
   exhaustive sweep.
5. Update `mol-dog-compactor`: tag the 4 steps with
   `zfc_exempt = true` and `executor = "(daemon-only)"` (sentinel
   indicating "deferred-binding, must be flipped by PR2"). The
   regression test recognizes the sentinel as transitional. Cycle
   behaviour is unchanged: dogs continue to safely-skip.
6. New doc: `engdocs/contributors/executor-binding.md` defines the
   schema, the `(daemon-only)` sentinel, the binding resolution rules,
   and the ZFC acronym (Zero Framework Cognition).

**PR2 — CLI + dog formula** (narrow scope, full behaviour):

1. New Cobra command `gc dolt-compact` (top-level), files:
   - `cmd/gc/cmd_dolt_compact.go` — Cobra wiring, `runDoltCompact`
     entry point.
   - `cmd/gc/dolt_compact_inspect.go` — auto-discovery, identifier
     validation, commit-count probe, candidate selection.
   - `cmd/gc/dolt_compact_compact.go` — flatten + surgical algorithms.
   - `cmd/gc/dolt_compact_verify.go` — pre/post row count integrity.
   - `cmd/gc/dolt_compact_envelope.go` — `gc.dolt.compact.v1` types.
   - `cmd/gc/cmd_dolt_compact_test.go` — unit tests.
   - `cmd/gc/cmd_dolt_compact_integration_test.go` — `//go:build
     integration`, real Dolt sql-server.
2. New pack delegate:
   - `examples/dolt/commands/compact/command.toml` — one-line
     description.
   - `examples/dolt/commands/compact/run.sh` — thin wrapper that
     forwards args to `gc dolt-compact`.
   - `examples/dolt/commands/compact/README.md` — operator examples.
3. Update `examples/dolt/formulas/mol-dog-compactor.toml`: drop
   `zfc_exempt`, set `executor = "gc dolt compact"` on inspect,
   compact, verify, report steps. Step descriptions updated to point
   at the CLI's behaviour.
4. New doc: `engdocs/operations/dolt-compact-runbook.md` — operator
   incident playbook (integrity-mismatch recovery, surgical-mode
   crash recovery, etc.).

### Runtime architecture

```
24h order ─► dog session ─► claim wisp(mol-dog-compactor)
                              │
                              ▼
                   read step.executor = "gc dolt compact"
                              │
                              ▼
                  exec gc dolt compact <flags from formula vars> --json
                              │
                              ▼
        ┌─────────────────────┼─────────────────────┐
        │   pack delegate run.sh forwards to        │
        │           gc dolt-compact (Go)            │
        ▼                     ▼                     ▼
    inspect()            compact()              verify()
    candidates           per-DB                 row counts
        │                     │                     │
        └──────────┬──────────┴──────────┬──────────┘
                   ▼                     ▼
            CompactReport (gc.dolt.compact.v1)
                              │
                              ▼
              dog parses envelope, closes 4 step beads
                              │
                              ▼
                 dog nudges deacon "DOG_DONE: compactor"
```

Single CLI invocation per cycle. All four formula step beads close from
the one envelope.

---

## Key Components

### Cobra command (`cmd/gc/cmd_dolt_compact.go`)

Top-level command `gc dolt-compact` with these flags:

| Flag           | Type   | Default  | Notes                                                  |
|----------------|--------|----------|--------------------------------------------------------|
| `--mode`       | string | `flatten`| Enum `flatten`\|`surgical`. Validator at parse.        |
| `--threshold`  | int    | `500`    | Min commit count to mark candidate. Reject ≤0.         |
| `--keep-recent`| int    | `50`     | Surgical only. Silently ignored when mode=flatten.     |
| `--databases`  | string | `""`     | CSV. Unioned with positional args. Empty = auto.       |
| `--port`       | string | `""`     | Mirrors cleanup's `--port`. Resolution chain reused.   |
| `--probe`      | bool   | `false`  | Mirrors cleanup. Early TCP-only reachability exit.     |
| `--json`       | bool   | `false`  | Emit `gc.dolt.compact.v1` envelope on stdout.          |

Positional args: `gc dolt-compact [databases...]`. Database tokens are
unioned with `--databases` then deduplicated. Identifier validation runs
on the union before any SQL.

Pack delegate `examples/dolt/commands/compact/run.sh` is ~30 lines:

1. Resolves the gc binary via the standard pack runtime sourcing.
2. Maps formula-var env names (e.g., `GC_VAR_MODE`,
   `GC_VAR_THRESHOLD`, `GC_VAR_KEEP_RECENT`, `GC_VAR_DATABASES`) to
   short flags when present.
3. Forwards remaining args verbatim to `gc dolt-compact "$@"`.
4. No SQL of its own.

### Algorithm core

Two algorithms ported verbatim from the formula prose
(`mol-dog-compactor.toml:109-139`).

**Flatten** (per database):

```
1. Acquire advisory lock (cleanup's tryManagedDoltLifecycleLock).
   Lock failure → emit error class=database-locked, skip cycle.
2. Probe row counts for all user tables (excl. dolt_*).
3. Find root commit hash.
4. USE database.
5. CALL DOLT_RESET('--soft', '<root>').
6. CALL DOLT_COMMIT('-Am', 'compaction: flatten history').
7. Re-probe row counts; mismatch → error class=integrity-mismatch.
8. Verify final commit count.
9. CALL dolt_gc().
10. Release lock; record CompactResult.
```

Concurrent-write safe (merge base shifts, data preserved).

**Surgical** (per database):

```
1. Acquire advisory lock.
2. Drop leftover compact-base / compact-work branches (post-lock,
   per PRD Q5).
3. Record HEAD hash + row counts.
4. Find root commit hash.
5. CALL DOLT_BRANCH('compact-base', '<root>').
6. CALL DOLT_BRANCH('compact-work', 'main').
7. Checkout compact-work.
8. CALL DOLT_REBASE('--interactive', 'compact-base').
9. Read rebase plan, mark old commits as squash (keep last
   keep_recent).
10. CALL DOLT_REBASE('--continue').
    On graph-change error: pause 2s, retry once.
    On second failure: error class=concurrent-write-collision, exit
    this DB cleanly.
11. Re-probe row counts; mismatch → error class=integrity-mismatch.
12. Verify HEAD hash unchanged on main during rebase.
13. Delete main, rename compact-work to main.
14. Drop compact-base.
15. CALL dolt_gc().
16. Release lock; record CompactResult.
```

NOT concurrent-write safe; retried once with 2s pause.

### Inspection (`dolt_compact_inspect.go`)

- Auto-discovery via `gc rig list --json` ONLY. Never `SHOW
  DATABASES`. (PRD Q4 firm.)
- Identifier validation reuses
  `validDoltDatabaseIdentifier` from
  `cmd/gc/dolt_cleanup_drop_planner.go:135`.
- Commit-count probe per database (LIMIT 10000 prefilter, matching
  formula prose).
- Returns `[]InspectResult` populated into the envelope.

### Verification (`dolt_compact_verify.go`)

- Pre-flight row count probe before compaction; post-flight after.
- All user tables (excludes `dolt_*` system tables).
- `RowsPre` / `RowsPost` keyed by table name (typed
  `map[string]int`).
- Mismatch → `VerifyResult.OK = false`, populates
  `Mismatches[]`.

### JSON envelope (`dolt_compact_envelope.go`)

Schema: `gc.dolt.compact.v1`. See **Data Model** below for the full
shape. `MarshalJSON` ensures empty slices serialize as `[]`, never
`null` (mirrors cleanup).

### Lifecycle lock

Reused unchanged from `cmd/gc/dolt_lifecycle_lock.go`. Per-city
exclusive `syscall.Flock` on `<cityPath>/.beads/dolt.lock`. Compact and
cleanup serialize against each other (intentional).

### Port resolution

Reused unchanged from `cmd/gc/dolt_cleanup_port.go`. Chain: `--port`
→ city.toml `dolt.port` → `<rigRoot>/.beads/dolt-server.port` (HQ
first) → 3307. Same audit-trail in `PortResolution.Tried`.

### Executor-binding regression test (PR1)

Located at `cmd/gc/executor_binding_test.go`. Walks all embedded
formulas under `examples/*/formulas/`. For each step:

- If `Executor != ""`:
  - Validate against a closed prefix set:
    - `gc <subcommand>`: must resolve to a registered Cobra command in
      `cmd/gc/`. The test introspects the root Cobra command tree.
    - `examples/<pack>/commands/<name>`: must resolve to a directory
      in the embedded FS containing both `command.toml` and `run.sh`.
    - `(daemon-only)`: sentinel. Must pair with `ZFCExempt=true`. The
      test treats this as "transitional, deferred binding."
- If `ZFCExempt=true`:
  - Require `Executor != ""` (orphan prevention).
- If `Executor == ""` and `ZFCExempt=false`:
  - Step is agent-cookable; nothing to check.

Test fails on:

- Empty/blank `Executor` with `ZFCExempt=true`.
- `Executor` pointing at a Cobra command that doesn't exist.
- `Executor` pointing at a pack script path that doesn't exist.

After PR2 lands, `mol-dog-compactor` carries `executor = "gc dolt
compact"`, so the test verifies the binding resolves to the new Cobra
command.

---

## Interface

### CLI invocation forms

```bash
# Operator ad-hoc — single database, default mode:
gc dolt compact hq

# Operator with mode override:
gc dolt compact hq --mode=flatten

# Multiple databases:
gc dolt compact hq loomington --threshold=1000

# Auto-discovery (all registered rigs):
gc dolt compact

# Surgical with explicit keep_recent:
gc dolt compact hq --mode=surgical --keep-recent=100

# Dog-style (json output):
gc dolt-compact --mode=flatten --threshold=500 --databases=hq,loomington --json

# Pre-cycle probe (port reachability only, no SQL):
gc dolt-compact --probe
```

### Human-readable output (default)

```
PORT
  3307 (city.toml dolt.port)

INSPECT
  hq           1611 → candidate (threshold 500)
  loomington    234 → skip (below threshold)
  gascity        45 → skip (below threshold)
  hangk          87 → skip (below threshold)

COMPACT
  hq    flatten   1611 → 1   (1.2 GB reclaimed, 2.4s)

VERIFY
  hq    OK

SUMMARY
  inspected: 4   compacted: 1   skipped: 3 (all below threshold)   failed: 0
  duration:  3.1s   reclaimed: 1.2 GB
```

The "all below threshold" qualifier on the skipped count is mandatory
when no DB qualified — explicit reason wording prevents the silent-
success failure mode that motivated this work.

### JSON envelope (with `--json`)

See **Data Model** for full shape. Single line on stdout at exit, exit
code reflects fatal errors only (per-DB failures don't fail the
process).

### Exit codes

| Code | Meaning                                                            |
|------|--------------------------------------------------------------------|
| 0    | All requested DBs processed (some may have errored at per-DB level)|
| 1    | Fatal: port resolution failed, lock held, no DBs found, etc.       |
| 2    | Operator error: invalid identifier, bad flag value.                |

---

## Data Model

### `gc.dolt.compact.v1` envelope

```go
const CompactSchemaVersion = "gc.dolt.compact.v1"

type CompactReport struct {
    Schema     string             `json:"schema"`
    Mode       string             `json:"mode"`        // flatten|surgical
    Threshold  int                `json:"threshold"`
    KeepRecent int                `json:"keep_recent"` // 0 if mode!=surgical
    Port       CleanupPortReport  `json:"port"`        // reuse cleanup type
    Inspected  []InspectResult    `json:"inspected"`
    Compacted  []CompactResult    `json:"compacted"`
    Verified   []VerifyResult     `json:"verified"`
    Summary    CompactSummary     `json:"summary"`
    Errors     []CompactError     `json:"errors"`
}

type InspectResult struct {
    Database    string `json:"database"`
    CommitCount int    `json:"commit_count"`
    Threshold   int    `json:"threshold"`
    Candidate   bool   `json:"candidate"`
    SkipReason  string `json:"skip_reason,omitempty"`
}

type CompactResult struct {
    Database       string         `json:"database"`
    Mode           string         `json:"mode"`
    CommitsBefore  int            `json:"commits_before"`
    CommitsAfter   int            `json:"commits_after"`
    RowsPre        map[string]int `json:"rows_pre"`
    RowsPost       map[string]int `json:"rows_post"`
    DurationMS     int64          `json:"duration_ms"`
    BytesReclaimed int64          `json:"bytes_reclaimed,omitempty"`
    Retried        bool           `json:"retried"`
    GCRan          bool           `json:"gc_ran"`
    Error          *CompactError  `json:"error,omitempty"`
}

type VerifyResult struct {
    Database   string             `json:"database"`
    OK         bool               `json:"ok"`
    Mismatches []TableRowMismatch `json:"mismatches,omitempty"`
}

type TableRowMismatch struct {
    Table    string `json:"table"`
    Expected int    `json:"expected"`
    Got      int    `json:"got"`
}

type CompactSummary struct {
    Inspected      int   `json:"inspected"`
    Compacted      int   `json:"compacted"`
    Skipped        int   `json:"skipped"`
    Failed         int   `json:"failed"`
    DurationMS     int64 `json:"duration_ms"`
    BytesReclaimed int64 `json:"bytes_reclaimed"`
}

type CompactError struct {
    Stage    string `json:"stage"`               // inspect|compact|verify
    Database string `json:"database,omitempty"`
    Class    string `json:"class"`
    Message  string `json:"message"`
}
```

Custom `MarshalJSON` ensures empty arrays serialize as `[]`, never
`null`. Mirrors `cmd/gc/cmd_dolt_cleanup.go:122-152`.

### Error class enum

Closed set in v1. Future expansions mint v2 of the schema.

| Class                       | Recoverable | Dog action                                |
|-----------------------------|-------------|-------------------------------------------|
| `concurrent-write-collision`| yes (retried internally) | logged; surfaced only if retry exhausted |
| `integrity-mismatch`        | no          | escalate (mayor)                           |
| `database-locked`           | yes         | skip; retry next cycle                     |
| `database-unreachable`      | yes         | skip                                       |
| `identifier-invalid`        | no          | reject (operator error)                    |
| `port-resolution`           | no          | escalate                                   |
| `internal`                  | no          | escalate                                   |

### Step schema additions (PR1)

```go
// In internal/formula/types.go, on Step:

// Executor names the binding that runs this step. Empty for steps
// agents cook into prompts. Non-empty values resolve via the
// executor-binding contract (see engdocs/contributors/executor-binding.md).
Executor string `json:"executor,omitempty" toml:"executor,omitempty"`

// ZFCExempt declares this step bypasses Zero-Framework-Cognition: the
// step's behaviour cannot be cooked into an agent prompt and must run
// via Executor. Always pairs with a non-empty Executor.
ZFCExempt bool `json:"zfc_exempt,omitempty" toml:"zfc_exempt,omitempty"`
```

The `stepTOMLAlias` parser and `toStep` copier get matching new
fields. Backwards-compatibility is automatic (zero-value defaults).

---

## Trade-offs and Decisions

### Single CLI invocation per cycle vs sub-subcommands

**Decided: single invocation.** `gc dolt-compact run` / `gc
dolt-compact inspect` (Option B from Leg 1) doubles the surface. The
formula's per-step inspect/compact/verify/report decomposition is
satisfied by the JSON envelope's per-phase arrays.

### Per-DB vs aggregated `dolt_gc`

**Decided: per-DB.** Matches formula prose verbatim, gives failure
isolation, and yields predictable per-DB duration. Aggregated GC
saves marginal time but couples failure modes. v2 may add `--no-gc`
if a backfill story emerges.

### Hybrid envelope (phase-keyed top-level + per-DB arrays)

**Decided: hybrid (Option C from Leg 2).** Mirrors cleanup's
shape one-for-one. Easiest dog parsing (one phase per step bead, array
of per-DB rows). Natural human rendering (group by phase).

### `--inspect-only` / `--dry-run` deferral

**Decided: deferred to v2.** PRD review's recommendation. v1 ships the
smallest surface. The dog reads `inspected[].candidate=true/false` from
the envelope; operators can do the same. A `--dry-run` flag arrives
later if real demand emerges.

### Pack delegate as thin wrapper, not autonomous

**Decided: thin wrapper.** Cleanup's pack does its own SQL today; for
compact this is too risky (Dolt SQL ceremony, retry logic, lock
semantics). Pack delegate forwards to `gc dolt-compact`. All
behaviour lives in Go. Cleanup's pack may eventually be migrated to
the same shape; out of v1 scope.

### `(daemon-only)` sentinel for PR1

**Decided: sentinel string.** Of the three options in Leg 6 I2:
- `Executor=""`, `ZFCExempt=true` (relax the test): orphan
  formulas indistinguishable from intentionally-unbound ones.
- `Executor="(daemon-only)"`, `ZFCExempt=true` (sentinel): the
  test recognizes the sentinel as transitional. Clean.
- A new "deferred-binding" boolean: extra field for one transient
  case. Cluttered.

The sentinel approach is the chosen design. PR2 flips to `gc dolt
compact`. The regression test prints a transitional warning whenever
it sees the sentinel — a reviewer cue that the sentinel must be
removed before the cycle is considered fixed.

### Identifier validation: defense in depth

**Decided: validate at flag parse AND at every SQL-construction site.**
Cleanup does this; mirror exactly. v1 doesn't introduce a typed
`DBName` wrapper (Option C of Leg 5); when a third compaction-style
command appears, refactor.

### Single-process compact (no goroutine fan-out)

**Decided: sequential per-DB processing.** With 4-6 production rigs,
goroutines complicate lock semantics, error spread, and envelope
ordering for negligible win. Sequential is fine.

### Lock semantics: compact serializes against cleanup

**Decided: same lock as cleanup.** Both are mutating data-plane
operations. Concurrent execution is unsafe. Documented in the
runbook.

### `report` step gets `executor = "gc dolt compact"` in PR2

**Decided: yes, all four steps bind to the CLI** (per Leg 6 I1b). The
envelope IS the canonical report; the dog's "report" responsibility
is bead closure plus a nudge to the deacon. Documented in PR2's
formula update.

### Threshold applies even to explicitly-named DBs

**Decided: threshold always applies; `--threshold=0` for "compact
regardless"** (per Leg 1 A1). PRD Q14 was unresolved; this is the
straightforward answer. An operator naming `gc dolt compact hq`
expecting a default-threshold pass is consistent with the dog's
24h cycle behaviour.

### Q15: ZFC mismatch resolution

**Decided: PR2 flips `mol-dog-compactor` from `zfc_exempt = true` to
`executor = "gc dolt compact"`.** PRD Q15 was the design-exploration
follow-up; the daemon path is rejected as a Non-Goal, so flipping is
the only consistent option. No coexistence; the CLI replaces the
imagined daemon.

### Q16: ZFC acronym expansion

**Decided: ZFC = Zero Framework Cognition.** Documented in
`engdocs/contributors/executor-binding.md`.

---

## Risks and Mitigations

| Risk                                                                | Mitigation                                                                                  | Residual                  |
|---------------------------------------------------------------------|---------------------------------------------------------------------------------------------|---------------------------|
| Identifier-injection / wrong-DB compaction                          | Charset validation at CLI parse + at every SQL-construction site; auto-discovery never SHOW DATABASES | Low                       |
| Concurrent-write retry exhausted on busy hq                         | Surgical retries once with 2s pause; emits class=concurrent-write-collision; dog skips, retries next cycle | Medium (busy hq surgical) |
| Crash during surgical mid-rebase leaves orphan branches             | Next run cleans up post-lock; manual recovery documented in runbook                          | Low                       |
| `dolt_gc` runs minutes-to-hours on a large DB                       | Per-DB invocation contains the cost; stderr heartbeats at 5min and 30min boundaries          | Medium for very large DBs |
| Formula author writes `executor = "rm -rf /"`                      | Regression test rejects executor not matching the closed prefix set                          | Low                       |
| PR2 lands without PR1                                              | Bead DAG enforces `blocks` dep; refinery refuses out-of-order merge                          | Low                       |
| Sentinel `(daemon-only)` survives PR2 by mistake                    | Regression test prints a transitional warning; reviewer cue                                  | Low                       |
| Existing TOMLs break on new schema fields                           | Fields default to zero values; PR1 round-trip test covers all embedded formulas              | Very low                  |
| Mock-vs-real divergence in unit tests                               | Integration test (`//go:build integration`) exercises real Dolt sql-server end-to-end        | Low                       |
| Operator runs `gc dolt compact` on production while dog is mid-cycle| Advisory lock fails-fast; operator sees `database-locked`; no data risk                      | Low                       |
| Cycle reports "compacted: 0" looking identical to old safely-skip   | Mandatory "compacted: 0 (all below threshold)" wording in summary; envelope captures candidate count | Very low                  |
| Surgical mode on a DB with >10k commits + heavy concurrent writes   | Documented as out-of-bounds for surgical; operators use flatten in that regime               | Documentation-mitigated   |

---

## Implementation Plan

### PR1 — Executor-binding contract

**Order of work:**

1. Add fields to `Step`, `stepTOMLAlias`, `toStep` in
   `internal/formula/types.go`.
2. Round-trip test in `internal/formula/types_test.go`: decode → encode
   → decode produces equal Step.
3. Audit all `examples/*/formulas/*.toml`. Confirm only
   `mol-dog-compactor` warrants the new fields. Document audit table
   in PR description.
4. Update `examples/dolt/formulas/mol-dog-compactor.toml`: add
   `executor = "(daemon-only)"` and `zfc_exempt = true` to all 4 steps.
5. Implement `cmd/gc/executor_binding_test.go`. Walk embedded formulas
   via `examples/dolt.PackFS` (and equivalents for other packs);
   resolve each step's executor via the closed prefix set.
6. New doc `engdocs/contributors/executor-binding.md` defining the
   contract, the sentinel, the prefix set, and the ZFC acronym.
7. Run `make test`, `go vet ./...`. All green.

**Done criteria:**

- New schema fields parse; round-trip test passes.
- Regression test passes against the migrated embedded-formula corpus.
- `mol-dog-compactor` cycle behaviour unchanged (still safely-skips).
- Doc shipped with the PR.

### PR2 — CLI + dog formula

**Order of work:**

1. Wire Cobra command in `cmd/gc/cmd_dolt_compact.go`. Stub out the
   inspect/compact/verify/envelope functions; verify `gc dolt-compact
   --help` prints expected text.
2. Implement `dolt_compact_envelope.go` with the full type set and
   `MarshalJSON`. Schema golden test covers shape regressions.
3. Implement `dolt_compact_inspect.go`: auto-discovery via
   `gc rig list --json`, identifier validation, commit-count probe.
4. Implement `dolt_compact_compact.go` flatten algorithm with mocked
   SQL responses; unit-test happy path + integrity-mismatch detection.
5. Implement surgical algorithm; unit-test happy path,
   concurrent-write retry, retry-exhausted failure.
6. Implement `dolt_compact_verify.go` row-count comparison; unit-test
   match + mismatch.
7. Implement `cmd_dolt_compact_integration_test.go` with `t.TempDir()`
   real-Dolt setup mirroring `examples/dolt/sql_test.go` patterns:
   - flatten end-to-end → commit count drops to 1.
   - surgical end-to-end → commits drop to keep_recent.
   - concurrent-write injection (background goroutine writing during
     rebase) → retry observed in envelope.
   - dolt_gc reclaim observable via disk size delta.
8. Pack delegate `examples/dolt/commands/compact/{command.toml,
   run.sh,README.md}`.
9. Update `mol-dog-compactor.toml`:
   - Drop `zfc_exempt = true` from each step.
   - Set `executor = "gc dolt compact"` on inspect, compact, verify,
     report.
   - Update step description prose to reference the CLI's behaviour.
10. New doc `engdocs/operations/dolt-compact-runbook.md`.
11. Run `make test`, integration shard, `go vet ./...`. All green.
12. Manual end-to-end on loomington (post-merge): one cycle's
    `gc dolt compact hq --json` shows non-zero compaction.

**Done criteria:**

- Flatten and surgical end-to-end integration tests pass.
- `mol-dog-compactor` formula step beads close from envelope-fields
  on a real dog cycle.
- Regression test still passes — no orphans introduced.
- Operator runbook published.

### Bead DAG (created by `mol-idea-to-plan.create-beads` step)

```
gc-???? (PR1: executor-binding contract)        priority=2
  ├── tracks gc-2189al
  └── (no blocks-on)
gc-???? (PR2: gc dolt compact CLI + formula)     priority=2
  ├── tracks gc-2189al
  └── blocks-on PR1 work bead
```

Both work beads carry `metadata.gc.routed_to` for the polecat pool;
metadata `coordinator`, `review_id=dolt-compact-cli` so traceability
back to this design doc is preserved.

---

## Open Questions

These remain for the three PRD-alignment rounds and three plan-self-
review rounds to refine. None blocks the design from advancing to
implementation planning:

- **D2 (data, scale).** `BytesReclaimed` measurement. Read directory
  size pre/post-`dolt_gc`? Best-effort with `bytes_reclaimed=0` when
  not measurable (cross-platform footgun)? Decide before PR2.
- **S1 (scale).** Stderr heartbeat thresholds for `dolt_gc` (5min,
  30min boundaries). Confirm these are right for the largest hq
  expected in the next 6 months.
- **Sec2 (security).** `compact-base` / `compact-work` branch name
  collision with operator branches. Keep formula names (current
  proposal) and document the collision risk, OR introduce
  timestamp-suffixed names. Defer to a self-review round.
- **U2 (ux).** Help text drawing from the formula prose: do we share
  via a runtime `embed.FS` read or accept some prose duplication?
  Defer.
- **I3 (integration).** Migration audit table — kept in PR1 description
  body or in `engdocs/`? Currently proposed: PR description body.
- **Beyond v1**: `--inspect-only`, `--dry-run`, event-bus emit,
  streaming NDJSON, dashboard tile, deacon recalibration once cycles
  emit non-zero results. None ships in PR1/PR2.

---

## Appendix: Inputs and Method

This baseline design doc was produced in the
`mol-idea-to-plan.design-exploration` step (bead `gc-oick69`) by
coordinator `gascity/gastown.furiosa` (polecat, Opus 4.7 / 1M context).

**Inputs read in full:**

- `.prd-reviews/dolt-compact-cli/prd-draft.md` (452 lines)
- `.prd-reviews/dolt-compact-cli/prd-review.md` (181 lines)
- `.plan-reviews/dolt-compact-cli/human-clarifications.md` (58 lines)
- `.plan-reviews/dolt-compact-cli/prd-review-legs.md` (396 lines)

**Six design-analysis legs executed inline** (not via `gc sling`
fan-out) as separate-lens passes by the coordinator. Same pattern as
the prior PRD-review round; full leg reports in
`.plan-reviews/dolt-compact-cli/design-legs.md`. Round note at the
foot of that file documents the deviation. This document synthesizes
findings across:

- **api**: CLI / API shape (Cobra flags, pack delegate, dual surface).
- **data**: data model, storage, migrations (envelope schema, Step
  schema additions, error enum).
- **ux**: mental model, workflow fit, error experience, docs.
- **scale**: bottlenecks, scale limits, lock contention,
  concurrent-write retry.
- **security**: trust boundaries, identifier validation,
  executor-binding test scope.
- **integration**: code placement, PR1/PR2 split, test strategy,
  rollout sequence.

**Codebase grounding** (Explore subagents):

- `cmd/gc/cmd_dolt_cleanup.go` (the canonical cleanup CLI, lines
  22-813).
- `cmd/gc/dolt_cleanup_drop.go`, `dolt_cleanup_drop_planner.go` (drop
  stage and identifier validator).
- `cmd/gc/dolt_cleanup_port.go` (port resolution chain).
- `cmd/gc/dolt_lifecycle_lock.go` (advisory lock).
- `examples/dolt/formulas/mol-dog-compactor.toml` (the orphaned
  formula, lines 1-192).
- `examples/dolt/orders/mol-dog-compactor.toml` (the dispatch order).
- `internal/formula/types.go` Step struct (lines 196-304).
- `examples/dolt/commands/cleanup/{command.toml,run.sh}` (pack
  delegate prior art).
- `cmd/gc/cmd_rig.go` (the `gc rig list --json` shape).
- `examples/dolt/sql_test.go` (integration test scaffolding).

The next pipeline steps (`prd-align-1` through `plan-review-3`,
followed by `create-beads`) refine THIS document in place rather than
producing successor documents. Subsequent rounds add the parallel-
perspective counterweight to the inline coordinator-only synthesis.
