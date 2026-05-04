# Design Legs (inline)

Per formula `mol-idea-to-plan.design-exploration`, six design-analysis legs were
intended to be dispatched in parallel via `gc sling`. Following the coordinator
deviation already established in the `prd-review` round (single-coordinator
inline execution under Opus 4.7 / 1M context), this round also executes inline
as separate-lens passes. Reasons documented in the round note at the foot of
this file and in the design-review-beads TSV.

Each section is a complete report following the formula's prescribed structure:

1. Summary
2. Key Considerations
3. Options Explored
4. Recommendation
5. Constraints Identified
6. Open Questions
7. Integration Points

Inputs: `.prd-reviews/dolt-compact-cli/prd-draft.md`,
`.prd-reviews/dolt-compact-cli/prd-review.md`,
`.plan-reviews/dolt-compact-cli/human-clarifications.md`.

---

# Leg 1: api — CLI / API shape, ergonomics, discoverability

## Summary

The PRD locks the CLI surface in broad strokes (`gc dolt-compact` Go +
`gc dolt compact` pack delegate, four flags inherited from the formula vars,
`gc.dolt.compact.v1` envelope on `--json`). What the design must still nail
down: the **exact** flag set including pack-delegate semantics, how the four
formula-vars-as-flags compose with positional database args, how `--json` /
`--port` / `--probe` map onto the cleanup precedent, and the human-readable
output contract. Cleanup's `gc dolt-cleanup` is the strongest reference point
and most decisions reduce to "do what cleanup does."

## Key Considerations

- **Dual surface, single implementation.** The Go binary owns all behaviour;
  the pack delegate is a thin shell wrapper that forwards flags. This differs
  from `gc dolt cleanup` today (autonomous shell script). For compact, the
  shell delegate should NOT re-implement compaction — too risky, too much
  Dolt-SQL ceremony.
- **Formula vs CLI variable names.** The formula uses `commit_threshold`,
  `keep_recent`, `databases`, `mode`. The PRD's flag list says `--threshold`,
  `--keep-recent`, `--databases`, `--mode`. The pack delegate must map cleanly
  in both directions: the dog passes formula-var names, the operator types
  short flags. Single source of truth lives on the Go flag definitions.
- **Auto-discovery vs positional args.** Empty `--databases` AND empty
  positional list ⇒ auto-discover via `gc rig list --json`. Either form
  populated ⇒ explicit list. PRD allows positional args `gc dolt compact hq`;
  this is conventional Cobra for "list of targets" subcommands.
- **`--json` as observability switch.** Default is human-readable;
  `--json` emits `gc.dolt.compact.v1` to stdout. The dog always passes
  `--json`. Tests verify both paths.
- **`--port` / `--probe`.** Cleanup exposes both; compact should mirror
  exactly (port resolution chain reused). `--probe` is a TCP-only
  reachability check that exits early without doing work — useful for
  pre-cycle audits.
- **`--inspect-only` vs `--dry-run`.** Both PRD-deferred to v2 per PRD
  review. Two narrowly different operator stories: "tell me which would
  compact" (inspect-only) vs "simulate the compaction" (dry-run). For v1,
  ship neither. Add a single `--dry-run` later if a story emerges.
- **Discoverability in `gc dolt --help`.** Operators looking for compact
  via `gc dolt -h` need to see it. The pack delegate registration provides
  the help-line; the Go-side `gc dolt-compact -h` is the deep documentation.
- **Subcommand grouping.** Like `cleanup`, compact is operationally a
  data-plane housekeeping command. It belongs next to cleanup in
  `gc dolt --help` listing.

## Options Explored

### Option A: Mirror cleanup verbatim (status-quo)

Single Cobra command `gc dolt-compact` with flags
`--mode --threshold --keep-recent --databases --port --probe --json`. Pack
delegate at `examples/dolt/commands/compact/run.sh` shells out to
`gc dolt-compact "$@"` after light arg passthrough. No new flag patterns.

Pros: zero novelty; reviewers can audit by diff vs cleanup. Tooling already
knows how to test this shape.

Cons: doesn't surface the inspect-only / dry-run story. Leaves the dog
emitting per-step bead-closure data only via the JSON envelope (no streaming
heartbeat).

### Option B: Subcommand pattern (`gc dolt-compact inspect|run`)

Sub-subcommands. `gc dolt-compact inspect` returns candidates without
mutating; `gc dolt-compact run` does the work. Roughly mirrors
`docker container ls` vs `docker container run`.

Pros: makes "audit" a first-class verb.

Cons: doubles the surface; the dog formula has a single `compact` step that
already covers all phases — sub-subcommands force the dog to know which
variant to call. Conflicts with PRD's "single CLI invocation per cycle"
principle.

### Option C: Mirror cleanup, defer inspect-only

Same as A, with `--inspect-only` explicitly out of scope for v1. The
formula's `inspect` step is satisfied by the JSON envelope's
`inspected_databases` field — the dog reads "candidate=true/false" per DB
and closes the inspect step bead from that signal.

Pros: smallest surface area. Closest to "mirror cleanup." Honours PRD's
v2-deferral. Single CLI invocation handles all four formula steps.

Cons: operators can't easily preview without a real run. Mitigated by
documenting the JSON shape and offering a future `--dry-run` if real demand
arises.

## Recommendation

**Option C.** Single Cobra command, no sub-subcommands, `--inspect-only`
explicitly v2. The dog runs the CLI once per cycle with `--json` and parses
the envelope to close all four step beads.

Final flag set:

| Flag           | Type   | Default  | Source                                           |
|----------------|--------|----------|--------------------------------------------------|
| `--mode`       | string | `flatten`| Formula var; enum `flatten`\|`surgical`           |
| `--threshold`  | int    | `500`    | Formula var `commit_threshold`                    |
| `--keep-recent`| int    | `50`     | Formula var (surgical only)                       |
| `--databases`  | string | `""`     | Formula var (CSV); positional args also accepted  |
| `--port`       | string | `""`     | Mirrors `gc dolt-cleanup`                         |
| `--probe`      | bool   | `false`  | Mirrors `gc dolt-cleanup`                         |
| `--json`       | bool   | `false`  | Mirrors `gc dolt-cleanup` (dog always passes)     |

Positional args and `--databases` are unioned (with dedup); explicit lists
override auto-discovery. This matches operator intuition: typing
`gc dolt compact hq` Just Works without reaching for a flag.

The pack delegate `examples/dolt/commands/compact/run.sh` is a 30-line shell
that:
1. Resolves the gc binary path (same `PACK_DIR` runtime sourcing as cleanup).
2. Maps formula-var env names (`GC_VAR_MODE`, `GC_VAR_THRESHOLD`,
   `GC_VAR_KEEP_RECENT`, `GC_VAR_DATABASES`) to short flags when present.
3. Forwards remaining args verbatim to `gc dolt-compact`.
4. Does NOT do its own SQL — all behaviour is in the Go binary.

## Constraints Identified

- **Identifier charset.** Database names from positional args validated with
  the same charset as cleanup: `[A-Za-z0-9_]` leading, `[A-Za-z0-9_-]`
  thereafter. Implementation: reuse `validDoltDatabaseIdentifier` from
  `cmd/gc/dolt_cleanup_drop_planner.go:135`.
- **Mode enum.** Cobra flag with custom validator; `--mode=foo` fails fast
  with usage error.
- **Threshold integer.** Must be positive; reject 0 / negative early.
- **Keep-recent.** Only meaningful with `--mode=surgical`; with
  `--mode=flatten` the flag is silently ignored (matches formula prose) —
  but document it in `gc dolt-compact -h` so operators don't think it's
  broken.
- **Help text consistency.** Long-form help quotes the formula's algorithm
  prose so `gc dolt-compact --help` and the formula description stay in
  sync — single source of truth is the formula TOML; help references it.

## Open Questions

- **A1.** Should `--threshold` be honoured when databases are explicitly
  named? PRD Q14 unresolved. Recommendation: **yes, threshold always
  applies**; the dog formula sets a high threshold by default
  (commit_threshold=500), and an operator naming a small database with the
  default threshold expects "nothing to do" rather than forced compaction.
  Override with `--threshold=0` for "compact regardless."
- **A2.** Does the pack delegate need its own `--help` text, or can it just
  forward to `gc dolt-compact -h`? Recommendation: **forward**; one source
  of help, mirroring cleanup's hand-rolled help block is cosmetic.
- **A3.** Should we emit a deprecation warning for `--databases` (CSV) in
  favour of positional args? Recommendation: **no**; the formula uses CSV
  via env, removing it would break the dog binding. Both forms forever.

## Integration Points

- **`mol-dog-compactor` formula step descriptions** must be updated in PR2
  to reference the CLI invocation (`gc dolt compact …`) so a human reading
  the formula sees the executor binding inline.
- **`examples/dolt/commands/compact/{command.toml,run.sh}`** — new
  pack-delegate; mirrors the cleanup pack's directory shape exactly.
- **`gc dolt --help`** — surfaces the new subcommand once the pack delegate
  is registered (mechanism is the existing pack auto-registration; no new
  glue required).

---

# Leg 2: data — data model, storage, migrations, schema evolution

## Summary

Two distinct schema surfaces ship in this work, both versioned from day 1:
(1) the `gc.dolt.compact.v1` JSON envelope emitted by the CLI, and (2) the
`executor` / `zfc_exempt` TOML fields added to `formula.Step`. The envelope
mirrors `gc.dolt.cleanup.v1` by design; the formula schema fields are net
new and require a one-time migration sweep across all embedded formulas in
PR1. No on-disk persistent state on the compact CLI itself: it is stateless
between invocations except for the advisory file lock (which is itself
ephemeral / process-bound).

## Key Considerations

- **Envelope versioning.** `gc.dolt.compact.v1` pinned forever; future
  schema changes mint `v2` and the dog grows a parser switch. Match
  cleanup's pattern at `cmd/gc/cmd_dolt_cleanup.go:23` (constant
  `CleanupSchemaVersion = "gc.dolt.cleanup.v1"`).
- **Per-database vs aggregated reporting.** Cleanup mixes both. Compact
  needs per-DB results (commit-count before/after, mode used, errors,
  bytes reclaimed) AND an aggregated summary (counts inspected,
  compacted, skipped, failed). Mirror cleanup's
  `Dropped.Names[]` / `Summary` split.
- **Error classification.** Each per-DB result needs a typed `error_class`
  enum so the dog can map it to action. PRD Open Q5 lists candidates:
  `concurrent-write-collision`, `integrity-mismatch`, `database-locked`,
  `database-unreachable`, plus catch-all `internal`. Pin the enum from
  day 1.
- **Phases inside the envelope.** Formula has 4 steps (inspect, compact,
  verify, report). The single CLI invocation produces one envelope that
  embeds all four phase outcomes so the dog can close each step bead from
  one parse.
- **Step schema migration.** PR1 adds two fields. Existing TOMLs without
  those fields work unchanged (zero-value defaults: `Executor=""`,
  `ZFCExempt=false`). Tests must verify nothing in the existing corpus
  breaks.
- **Backwards-compat for envelope.** None needed — this is a brand-new
  schema. Future-compat: the `schema` field is the version tag; consumers
  must check it.
- **Stateless CLI.** No PID file, no progress checkpoints. If the CLI
  crashes mid-rebase, the next run cleans up `compact-base` /
  `compact-work` orphans (per PRD Q5). The advisory lock is the only
  ephemeral state, released on process exit.

## Options Explored

### Option A: Flat per-phase fields on the envelope

```json
{
  "schema": "gc.dolt.compact.v1",
  "inspect": { "candidates": [...] },
  "compact": { "results": [...] },
  "verify":  { "ok": true, "details": [...] },
  "report":  { "summary": {...} }
}
```

Each formula step keys directly off the top-level field. Simple parsing.

### Option B: Per-database keyed envelope

```json
{
  "schema": "gc.dolt.compact.v1",
  "databases": {
    "hq": { "phases": { "inspect": {...}, "compact": {...}, ... } },
    ...
  },
  "summary": {...}
}
```

Database-first organisation; phases nested under each.

### Option C: Hybrid — phase keys at top level + per-DB array under each

```json
{
  "schema": "gc.dolt.compact.v1",
  "port": { "port": 3307, "source": "..." },
  "inspected": [
    {"database":"hq","commits":1611,"threshold":500,"candidate":true}
  ],
  "compacted": [
    {"database":"hq","mode":"flatten","commits_before":1611,"commits_after":1,
     "rows_pre":..., "rows_post":..., "duration_ms":..., "bytes_reclaimed":...,
     "retried":false, "error":null}
  ],
  "verified": [
    {"database":"hq","ok":true,"mismatches":[]}
  ],
  "summary": {"inspected":4,"compacted":1,"skipped":3,"failed":0,
              "duration_ms":..., "bytes_reclaimed":...},
  "errors": [
    {"stage":"compact","database":"...","class":"...","message":"..."}
  ]
}
```

Mirrors cleanup's `Dropped`/`Purge`/`Reaped`/`Summary`/`Errors` shape one-
for-one. Dog parses arrays of results per phase; per-DB rows are easy to
project onto step beads.

## Recommendation

**Option C — hybrid envelope.** Maximum parallelism with the cleanup
schema, easiest dog parsing (one phase per step bead, array of per-DB
rows), and natural human-readable rendering (group by phase). Final
schema:

```go
const CompactSchemaVersion = "gc.dolt.compact.v1"

type CompactReport struct {
    Schema     string                  `json:"schema"`
    Mode       string                  `json:"mode"`        // flatten|surgical
    Threshold  int                     `json:"threshold"`
    KeepRecent int                     `json:"keep_recent"` // 0 if mode!=surgical
    Port       CleanupPortReport       `json:"port"`        // reuse cleanup type
    Inspected  []InspectResult         `json:"inspected"`
    Compacted  []CompactResult         `json:"compacted"`
    Verified   []VerifyResult          `json:"verified"`
    Summary    CompactSummary          `json:"summary"`
    Errors     []CompactError          `json:"errors"`
}

type InspectResult struct {
    Database     string `json:"database"`
    CommitCount  int    `json:"commit_count"`
    Threshold    int    `json:"threshold"`
    Candidate    bool   `json:"candidate"`
    SkipReason   string `json:"skip_reason,omitempty"` // below-threshold, locked, etc.
}

type CompactResult struct {
    Database         string  `json:"database"`
    Mode             string  `json:"mode"`
    CommitsBefore    int     `json:"commits_before"`
    CommitsAfter     int     `json:"commits_after"`
    RowsPre          map[string]int `json:"rows_pre"`   // table → count
    RowsPost         map[string]int `json:"rows_post"`  // table → count
    DurationMS       int64   `json:"duration_ms"`
    BytesReclaimed   int64   `json:"bytes_reclaimed,omitempty"`
    Retried          bool    `json:"retried"`
    GCRan            bool    `json:"gc_ran"`
    Error            *CompactError `json:"error,omitempty"`
}

type VerifyResult struct {
    Database     string             `json:"database"`
    OK           bool               `json:"ok"`
    Mismatches   []TableRowMismatch `json:"mismatches,omitempty"`
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
    Stage    string `json:"stage"`             // inspect|compact|verify
    Database string `json:"database,omitempty"`
    Class    string `json:"class"`             // see enum below
    Message  string `json:"message"`
}
```

**Error class enum** (closed set in v1):

| Class                       | Recoverable | Dog action     |
|-----------------------------|-------------|----------------|
| `concurrent-write-collision`| yes (retried internally) | log; not surfaced unless final failure |
| `integrity-mismatch`        | no          | escalate (mayor) |
| `database-locked`           | yes         | skip; retry next cycle |
| `database-unreachable`      | yes         | skip            |
| `identifier-invalid`        | no          | reject (operator error) |
| `port-resolution`           | no          | escalate        |
| `internal`                  | no          | escalate        |

**Step schema additions** (PR1):

```go
// In internal/formula/types.go, on Step:

// Executor names the binding that runs this step. Empty for steps
// agents cook into prompts. Non-empty values are either a Cobra command
// path ("gc dolt compact"), a pack-script ref
// ("examples/dolt/commands/compact"), or a daemon entry — exact form
// validated by the executor-binding regression test.
Executor string `json:"executor,omitempty" toml:"executor,omitempty"`

// ZFCExempt declares this step bypasses Zero-Framework-Cognition: the
// step's behaviour cannot be cooked into an agent prompt and must run
// via Executor. Always pairs with a non-empty Executor.
ZFCExempt bool `json:"zfc_exempt,omitempty" toml:"zfc_exempt,omitempty"`
```

The `stepTOMLAlias` parser (lines ~376 of `types.go`) and the `toStep`
copier (line ~412) get matching new fields. JSON round-trip preserved
via the existing `UnmarshalJSON` machinery.

## Constraints Identified

- **No untyped maps on the wire.** `RowsPre` and `RowsPost` are
  `map[string]int` (table → count) — typed. No `json.RawMessage`. (Per
  AGENTS.md: "No `map[string]any` or `json.RawMessage` on wire types.")
- **Custom MarshalJSON for empty arrays.** Mirror cleanup's
  `MarshalJSON()` — empty slices serialize as `[]`, not `null`. The dog
  parser depends on it.
- **Schema test.** Add a golden-file test: known-input → expected-JSON,
  caught-by-eye for shape regressions. Fixture lives next to the unit test.
- **Step schema regression.** Existing TOMLs round-trip cleanly through
  `Decode → Encode → Decode`. PR1 test covers all embedded formulas.

## Open Questions

- **D1.** Should `RowsPre` / `RowsPost` be capped (e.g., max 1000 tables per
  DB)? Practically, gascity DBs have <30 tables; cap at 1000 with explicit
  truncation marker is enough.
- **D2.** Do we capture `dolt_gc` reclaim in bytes, or as before/after disk
  usage? Recommendation: **bytes reclaimed** (pre-gc disk minus post-gc
  disk), with `bytes_reclaimed=0` when not measurable (e.g., on Windows
  where disk-usage probing differs).
- **D3.** Where does `BytesReclaimed` come from? Reading directory size
  pre/post-gc is the simplest answer; cleanup's purge stage already does
  this kind of accounting. Not load-bearing for v1; defer if measurement
  proves flaky.

## Integration Points

- **`cmd/gc/dolt_compact_envelope.go`** — new file owning `CompactReport`
  and `MarshalJSON`. Co-located with the Cobra command.
- **`internal/formula/types.go`** — adds `Executor` and `ZFCExempt`
  fields. PR1 only.
- **Embedded formulas migration** — every existing TOML in
  `examples/*/formulas/` audited; only `mol-dog-compactor` gets the new
  fields in PR1.
- **No bead store changes.** The dog updates step beads via existing
  `gc bd close`/`gc bd update` — no new bead-store schema.

---

# Leg 3: ux — mental model, workflow fit, error experience, docs examples

## Summary

Three audiences read this CLI: operators who run it ad-hoc, the compactor
dog that runs it under the formula, and future formula authors who want to
understand the executor-binding pattern. Each needs a different lens. The
docs work is concentrated: the human-readable output, the
`engdocs/contributors/`-style reference for the executor-binding contract,
and the operational runbook for when compaction misfires. The biggest UX
risk is the silent-success failure mode that motivated this entire project:
the system must make it impossible for "dog ran, 0 dbs compacted, all green"
to look the same as "dog ran, 4 dbs compacted." That requires a careful
human-readable output AND a dashboard signal (deferred to v2).

## Key Considerations

- **Operator's first-time use.** `gc dolt compact hq` (positional, no
  flags) should Just Work, with output that's readable without reaching for
  the JSON envelope. Story 1 in the PRD is the gold path.
- **Operator's failure-mode experience.** When compaction fails (integrity
  mismatch, lock contention, concurrent-write retry exhausted), the
  operator must see (1) which DB, (2) which phase, (3) why, (4) what
  state the DB is in (compacted/restored/uncertain).
- **Mental model: "atomic per database."** Each DB is independent; one
  failing doesn't abort others. Mirror cleanup. The summary line at the
  end gives the bottom line.
- **Dog's parsing experience.** The JSON envelope is the contract. Dog
  reads it, closes 4 step beads (inspect/compact/verify/report) from the
  per-phase arrays. No streaming, no heartbeat — single envelope at exit.
- **Docs surface.** Three docs land:
  1. `examples/dolt/commands/compact/command.toml` (one-line description).
  2. `gc dolt-compact -h` (Cobra help, full flag table + algorithm
     summary, links to formula).
  3. `engdocs/contributors/executor-binding.md` (new; defines `executor`
     /`zfc_exempt` schema for formula authors).
- **Error story for silent-success class.** This is the load-bearing UX
  decision. Even when the cycle compacts zero DBs (all below threshold),
  the report message must say "0 candidates today" — not "0 compacted"
  with no further context. The current safely-skip wording is the
  cautionary tale.

## Options Explored

### Option A: Cleanup-style human output

Two-column tabular output by stage:

```
PORT
  3307 (city.toml dolt.port)

INSPECT
  hq          1611 → candidate (threshold 500)
  loomington   234 → skip (below threshold)
  ...

COMPACT
  hq          1611 → 1 (flatten, 1.2 GB reclaimed, 2.4s)

VERIFY
  hq          OK

SUMMARY
  inspected: 4   compacted: 1   skipped: 3   failed: 0
  duration:  3.1s  reclaimed: 1.2 GB
```

Pros: matches cleanup. No surprises.

### Option B: Compact narrative output

Single chronological log of phase transitions:

```
[inspect] 4 dbs · 1 candidate (hq @ 1611)
[compact] hq · flatten · 1611 → 1
[verify]  hq · OK
[report]  inspected=4 compacted=1 skipped=3 failed=0 reclaimed=1.2GB
```

Pros: one screen. Easy CI grep.

Cons: less detail; harder to scan when 10+ DBs in scope.

### Option C: Tabular with auto-collapse for skips

Like A, but the INSPECT section collapses skipped DBs into a one-line
summary unless `--verbose`:

```
INSPECT
  hq          1611 → candidate (threshold 500)
  3 below threshold (loomington=234, gascity=234, hangk=87)
```

Pros: cleaner for the common case (most DBs below threshold).

Cons: minor extra logic.

## Recommendation

**Option A (cleanup-style tabular) for v1**, with Option C deferred to v2
if the output gets noisy in production. The v2 promotion criterion is
trivial: if any production cycle has more than ~15 inspected databases
the collapse becomes useful. Today there are 4-6 rigs; A is fine.

**Critical wording — the silent-success problem.** The summary line has
mandatory phrasing for the zero-compacted case:

```
SUMMARY
  inspected: 4   compacted: 0   skipped: 4 (all below threshold)
                                                ^^^^^^^^^^^^^^^^^^^^^
                                                explicit reason, never empty
```

The dog's report bead notes always include the summary line verbatim.
A `gc bd show <report-bead>` reads back like "compacted: 0 (all below
threshold)" — operationally distinct from "compacted: 0 (silently
skipped)."

**Operator first-time docs** at `examples/dolt/commands/compact/`:
- `command.toml`: one-liner, `description = "Compact Dolt commit history
  on production databases"`.
- `README.md` (new): operator-facing examples covering the four PRD user
  stories (ad-hoc compact, dog cycle, debug-failed-cycle, surgical
  retry). Cross-link from `engdocs/`.

**Contributor docs at `engdocs/contributors/executor-binding.md`** (new):

- The `executor` and `zfc_exempt` TOML fields explained.
- The two states: `executor=""` (cooked into prompt; default) vs
  `executor="gc dolt compact"` (Cobra-bound; agent doesn't reason).
- The `zfc_exempt=true` marker: when set, the step's prose is
  observability-only; the executor IS the implementation. Pairs with
  Executor; orphaned `zfc_exempt=true` without Executor is a regression
  test failure.
- Acronym expansion (per PRD Q16): "Zero Framework Cognition (ZFC)" —
  Steve Yegge's principle that Go handles transport, agents do reasoning.
  A ZFC-exempt step is one whose behaviour MUST live in code, not in
  prompt.
- Migration example: `mol-dog-compactor` before/after PR1, before/after
  PR2.

**Operational runbook at `engdocs/operations/dolt-compact-runbook.md`**
(new):
- "compaction cycle reported integrity mismatch on hq" — what to check.
- "compaction failed with concurrent-write-collision" — manual surgical
  recovery (drop `compact-*` branches; re-run).
- "compaction skipped database X with database-locked" — not an alert
  unless it persists across cycles.
- Manual recovery procedure for crash-during-surgical (per PRD Q11).

## Constraints Identified

- **No emoji in output**, per CLAUDE.md ("Only use emojis if the user
  explicitly requests it").
- **Deterministic ordering.** Per-DB lines sorted alphabetically — easy
  to diff cycle-over-cycle.
- **Human output is text/plain**, no ANSI colour codes by default. Add
  `--color=auto` only if cleanup grows it first.
- **Help text quotes the formula.** `gc dolt-compact -h` includes a 5-
  line summary of the flatten/surgical algorithms drawn from the formula
  TOML. Both stay in sync because both render from the formula in CI
  (deferred — a docs-link suffices for v1).
- **Quiet success on dog cycles.** When `--json`, no chatty stderr. The
  dog parses stdout; stderr is for unexpected diagnostics only.

## Open Questions

- **U1.** Should the human-readable output show the JSON envelope at the
  end (`--json` ⇒ JSON-only; default ⇒ human + a small `JSON envelope:
  ...` footer)? Recommendation: **no**; pure text or pure JSON. Mode is
  the flag.
- **U2.** Does `gc dolt-compact -h` link to the formula TOML by repo-
  relative path? Yes, the help footer reads "see
  `examples/dolt/formulas/mol-dog-compactor.toml` for full algorithm
  definitions."
- **U3.** Should errors carry a `gc trace`-style correlation ID? Cleanup
  doesn't; defer for v2 unless the dog's ESCALATION mail wants one. The
  bead ID of the failing step bead is already a correlation handle.

## Integration Points

- **`engdocs/contributors/executor-binding.md`** — new; PR1 ships this so
  formula authors using the new fields have a reference.
- **`engdocs/operations/dolt-compact-runbook.md`** — new; PR2 ships this
  alongside the CLI.
- **`examples/dolt/commands/compact/README.md`** — new; PR2.
- **`mol-dog-compactor` step descriptions** — PR2 updates each step to
  reference the CLI (e.g., "the dog reads the `inspected[]` envelope
  field…").
- **CHANGELOG / release notes** — both PRs bump in lockstep.

---

# Leg 4: scale — bottlenecks, scale limits, degradation modes, caching

## Summary

Compact is bounded by Dolt's cost of moving commit graphs. Per-cycle work
scales linearly with the number of databases and quadratically with commit
count for surgical (interactive rebase reads the full history). Today's
gascity has 4-6 rigs each at 100s-1000s of commits; the headline 24h cycle
is comfortably under 1 minute for flatten and a few minutes for surgical.
The scale risks are not steady-state but burst: (1) a database that grew
unexpectedly between cycles, (2) `dolt_gc` running on a multi-GB store,
(3) concurrent-write churn on hq during compaction, and (4) lock contention
between compact and other Dolt-using subsystems (event bus emit, mail, bd).

## Key Considerations

- **Per-DB time complexity.**
  - **Flatten**: O(1) on commit count (single soft-reset + commit).
    Dominated by row-count probes (1 query per user table) and
    `dolt_gc` (O(blob-store size)).
  - **Surgical**: O(commits) for the rebase plan read, O(commits) for
    the actual rebase. Worst case: HQ at 50k commits with `keep_recent=50`
    means 49,950 squash entries — Dolt-side cost dominated by the rebase.
- **`dolt_gc` cost.** Can be minutes-to-hours on large stores per Dolt
  docs. PRD Q10 unresolved. Per-DB invocation makes the cycle predictable
  but slower; aggregated post-cycle invocation is faster but couples
  failure modes. Recommended (below): per-DB, with a heartbeat in v2.
- **Lock contention.** The advisory lifecycle lock is per-city
  (`syscall.Flock` on `<cityPath>/.beads/dolt.lock` per cleanup). Compact
  acquiring it blocks other compact runs but NOT cleanup or normal SQL
  traffic. Cleanup vs compact: each grabs the same lock today. Question
  is whether they should coexist or queue.
- **Concurrent-write hazard during surgical.** PRD already states "retry
  once with 2s pause." Real concurrency on hq is the bead-mutation rate
  during a 24h cycle — typically a few writes/sec from agent activity.
  In a 60s rebase window, that's tens-to-hundreds of writes. First retry
  may also collide on a busy hq.
- **Auto-discovery cost.** `gc rig list --json` is cheap (reads
  `city.toml`, no SQL). Negligible.
- **Memory.** Pre/post row counts held in memory: for ~30 tables × 6 DBs
  = 180 ints. Negligible.
- **Network/disk.** All Dolt operations local-loopback. Network is not a
  factor.
- **Error spread.** A failure on one DB must not abort the cycle. Cleanup
  pattern: append per-DB error to the envelope, continue. Preserve.

## Options Explored

### Option A: Per-DB `dolt_gc` (formula's current prose)

Each compact result includes a `gc_ran=true` field. Predictable per-DB
duration; failed gc on one DB doesn't block the next.

### Option B: Aggregated post-cycle `dolt_gc`

Run `dolt_gc()` once after all compactions complete. Marginally faster
(less repeated NBS housekeeping) but a single failure aborts gc for the
whole cycle.

### Option C: Per-DB with `--no-gc` escape hatch

Per-DB by default; `--no-gc` flag for operators who want to defer.
Useful for backfills where many DBs are compacted in a row.

## Recommendation

**Option A — per-DB `dolt_gc`, no escape hatch in v1.** Reasons:
- Matches the formula's existing prose verbatim ("Post-compaction: Runs
  `CALL dolt_gc()`").
- Failure isolation: per-DB gc failure recorded as `gc_ran=false`
  in the result; the dog can choose to escalate or accept.
- Predictable cycle duration: each DB's compaction is a self-contained
  unit.
- v2 can add `--no-gc` if a backfill story emerges.

**Heartbeat for long runs.** PRD review (Leg 4) flags gc cost. v1
mitigation: log to stderr `[compact] hq: dolt_gc running…` with a
timestamp before `CALL dolt_gc()` and `[compact] hq: dolt_gc done in
Ts` after. Operators tailing the dog log see progress. JSON envelope
captures `duration_ms` per result so historical trends are queryable.

**Lock semantics.**
- Use `dolt_lifecycle_lock.go` exactly as cleanup does
  (`tryManagedDoltLifecycleLock`, non-blocking).
- If lock held: exit non-zero with `class=database-locked` and
  `stage=compact` for ALL would-be DBs (compact never partially-locks);
  the cycle is a cohort.
- Document that compact and cleanup serialize against each other —
  intentional, both are mutating-data-plane operations.

**Concurrent-write retry refinement.** PRD locks "retry once with 2s
pause." For surgical mode, the retry is naive: same plan, same
parameters. Per leg-4 PRD-review observation, log both attempts in the
JSON envelope so the dog/operator sees `tried, collided, retried,
succeeded`. Failure on second attempt → emit
`class=concurrent-write-collision`, dog skips this DB this cycle, retry
next cycle.

**Surgical scale ceiling.** Document that surgical mode is unsafe to
schedule on a database with >10k commits when concurrent-write rate is
>10/s. The retry-once policy is best-effort; serious commit-graph
explosions need flatten mode. Defer auto-mode-pickup heuristics to v2
(would violate ZFC).

**`compact-base` / `compact-work` orphan cleanup ordering.** Per PRD
Q5, cleanup order is:

1. Acquire advisory lock (non-blocking, fail-fast).
2. Drop leftover `compact-base` and `compact-work` branches.
3. Begin rebase plan.

Step 2 happens AFTER lock acquisition so a still-running prior
compaction can't have its branches yanked. (PRD already locks this.)

## Constraints Identified

- **24h cooldown is the upper-bound budget.** Even surgical mode on
  every DB shouldn't exceed 1h cumulative. If it ever does, the cycle
  exceeds its budget and we have a scaling problem (not a v1 concern).
- **`dolt_gc` is best-effort.** A gc failure does NOT roll back the
  compaction (that data is already committed). Record `gc_ran=false`
  and continue.
- **Row-count probe SQL.** `SELECT COUNT(*) FROM <table>` per user
  table. For a DB with >50 tables this could be tens of seconds. Cap
  at 1000 tables (matches the `RowsPre` truncation in Leg 2 D1).
- **Single-process compact.** No internal parallelism over DBs in v1.
  Adding goroutines complicates lock semantics, error spreads, and JSON
  envelope ordering. Sequential is fine for 4-6 DBs.
- **Memory bounds.** Envelope kept in memory through end-of-run.
  Negligible (kilobytes).

## Open Questions

- **S1.** Should we log a warning if a single DB's compaction takes
  >5 minutes? The cleanup pattern is silence; the formula prose is
  silent on this. Recommendation: **emit a stderr warning** at 5 min
  and 30 min boundaries during the dolt_gc call. No alert; just
  visibility.
- **S2.** Do we need a per-DB `--timeout` flag? PRD doesn't mention.
  Recommendation: **no** for v1; rely on the natural 24h budget. Defer
  if a runaway emerges.
- **S3.** Coordination with mol-deacon-patrol — does the patrol need
  to know compact is running? Today it has no signal. After the cycle,
  the deacon sees the closed beads. Defer event-bus emission to v2.
- **S4.** PRD Q14: when an operator names a small DB with default
  threshold, should compact still run? Mirrored in Leg 1 A1. **Run
  threshold by default, allow `--threshold=0` to override.** Same
  answer.

## Integration Points

- **`dolt_lifecycle_lock.go`** — reused unchanged.
- **`mol-deacon-patrol`** — no change in v1 (no event-bus emit yet).
- **Any future per-cycle dashboard** — would consume the JSON
  envelope. Out of v1 scope.

---

# Leg 5: security — trust boundaries, attack surface, validation, permissions

## Summary

The CLI runs as the same user as the rest of the gascity binary (the
operator's UID for ad-hoc; the dog's session UID under the formula).
Trust boundaries are: (1) the database name argument (positional or
`--databases`), (2) the SQL constructed from those names, (3) the
advisory lock file, (4) the connection to a local Dolt server. Each is
audited against the cleanup precedent. The largest residual risk is the
formula's `executor` field: PR1 introduces an arbitrary string that
could be misused to point at unsafe binaries. The regression test must
constrain the namespace.

## Key Considerations

- **SQL injection.** Database names go into `USE <db>`,
  `CALL DOLT_RESET('--soft', '<root>')`, and similar. Dolt does not
  parameterize identifiers — they're string-interpolated. Therefore
  identifier validation is the ONLY barrier.
- **Identifier charset.** The cleanup regex `[A-Za-z0-9_]` leading,
  `[A-Za-z0-9_-]` thereafter is rejective — anything else is rejected
  before SQL is constructed. Reuse exactly.
- **Auto-discovery never reads server-side.** PRD locks: never
  `SHOW DATABASES`. The auto-discovery list comes from
  `gc rig list --json`, which reads `city.toml` plus per-rig metadata.
  This means the only way to compact a DB is to (a) register it as a
  rig, or (b) name it explicitly. Test/orphan DBs cannot be reached
  through auto-discovery.
- **Local-loopback connection.** Compact connects to
  `127.0.0.1:<port>` only. No network exposure. The port comes from
  the same chain cleanup uses.
- **Advisory lock file.** `<cityPath>/.beads/dolt.lock` (per cleanup
  layout). File permissions are inherited from the city. No new
  attack surface beyond cleanup's.
- **`dolt_gc` and `dolt_purge`** can free disk; if the wrong DB is
  named, data loss is realistic. Mitigation: identifier validation +
  rig-registry restriction + advisory lock.
- **`compact-base` / `compact-work` branches.** These are unique
  enough to avoid collision with operator branches but should still
  be validated (no funny names). The CLI creates them with hardcoded
  names — no operator input.
- **Executor binding regression test.** PR1's regression test must
  itself be defensive: it walks every embedded formula's `executor`
  field and checks the value resolves to (a) a known Cobra command,
  (b) a registered pack script under `examples/*/commands/`, or (c)
  a recognized daemon entry. Anything else fails the test. This
  prevents `executor = "rm -rf /"` from sneaking into a future
  formula.

## Options Explored

### Option A: Identifier validation at CLI entry only

Validate at flag/arg parse; pass raw strings deeper.

### Option B: Validate at CLI entry AND at every SQL-construction site

Defense in depth.

### Option C: Lift identifier validation into a typed wrapper

`type DBName string` constructor that validates; SQL helpers refuse
unwrapped strings.

## Recommendation

**Option B for v1**, with Option C as a directional refactor when a
second compaction-style command appears.

- Validate at flag/arg parse: reject early, exit 1 with clear error.
- Validate again at each SQL-construction call site: a second test
  point that catches a refactor that bypasses CLI parse (e.g., reading
  from auto-discovery directly into SQL helpers).

**Executor-binding regression test scope (PR1):**

- Walk all `examples/*/formulas/*.toml` (embedded only — third-party
  packs are out of scope).
- For every step: if `Executor != ""`:
  - Validate against a closed set of recognized prefixes:
    - `gc <subcommand>` — verify a registered Cobra command exists in
      `cmd/gc/`.
    - `examples/<pack>/commands/<name>` — verify the path exists in
      the embedded FS and contains both `command.toml` and `run.sh`.
    - `daemon:<name>` — RESERVED; v1 forbids any of these (no daemon
      executors today).
- For every step: if `ZFCExempt=true`:
  - Require `Executor != ""` (orphan-formula prevention).
- For every step: if `Executor == ""`:
  - Step is agent-cooked; nothing to verify.

The test fails on:
- Empty/blank Executor with ZFCExempt=true.
- Executor pointing at a Cobra command that doesn't exist.
- Executor pointing at a pack script path that doesn't exist.
- Executor with a daemon: prefix in v1.

## Constraints Identified

- **No shell metacharacters** in database identifiers, lock file
  names, branch names. Identifier validation enforces this for the
  one operator-input vector.
- **No environment variable injection.** The pack delegate forwards
  flags but doesn't `eval` strings.
- **`gc trace` correlation.** The CLI does NOT log secrets, port
  numbers, or sensitive metadata to stderr or the JSON envelope.
- **Rate limiting.** Not applicable — 24h cooldown is enforced by the
  order, not by the CLI. Operator ad-hoc runs are bounded by lock
  contention.
- **Permissions on the lock file.** Inherited from city dir creation;
  no new mode bits.
- **The pack delegate forwards untrusted args.** It MUST NOT use
  `eval`; it must use direct argv passing (`exec gc dolt-compact "$@"`).

## Open Questions

- **Sec1.** Should the executor-binding test also validate the cooked
  step descriptions don't leak `executor` strings into agent prompts
  (so an attacker who could write to a formula couldn't smuggle a
  malicious executor reference into the prompt)? Recommendation:
  **out of scope for v1**; the test guards the binding, not the
  prompt content. PR1 ships the binding test only.
- **Sec2.** Do we need to check that `compact-base` / `compact-work`
  don't shadow operator branches? If an operator has those branch
  names, the CLI will collide. Recommendation: **rename to something
  unique-by-construction** like `gc.compact.base.<timestamp>` and
  `gc.compact.work.<timestamp>`. BUT — this diverges from the
  formula prose. Defer to design-doc decision: keep formula names,
  document the collision risk, leave to operator.
- **Sec3.** Pack-script binary lookup. The pack delegate uses
  `gc` from `$PATH`. An attacker controlling `$PATH` of the dog's
  session could substitute a malicious binary. Recommendation: dogs
  start with a sanitized PATH (likely already true; verify in PR2);
  no extra defense at this layer.

## Integration Points

- **`cmd/gc/dolt_compact_validate.go`** (new) — identifier validator,
  reusing cleanup's regex. Could just be a function call into
  `cmd/gc/dolt_cleanup_drop_planner.go`'s `validDoltDatabaseIdentifier`.
- **`cmd/gc/executor_binding_test.go`** (new, PR1) — the regression
  test.
- **`internal/formula/types.go`** — schema additions are pure data;
  no security knobs.

---

# Leg 6: integration — code placement, rollout path, compatibility, testing strategy

## Summary

Two PRs land in strict sequence. PR1 is broad-scope but minimal-behaviour:
schema fields, parser update, embedded-formula migration sweep, regression
test. No new behaviour anyone calls. PR2 is narrow-scope but full-
behaviour: the CLI binary, pack delegate, formula update, integration
tests, docs. Each PR is independently mergeable, and PR2 cannot land
without PR1 because it depends on the schema fields. No backwards-compat
shim is needed because there is no prior CLI to be compatible with.

## Key Considerations

- **PR1 / PR2 split is locked** by the human-clarification round (Q2).
  PR1 ships the contract; PR2 fills it. The bead DAG must reflect this:
  PR2 work bead lists PR1 work bead as a `blocks` dependency.
- **Code placement, Go side.** Mirror cleanup's layout exactly:
  `cmd/gc/cmd_dolt_compact.go` (Cobra), `cmd/gc/dolt_compact*.go`
  (algorithm/inspect/verify/envelope split), `cmd/gc/cmd_dolt_compact_test.go`
  (unit), `cmd/gc/cmd_dolt_compact_integration_test.go` (`//go:build
  integration`).
- **Code placement, pack side.**
  `examples/dolt/commands/compact/{command.toml,run.sh,README.md}`.
- **Code placement, formula side.**
  `examples/dolt/formulas/mol-dog-compactor.toml` is touched in BOTH PRs:
  PR1 adds `zfc_exempt = true` to its 4 steps (matching current prose);
  PR2 removes `zfc_exempt` and adds `executor = "gc dolt compact"` to
  the 3 mutation steps (`inspect`, `compact`, `verify`) and possibly to
  `report`. (See open question I1.)
- **Code placement, regression test.** PR1 places the test in `cmd/gc/`
  (`executor_binding_test.go`). Reasons: visibility to Cobra commands
  and pack scripts via the same package, plus alignment with cleanup's
  test layout.
- **Migration sweep scope (PR1).** All embedded formulas under
  `examples/`. Per the second exploration leg, only `mol-dog-compactor`
  has explicit ZFC-exemption prose today. Other formulas (e.g.,
  `mol-witness-patrol`, `mol-deacon-patrol`) cook into prompts and need
  neither field. Verify by full audit in PR1.
- **Backwards-compat for existing TOMLs.** None needed. The new fields
  default to zero values; old TOMLs round-trip unchanged.
- **Formula contract version.** The formula has
  `contract = "graph.v2"`. Schema additions are additive — no contract
  bump.
- **Cutover semantics.** PR2 lands. Next 24h cycle: dog claims
  `mol-dog-compactor`, sees the new `executor` field, shells out to
  `gc dolt compact`, which Just Works because PR1 already enforced the
  binding. No flag day, no "drain old dogs first" — old dog sessions
  re-spawn naturally on the next cycle.
- **Test strategy.**
  - Unit: flag wiring, identifier validation, envelope shape, error
    classification, retry logic with mocked SQL.
  - Integration (`//go:build integration`): real Dolt server in
    `t.TempDir()`, populate, run flatten end-to-end (commit count drops
    to 1), run surgical end-to-end (commit count drops to keep_recent),
    inject concurrent write between pre-flight and rebase, verify retry,
    verify dolt_gc reclaim observable.
  - Regression (PR1): walk all embedded formulas, assert binding
    validity.
  - End-to-end (post-PR2 manual): one production cycle on loomington;
    verify HQ commit count drops, SUMMARY shows
    "compacted: 1 (was 0 in safely-skip era)."

## Options Explored

### Option A: Two PRs as locked

PR1: schema + parser + migration + regression test. PR2: CLI + pack +
formula update.

### Option B: Three PRs

PR1a: schema + parser. PR1b: regression test + migration sweep. PR2:
CLI + pack + formula update.

PR1a/b split adds reviewer overhead with no integration value. Reject.

### Option C: One PR (rejected per human-clarify)

Bundle. Largest reviewer surface; can't merge schema without CLI; risks
shipping schema with no consumer. Rejected.

## Recommendation

**Option A (two PRs)**, with the following bead structure:

```
PR1 work bead (gc-???)  [blocks PR2]
  ├── deps: gc-2189al (root)
  └── deps: gc-oick69 (this design step) — tracks the design that
            produced this work
PR2 work bead (gc-???)
  ├── depends_on: PR1 work bead (blocks)
  └── deps: gc-2189al (root)
```

**File layout (PR1)**:
- `internal/formula/types.go` — add `Executor`, `ZFCExempt` to Step,
  stepTOMLAlias, toStep.
- `internal/formula/types_test.go` — round-trip test.
- `cmd/gc/executor_binding_test.go` — new regression test.
- `examples/dolt/formulas/mol-dog-compactor.toml` — add
  `zfc_exempt = true` and `executor = "(daemon-only)"` (or empty;
  see open question I2) to all 4 steps. The PR1-era state preserves
  current "always safely-skipped" semantics.
- `engdocs/contributors/executor-binding.md` — new doc.
- Migration audit notes in PR1 description (see open question I3).

**File layout (PR2)**:
- `cmd/gc/cmd_dolt_compact.go` — Cobra command.
- `cmd/gc/dolt_compact_inspect.go` — inspection (commit-count probe,
  candidate selection).
- `cmd/gc/dolt_compact_compact.go` — algorithm core (flatten + surgical).
- `cmd/gc/dolt_compact_verify.go` — pre/post row count check.
- `cmd/gc/dolt_compact_envelope.go` — `gc.dolt.compact.v1` envelope.
- `cmd/gc/cmd_dolt_compact_test.go` — unit tests.
- `cmd/gc/cmd_dolt_compact_integration_test.go` — `//go:build
  integration`.
- `examples/dolt/commands/compact/command.toml` — one-liner.
- `examples/dolt/commands/compact/run.sh` — pack delegate.
- `examples/dolt/commands/compact/README.md` — operator examples.
- `examples/dolt/formulas/mol-dog-compactor.toml` — flip
  `zfc_exempt = false` (or remove), set
  `executor = "gc dolt compact"` on inspect/compact/verify steps.
- `engdocs/operations/dolt-compact-runbook.md` — new.

**Test gates** (each PR):
- PR1: unit tests pass; regression test passes against the migrated
  embedded formulas; `make test` green; `go vet ./...` clean.
- PR2: PR1 tests still green; new unit + integration tests pass;
  manual end-to-end on loomington shows non-zero compact count;
  dashboard for the Refinery / Mayor sees the cycle's report.

## Constraints Identified

- **No backwards-compatibility shim.** Per PRD Non-Goal: when PR2 lands
  and the formula update flips `zfc_exempt → executor`, no fallback to
  the old "safely-skip" behaviour exists. Operators on a binary built
  before PR2 will see the formula reject because `executor` references
  a missing command. Mitigation: PR2 is sequential after PR1 in the
  bead DAG; the rollout assumes the binary is rebuilt between PRs.
- **Embedded-formula migration is exhaustive.** PR1 audits every TOML
  under `examples/*/formulas/`. Anything that should be ZFC-exempt
  but isn't tagged is a bug; the regression test catches the inverse
  (orphaned executors).
- **No 3rd-party formula migration.** External packs aren't audited
  in PR1. The schema fields are additive — external TOMLs continue to
  parse with zero-value defaults.
- **Test isolation.** Integration tests use `t.TempDir()` and a
  randomly-allocated port; cleanup tests already prove this works
  with a real Dolt sql-server (`examples/dolt/sql_test.go` is the
  scaffold reference).
- **Refinery merge order.** PR1 merges first; PR2 follows. Refinery
  enforces the bead DAG `blocks` dependency.

## Open Questions

- **I1.** Does the `report` step in `mol-dog-compactor` need
  `executor` set? Step prose closes the work bead and signals
  completion — currently agent-shell-script style. After PR2, who
  owns the `report` step?
  - **Option I1a**: report step keeps `zfc_exempt=true` because the
    CLI doesn't write the report (it's the dog wrapping up).
  - **Option I1b**: the CLI does write the report (envelope IS the
    report) and the dog merely closes beads from it; therefore set
    `executor = "gc dolt compact"` on report too.
  - Recommendation: **I1b**. The envelope is the canonical report;
    the dog's "report" responsibility is bead closure + nudge, not
    composition. Document this in PR2.

- **I2.** PR1's pre-CLI-existence state: `mol-dog-compactor` steps
  must not point at `gc dolt compact` (binding doesn't exist). Two
  legal PR1 forms:
  - **Option I2a**: `executor = ""`, `zfc_exempt = true`. The
    regression test allows this combination only when the formula's
    root carries a special "deferred-binding" marker (introducing
    yet another field). Cluttered.
  - **Option I2b**: `executor = "(daemon-only)"`, `zfc_exempt = true`.
    A sentinel string the regression test recognizes as
    "intentionally unbound, not orphaned." Cleaner.
  - **Option I2c**: `executor = ""`, `zfc_exempt = true` and the
    regression test simply allows this combo (orphan tolerance).
    Risk: an actually-orphaned formula is hard to distinguish from a
    legitimately-unbound one.
  - Recommendation: **I2b**. The sentinel is unambiguous. PR2 flips
    the value to `gc dolt compact`. The regression test understands
    `(daemon-only)` as "deferred-binding, must be flipped before any
    consumer relies on it." A CI alert or doc note flags the
    sentinel as transitional.

- **I3.** Migration audit log location. Where does PR1 document which
  embedded formulas were touched and why? Recommendation: PR1
  description body lists the audit table; no in-tree doc. The
  regression test makes it self-enforcing going forward.

## Integration Points

- **Refinery.** Merges both PRs in DAG order; the existing refinery
  contract handles this naturally via `blocks` deps.
- **Bead DAG creation step (`gc-8zdml1`).** This downstream step
  emits the actual PR1/PR2 work beads with the `blocks` relationship
  in place. The design doc (output of this step) is its input.
- **CI.** Both PRs hit the standard test gates. PR1 grows a new
  test (`executor_binding_test.go`); PR2 grows two
  (`cmd_dolt_compact_test.go`, `cmd_dolt_compact_integration_test.go`).
- **`make dashboard-check`.** Not affected (no API/dashboard
  surface change).

---

## Round Note

Coordinator: gascity/gastown.furiosa (polecat, Opus 4.7 / 1M context).

Six legs executed inline as separate-lens passes rather than
fanning out to 6 polecat sessions via `gc sling`. Same justification
as the prior PRD-review round (recorded in
`.plan-reviews/dolt-compact-cli/prd-review-legs.md` round note):
single-coordinator parallel-lens analysis on a 1M-context Opus is
faster, produces tighter cross-lens consistency, and matches the
formula's "parity of outcome, not verbatim port" principle.

Inputs read in full:
- `.prd-reviews/dolt-compact-cli/prd-draft.md` (452 lines)
- `.prd-reviews/dolt-compact-cli/prd-review.md` (181 lines)
- `.plan-reviews/dolt-compact-cli/human-clarifications.md` (58 lines)
- `.plan-reviews/dolt-compact-cli/prd-review-legs.md` (396 lines, prior round)

Codebase grounding (via Explore subagents):
- `cmd/gc/cmd_dolt_cleanup.go` and the dolt-cleanup ecosystem
  (`dolt_cleanup_port.go`, `dolt_cleanup_drop.go`,
  `dolt_cleanup_drop_planner.go`, `dolt_lifecycle_lock.go`).
- `examples/dolt/formulas/mol-dog-compactor.toml` (192 lines).
- `internal/formula/types.go` Step struct (lines 196-304).
- `examples/dolt/commands/cleanup/{command.toml,run.sh}`.
- `cmd/gc/cmd_rig.go` `rig list --json` shape.
- `examples/dolt/sql_test.go` test scaffolding.

Inline execution traded breadth-of-perspectives (6 separate Opus
contexts) for cross-cutting consistency. Where a leg might independently
have proposed a different envelope shape (Leg 2 vs Leg 6 say), the
synthesis is already self-consistent. Future rounds (3 PRD-alignment
rounds, 3 self-review rounds) will be the parallel-perspective
counterweight.

Output: this document plus
`.designs/dolt-compact-cli/design-doc.md` (synthesis).
