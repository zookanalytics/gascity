# Plan Self-Review Round 2 — Leg Reports

Round: plan-review-2 (mol-idea-to-plan.plan-review-2, bead `gc-s6epvv`)
Date: 2026-05-04
Coordinator: gascity/gastown.furiosa

Two legs (`risk`, `scope-creep`) executed inline, matching the
round-1 + prd-align-round-{1,2,3} pattern of inline coordinator
review with TSV manifest. Inputs read end-to-end:

- `.designs/dolt-compact-cli/design-doc.md` (post round-1 plan-review)
- `.prd-reviews/dolt-compact-cli/prd-draft.md`
- `.plan-reviews/dolt-compact-cli/prd-align-round-{1,2,3}.md`
- `.plan-reviews/dolt-compact-cli/review-round-1.md`
- `.plan-reviews/dolt-compact-cli/human-clarifications.md`
- `examples/dolt/formulas/mol-dog-compactor.toml` (current formula state)
- `examples/gastown/packs/maintenance/agents/dog/prompt.template.md`
  (dog runtime model — agent-driven, prompt-templated)

Each leg produces `must-fix` and `should-fix` findings with concrete
pointers into the design doc. Per the step description, all are applied
to the design doc unless objectively wrong; rejections are recorded in
the round log.

This round shifts focus from completeness/sequencing (round 1) to
**risk and scope-creep** — technical, dependency, rollback, and
unknown-unknown risks the design carries that warrant explicit
mitigation; gold-plating, over-engineering, premature optimization,
and defer candidates that could be cut without compromising the
acceptance criteria.

---

## Leg 1 — risk

**Charter.** Walk the design and plan looking for risks the design
does not yet mitigate explicitly: technical (algorithm re-entrancy,
SQL execution, error classes), dependency (substrate availability,
cross-PR coordination), rollback (PR-level, intra-cycle), and
unknown-unknown (multi-host, multi-server, future evolution).

The plan covers the consequential algorithmic risks (R-3 concurrent-
write surgical, R-4 dolt_gc deadline, R-7 lock leaks, R-10 in-CLI
rollback, R-12 cache-write failure). What this leg surfaces is
**one structural contradiction** between the design's "dog writes
metadata" contract and two plan/risk artifacts that read as if the
CLI writes (M1), plus four taxonomy/clarity gaps that an
implementer or operator could trip over (S1-S4).

### M1 (must-fix) — Cache-write authority is ambiguous between "CLI writes" and "dog writes"

**Finding.** The design's Step-execution contract / Dog-runtime
mechanism subsection (`design-doc.md` lines 256-278) binds the cache
write to the dog:

> The dog updates the molecule via `gc bd update <molecule-id>
> --set-metadata gc.formula.envelope='<json>'` immediately after
> parsing the CLI output and before closing the `compact` step bead.

Per round 1's M1 binding, the dog (an LLM agent driven by formula
step descriptions and `examples/gastown/packs/maintenance/agents/
dog/prompt.template.md`) is the writer; the CLI's only output to
the substrate is the envelope on stdout, which the dog reads, parses,
and writes via the bd shell command.

Two artifacts elsewhere in the design read as if the CLI itself writes
the metadata:

1. **Phase 2 step 10's "cache-write happy path" unit test** (`design-
   doc.md` lines 1342-1346):
   > "the **cache-write happy path** (the compact-step orchestration
   > writes `gc.formula.envelope` to the molecule's metadata before
   > exiting — discharges M1's contract, R-12's CLI-side detection)"

   Phase 2 step 10 is the consolidated CLI-unit-test checklist (CLI
   modules in `cmd/gc/` per Phase 2 steps 1-9). A unit test in
   `cmd/gc/` cannot exercise dog-side prose-driven behavior. Read
   straight, this entry directs an implementer to add CLI code that
   issues `gc bd update --set-metadata` itself — a feature the
   design does not describe (no `--molecule-id` flag on the CLI
   surface; no bd dependency on the CLI side).

2. **R-12's last sentence** (`design-doc.md` lines 1089-1091):
   > "Additionally, the integration test asserts the cache write
   > happens; CI catches a missing-write regression on the CLI side
   > (independent of the bd-unavailable runtime case)."

   The "CLI side" framing is wrong if cache-write authority is
   dog-side. The integration test that exercises the cache write is
   Phase 2 step 11's formula-driven leg (load `mol-dog-compactor.
   toml`, exec the executor, assert the dog claim path's outcome).
   That test is dog-and-formula side, not CLI side.

The contradiction is load-bearing for two reasons:

- An implementer following Phase 2 numerically may add a CLI-side
  `--molecule-id` flag and bd-shell-out to satisfy the step 10 test,
  duplicating the dog-side write and creating belt-and-suspenders
  the design does not contemplate. The duplicate write would race
  with the dog's own write under retry scenarios.
- A reviewer reading R-12 alone sees "CI catches the regression on
  the CLI side" and may approve a PR2 that ships CLI-only test
  coverage, missing the dog-side prose-driven write that is the
  actual regression surface.

**Resolution options.**

| Option | Description | Pro | Con |
|--------|-------------|-----|-----|
| **A** — keep "dog writes" model; reframe Phase 2 step 10 + R-12 | The CLI emits envelope to stdout only; dog parses and writes via bd shell. Phase 2 step 10's "cache-write" entry is reframed as an envelope-shape/stdout test. R-12's "CLI side" wording is corrected to point at the formula-driven integration test as the regression catcher. | No new CLI dependency on bd; matches design's existing dog-runtime mechanism prose; smallest delta to the plan. | Cache write happens via dog prose-instruction, not enforceable by Go-side type-checking; only the formula-driven integration test catches a regression in the dog-instruction prose. |
| **B** — make CLI also write (belt-and-suspenders) | Add `--molecule-id` flag to CLI; CLI and dog both write metadata. Phase 2 step 10's entry stays; the CLI gains a bd-shell dependency. | CI catches the missing-write regression on the CLI side (the existing R-12 wording becomes correct); dog-side prose is a backup, not the only path. | Adds a CLI dependency on bd substrate (the CLI now must know how to address the molecule and shell out to bd); duplicate writes race under retry; new flag on the CLI surface; new integration scenarios. |
| **C** — make CLI the sole writer; dog never writes | CLI gets `--molecule-id` flag; dog only reads. | Single substrate-writer, simpler authority. | Dog must pass the molecule id to the CLI on every invocation (new formula-var); first-CLI-cycle requires the molecule to exist before invocation; cache write becomes a CLI failure mode that loses graceful-degradation (R-12's pre-existing fallback on the dog side). |

Option **A** is the closest to the design's existing prose and the
smallest delta to round 1's M1 binding. Option B trades a minor
race-under-retry concern for a stronger CI catch. Option C reverses
round 1's binding and is the largest delta.

**Recommendation.** Apply option **A**:

- Phase 2 step 10's "cache-write happy path" entry → reframed as
  "envelope-stdout shape and parseability test" (the CLI emits a
  parseable `gc.dolt.compact.v1` envelope to stdout that the dog
  can shell-pipe through `gc bd update --set-metadata`). The CLI's
  responsibility ends at stdout.
- R-12's last sentence → rewritten to point at Phase 2 step 11
  (formula-driven integration test) as the regression catcher. The
  test loads `mol-dog-compactor.toml`, exercises the dog-claim path
  (or its mocked equivalent), and asserts the metadata is populated
  by the prose-instructed write.
- Step-execution contract / Dog-runtime mechanism subsection unchanged
  (the binding paragraph already reflects option A).

### S1 (should-fix) — `dolt_gc` failure not enumerated in the error taxonomy

**Finding.** The Algorithms / Flatten and Algorithms / Surgical
pseudocode both end with `CALL dolt_gc()` immediately before
releasing the lock. The 9-class error taxonomy (`design-doc.md`
lines 471-486) covers `database-deadline-exceeded` (gc hung past
`--per-db-timeout`) and `internal-error` (catch-all unexpected SQL
or Go-side error), but does not enumerate non-deadline gc failures
(out of disk, gc-internal error, gc-aborted-by-another-connection,
gc returning a non-fatal warning class).

The compaction itself succeeded at this point (commit was made,
row counts matched). What is the per-DB outcome?

- Map to `internal-error` (escalate to mayor): alarmist —
  compaction is healthy, only the disk-reclaim step failed. The
  next cycle's gc will reclaim what this one missed (R-4 mitigation
  paragraph already articulates this for the deadline case).
- Map to `ok` and silently swallow: loses operator visibility into
  a real disk-reclaim regression.
- Add a new class `dolt-gc-failed` (close `closed` with a
  warning-level log line): captures the operational state precisely
  but adds a 10th class, breaking TD-6's "9 classes" framing.
- Extend `internal-error` semantics to distinguish gc-only failure
  via the per-DB `error_message` field: keeps the 9-class count;
  operators read the message to disambiguate.

**Apply.** Add a paragraph under "Error Class Taxonomy" enumerating
the per-DB outcome on `dolt_gc` failure. Recommendation: extend
`internal-error` semantics — the per-DB `outcome = internal-error`
with `error_message` carrying the gc-failure detail — and add a
parenthetical to the table row spelling this out. R-4 also extends
to mention the non-deadline gc failure path. Phase 2 step 10's
unit-test list adds an "internal-error on gc failure carries the
gc-failure detail in error_message" case.

### S2 (should-fix) — Dog idempotence on restart loses first-invocation observability

**Finding.** The cache write happens after CLI exit and before the
compact step bead closes (per Step-execution contract / Dog-runtime
mechanism). If the dog crashes after CLI exit but before the
`gc bd update --set-metadata` write, the metadata is empty when the
next dog claims the (still-open) compact step bead. The next dog's
instruction reads metadata empty and re-invokes the CLI.

The re-invocation is **safe** at the DB level: the previous
invocation's flatten or surgical work has already landed on the DB
(commit count is now below threshold, or HEAD is at the keep-recent
state); the second invocation sees `below-threshold` or skips,
producing a "no-op-ish" envelope. The flock prevents truly-concurrent
re-invocation.

The risk is **observability**: the second invocation's envelope
clobbers the first's via the metadata write. The first envelope
(which actually performed the compaction) is lost. Operators reading
metadata see the second (no-op) envelope and may conclude no
compaction happened this cycle. The deacon report and post-cycle
verification (Phase 3 step 4) would diverge from the metadata
record.

**Mitigations the design has implicitly:**

- Dolt commit-count delta is observable independently (Phase 3 step
  3); the metadata loss is a layered-observability degradation, not
  a state corruption.
- The lock-then-cleanup ordering (Algorithms / Surgical) is unaffected
  — the second invocation's no-op path drops nothing important.

**Mitigation the design lacks:** a "read first, skip if cached"
guard on the compact step's formula description so a second dog
claim consumes the existing cache instead of re-invoking the CLI.

**Apply.** Extend the compact step's formula description (per
PR2 step 11 / Phase 2 step 12) with a leading guard:

> Read `metadata.gc.formula.envelope` from the molecule via
> `gc bd show <molecule-id> --json | jq -r '.[0].metadata."gc.
> formula.envelope"'`. If non-empty and parseable, skip the CLI
> invocation and the metadata write — proceed directly to closing
> the compact step bead from the cached envelope per the Per-step
> bead mapping table. If empty or unparseable, invoke the CLI as
> currently described, parse the envelope from stdout, and write
> back to metadata.

This makes the compact step idempotent across dog restarts: the
first dog's CLI invocation produces the canonical envelope; the
second dog (if it claims the same bead) consumes the cache and
closes the bead from the canonical record. R-12's existing
"empty-or-unparseable → close as `closed`" fallback applies only
to the bookkeeping steps (whose closure depends on the cache),
not to the compact step itself.

A new R-14 captures the failure mode and mitigation; the Phase 2
step 11 formula-driven integration test scope extends to cover
the cache-hit path explicitly.

### S3 (should-fix) — `database-not-found` not enumerated in the error taxonomy

**Finding.** A typo in a positional DB arg or a stale rig-registry
entry can result in `gc dolt-compact nonexistent_db` reaching SQL
execution with `USE nonexistent_db`. The validator passes (charset
OK); `USE` fails with the Dolt server's "Unknown database" error.

The 9-class taxonomy maps this to either `database-unreachable`
(misleading — the server is up; only this DB is missing) or
`internal-error` (alarmist — escalate to mayor for an operator
typo). Neither is the right operator-facing signal.

**Apply.** Add a 10th class `database-not-found` to the Error
Class Taxonomy:

| Class | Meaning | Dog action |
|-------|---------|------------|
| `database-not-found` | Dolt server is reachable; the named DB does not exist (typo, stale rig-registry, manual DB drop) | close `failed` (operator error); deacon nudge |

Class string is stable and additive — pre-PR2 envelopes never
emitted it (compactor never ran). Per-DB record's `error_message`
carries the Dolt error verbatim ("Unknown database 'foo'"). Phase
2 step 10 unit-test list extends to cover the `database-not-found`
path (mocked `*sql.DB` returning `Error 1049`); Phase 2 step 11
integration test extends to drive a real `USE` against a missing DB.

This temporarily brings the taxonomy to 10 error classes (plus
`ok`); leg 2's S5 fold returns it to 9 error classes (plus
`ok`); the count remains within TD-6's "fewer than 15" framing.

### S4 (should-fix) — R-12's "CLI side" detection wording is incorrect

**Finding.** R-12's last sentence:

> "Additionally, the integration test asserts the cache write
> happens; CI catches a missing-write regression on the CLI side
> (independent of the bd-unavailable runtime case)."

If cache-write authority is dog-side (per M1's resolution and the
existing Step-execution contract / Dog-runtime mechanism prose),
"CLI side" detection is the wrong framing. The integration test
that asserts the cache write is Phase 2 step 11's formula-driven
leg, which exercises the dog claim path (the prose-instructed
write), not a CLI-side `gc bd update` call.

**Apply.** Rewrite R-12's last sentence:

> "Additionally, the formula-driven integration test (Phase 2
> step 11) loads `mol-dog-compactor.toml`, exercises the compact
> step's executor invocation, and asserts the molecule's
> `gc.formula.envelope` metadata is populated post-cycle. CI
> catches a missing-write regression in the formula step
> description prose (independent of the bd-unavailable runtime
> case)."

The wording aligns the regression-detection surface (formula
description prose) with the actual write authority (dog).

---

## Leg 2 — scope-creep

**Charter.** Walk the design looking for items that could be cut,
deferred, or simplified without compromising acceptance criteria
or the silent-failure-recovery objective. Gold-plating
(unjustified-addition), over-engineering (more-mechanism-than-
needed), premature optimization (perf work without measurement),
defer candidates (nice-to-have but not v1).

The design is generally well-disciplined here — TD-10 already defers
`--inspect-only`, `--dry-run`, event-bus emission, and NDJSON to v2;
TD-9 rejects the daemon path; OQ-4's resolution defers
`per_db_timeout` formula-var promotion to v2 with an explicit trigger.
Round-1 added scope (CHANGELOG entries, PR Rollback subsection, Owner
paragraph for Phase 3, R-12, R-13) but each addition discharges a
specific contract gap.

What this leg finds is **one error-class redundancy** that adds
taxonomy noise without surfacing operator action (S5). No further
gold-plating or premature optimization to flag. No defer candidates
beyond TD-10's existing list.

### S5 (should-fix) — `concurrent-write-collision` error class is semantically empty

**Finding.** The Error Class Taxonomy (`design-doc.md` lines 471-486)
lists `concurrent-write-collision` with:

| Class | Meaning | Dog action |
|-------|---------|------------|
| `concurrent-write-collision` | surgical retried internally | observability only; final outcome may be `ok` |

The class is defined as "observability only" with no terminal-
state semantics. Walking the surgical algorithm pseudocode:

- Surgical retries once on graph-change error with a 2s pause.
- If retry succeeded: the per-DB record's `outcome = ok`,
  `attempts = 2`.
- If retry failed: the per-DB record's `outcome =
  concurrent-write-fatal`, `attempts = 2`.

The per-DB `outcome` field is never `concurrent-write-collision`
in either branch. The "collision happened, retry succeeded"
observability is captured by `attempts > 1` AND `outcome = ok`.
The "collision happened, retry failed" observability is captured
by `outcome = concurrent-write-fatal` (already in the taxonomy as
the escalate path).

The class adds taxonomy noise (10th row, soon to be 11 with leg 1's
`database-not-found`) without a dog action distinct from "observe
`attempts` and `outcome` together." TD-6's framing ("9 classes
that map to four dog actions") is undermined by a class that has
no dog action.

**Possible interpretations the class might intend (and why each
collapses):**

| Interpretation | Why it collapses |
|----------------|-------------------|
| Per-DB transient-state class for retries-in-flight | Envelope is post-execution; transient states are not surfaced. |
| Top-level summary annotation | Summary block has `compacted`/`skipped`/`failed` counts; collisions are not a counter target. |
| Future-proofing for multi-retry semantics | TD-10 defers retry-tuning; current retry is bound at one. |

**Apply.** Remove `concurrent-write-collision` from the Error
Class Taxonomy table. The per-DB `attempts` field (already in the
envelope shape on `design-doc.md` lines 695-720 — see the "attempts":
1 example) becomes the sole observability handle for retries:
`attempts > 1` AND `outcome = ok` — retry succeeded; `attempts > 1`
AND `outcome = concurrent-write-fatal` — retry failed.

The taxonomy drops to 8 error classes (plus `ok`); combined with
leg 1's S3 add, net is back to 9 error classes (plus `ok`).
TD-6's "9 classes" prose framing intact (counts error classes
only). Phase 2 step 7 (envelope) and step 10 (envelope-shape
unit test) carry forward unchanged — the `attempts` field is
already first-class. The post-PR2 dashboard or deacon-patrol
queries for "did surgical retry?" use `attempts > 1` instead of
`outcome IN ('concurrent-write-collision')`.

### Items rejected as defer candidates

The following were considered as defer candidates and rejected;
recording the rejection rationale:

- **`phases.{inspect,compact,verify,report}` aggregate block.**
  Discharges Goal 6 ("close inspect/compact/verify/report step
  beads independently"). Without it, dogs derive aggregate
  status from the per-DB array — splitting the closure rule
  between CLI Go and dog-side prose. Keep.
- **9-class (now 8-after-S5, 9-after-S3) error taxonomy.** TD-6
  defended this. Operator-facing observability classes are
  warranted. Keep.
- **CHANGELOG entries (Phase 1 step 6, Phase 2 step 14).** Repo
  convention; round 1 S1 added these for cause. Keep.
- **PR Rollback subsection (round 1 S2).** Compliance/incident-
  response value. Keep.
- **Phase 3 Owner paragraph and step 5 recording (round 1 S7).**
  Auditability of PRD Acceptance 2's verification. Keep.
- **R-13 (formula reorder surprises reviewers).** Preserves the
  conceptual narrative in step descriptions; comment block is a
  small doc cost. Keep.
- **`--per-db-timeout` (TD-5) and the `database-deadline-
  exceeded` error class.** Mitigates R-4 (hung dolt_gc). Keep.
- **Pack delegate (`gc dolt compact`).** Cleanup parity per PRD
  default principle. Keep.
- **`pre_hash` field on per-DB record.** Discharges R-10 manual
  recovery. Keep.
- **Bookkeeping steps' `executor = "gc dolt-compact"` annotation.**
  Already explained in design (regression-test contract; documents
  underlying binding). Keep.

### Items already-deferred per existing trade-offs

For audit traceability, the following were correctly deferred to v2
in earlier rounds (no action needed in round 2):

- `--inspect-only`, `--dry-run`, event-bus emission, NDJSON streaming
  (TD-10).
- Daemon path (TD-9, Non-Goal).
- Structured-tag `executor` form (OQ-1 resolution).
- Formula-bound `per_db_timeout` var (OQ-4 resolution; promotion
  trigger documented).
- Per-database advisory lock (TD-4; promotion trigger: measured
  pain on city-wide flock).
- `--force-named` / per-DB threshold overrides (Threshold
  Semantics paragraph).

---

## Findings summary

| ID | Class | Leg | Topic |
|----|-------|-----|-------|
| M1 | must-fix | risk | Cache-write authority is ambiguous between "CLI writes" and "dog writes" |
| S1 | should-fix | risk | `dolt_gc` failure not enumerated in error taxonomy |
| S2 | should-fix | risk | Dog idempotence on restart loses first-invocation observability |
| S3 | should-fix | risk | `database-not-found` not enumerated in error taxonomy |
| S4 | should-fix | risk | R-12's "CLI side" detection wording is incorrect |
| S5 | should-fix | scope-creep | `concurrent-write-collision` error class is semantically empty |

1 must-fix, 5 should-fix, 0 rejected. All findings yield
mechanical, additive, or wording-only edits to the design doc and
the implementation plan; no structural reorganization of the design
is required.
