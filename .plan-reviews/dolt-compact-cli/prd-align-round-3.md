# PRD Alignment Round 3 — Round Log

Round: prd-align-3 (mol-idea-to-plan.prd-align-3, bead `gc-lygvkw`)
Date: 2026-05-04
Coordinator: gascity/gastown.furiosa

## Overview

Two legs (`user-stories-coverage`, `open-questions-resolution`) executed
inline. Reports captured in `prd-align-round-3-legs.md`; bead manifest
in `prd-align-round-3-beads.tsv` (matches the round-1 / round-2 pattern
of inline legs with TSV manifest).

The legs surfaced **1 must-fix** and **4 should-fix** items. Per the
step description ("Apply every must-fix and should-fix item unless it
is objectively wrong, and if you reject one, record why in the round
log"), all five were applied. None rejected.

This round closes the last remaining design-vs-PRD alignment gaps:
the contract ambiguity at the formula-step ↔ CLI-invocation boundary
that round 1's `phases` block addition surfaced but did not resolve
(M1), plus four design-Open-Question recommendations that needed
promotion to bound resolutions before plan-review begins (S1-S4).
The next phase is plan-review-1 (`gc-imyo2c`), which reviews the plan
itself rather than PRD alignment.

## Findings → Applied changes

### M1 — Story 2: step↔CLI invocation contract is ambiguous

**Finding (Leg 1).** PRD Story 2 (`prd-draft.md` lines 128-136) binds
the dog cycle path: "Dog claims a step bead, shells out to the CLI
with structured output, and closes per-step beads from the parsed
result." The design's Shape § said "[CLI] runs as one process that
emits a single final JSON envelope describing all four phases" while
PR2 step 11 said "Set `executor = "gc dolt-compact"` on every
executable step." A literal reading of the second statement implies
four CLI invocations per cycle — contradicting the first, wasting the
city-wide advisory lock, and racking up 4× `dolt_gc` overhead. The
reverse reading ("one invocation, four step closures from one
envelope") was consistent with Shape § and the round-1 `phases` block,
but the design never said which step triggers the invocation, how the
envelope reaches the non-trigger steps, or why the executor field
appears on every step if only one invokes the binding.

**Applied.**

- New subsection `### Step-execution contract` inserted between
  `### Shape` and `### Two-PR Shipping Order` (Proposed Design). The
  subsection binds:
  - **Per-cycle invocation rule.** The CLI is invoked exactly once
    per dispatched cycle. Three forces require this (lock contention,
    algorithm re-entrancy, envelope-driven step closure); each is
    spelled out as a numbered justification.
  - **Designated trigger step.** The `compact` step is the trigger.
    The dog claims the `compact` step bead, invokes
    `gc dolt-compact --json` with formula-bound flags, parses the
    envelope, closes `compact` immediately, and uses the cached
    envelope to close the other three step beads as they are claimed.
  - **Bookkeeping steps.** `inspect`, `verify`, `report` retain
    `executor = "gc dolt-compact"` (declares the binding for the
    Component-8 regression test) plus `zfc_exempt = true` (declares
    that the dog does not re-invoke the executor for these steps).
    The `executor` + `zfc_exempt` pair on the bookkeeping steps is
    the same shape the regression test accepts — the contract is
    unchanged, only the per-step binding posture differs.
  - **Dog-runtime mechanism.** Cache the envelope keyed by molecule
    id; consume from cache when a non-`compact` step bead is claimed.
    Bounded as a PR2 implementation detail; the design constrains the
    contract, not the cache shape.
- Two-PR Shipping Order PR2 step 11 reworded:
  - Was: "Set `executor = "gc dolt-compact"` on every executable
    step."
  - Now: "Set `executor = "gc dolt-compact"` on every step (declares
    the binding for the executor-binding regression test). Set
    `zfc_exempt = true` on the `inspect`, `verify`, and `report`
    bookkeeping steps; **only the `compact` step drops `zfc_exempt`
    and runs the CLI** (per Step-execution contract). Update the
    `compact` step's description to reference CLI flags using
    formula variable substitution; update `inspect` / `verify` /
    `report` step descriptions to reference the cached-envelope
    closure path."
- Per-step bead mapping (resolves Goal 6) — appended one cross-
  reference sentence to the closing paragraph: "The envelope itself
  is produced by the `compact` step's CLI invocation per
  Step-execution contract; the dog reads the cached envelope when it
  claims the `inspect`, `verify`, and `report` bookkeeping step
  beads."
- Implementation Plan / Phase 2 step 12 (Formula update) reworded to
  match PR2 step 11: cite Step-execution contract; specify the
  per-step `zfc_exempt` distribution (drop on `compact`, retain on
  the bookkeeping trio); only `compact` carries the full CLI
  invocation; bookkeeping steps reference the cached-envelope
  closure path.
- Risks and Mitigations / R-9 extended: the formula-driven
  integration test (Phase 2 step 11) now explicitly exercises the
  per-cycle invocation rule from Step-execution contract — it loads
  `mol-dog-compactor.toml`, resolves the `compact` step's executor,
  invokes the CLI exactly once, and asserts all four `phases.<step>`
  values plus per-DB outcomes drive correct step-closure decisions.
- Executive Summary: added a paragraph summarising round 3's five
  changes alongside the existing round 1 / round 2 paragraphs.

### S1 — OQ-1: bind `executor` field to free-form for PR1

**Finding (Leg 2).** OQ-1 (free-form vs structured `executor` field
shape) carried a recommendation ("free-form for PR1; revisit if a
non-CLI binding emerges") but no binding decision. PR1 lands the
schema; the schema choice is a contract, not a recommendation. If
left as a recommendation, PR1's implementer revisits the
structured-vs-free-form question without the prior reasoning anchored.
Future migration from free-form to structured is additive, so
committing now costs nothing.

**Applied.**

- OQ-1 promoted from "open" to a `### Resolutions` subsection
  (`#### OQ-1 (resolved). Free-form executor field for PR1`). The
  resolution binds:
  - The `executor` field is a free-form string in PR1.
  - Rationale: every binding the design contemplates today (Cobra,
    pack scripts) is identifiable by a single string token; the
    Component-8 regression test consumes the string directly.
  - The structured-tag form (`{ kind, command }`) is additive —
    deferred to v2; will be revisited only if a non-CLI binding
    emerges.
- PR1 step 1 (Implementation Plan / Phase 1) extended: appended a
  sentence noting "The `Executor` field is typed as `string` (a
  free-form binding identifier, e.g., `"gc dolt-compact"`) per
  OQ-1's resolution; the structured-tag form (`{ kind, command }`)
  is deferred to v2 and is additive when needed." This pins the
  schema choice at the implementation-spec level so PR1's
  implementer doesn't revisit.

### S2 — OQ-4: bind per-DB timeout default placement to CLI-only for v1

**Finding (Leg 2).** OQ-4 (per-DB timeout default placement) carried a
recommendation ("no formula var until pain emerges") but no binding
decision. TD-5 (extended in round 2) already documented
`--per-db-timeout` as a wrapper-level execution budget; OQ-4 was the
same question, just in different phrasing. Promoting OQ-4 to a bound
resolution closes the loop and adds a concrete trigger for future
promotion.

**Applied.**

- OQ-4 promoted to a Resolutions entry (`#### OQ-4 (resolved).
  --per-db-timeout lives on the CLI surface only for v1`):
  - `--per-db-timeout` defaults to `1h` and lives on the CLI surface
    only; the formula does not expose a `per_db_timeout` var in v1.
  - Rationale anchored to TD-5: wrapper-level execution budget vs
    formula-bound algorithm parameter.
  - Explicit promotion trigger: "at least one city's
    `database-deadline-exceeded` outcomes become a recurring patrol
    signal (a single timeout is operator-tunable from the CLI;
    persistent timeouts across cycles indicate the default is wrong
    for that city)."
  - Migration-path pointer: when promoted, the consistency test
    (Phase 2 step 10) extends to cover the new var; TD-5's "additive
    evolution" paragraph already describes the path.

### S3 — OQ-5: bind exit code precision

**Finding (Leg 2).** OQ-5 (exit code precision) carried a
recommendation ("Cleanup uses `0` for a no-op dry-run, so consistency
says `0` here too") but the design's existing exit-code paragraph
(top of Error Class Taxonomy section) didn't reflect it. The Story-2
auto-discovery-empty case (Leg 1 "items not flagged") fell cleanly
under this OQ.

**Applied.**

- The exit-code paragraph at the bottom of Error Class Taxonomy was
  rewritten as a 3-row table:
  - `0` — full success **or** no-op (zero candidates from
    auto-discovery; every named DB below `--threshold`; every per-DB
    outcome in {`ok`, `below-threshold`, `database-locked`
    (skipped)}).
  - `1` — at least one per-DB outcome in the escalate set
    ({`concurrent-write-fatal`, `integrity-mismatch`, `internal-error`,
    `database-deadline-exceeded`}).
  - `2` — invocation error before per-DB execution begins (bad flag,
    port unresolved, identifier failed charset check, explicit DB
    list with zero matches in rig-registry).
  - Cleanup-parity rationale appended.
- OQ-5 promoted to a Resolutions entry (`#### OQ-5 (resolved). Exit
  code precision`):
  - References the table inlined above.
  - Story-2 corollary documented: when auto-discovery returns empty,
    the CLI emits a well-formed envelope with
    `summary.databases_inspected = 0` and exits `0`; the dog closes
    all four step beads `closed`.

### S4 — OQ-7: move from Open Questions to Resolutions

**Finding (Leg 2).** OQ-7 (ZFC expansion in user-facing docs) was
already bound by TD-9, the PR1 step 1 spec, and the `ZfcExempt`
doc-comment in the Data Model section. Listing it as an unresolved
Open Question was misleading — plan-review readers would treat it as
outstanding work when it had already been decided. The fix is
audit-trail polish: explicit "resolved" entry preserves the
question-was-considered record while removing the misleading "open"
label.

**Applied.**

- OQ-7 promoted to a Resolutions entry (`#### OQ-7 (resolved). ZFC
  expansion is "Zero Framework Cognition"`):
  - "ZFC" expands to "Zero Framework Cognition" per AGENTS.md.
  - Three pin sites named: (1) `ZfcExempt` field's doc-comment in
    Data Model / Formula schema additions; (2) PR1 step 1 spec;
    (3) engdocs operator guide (Phase 2 step 13).
- The Open Questions prelude was rewritten to acknowledge round 3's
  four promotions (OQ-1, OQ-4, OQ-5, OQ-7) and to call out that
  OQ-2, OQ-3, and OQ-6 remain appropriately deferred to the next
  resolving phase.

## Items rejected

None.

## Verification

- Read the resulting design doc end-to-end (sections changed:
  Executive Summary; Proposed Design / Step-execution contract
  (new); Two-PR Shipping Order PR1 step 1 (extended) and PR2 step 11
  (reworded); Error Class Taxonomy exit-code paragraph (rewritten as
  3-row table); JSON envelope / Per-step bead mapping closing
  paragraph (cross-reference appended); Implementation Plan / Phase 2
  step 12 (reworded); Risks and Mitigations / R-9 (extended); Open
  Questions (prelude rewritten + Resolutions subsection added with
  OQ-1/OQ-4/OQ-5/OQ-7 + Remaining open questions subsection holding
  OQ-2/OQ-3/OQ-6).
- Section-heading audit (`grep -n '^## \|^### \|^####'`) confirms
  structure is intact:
  - 12 top-level `##` sections (Step-execution contract is `###`,
    nested under Proposed Design — top-level count unchanged from
    round 2's 11+1=12 since the §Operations addition).
  - Key Components 1-10 sequential and 1:1 with the PR2 module list
    (no renumber from round 2).
  - Manual Recovery sub-cases MR-1 through MR-4 all present.
  - Open Questions: 4 Resolutions (OQ-1, OQ-4, OQ-5, OQ-7) +
    3 Remaining (OQ-2, OQ-3, OQ-6) = 7 total, matching the
    design-exploration baseline count.
  - TD-1 through TD-10, R-1 through R-11, Phase 1 / Phase 2 / Phase
    3 all present.
- Cross-reference audit:
  - PR2 step 11 cross-references "Step-execution contract" — present
    above as a Proposed Design subsection.
  - Per-step bead mapping cross-references "Step-execution contract"
    — present.
  - Phase 2 step 12 cross-references "Step-execution contract" —
    present.
  - R-9 cross-references "Step-execution contract" — present.
  - PR1 step 1 cross-references OQ-1's resolution — present in
    Resolutions subsection.
  - Exit-code table cross-references OQ-5's resolution — present in
    Resolutions subsection.
- All findings either applied or explicitly justified. No items left
  open.

## Round 3 fix counts (live-conversation summary)

- **Must-fix applied:** 1
  - M1 — Story 2 step↔CLI invocation contract bound (Step-execution
    contract subsection + PR2 step 11 reword + Per-step bead mapping
    cross-reference + Phase 2 step 12 reword + R-9 extension).
- **Should-fix applied:** 4
  - S1 — OQ-1 bound to free-form `executor` for PR1.
  - S2 — OQ-4 bound to CLI-only `--per-db-timeout` for v1 with
    explicit promotion trigger.
  - S3 — OQ-5 bound to 3-row exit-code table with no-op-as-`0`.
  - S4 — OQ-7 moved from Open Questions to Resolutions.
- **Items rejected:** 0
- **PRD-alignment scope this round:** PRD-vs-design contract gaps
  (M1) plus design-Open-Question promotions (S1-S4). No new content
  added to Algorithms, Trust and Discovery, Components, or Manual
  Recovery — those were closed by rounds 1-2.

## Next round

Plan self-review round 1 (bead `gc-imyo2c`, formula step
`mol-idea-to-plan.plan-review-1`). Per the bead description, that
round shifts from PRD alignment to plan self-review with two new
legs (`completeness`, `sequencing`) against the now-thrice-aligned
design doc. PRD-alignment is complete after this round; the design
doc is ready for plan-review.
