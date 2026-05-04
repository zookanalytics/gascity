# PRD Alignment Round 3 — Leg Reports

Round: prd-align-3 (mol-idea-to-plan.prd-align-3, bead `gc-lygvkw`)
Date: 2026-05-04
Coordinator: gascity/gastown.furiosa

Two legs (`user-stories-coverage`, `open-questions-resolution`) executed
inline, matching the round-1 and round-2 pattern. Inputs read end-to-end:

- `.prd-reviews/dolt-compact-cli/prd-draft.md`
- `.designs/dolt-compact-cli/design-doc.md` (post round 2)
- `.plan-reviews/dolt-compact-cli/prd-align-round-1.md`
- `.plan-reviews/dolt-compact-cli/prd-align-round-2.md`
- `.plan-reviews/dolt-compact-cli/human-clarifications.md`

Each leg produces `must-fix` and `should-fix` findings with concrete
pointers into the design doc. Per the step description, all are applied
to the design doc unless objectively wrong; rejections are recorded in
the round log.

---

## Leg 1 — user-stories-coverage

**Charter.** Walk every PRD User Story (lines 110-162) end to end and
verify the design discharges it concretely. Flag any story whose end-
to-end path is broken, ambiguous, or undefined.

The PRD has five stories (Story 1: operator ad-hoc; Story 2: dog cycle;
Story 3: operator debugging via `--inspect-only`; Story 4: concurrent-
write hazard; Story 5: CI/test verification). Four are concretely
covered after rounds 1-2's fixes. **Story 2 has a remaining contract
ambiguity at the formula-step ↔ CLI-invocation boundary** that round
1's `phases` block addition surfaced but did not resolve. One must-fix.

### Story coverage matrix

| Story | End-to-end path | Verdict |
|-------|----------------|---------|
| 1 — Operator ad-hoc compaction | CLI surface (Interface §); `gc dolt-compact` Cobra command (Component 1); pack delegate `gc dolt compact` (Component 9); Default human output example with success line + round-1 below-threshold-skip example; glyph indication (✓⚠✖) | OK |
| 2 — Dog cycle (claim step bead → shell out → close per-step beads) | Shape § (CLI runs as one process emitting one envelope); `phases` block + Per-step bead mapping (Goal 6 discharge); PR2 step 11 sets `executor = "gc dolt-compact"` on every executable step | **Partial — step↔CLI invocation contract ambiguous (M1)** |
| 3 — Operator debugging via `--inspect-only` | TD-10 explicitly defers `--inspect-only` to v2; round-1 added paragraph telling v1 operators to read `inspect:` lines (human) or per-DB `outcome`/`pre_commits` fields (JSON) for equivalent visibility | OK (deferred with v1 workaround) |
| 4 — Concurrent-write hazard during surgical | Algorithms / Surgical (retry once with 2s pause; halt-before-swap on retry failure from round 1); Error Class Taxonomy (`concurrent-write-collision` retried internally, `concurrent-write-fatal` escalates); R-3 (extended in round 1 to spell out halt-before-swap); Phase 2 step 11 integration test injects concurrent write | OK |
| 5 — CI/test verification (test author following pattern) | Phase 2 unit + integration test list including charset, envelope shape, mode selection, error-class mapping, retry semantics, threshold-on-explicit-DB skip, formula/CLI defaults consistency, formula-driven integration test (round 1 addition); R-8 acknowledges flake risk and points at `examples/dolt/sql_test.go` patterns | OK |

### M1 (must-fix) — Story 2: step↔CLI invocation contract is ambiguous

**Story text.** PRD Story 2 (lines 128-136):

> **Story 2: Compactor dog running the formula**
>
> > Dog claims a step bead, shells out to the CLI with structured output,
> > and closes per-step beads from the parsed result.
>
> The CLI emits structured output (NDJSON or JSON-final-line) the dog can
> read. Each phase corresponds to a formula step. Failures bubble up so
> the dog can escalate as the formula prescribes.

**The two design statements that don't reconcile.**

1. **Shape §** (lines 146-150 of design-doc.md):

   > The CLI follows the formula's existing 4-step structure (inspect,
   > compact, verify, report) but does not literally run as four
   > processes — it runs as one process that emits a single final JSON
   > envelope describing all four phases. The dog parses the envelope
   > once after the CLI exits and closes its per-step beads accordingly.

2. **Two-PR Shipping Order PR2 step 11** (lines 218-224 of design-doc.md):

   > 11. `examples/dolt/formulas/mol-dog-compactor.toml`:
   >     - Drop `zfc_exempt = true` from compactor steps.
   >     - **Set `executor = "gc dolt-compact"` on every executable step.**
   >     - Update step descriptions to reference CLI flags using
   >       formula variable substitution
   >       (`gc dolt-compact --mode={{mode}} --threshold={{commit_threshold}}
   >       --keep-recent={{keep_recent}} --databases={{databases}} --json`).

The reading that fires: if every step has `executor = "gc dolt-compact"`
and the formula's existing step-execution model is "claim a step bead,
read its description, invoke the named executor," the dog invokes the
CLI **four times per cycle** — once per step bead. That contradicts
Shape § ("does not literally run as four processes"), wastes the
per-DB advisory lock acquired inside the algorithm body (every
re-acquire either blocks or returns `database-locked` after the first
invocation), and racks up four rounds of `dolt_gc` overhead per cycle.

The reverse reading — "the CLI is invoked once per cycle and the dog
closes all four step beads from the cached envelope" — is consistent
with Shape § and with the `phases` block's design intent ("the dog can
close `inspect` / `compact` / `verify` / `report` step beads from a
single envelope read"), but the design doc never says **which** step
triggers the invocation, **how** the envelope is propagated to the
non-trigger steps, or **why** the executor field is set on every step
if only one actually invokes the binding.

**Why this matters.** PR2's implementer faces three choices today:

1. Run the CLI on each step (literal reading of "executor on every
   step"); accept 4× cost and lock contention.
2. Restructure the formula so only one step (e.g., `compact`) carries
   the executor and the others have a different binding mode that the
   dog runtime knows to dispatch as "close from cached envelope."
3. Add a dog-runtime mechanism that caches the envelope across step
   beads in the same cycle and short-circuits subsequent invocations.

All three have material design consequences (formula schema, dog
runtime, regression-test rules). The design doc must pick one (or
constrain the choice) so plan-review and PR2 are not relitigating
this contract.

**Why it surfaced now.** Round 1 added the `phases` block and the
Per-step bead mapping subsection to discharge Goal 6's per-step
observability requirement. That addition is correct, but it implicitly
assumed a single invocation per cycle without spelling out the
contract. Story 2 — the only PRD story that walks the dog's full path —
is the natural place this surfaces.

**Required fix.** Add a "Step-execution contract" subsection to the
Proposed Design (between "Shape" and "Two-PR Shipping Order") binding:

1. **Per-cycle invocation rule.** The CLI is invoked **exactly once per
   dispatched cycle**, not once per formula step. The justification is
   threefold: (a) all four phases share the city-wide advisory lock
   acquired inside the algorithm body (TD-4); (b) the algorithms are
   not re-entrant within a single cycle without re-acquiring the lock,
   which would deadlock; (c) the `phases` block is already designed to
   drive all four step closures from one envelope.
2. **Designated trigger step.** The `compact` step is the trigger.
   When the dog claims the `compact` step bead, it invokes
   `gc dolt-compact --json` with the formula-bound flags substituted
   from the formula vars; the dog parses the resulting envelope and
   uses it to close `compact` immediately and the other three step
   beads as they are claimed (or atomically, depending on the dog's
   formula-execution model — the design constrains the outcome, not
   the dispatch order).
3. **Bookkeeping steps.** The `inspect`, `verify`, and `report`
   steps remain in the formula as bookkeeping anchors for the four
   phases of the JSON envelope. They retain `executor = "gc dolt-compact"`
   (declares the underlying binding for the executor-binding regression
   test) **and `zfc_exempt = true`** (declares that the dog does not
   re-invoke the executor for these steps; closure happens via dog-
   runtime logic that consumes the envelope produced by the `compact`
   step). The `executor` + `zfc_exempt` pair on the bookkeeping steps
   is the same shape the regression test accepts (Component 8).
4. **PR2 step 11 update.** Reword "Set `executor = "gc dolt-compact"`
   on every executable step" → "Set `executor = "gc dolt-compact"` on
   every step; also set `zfc_exempt = true` on the `inspect`, `verify`,
   and `report` bookkeeping steps. Only the `compact` step's
   description carries the full CLI invocation; the bookkeeping steps'
   descriptions reference the cached-envelope closure path."
5. **Cross-reference from Per-step bead mapping.** Add one sentence to
   the existing Per-step bead mapping subsection (lines 567-600) that
   links forward to Step-execution contract: "The dog reads the
   envelope produced by the `compact` step's CLI invocation
   (Step-execution contract); the `phases.<step>` field then drives
   each step bead's closure as the table above prescribes."

**Why this is the right shape, not a redesign.** The fix preserves the
formula's 4-step structure (per-phase observability for deacon /
dashboards), preserves the single-CLI-invocation efficiency
(no 4× lock contention), preserves the `phases` block (no envelope-
schema change), and stays within the PR1 schema fields
(`executor` + `zfc_exempt`). The only material change is naming the
trigger step explicitly and tagging the bookkeeping steps as
`zfc_exempt = true` so the regression test interprets them correctly.
The dog-runtime mechanism that propagates the envelope across step
beads is a pre-existing concern (or a small PR2 addition); the
design doc's job here is to constrain the contract, not specify the
mechanism.

**Design sections to touch.**

- New subsection in Proposed Design between "Shape" and "Two-PR
  Shipping Order": "Step-execution contract."
- Two-PR Shipping Order PR2 step 11 (rewording).
- Per-step bead mapping (one cross-reference sentence).
- Implementation Plan / Phase 2 step 12 (formula update language
  aligns with PR2 step 11).
- Risks and Mitigations: extend R-9 to note that the per-cycle
  invocation contract is exercised by the formula-driven integration
  test (Phase 2 step 11) — the test loads the formula, resolves the
  `compact` step's executor, and asserts the envelope drives all four
  step closures from one CLI call.

### Items not flagged

- **Story 1 — failure-case human output examples.** The Default human
  output section shows the happy path and (after round 1) the
  below-threshold-skip case. It does not show error glyphs (`⚠ ✖`)
  in action — e.g., what `integrity-mismatch` looks like in the
  human output. The design references the glyphs and matches
  cleanup's pattern. This is polish, not a coverage gap; PR2's
  implementer can mirror cleanup's failure-case formatting without
  further design guidance. **Not flagged.**
- **Story 2 — auto-discovery returns empty case.** When `--databases`
  is empty and rig-registry returns no candidates, exit code is
  ambiguous (OQ-5 captures the broader exit-code precision question).
  This is a real ambiguity but it falls cleanly under OQ-5; it does
  not need a separate Leg-1 finding. **Not flagged here; covered by
  Leg 2 OQ-5 binding.**
- **Story 4 — failure mode for surgical with no concurrent writes.**
  If surgical's rebase succeeds without graph-change errors, the
  retry path is never exercised; the algorithm proceeds to verify,
  then potentially halt-before-swap on integrity-mismatch (round-1
  fix). Coverage is complete. **Not flagged.**

### Open Questions raised by this leg

None. The M1 finding is concrete and actionable in this round.

---

## Leg 2 — open-questions-resolution

**Charter.** Walk every Open Question (PRD §Open Questions and the
design doc's §Open Questions OQ-1 through OQ-7) and verify each is
either bound (concrete decision in the design) or explicitly deferred
(named target phase / artifact). Flag any question that lingers as a
recommendation without a binding decision.

The PRD's seven enumerated open questions (Q5, Q10, Q12, Q13, Q14,
Q15, Q16) are all bound or deferred. The design's seven open questions
(OQ-1 through OQ-7) split: three are appropriately deferred to later
phases (OQ-2, OQ-3, OQ-6); three carry recommendations that need to
be promoted to bindings before the design closes (OQ-1, OQ-4, OQ-5);
one is already-bound elsewhere but still listed as open (OQ-7).
Four should-fix findings.

### PRD Open Questions resolution status

| ID | PRD Topic | Status | Anchor in design |
|----|-----------|--------|------------------|
| Q5 | Error classification taxonomy | Bound | TD-6 (9-class table); Error Class Taxonomy section |
| Q10 | `dolt_gc` cost (per-DB vs aggregated) | Bound | TD-8 (per-DB; --per-db-timeout safety valve) |
| Q12 | Observability budget (event bus) | Deferred to v2 | TD-10 |
| Q13 | `--inspect-only` / `--dry-run` | Deferred to v2 | TD-10 |
| Q14 | `--threshold` semantics on explicit DBs | Bound (round 1) | Threshold Semantics subsection |
| Q15 | CLI flips `mol-dog-compactor` exemption | Bound | TD-9 |
| Q16 | ZFC expansion in user-facing docs | Bound | TD-9; PR1 step 1 doc-comment spec; OQ-7 (binding decision present, listed as open — see S4) |

All seven PRD open questions are accounted for. No PRD Open Question
finding.

### Design Open Questions (OQ-1 through OQ-7) resolution status

| ID | Topic | Current state | Verdict |
|----|-------|--------------|---------|
| OQ-1 | Free-form vs structured `executor` field shape | Recommendation: free-form for PR1; revisit if a non-CLI binding emerges | **Recommendation, not bound (S1)** |
| OQ-2 | Regression-test resolution heuristic | Drafted as "step description starts with a `gc <command>` invocation or standard-Unix command"; plan-review-1 to pressure-test | Deferred to plan-review-1 (acceptable) |
| OQ-3 | Where the regression test lives | `cmd/gc/embedded_formula_executor_test.go` recommended; choosable in PR1 | Deferred to PR1 (acceptable — code-org detail) |
| OQ-4 | Per-DB timeout default placement | Recommendation: no formula var until pain emerges | **Recommendation, not bound (S2)** |
| OQ-5 | Exit code precision | Recommendation: 0 for no-op (cleanup parity); current 0/1/2 | **Recommendation, not bound (S3)** |
| OQ-6 | Pack-delegate flag-passing for `--port` | "Confirm in PR2 review" — match cleanup's `GC_DOLT_PORT` forward | Deferred to PR2 review (acceptable) |
| OQ-7 | ZFC expansion in user-facing docs (Q16) | Already bound: "Zero Framework Cognition" per AGENTS.md; pin in schema field's doc-comment and engdocs | **Bound but listed as Open (S4)** |

OQ-2, OQ-3, and OQ-6 are appropriately deferred to later phases that
will resolve them naturally:
- OQ-2 falls within plan-review-1's "completeness" charter (the
  heuristic's strictness is a completeness concern).
- OQ-3 is a code-org detail with two reasonable answers; PR1 review
  picks one.
- OQ-6 is a pack-script implementation detail that PR2 review confirms
  by mirroring cleanup.

OQ-1, OQ-4, OQ-5, and OQ-7 should be promoted to bound decisions in
the design doc.

### S1 (should-fix) — OQ-1: bind `executor` field shape to free-form for PR1

**Current OQ-1 text:**

> Currently designed as a free-form string (e.g., `"gc dolt-compact"`).
> A structured tag (`{ kind = "cobra", command = "dolt-compact" }`)
> would support future binding kinds. Recommend free-form for PR1;
> revisit if a non-CLI binding emerges.

**Why bind now.** PR1 lands the schema. The schema choice is a
contract, not a recommendation. If left as a recommendation, PR1's
implementer revisits the structured-vs-free-form question without
the prior reasoning anchored. A future migration from free-form to
structured would also be additive (new fields), so committing now
costs nothing.

**Recommended fix.** Rewrite OQ-1 as a "Resolved" entry (or move into
a new "Resolutions" subsection of Open Questions) with the binding:

> **OQ-1 (resolved).** The `executor` field is a **free-form string**
> in PR1 (e.g., `"gc dolt-compact"`). Rationale: every binding the
> design contemplates today (Cobra commands, pack scripts) is
> identifiable by a single string token, and the executor-binding
> regression test (Component 8) consumes the string directly. A
> future structured-tag form (`{ kind, command }`) is additive — a
> new optional field on the same `Step` struct can carry the
> structured form without breaking PR1's schema. The structured form
> is deferred to v2 and will be revisited only if a non-CLI binding
> emerges (e.g., a Go-internal callback registry).

Also add one sentence to PR1 step 1 in the Implementation Plan
("Add fields to `formula.Step`") explicitly noting the field type:

> The `Executor` field is `string` (not a structured type); the
> structured-tag form (`{ kind, command }`) is deferred to v2 per
> OQ-1.

### S2 (should-fix) — OQ-4: bind per-DB timeout default placement to CLI-only for v1

**Current OQ-4 text:**

> Currently `1h` hard-coded as the CLI default. Should the formula
> expose a `per_db_timeout` var? Recommend no until a city hits the
> limit.

**Why bind now.** TD-5 (extended in round 2) already documents
`--per-db-timeout` as a wrapper-level execution budget. The "should
the formula expose a `per_db_timeout` var?" question is the same
question TD-5 answered in the negative. OQ-4 is just a leftover
phrasing of the same decision.

**Recommended fix.** Rewrite OQ-4 as a "Resolved" entry tying back to
TD-5:

> **OQ-4 (resolved).** `--per-db-timeout` defaults to `1h` and lives
> on the CLI surface only; **the formula does not expose a
> `per_db_timeout` var in v1.** Rationale: per TD-5,
> `--per-db-timeout` is a wrapper-level execution budget, not a
> formula-bound algorithm parameter; binding it into the formula
> requires either a corresponding consistency-test extension
> (Phase 2 step 10) or a deliberate exclusion. Deferral is correct
> until a city actually hits the deadline and demands per-formula
> tuning. **Trigger for promotion to a formula var:** at least one
> city's `database-deadline-exceeded` outcomes become a recurring
> patrol signal (i.e., a single timeout is operator-tunable from the
> CLI; persistent timeouts across cycles indicate the default is
> wrong for that city). When promoted, the consistency test (Phase 2
> step 10) extends to cover the new var; TD-5's "additive evolution"
> paragraph already describes the migration path.

### S3 (should-fix) — OQ-5: bind exit code precision

**Current OQ-5 text:**

> Currently `0` / `1` / `2`. Should we add a class for "skipped
> because below threshold" (no work done)? Cleanup uses `0` for a
> no-op dry-run, so consistency says `0` here too.

**Why bind now.** Exit code semantics are part of the dog's
step-closure contract (the dog reads the exit code as a signal
alongside the envelope). Leaving the no-op case ambiguous risks
dog-side defensive code, and the recommendation to mirror cleanup is
already unambiguous. Also closes the Story-2 auto-discovery-empty
case (Leg 1 "items not flagged") that this OQ subsumes.

**Recommended fix.** Rewrite OQ-5 as a "Resolved" entry:

> **OQ-5 (resolved).** Exit codes are bound at three values, matching
> the existing top-level Error Class Taxonomy paragraph:
>
> | Exit | Meaning |
> |------|---------|
> | `0` | Full success **or** no-op (zero candidates from auto-discovery; every named DB below `--threshold`; every per-DB outcome in {`ok`, `below-threshold`, `database-locked` (skipped)}) |
> | `1` | At least one per-DB outcome in the escalate set ({`concurrent-write-fatal`, `integrity-mismatch`, `internal-error`, `database-deadline-exceeded`}) |
> | `2` | Invocation error before per-DB execution begins (bad flag, port unresolved, identifier failed charset check, explicit DB list with zero matches in rig-registry) |
>
> Rationale: cleanup parity (cleanup's `0` covers no-op runs); the
> dog's step-closure code reads the exit code only as a coarse
> signal alongside the envelope's `phases` and per-DB outcomes
> (which carry the precise breakdown), so finer-grained exit codes
> would duplicate envelope fields without informing dog action.
>
> **Story-2 corollary (auto-discovery returns empty).** When
> `--databases` is empty and rig-registry returns no candidates, the
> CLI emits a well-formed envelope with `summary.databases_inspected
> = 0` and exits `0`. The dog closes all four step beads `closed`
> (no work done is a successful no-op, not a failure).

Also align the existing CLI exit-code paragraph (lines 339-344 in
the design-doc.md) with the table — currently it reads:

> The CLI exit code is `0` on full success, `1` if any DB hit an
> escalate-class outcome (`concurrent-write-fatal`, `integrity-mismatch`,
> `internal-error`, `database-deadline-exceeded`), and `2` on
> invocation errors (bad flag, port unresolved, no DBs to compact
> when explicit list given).

Extend `0` to "full success **or** no-op (zero candidates; below-
threshold-only; or skipped-only)" and `2` to include "identifier
failed charset check" so the exit-code source-of-truth and OQ-5's
table agree.

### S4 (should-fix) — OQ-7: move from Open Questions to Resolutions (or remove)

**Current OQ-7 text:**

> "Zero Framework Cognition" per `AGENTS.md`. Pin in the schema
> field's doc-comment and in `engdocs/contributors/dolt-compact.md`.

**Why fix.** OQ-7 is **already bound** by TD-9 ("the daemon path was
rejected as a Non-Goal" plus the subsequent expansion-spelling) and by
the PR1 step 1 spec ("explicitly spell out 'Zero Framework Cognition
(ZFC)'"). The expansion is also concretely specified in the
`ZfcExempt` doc-comment in the Data Model / Formula schema additions
section. Listing OQ-7 as an unresolved Open Question is misleading —
plan-review readers will treat it as outstanding work when it has
already been decided.

**Recommended fix.** Either:

(a) **Move OQ-7 to a "Resolutions" subsection** (mirroring the
treatment of OQ-1, OQ-4, OQ-5 above), with a one-line entry pointing
at TD-9, the PR1 step 1 spec, and the `ZfcExempt` doc-comment:

> **OQ-7 (resolved).** "ZFC" expands to **Zero Framework Cognition**
> per AGENTS.md. The expansion is pinned in the `ZfcExempt` field's
> doc-comment (Data Model / Formula schema additions), in the PR1
> step 1 implementation spec, and in the engdocs operator guide
> (Phase 2 step 13).

(b) **Remove OQ-7 entirely** (the binding decisions live in TD-9,
the PR1 spec, and the schema doc-comment; the OQ-7 line is a
duplicate placeholder). Recommend (a) — the explicit "resolved" entry
preserves the audit trail showing the question was considered and
closed.

### Items not flagged

- **PRD Q12 / Q13 (event bus, --inspect-only).** Both deferred to v2
  by TD-10 with explicit rationale (no consumer; gold-plating).
  Round 1 added the Story-3 fallback. No further action needed.
- **OQ-2 / OQ-3 / OQ-6.** All three are appropriately deferred to
  the next phase that will naturally resolve them (OQ-2 → plan-
  review-1 completeness; OQ-3 → PR1 review code-org choice;
  OQ-6 → PR2 review pack-script confirmation). Forcing a binding
  here would either pre-empt a review the design has explicitly
  delegated (OQ-2) or commit to a code-org/implementation detail
  that's better decided in the actual PR (OQ-3, OQ-6).
- **The "Open implementation questions" line in the PRD's Rough
  Approach** (line 358-359 of prd-draft.md). This was a draft
  placeholder that the design-exploration phase replaced with the
  full design. No design action required.

### Open Questions raised by this leg

None. All four findings are concrete and actionable in this round.

---

## Summary

**Leg 1 — user-stories-coverage:**

- Must-fix: 1 (M1: Story 2 — step↔CLI invocation contract is
  ambiguous)
- Should-fix: 0

**Leg 2 — open-questions-resolution:**

- Must-fix: 0
- Should-fix: 4 (S1: bind OQ-1 free-form; S2: bind OQ-4 CLI-only;
  S3: bind OQ-5 exit code precision; S4: move OQ-7 to resolved)

Total: 1 must-fix, 4 should-fix. All five are concrete, fixable in
the design doc, and rooted in either PRD text (Story 2; Q5/Q10/Q14
implicit corollaries) or the design doc's own Open Questions list.
Per the round-1 / round-2 standard, all five should be applied unless
objectively wrong.

Findings to apply, in order of impact:

1. **M1** — Add a "Step-execution contract" subsection to Proposed
   Design (between Shape and Two-PR Shipping Order); reword PR2
   step 11; add one cross-reference sentence to Per-step bead
   mapping; align Phase 2 step 12 language; extend R-9.
2. **S1** — Promote OQ-1 to a Resolutions subsection with the
   free-form binding for PR1; align PR1 step 1 to spell out the
   field type.
3. **S2** — Promote OQ-4 to Resolutions with the CLI-only binding
   plus the explicit promotion trigger.
4. **S3** — Promote OQ-5 to Resolutions with the exit-code table;
   align the existing exit-code paragraph; add the Story-2 auto-
   discovery-empty corollary.
5. **S4** — Move OQ-7 to Resolutions (or delete) with the binding
   pointing at TD-9, the PR1 step 1 spec, and the `ZfcExempt`
   doc-comment.
