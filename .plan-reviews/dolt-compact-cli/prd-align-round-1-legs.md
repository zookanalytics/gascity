# PRD Alignment Round 1 — Legs

Round: prd-align-1 (mol-idea-to-plan.prd-align-1, bead `gc-qn5p3r`)
Date: 2026-05-04
Coordinator: gascity/gastown.nux

Two legs of PRD-against-design verification, executed inline (matching the
prd-review and design-exploration predecessors). Each leg consumed:

- `.prd-reviews/dolt-compact-cli/prd-draft.md` (post human-clarification round)
- `.designs/dolt-compact-cli/design-doc.md` (round-0 baseline)
- `.plan-reviews/dolt-compact-cli/human-clarifications.md` (binding decisions)
- `.plan-reviews/dolt-compact-cli/design-review-legs.md` (where source legs
  raised an item and the design doc dropped it)

Findings are classified `must-fix` or `should-fix` and reference the exact
design-doc sections to update. The round log
(`.plan-reviews/dolt-compact-cli/prd-align-round-1.md`) records the applied
changes (or, where rejected, why).

---

## Leg 1 — `requirements-coverage`: every stated requirement is concretely covered

### Summary

Walked every PRD requirement (Goals 1-8, Acceptance Criteria 1-3,
Constraints, Open Questions Q5/Q10/Q12/Q13/Q14/Q15/Q16, Stories 1-5) and
matched each to the design doc. The bulk are concretely covered. Five gaps
surfaced — one is a binding contract conflict the design has inherited
without resolving (must-fix), one is an unresolved PRD Open Question that
never propagated into the design (must-fix), and three are
under-specifications that will bite during PR2 (should-fix).

### Coverage Matrix (selected entries; full walk in scratch notes)

| PRD item | Design coverage | Verdict |
|---|---|---|
| Goal 1 (replace safely-skip) | TD-9, PR2 plan §11 | covered |
| Goal 2 (single source of truth) | step descriptions reference CLI flags via `{{var}}` substitution | partial — no enforcement (S3) |
| Goal 3 (operator UX) | "Default human output" §463-479 | covered |
| Goal 4 (algorithm parity) | "Algorithms" §211-258 | covered |
| Goal 5 (integrity guaranteed) | Verifier §331-336, error class taxonomy §269-289, **pre-compaction state rollback** | partial — no rollback path (M1) |
| Goal 6 (per-step observability) | single envelope §414-456; mapping to per-phase steps | partial — derivation undocumented (S1) |
| Goal 7 (regression-proof) | §344-369 | covered |
| Goal 8 (test coverage) | Phase 2 §707-737 | covered |
| Acceptance 1 (`gc dolt compact hq`) | integration test §726-731 | covered |
| Acceptance 2 (mol-dog-compactor cycle e2e) | R-9 manual smoke, no automated coverage | partial (S2) |
| Acceptance 3 (regression test in PR1) | §344-369, Phase 1 | covered |
| Constraint: formula vars | flag list §397-403 | covered |
| Constraint: dual CLI surface | §110-114, Component 8 | covered |
| Constraint: rig-registry-only discovery | §292-298 | covered |
| Constraint: integrity verify (excluding `dolt_*`) | §332-336 | covered |
| Constraint: integrity mismatch → pre-compaction state | not addressed | **gap (M1)** |
| Constraint: identifier charset validation | §297-301 | covered |
| Constraint: surgical lock-then-cleanup | §233 (Q5 marker), TD-4 | covered |
| Constraint: ZFC (no judgment in Go) | implicit (rule-based dispatch) | covered |
| Constraint: mirror cleanup default principle | Executive Summary, TD-3, TD-4, R-5/R-6 | covered |
| Open Q5 (error taxonomy) | TD-6, §269-289 | covered |
| Open Q10 (`dolt_gc` cost) | TD-8 | covered |
| Open Q12 (event bus) | TD-10 | covered |
| Open Q13 (`--inspect-only` / `--dry-run`) | TD-10 | covered (but Story 3 not noted as deferred — S4) |
| Open Q14 (`--threshold` on explicit DBs) | not addressed | **gap (M2)** |
| Open Q15 (CLI flips exemption) | TD-9 | covered |
| Open Q16 (ZFC expansion) | PR1 spec §137-142, OQ-7 | covered |
| Story 1 (operator ad-hoc) | §463-476 | covered |
| Story 2 (dog cycle) | §107-128 | covered |
| Story 3 (debugging via `--inspect-only`) | TD-10 (deferred), no explicit call-out | partial (S4) |
| Story 4 (concurrent-write retry) | §244-247, R-3 | covered |
| Story 5 (CI/test) | Phase 2 §726-731 | covered |
| Non-goal: no daemon | TD-9, Executive Summary | covered |
| Non-goal: no formula split | not contradicted | covered |
| Non-goal: no compat shim | PR2 drops `zfc_exempt` outright | covered |

### Findings

#### must-fix

- **M1 (Constraint: integrity-mismatch → leave db in pre-compaction state).**
  PRD line 187-188 binds: "pre/post row counts on all user tables (excluding
  `dolt_*` system tables); mismatch → error, leave db in pre-compaction
  state."
  - Design's flatten pseudocode (`design-doc.md:213-225`) commits before
    verifying — the `DOLT_RESET --soft` + `DOLT_COMMIT` finalize the new
    history before the post-row-count check. There is no rollback path.
  - Design's surgical pseudocode (`design-doc.md:231-255`) sets
    `outcome = integrity-mismatch` but proceeds straight into `delete main,
    rename compact-work → main` — main is overwritten regardless of the
    mismatch verdict.
  - Design-review-legs Leg 4 (scale, line 443-445) explicitly acknowledges
    "Database is left at the failed-post-compaction state — operator must
    manually verify before re-running" — directly contradicting the PRD
    constraint, but the contradiction was not surfaced in the design doc.
  - Required fixes:
    1. Flatten: capture `pre_hash` from the live HEAD before
       `DOLT_RESET --soft`. On post-row-count mismatch, issue
       `DOLT_RESET --hard pre_hash` to restore. Then `outcome =
       integrity-mismatch`. `dolt_gc` runs *after* the rollback (or is
       skipped — operator decision; recommend skip on mismatch so the
       operator can re-inspect the original state).
    2. Surgical: explicitly halt before the branch swap when
       `outcome ∈ {integrity-mismatch, concurrent-write-fatal}`. Drop the
       `compact-*` working branches; leave `main` untouched.
    3. Document the residual case in **Risks**: if the rollback itself
       fails (e.g., Dolt server crash mid-rollback), the operator-recovery
       path is `DOLT_RESET --hard <pre_hash>` from `dolt sql` directly. The
       envelope must surface `pre_hash` for this case.
  - Design sections to touch: "Algorithms" (flatten + surgical pseudocode);
    "Error Class Taxonomy" (note rollback semantics); "Risks and
    Mitigations" (extend R-3, add "rollback failure" risk if not already
    covered); "JSON envelope" (add `pre_hash` to per-DB record).

- **M2 (Open Q14: `--threshold` semantics on explicit DBs).**
  PRD Open Question 14 (line 274-276) is still listed as unresolved:
  "How does the CLI handle databases that don't meet the commit threshold
  when explicitly named (`gc dolt compact hq --threshold=10000`)? Does
  `--threshold` override per-database, or skip with a message?" The PRD
  marks it as a design-phase question.
  - Design-review-legs Leg 1 (api, lines 67-73) recommends:
    "`--threshold` remains effective… Skip with `below-threshold` if not.
    Operators wanting unconditional compaction can pass `--threshold=0`."
  - The design doc never states this rule. The interface section
    (`design-doc.md:386-404`) lists the flag without semantics; the human
    output example (line 466-476) shows only the `→ candidate` happy path.
  - Required fix: add an explicit "Threshold semantics" subsection (or
    paragraph in **Interface**) stating the binding rule
    (threshold gates every candidate — explicit and auto-discovered alike;
    operators wanting unconditional compaction pass `--threshold=0`), and
    extend the human-output example to show the explicit-DB-skip case
    (e.g., `gascity: 234 commits → below threshold, skip`).
  - Design sections to touch: "Interface" (after CLI surface block);
    "Default human output"; "Open Questions" (record Q14 as resolved).

#### should-fix

- **S1 (Goal 6: per-step observability under-specified).**
  The single final envelope (TD-2) is the right shape, but the design
  doesn't describe how the dog converts the envelope's per-DB array into
  per-formula-step status (`inspect`, `compact`, `verify`, `report`). The
  formula has four steps; the envelope has per-DB results. Mapping is
  implicit:
  - `inspect` step status = union of per-DB pre-flight outcomes
    (was the candidate identified? did the commit-count probe succeed?)
  - `compact` step status = union of per-DB compaction outcomes
  - `verify` step status = union of per-DB verify outcomes
  - `report` step status = closed when summary is non-nil
  - Required fix: either add an explicit `phases` block to the envelope
    (each phase = `{ outcome, message?, contributing_dbs?: [...] }`), or
    document the aggregation rule in a "Per-step bead mapping" subsection
    so PR2 doesn't have to invent it.
  - Design sections to touch: "JSON envelope" (`design-doc.md:414-456`);
    add subsection or new component for per-step mapping.

- **S2 (Acceptance 2: mol-dog-compactor cycle e2e — automated coverage missing).**
  PRD Acceptance Criteria 2 binds: "`mol-dog-compactor` cycle (24h cooldown
  order) runs end-to-end: dog claims, shells out to the CLI, compaction
  actually occurs, integrity verified, `dolt_gc` runs, report sent."
  - Design's only end-to-end coverage is R-9 ("manual smoke after PR2").
  - The full 24h-cooldown order is not testable in CI, but the
    formula→CLI→envelope→step-close path IS testable as an integration
    test that simulates the dog claiming a step bead, invoking the CLI
    with `--json`, and asserting the parsed envelope drives correct
    step closure decisions.
  - Required fix: add an integration test (Phase 2 §11) that exercises
    `examples/dolt/formulas/mol-dog-compactor.toml`'s executor invocation
    against a real Dolt server with seeded commits, asserts the envelope
    is well-formed, and asserts the per-step closure rule is computable.
    OR: document explicitly in TD-10 / a new TD that Acceptance 2's
    24h-cooldown leg is operator-gated post-deploy verification, with
    R-9 as the discharge.
  - Design sections to touch: "Implementation Plan" Phase 2 step 11
    (`design-doc.md:726-731`); "Risks and Mitigations" R-9.

- **S3 (Goal 2: single source of truth — no drift detection).**
  Goal 2 (PRD line 36-38) says "No hardcoded values that could drift from
  the formula." Design enforces this only by convention — formula step
  descriptions reference `{{commit_threshold}}` etc., and the CLI defaults
  match the formula's defaults. Nothing prevents a future PR from changing
  `--threshold` to `750` while the formula still says `commit_threshold = 500`.
  - Required fix: add a unit test (PR2) that loads
    `examples/dolt/formulas/mol-dog-compactor.toml`, extracts the formula
    vars (`commit_threshold`, `keep_recent`, `mode`, `databases`), and
    asserts the CLI's flag defaults match. The test ships with PR2.
  - Design sections to touch: "Implementation Plan" Phase 2 step 10
    (unit tests list); "Risks and Mitigations" (new R for formula-CLI drift).

- **S4 (Story 3: `--inspect-only` deferral not called out).**
  PRD Story 3 (lines 138-146) shows operator debugging via
  `gc dolt compact --inspect-only`. The PRD's Open Question 13 defers
  `--inspect-only` to v2. TD-10 confirms the deferral. But the design doc
  never says "Story 3 is partially deferred" — a reader following the
  Story 3 trail will hit a dead end.
  - Required fix: add one sentence to TD-10 (or a "Deferred Stories"
    subsection) explicitly stating that Story 3's debugging-via-flag flow
    is out of scope for v1; v1 operators inspect by running the full CLI
    and reading the human/JSON output (or the per-DB envelope outcomes).
  - Design sections to touch: TD-10 (`design-doc.md:607-611`).

### Open Questions raised by this leg

None. All findings actionable in this round.

---

## Leg 2 — `goals-alignment`: the design as written actually achieves every goal

### Summary

Mapped each PRD Goal (1-8) to the design's mechanism that achieves it and
judged whether the mechanism is sufficient. Five of eight goals fully
achieved (1, 3, 4, 7, 8); three are partial (2, 5, 6). Goal 5 is the
contract-conflict goal (must-fix); Goals 2 and 6 are under-specified
mechanisms (should-fix).

Where this leg's findings overlap Leg 1 (M1 ↔ M3, S1 ↔ S5, S3 ↔ S6), the
overlap is intentional — Leg 1 anchors the gap to a PRD Constraint or
Open Question; Leg 2 anchors the same gap to a Goal. The design-doc fixes
are unified (one fix discharges both anchors).

### Goal-by-goal analysis

#### Goal 1 — Replace the safely-skip with real work

**Design mechanism:** PR2 drops `zfc_exempt = true` from
`mol-dog-compactor`, sets `executor = "gc dolt-compact"`, and the new
CLI runs the algorithm. Phase 3 verifies on the next 24h cycle.

**Verdict:** Achieved. The mechanism is sufficient — the dog will read
the executor binding, shell out, and the CLI will compact. The only
remaining concern is automated coverage (S2 in Leg 1) but that's an
acceptance-coverage gap, not a goal-mechanism gap.

#### Goal 2 — Single source of truth

**Design mechanism:** Formula step descriptions use `{{var}}` substitution
to pin the CLI flag values; CLI defaults mirror formula defaults; nothing
else.

**Verdict:** Partial. The mechanism documents the linkage but doesn't
enforce it. A future change to one side without the other will silently
drift, and only a runtime mismatch (or careful code review) will surface
it. **Same gap as Leg 1 S3.** — should-fix. Add a CLI/formula-vars
consistency unit test.

#### Goal 3 — Operator UX

**Design mechanism:** Default human output mirrors PRD Story 1
(`design-doc.md:463-479`); errors include class + recovery hint
(per design-review-legs Leg 3 §297-308); `--mode=flatten` is a flag
not a subcommand; pack delegate exposes `gc dolt compact`.

**Verdict:** Achieved. Operator can `gc dolt compact hq --mode=flatten`
without standing up a dog; both surfaces work; output is operator-readable.

#### Goal 4 — Algorithm parity

**Design mechanism:** Both algorithms are pseudocoded in the design;
prose says they are "ports of the formula's existing prose"; integration
tests assert end-state matches the formula's contract.

**Verdict:** Achieved. The pseudocode is faithful to the formula's
documented behavior. The lock-then-cleanup ordering (Q5) is encoded.
Retry-on-collision (2s pause, once) is encoded.

#### Goal 5 — Integrity guaranteed

**Design mechanism:** Verifier (Component 5) runs pre/post row counts;
`dolt_gc` runs per-DB post-compaction; nine-class error taxonomy.

**Verdict:** Partial. Pre/post counts and `dolt_gc` are present, but
the formula's contract clause "leave db in pre-compaction state" is
not implementable as designed (flatten commits before verifying;
surgical proceeds to swap regardless of verdict). **Same gap as Leg 1 M1.**
— must-fix. The fix: capture `pre_hash` for flatten and `DOLT_RESET --hard`
on mismatch; halt-before-swap for surgical on any non-ok outcome.

#### Goal 6 — Per-step observability

**Design mechanism:** Single final JSON envelope `gc.dolt.compact.v1`;
dog parses once after CLI exits; closes per-step beads accordingly.

**Verdict:** Partial. Goal binds "close inspect, compact, verify, and
report step beads independently." Single-envelope design satisfies the
"structured CLI output" half; the "close independently" half requires
the dog to derive per-phase status from per-DB outcomes, and the
derivation rule is not documented. **Same gap as Leg 1 S1.** — should-fix.
Add either explicit `phases` block to the envelope or a documented
aggregation rule for the dog.

#### Goal 7 — Regression-proof

**Design mechanism:** Executor-binding test in PR1 walks every embedded
formula; each step must resolve to a real executor or carry the
`zfc_exempt = true` + `executor = "..."` exemption pair. The test ships
in PR1 ahead of the CLI.

**Verdict:** Achieved. The test discharge is concrete (Component 7),
the heuristic is drafted (R-2), and the migration sweep is part of PR1.

#### Goal 8 — Test coverage

**Design mechanism:** Unit tests on charset validator, envelope shape,
mode selection, error mapping, retry semantics; integration tests on
real Dolt server for both modes, concurrent-write injection, lock
back-to-back, and `dolt_gc` reclaim.

**Verdict:** Achieved. Test plan is concrete and proportionate.

### Findings

#### must-fix

- **M3 (Goal 5: pre-compaction state on integrity-mismatch).**
  Same content and remedy as Leg 1 M1. Re-anchored here against Goal 5.

#### should-fix

- **S5 (Goal 6: per-phase mapping under-specified).**
  Same content and remedy as Leg 1 S1. Re-anchored here against Goal 6.

- **S6 (Goal 2: drift-detection mechanism missing).**
  Same content and remedy as Leg 1 S3. Re-anchored here against Goal 2.

### Open Questions raised by this leg

None. All findings actionable in this round.

---

## Consolidated finding count

- must-fix: 2 distinct gaps (M1≡M3, M2)
- should-fix: 4 distinct gaps (S1≡S5, S2, S3≡S6, S4)
- Total design-doc edit sites: ~7 sections (Algorithms, Interface, Default
  human output, JSON envelope, TD-10, Implementation Plan Phase 2 step 10
  & step 11, Risks R-3/R-9 + new R for drift, Open Questions)
