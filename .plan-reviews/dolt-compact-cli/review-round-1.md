# Plan Self-Review Round 1 — Round Log

Round: plan-review-1 (mol-idea-to-plan.plan-review-1, bead `gc-imyo2c`)
Date: 2026-05-04
Coordinator: gascity/gastown.furiosa

## Overview

Two legs (`completeness`, `sequencing`) executed inline. Reports
captured in `review-round-1-legs.md`; bead manifest in
`review-round-1-beads.tsv` (matches the prd-align-round-{1,2,3}
pattern of inline legs with TSV manifest).

The legs surfaced **2 must-fix** and **6 should-fix** items
(M1, M2 from the completeness/sequencing seam introduced by round 3's
Step-execution contract; S1-S4 completeness gaps; S5-S8 sequencing
clean-ups). Per the step description ("Apply every must-fix and
should-fix item unless it is objectively wrong, and if you reject
one, record why in the round log"), all eight were applied. None
rejected.

This round shifts from PRD-vs-design alignment (rounds 1-3) to
**plan self-review against implementability** — whether the plan
itself, treated as a contract for a future implementer, is complete
and correctly sequenced. The most consequential findings (M1 cache
mechanism, M2 `needs`-chain reorder) close a structural gap that
round 3's Step-execution contract introduced but did not fully
resolve: the bookkeeping steps' formula descriptions could not be
authored without a concrete cache substrate (M1), and the
`phases.inspect` row of the Per-step bead mapping table was
unreachable under the existing `inspect → compact → verify → report`
`needs` chain (M2). Both are now bound.

## Findings → Applied changes

### M1 — Envelope cache mechanism unspecified

**Finding (Leg 1).** The Step-execution contract said "the design doc
does not specify the cache shape" — too aggressive a deferral for
the implementation plan to be implementable. PR2's Phase 2 step 12
authors must instruct the dog (in formula step descriptions) which
substrate to query; the design must commit to one.

**Applied.** Bind option A — molecule (root bead) metadata under the
reserved key `gc.formula.envelope`. Rationale: bd is the design's
persistence substrate; envelope is small (sub-kilobyte typical);
survives session boundaries; future-compat to file-on-disk or
notes-string is additive.

- Step-execution contract / Dog-runtime mechanism subsection rewrote
  the previously-deferred paragraph into a concrete binding
  (substrate, write/read commands, parse path, escalation hook).
- Two-PR Shipping Order PR2 step 11 extended: the bookkeeping steps'
  description bullets now spell out "read
  `metadata.gc.formula.envelope` from the molecule via `gc bd show`,
  parse, and close per `phases.<this-step>`."
- Implementation Plan Phase 2 step 12 similarly extended (matches
  PR2 step 11 wording).
- Implementation Plan Phase 2 step 10 unit-test list extended:
  cache-write happy-path test (the compact-step orchestration writes
  `gc.formula.envelope` to the molecule's metadata before exiting).
- Risks and Mitigations / R-12 (new): envelope cache write failure
  (bd unavailable mid-cycle) — mitigation paragraph documents the
  fall-back-to-`closed` behavior, the integration-test detection
  path, and operator re-trigger semantics.
- Executive Summary: paragraph added covering round-1's five
  binding-and-completeness fixes (M1, M2, S1-S4) and three
  sequencing clean-ups (S5-S8) alongside the existing rounds 1-3
  paragraphs.

### M2 — `needs` chain forces inspect to close before the envelope exists

**Finding (Leg 2).** The current `mol-dog-compactor.toml` declares
`inspect → compact → verify → report` via `needs = [...]` chains.
Under that chain, the inspect step bead must close before compact
becomes claimable; compact's CLI invocation is what produces the
envelope; therefore inspect closes before the envelope exists. The
Per-step bead mapping table's `phases.inspect` rows (`failed`,
`escalate`, etc.) can never be exercised — the dog has nothing to
read when it closes inspect.

**Applied.** Bind option A from the leg report — reorder the formula
`needs` chain so `compact` is dependency-free and `inspect`,
`verify`, `report` all `needs = ["compact"]`. The reorder makes the
trigger model match the dog's claim order.

- Step-execution contract / Designated trigger step subsection
  appended a new "Formula `needs`-chain alignment" paragraph that
  explicitly captures the reorder, the rationale (without it,
  `phases.inspect` is unreachable), and the cross-reference to PR2
  step 11 / Phase 2 step 12 where the change lands.
- Two-PR Shipping Order PR2 step 11 extended with a "Reorder the
  `needs` chain" sub-bullet: drop `needs = ["inspect"]` from compact;
  set `needs = ["compact"]` on each of inspect, verify, report.
- Implementation Plan Phase 2 step 12 similarly extended (matches
  PR2 step 11 wording).
- Risks and Mitigations / R-13 (new): the reorder may surprise
  reviewers reading the formula linearly; mitigation is that step
  `description` fields preserve the conceptual flow narratively, a
  TOML-top comment block captures rationale, and the formula-driven
  integration test exercises the actual claim order.

### M3 (procedural / audit) — PR2 step 11 (integration tests) lands before step 12 (formula update)

**Finding (Leg 2).** The formula-driven integration test in step 11
("load `mol-dog-compactor.toml`, resolve the step's `executor`,
exec it with `--json`, …") cannot pass until step 12 (formula
update) is applied. A naïve implementer following the numeric step
order might conclude the test infrastructure is broken when the
test is red on first run.

**Applied.** Phase 2 step 11 received an explicit "Sequencing note"
appendix calling out the expected red→green TDD rhythm, distinguishing
the formula-driven leg (red until step 12) from the other integration
tests (independent of the formula update). No structural change —
the order is correct; the note is documentation.

### S1 — CHANGELOG.md update missing from implementation plan

**Finding (Leg 1).** The repo's `CHANGELOG.md` follows the Keep-a-
Changelog convention with `## [Unreleased]` accumulator; recent
entries describe user-facing CLI/schema changes. PR1 (new schema
fields) and PR2 (new CLI + envelope) are user/contributor-facing;
both warrant CHANGELOG entries. Plan had no step for either.

**Applied.**

- Phase 1: new step 6 inserted between current step 5 and
  current step 6 (Open PR1) — "Update CHANGELOG.md" with
  `### Added` entries for the new schema fields and regression
  test. Open PR1 renumbered to step 7.
- Phase 2: new step 14 inserted between current step 13 and
  current step 14 (Open PR2) — "Update CHANGELOG.md" with
  `### Added` entries for `gc dolt-compact`, `gc dolt compact`
  pack delegate, `gc.dolt.compact.v1` envelope; `### Changed`
  entry for `mol-dog-compactor` migration off safely-skip. Open
  PR2 renumbered to step 15.

### S2 — PR rollback semantics not documented

**Finding (Leg 1).** Plan covered algorithm-level rollback (flatten
reverts to `pre_hash`; surgical halts before swap) and operator-
facing manual recovery (MR-1..MR-4) but not PR-level rollback for
incident response.

**Applied.** New `### PR Rollback` subsection added under
`## Operations` (after Manual Recovery). Three sub-paragraphs:

- **PR1 rollback.** Additive-only; revert is mechanical;
  optional-with-omitempty fields parse cleanly under pre-PR1
  schema; effect is the contract-test removal (silent-failure
  regression class re-opens until re-landed).
- **PR2 rollback.** Behavioral; revert restores `zfc_exempt = true`
  on compact, drops `executor`, returns formula `needs` chain to
  its pre-PR2 ordering; dog returns to safely-skip; `gc
  dolt-compact` CLI binary remains shipped (operator-facing
  surface preserved); `mol-dog-compactor` order unchanged.
  Acceptable degradation — the silent-failure baseline is the
  pre-PR2 state.
- **Sequencing of reverts.** PR2 must revert before PR1 can revert
  (PR1 is the prerequisite). Reverting PR1 with PR2 still landed
  fails formula-parse validation on the next dog claim.

### S3 — Auto-discovery error path absent from exit-code table

**Finding (Leg 1).** Exit-code table row 3 (exit `2`) listed four
example invocation errors but omitted `gc rig list --json` invocation
failure. A reader might miscategorize this case as `internal-error`
(escalate set, exit `1`).

**Applied.**

- Exit-code table row 3 example list extended: now includes
  "`gc rig list --json` invocation failure when auto-discovery is
  in scope — the registry file missing or unparseable."
- Phase 2 step 10 unit-test list extended: rig-list invocation
  failure path → assert exit code 2 and a well-formed envelope with
  `errors[].kind = "rig-registry"`, `inspect.value = "failed"`,
  `compact.value = "failed"`, no per-DB records.

### S4 — `--help` text content not specified

**Finding (Leg 1).** CLI surface section defined flags but not the
Cobra `Long` text content. Cleanup's `--help` is a multi-paragraph
operator surface; compact would benefit from matching depth. Phase 2
step 8 implementation plan didn't bind this, so an implementer might
ship a one-liner.

**Applied.** Phase 2 step 8 (Cobra command body) extended with an
explicit `--help` content checklist: dual-surface invocation
pattern, discovery rule (rig-registry-only / no SHOW DATABASES),
threshold semantics with the `--threshold=0` lever, 3-row exit-code
summary, pointer to engdocs operator guide; cleanup's Cobra strings
are the depth-of-coverage benchmark.

### S5 — Phase 1 sequencing violates TDD ordering

**Finding (Leg 2).** Phase 1 listed step 4 (regression test) after
step 3 (apply tags). Test never goes red — anti-AGENTS.md TDD
("Write the test first, watch it fail, make it pass").

**Applied.** Phase 1 steps 3 and 4 swapped:

- Step 3 (was 4): Land the regression test (red).
  Initial run is expected to fail on every formula step lacking
  annotation; failure messages name the exact formula+step.
- Step 4 (was 3): Audit embedded formulas (green).
  For each red step, either confirm resolvability or apply tags.
  After every formula is annotated, the regression test goes
  green.
- New introductory paragraph above step 1 captures the TDD rhythm.

### S6 — Phase 2 unit tests grouped at end (step 10) reads as anti-TDD

**Finding (Leg 2).** Plan had a single trailing step 10 ("Unit
tests") containing the entire unit-test checklist after all
implementation work in steps 1-9. Anti-TDD per AGENTS.md.

**Applied.**

- New introductory paragraph above Phase 2 step 1 captures
  test-alongside-each-module rhythm; step 10 is now a consolidated
  checklist, not a trailing pass.
- Phase 2 individual implementation steps 2-7 — each appended
  with a "test alongside" reminder calling out the specific
  unit-test scope landing with that module (rig-list parse / probe;
  charset boundary cases; algorithm sequence; rebase plan with
  retry; row-count comparison; envelope shape and aggregation
  rules).
- Phase 2 step 10 reframed as "consolidated checklist" with
  clarifying language; the formula/CLI defaults consistency test
  remains its only standalone-by-design entry.

### S7 — Phase 3 has no enforcement / acceptance criterion

**Finding (Leg 2).** Phase 3 listed four operator-gated steps with
no owner, no deadline, no recording mechanism. PRD Acceptance 2
("`mol-dog-compactor` cycle runs end-to-end") would be unverified if
Phase 3 is silently skipped.

**Applied.**

- New "**Owner**" paragraph above Phase 3 step 1: the PR2 author
  (or designated operator) runs Phase 3 within 48h of PR2 merging;
  results recorded as comments on the PR2 work bead.
- Step 1 reworded to recommend immediate trigger via `gc sling
  mol-dog-compactor` over the 24h-cooldown wait.
- New step 5: "Record verification result" — comment on the PR2
  work bead with report-bead ID, post-cycle commit-count delta,
  and deacon report excerpt confirming non-skip.
- R-9 cross-reference paragraph extended: "Phase 3 step 5 records
  the manual-smoke result on the PR2 work bead so the 48h
  verification window is auditable."

### S8 — Hidden dependency: `dolt_lifecycle_lock.go` reuse path

**Finding (Leg 2).** Algorithms / Flatten and Algorithms / Surgical
both call "acquire advisory lock"; the PRD Rough Approach references
`dolt_lifecycle_lock.go` reuse. Phase 2 step 1 ("Skeleton") doesn't
mention whether the lock helper is currently exported in a way the
new compact modules can consume. If not exported, factoring is a
hidden setup task for step 1.

**Applied.**

- Phase 2 step 1 extended: explicit factoring instruction — "If
  `dolt_lifecycle_lock.go`'s acquire/release functions are not
  currently exported …, factor them in this step. The factoring is
  mechanical (rename to exported identifiers; move to a shared
  sub-package if necessary); cleanup's existing call site is
  updated to the new API in the same commit to avoid two-step
  churn."
- Phase 2 step 5 (Surgical executor) appended: "Surgical's
  lock-then-cleanup ordering depends on the lock helper from step 1
  being callable from this module."

## Items rejected

None.

## Verification

- Read the resulting design doc end-to-end (sections changed:
  Executive Summary; Proposed Design / Step-execution contract
  (Designated trigger step extended with Formula `needs`-chain
  alignment paragraph; Dog-runtime mechanism rewritten as
  bd-metadata binding); Two-PR Shipping Order PR2 step 11 (extended
  with `needs` reorder + cache-write semantics); Error Class
  Taxonomy exit-code paragraph (exit-`2` example list extended);
  Risks and Mitigations / R-9 (Phase 3 step 5 cross-reference) and
  R-12, R-13 (new); Operations / PR Rollback (new subsection);
  Implementation Plan / Phase 1 (steps 3+4 swapped, new step 6
  CHANGELOG, renumber 6→7; intro TDD paragraph) / Phase 2 (intro
  TDD paragraph; step 1 lock factoring; steps 2-7 test-alongside
  reminders; step 8 --help text checklist; step 10 reframed as
  consolidated checklist with rig-list-failure + cache-write
  entries; step 11 sequencing note; step 12 reorder + cache-write;
  step 13 MR cross-ref; new step 14 CHANGELOG; renumber 14→15) /
  Phase 3 (Owner paragraph, step 1 trigger recommendation, new
  step 5 recording).
- Section-heading audit (`grep -n '^## \|^### '`) confirms
  structure intact:
  - 11 top-level `##` sections (unchanged from round 2; PR
    Rollback is a `### ` subsection nested under `## Operations`).
  - Risks R-1 through R-13 sequential and 1:1 with risk types
    discussed.
  - Operations subsections: Manual Recovery (4 MR sub-cases) +
    PR Rollback (3 sub-cases + sequencing) — total 2 subsections.
  - Implementation Plan: Phase 1 has 7 steps (was 6); Phase 2
    has 15 steps (was 14); Phase 3 has 5 steps (was 4).
  - Open Questions: 4 Resolutions (OQ-1, OQ-4, OQ-5, OQ-7) + 3
    Remaining (OQ-2, OQ-3, OQ-6) = 7 total, unchanged from
    round 3.
  - Key Components 1-10 sequential and 1:1 with the PR2 module
    list, unchanged from round 2.
- Cross-reference audit:
  - PR2 step 11 references "Step-execution contract / Formula
    `needs`-chain alignment" — present.
  - Phase 2 step 12 references "Step-execution contract / Formula
    `needs`-chain alignment" — present.
  - Phase 2 step 12 references "Per-step bead mapping table" —
    present.
  - R-9 references "Phase 3 step 5" — present.
  - R-12 references "Phase 2 step 10's cache-write unit test" —
    present.
  - Phase 2 step 5 references "step 1's lock helper" — present.
  - Phase 1 step 6 references "PR1 issue / bead id per repo
    convention" — convention is to cite the bead id; the
    implementer fills in.
- All findings either applied or explicitly justified. No items
  left open.

## Round 1 fix counts (live-conversation summary)

- **Must-fix applied:** 2
  - M1 — Envelope cache mechanism bound to molecule metadata
    `gc.formula.envelope` (Step-execution contract / Dog-runtime
    mechanism rewritten + PR2 step 11 + Phase 2 step 12 + Phase 2
    step 10 cache-write unit test + R-12).
  - M2 — Formula `needs` chain reordered so `compact` is
    dependency-free; bookkeeping steps `needs = ["compact"]`
    (Step-execution contract / Designated trigger step extended +
    PR2 step 11 + Phase 2 step 12 + R-13).
- **Should-fix applied:** 6
  - S1 — CHANGELOG.md updates added to Phase 1 and Phase 2.
  - S2 — `## Operations / ### PR Rollback` subsection added.
  - S3 — Exit-code table row 3 + Phase 2 step 10 extended for
    rig-list invocation failure.
  - S4 — Phase 2 step 8 `--help` text checklist added.
  - S5 — Phase 1 steps 3 and 4 swapped (TDD red→green); intro
    paragraph added.
  - S6 — Phase 2 intro TDD paragraph; per-module test-alongside
    reminders on steps 2-7; step 10 reframed as consolidated
    checklist.
  - S7 — Phase 3 owner paragraph + step 1 trigger recommendation
    + new step 5 recording.
  - S8 — Phase 2 step 1 lock-helper factoring instruction; step 5
    cross-reference.

(M3 was a procedural/audit observation about PR2 internal
sequencing; resolved by appending a "Sequencing note" to
Phase 2 step 11. Not counted as a separate must-fix because the
order is correct under TDD; the change is documentation.)

- **Items rejected:** 0
- **Plan-review-1 scope this round:** missing setup/migrations/
  tests/docs/rollback/hidden dependencies (Leg 1) plus
  ordering/serialization/circularity (Leg 2). The dual-leg
  pressure-test surfaced the cache-mechanism + `needs`-chain seam
  as the consequential gap; the remaining items are
  documentation/sequencing clean-ups.

## Next round

Plan self-review round 2 (bead `gc-s6epvv`, formula step
`mol-idea-to-plan.plan-review-2`). Per the bead description, that
round shifts focus from completeness/sequencing to **risk** and
**scope-creep** — technical/dependency/rollback/unknown-unknown
risks; gold-plating, over-engineering, premature optimization,
defer candidates. The design is now structurally implementable
end-to-end; round 2's pressure-test is "is this design carrying risk
that needs an explicit mitigation, or scope that can be deferred."
