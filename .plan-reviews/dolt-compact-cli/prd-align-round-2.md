# PRD Alignment Round 2 — Round Log

Round: prd-align-2 (mol-idea-to-plan.prd-align-2, bead `gc-fm7r62`)
Date: 2026-05-04
Coordinator: gascity/gastown.nux

## Overview

Two legs (`constraints-compliance`, `non-goals-enforcement`) executed
inline. Reports captured in `prd-align-round-2-legs.md`; bead manifest
in `prd-align-round-2-beads.tsv` (matches round 1's pattern of inline
legs with TSV manifest).

The legs surfaced **1 must-fix** and **2 should-fix** items. Per the
step description ("Apply every must-fix and should-fix item unless it
is objectively wrong, and if you reject one, record why in the round
log"), all three were applied. None rejected.

This round is structurally smaller than round 1 because round 1 closed
the largest gaps — the integrity-mismatch rollback path, Q14 threshold
semantics, per-step observability, single-source-of-truth drift, and
the formula-driven integration test were all added there. Round 2's
remit (Constraints + Non-Goals) found a tighter set of remaining
gaps: a documentation requirement (M1), a structural inconsistency
(S1), and a Non-Goal-defense framing (S2).

## Findings → Applied changes

### M1 — Manual recovery procedure missing from design doc

**Finding (Leg 1).** PRD Constraint 11 + Q5 clarification mandates
"Document the manual recovery procedure for operators in the design
doc." The design covered auto-recovery (Algorithms / Surgical lock-
then-cleanup line 261-262) and partial manual recovery (R-10's flatten-
failed-rollback procedure) but deferred the full procedure to
`engdocs/contributors/dolt-compact.md` (a PR2 deliverable, not part
of the design doc). Plan-review rounds cannot audit a procedure that
doesn't exist yet.

**Applied.**

- New top-level section `## Operations` inserted between `## Risks
  and Mitigations` and `## Implementation Plan`. The placement reflects
  the section's role: operator-facing concerns that follow the design
  and risk descriptions but precede the implementation plan.
- New subsection `### Manual Recovery (resolves PRD Q5 / Q11
  clarification)` consolidates the four operator-facing sub-cases:
  - **MR-1. Crash mid-flatten with successful in-CLI rollback.** No
    operator action; envelope's `outcome = integrity-mismatch` plus
    surfaced `pre_hash` give context; `dolt_gc` skipped on the
    mismatch path so chunks remain reclaimable by the next successful
    cycle.
  - **MR-2. Crash mid-flatten with failed in-CLI rollback (R-10).**
    Step-by-step procedure: read `pre_hash` from the last envelope,
    connect via `gc dolt sql`, issue `CALL DOLT_RESET('--hard',
    '<pre_hash>')`, verify HEAD, confirm row counts. Subsumes R-10's
    procedure under the consolidated section.
  - **MR-3. Crash mid-surgical (any phase before swap).** Auto-cleanup
    via the next CLI invocation's lock-then-cleanup ordering; manual
    cleanup option documented (CLI-driven path preferred over direct
    `DOLT_BRANCH('--delete', '--force', ...)` to retain the lifecycle
    lock invariant).
  - **MR-4. Stuck advisory lock (pathological).** Investigation step
    (`SELECT IS_FREE_LOCK('dolt_lifecycle')`, `SHOW FULL PROCESSLIST`)
    plus the explicit "escalate to mayor before manually releasing"
    rule, since releasing the lock under a running compactor risks
    the integrity-mismatches the rest of the design's recovery paths
    are built to prevent.
- The engdocs file (`engdocs/contributors/dolt-compact.md`) remains
  the long-form operator guide (Phase 2 step 13 unchanged); the
  design doc's section is the auditable summary that plan-review
  rounds can verify against.
- Executive Summary: added a paragraph summarising round 2's three
  changes alongside the existing round 1 paragraph.

### S1 — Validator missing from Key Components section

**Finding (Leg 1).** PRD Constraint 8 ("no SQL injection") was covered
in three different places (Trust and Discovery line 362-364, Phase 2
module list line 191-193, R-6 mitigation) but the Key Components
section (lines 366-449) listed 9 components in a numbered structure
that omitted the Validator. The numbering also diverged from the PR2
Implementation Plan's module ordering — Component 7 in Key Components
was the executor-binding regression test (a PR1 deliverable) while
PR2 module 7 in the Implementation Plan was `dolt_compact_validate.go`.
A reader scanning Key Components for "where does identifier validation
live?" hit no match.

**Applied.**

- Inserted new component **`### 6. Validator —
  cmd/gc/dolt_compact_validate.go`** between Verifier and Envelope,
  matching the PR2 module ordering. The new component documents:
  - Charset rule (`[A-Za-z0-9_-]` body, leading char `[A-Za-z0-9_]`)
    discharging Constraint 8.
  - Where it runs (before any SQL is issued for a given DB ID).
  - Failure outcome (`outcome = invalid-identifier` per the Error
    Class Taxonomy) — surfaces the previously missing anchor for
    that taxonomy entry.
  - Cross-reference to R-6's "factor as a single shared helper with
    `gc dolt-cleanup`" mitigation.
  - Unit test scope (boundary cases: empty, leading-digit, hyphen-
    leading, non-ASCII).
- Renumbered subsequent Key Components: Envelope 6 → 7, Executor-
  binding regression test 7 → 8, Pack delegate 8 → 9, Formula update
  9 → 10. Renumbering matches the PR2 module list 1:1 (apart from
  the PR1 component, which sits in numeric position 8 with a clear
  PR1/PR2 marker in its heading).
- Updated the cross-reference in Two-PR Shipping Order PR1 step 4
  ("see Component 7" → "see Component 8") to track the renumber.

### S2 — `--per-db-timeout` introduces a new parameter; defend against Non-Goal 7

**Finding (Leg 2).** TD-5 introduces `--per-db-timeout` as a 1h-default
flag. PRD Non-Goal 7 ("no new modes, no parameter tuning") admits a
strict reading that forbids this. The parameter is actually fine — it
is a wrapper-level execution budget, not a parameter of the flatten
or surgical algorithms — but TD-5 doesn't draw that distinction
explicitly, leaving the Non-Goal-7 defense unstated.

**Applied.**

- Extended TD-5 with a new paragraph anchoring `--per-db-timeout` to
  the orchestration layer:

  > `--per-db-timeout` is a **wrapper-level execution budget**
  > enforced by `context.WithTimeout` around each per-DB invocation;
  > it is **not a parameter of the flatten or surgical algorithms**,
  > which continue to consume the four formula-bound vars
  > (`commit_threshold`, `keep_recent`, `mode`, `databases`)
  > verbatim. PRD Non-Goal 7 ("no new modes, no parameter tuning")
  > addresses algorithm internals; the deadline is orthogonal — it
  > bounds wall-clock cost in the orchestration layer without
  > changing flatten or surgical semantics. If a city later wants
  > the deadline to live in the formula (e.g., to tune per-formula
  > instead of per-binary), the addition is additive (new formula
  > var, new CLI flag default sourced from the var, no algorithm
  > change). This framing also explains why `--per-db-timeout` is
  > intentionally absent from the formula/CLI defaults consistency
  > test (Phase 2 step 10): it has no formula-bound counterpart.

- Aligned the Phase 2 step 10 consistency-test description: appended
  an explanatory parenthetical so a future test author who notices
  `--per-db-timeout` is missing from the consistency test understands
  why and what would trigger an extension:

  > (`--per-db-timeout` is intentionally not in scope; per TD-5 it
  > is a wrapper-level execution budget, not a formula-bound
  > algorithm parameter — when a city later wires it into the
  > formula, the test should be extended at the same time.)

## Items rejected

None.

## Verification

- Read the resulting design doc end-to-end (sections changed:
  Executive Summary, Key Components / 5 (Verifier — wording around
  `pre_hash` unchanged but renumbered context), Key Components / 6
  (Validator, new), Key Components / 7-10 (renumbered), Two-PR
  Shipping Order PR1 step 4 (cross-reference updated), TD-5
  (extended), Operations (new top-level section with 4 MR sub-cases),
  Implementation Plan / Phase 2 / step 10 (scope note appended).
- Section-heading audit (`grep -n '^## \|^### \|^#### '`) confirms
  structure is intact:
  - 11 top-level `##` sections (added Operations between Risks and
    Implementation Plan).
  - Key Components 1-10 sequential and 1:1 with the PR2 module list
    in Implementation Plan / Phase 2.
  - Manual Recovery sub-cases MR-1 through MR-4 all present.
- Cross-reference audit: the only "Component N" reference in the doc
  (Two-PR Shipping Order PR1 step 4) was updated; no other references
  needed changes.
- All findings either applied or explicitly justified. No items left
  open.

## Next round

PRD-alignment round 3 (bead `gc-lygvkw`, formula step
`mol-idea-to-plan.prd-align-3`). Per the manifest, that's the third
and final PRD-alignment pass before the plan-review-{1,2,3} rounds
begin. The exact leg charters are defined in the round-3 bead
description.
