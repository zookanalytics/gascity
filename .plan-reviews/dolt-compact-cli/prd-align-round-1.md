# PRD Alignment Round 1 — Round Log

Round: prd-align-1 (mol-idea-to-plan.prd-align-1, bead `gc-qn5p3r`)
Date: 2026-05-04
Coordinator: gascity/gastown.nux

## Overview

Two legs (`requirements-coverage`, `goals-alignment`) executed inline.
Reports captured in `prd-align-round-1-legs.md`; bead manifest in
`prd-align-round-1-beads.tsv` (matches the predecessor pattern of inline
legs with TSV manifest).

The legs surfaced **2 must-fix** and **4 should-fix** items. Per the step
description ("Apply every must-fix and should-fix item unless it is
objectively wrong, and if you reject one, record why in the round log"),
all six were applied. None rejected.

## Findings → Applied changes

### M1 / M3 — integrity-mismatch must leave db in pre-compaction state

**Finding (Leg 1 M1, Leg 2 M3).** PRD Constraint and Goal 5 both bind
"mismatch → leave db in pre-compaction state." Design's flatten algorithm
committed before verifying with no rollback; surgical algorithm proceeded
to swap branches regardless of the verify verdict. Design-review-legs
Leg 4 (line 443-445) had explicitly acknowledged the design left the DB
"at the failed-post-compaction state — operator must manually verify
before re-running" — a contradiction with the PRD that was inherited.

**Applied.**
- `design-doc.md` Algorithms / Flatten: capture `pre_hash := SELECT
  @@<db>_head` before `DOLT_RESET --soft`; on post-row-count mismatch,
  issue `DOLT_RESET --hard pre_hash`, set `outcome = integrity-mismatch`,
  skip `dolt_gc`, release lock. Added a paragraph explaining the rollback
  path discharges the PRD constraint and that the captured `pre_hash` is
  also surfaced in the envelope for operator manual recovery.
- `design-doc.md` Algorithms / Surgical: replace the unconditional swap
  block with **halt-before-swap** branches on each escalate-class
  outcome (`concurrent-write-fatal` after retry, `integrity-mismatch`,
  `concurrent-write-fatal` from main-HEAD-moved-since-pre_hash). On any
  escalate-class outcome, drop `compact-*` working branches and release
  the lock without touching `main`. Added the discharge paragraph.
- `design-doc.md` Key Components / 5. Verifier: extended to note that
  on integrity-mismatch the verifier returns `pre_hash` so the executor
  can roll back (flatten) or halt-before-swap (surgical).
- `design-doc.md` Risks and Mitigations / R-3: extended the
  concurrent-write surgical risk to spell out the halt-before-swap rule.
- `design-doc.md` Risks and Mitigations / R-10 (new): rollback failure
  on flatten — operator-recovery path documented (`pre_hash` surfaced in
  envelope; `dolt_gc` skipped on mismatch so chunks remain on disk).
- `design-doc.md` JSON envelope: added `databases[].pre_hash` field with
  rationale prose.
- `design-doc.md` Executive Summary: added one sentence summarising the
  rollback-path discharge.

### M2 — Open Question Q14 (`--threshold` semantics on explicit DBs)

**Finding (Leg 1 M2).** PRD Open Q14 (lines 274-276 of `prd-draft.md`)
listed unresolved; design-review-legs Leg 1 (api, lines 67-73) had
recommended a binding rule but it never propagated into the design doc.

**Applied.**
- `design-doc.md` Proposed Design / "Threshold Semantics (resolves PRD
  Q14)" (new subsection between Error Class Taxonomy and Trust and
  Discovery): `--threshold` gates every candidate uniformly; explicit
  DBs below threshold are reported `outcome = below-threshold` and
  skipped; `--threshold=0` is the unconditional-compaction lever.
  Rationale paragraph anchors it to formula contract uniformity.
- `design-doc.md` Default human output: extended example block with a
  second invocation showing `gc dolt compact hq gascity --threshold=500`
  producing one `→ candidate` and one `→ below threshold, skip` line.
- `design-doc.md` Open Questions: prelude paragraph notes Q14 resolved.

### S1 / S5 — per-step observability under-specified

**Finding (Leg 1 S1, Leg 2 S5).** Goal 6 binds "close inspect, compact,
verify, and report step beads independently." The single final envelope
(TD-2) carries per-DB results; the dog had to derive aggregate per-phase
status without a documented rule.

**Applied.**
- `design-doc.md` JSON envelope: added top-level `phases` block (one
  string-valued field per formula step: `inspect`, `compact`, `verify`,
  `report`). Allowed values are `ok` / `partial` / `escalate` /
  `failed`.
- `design-doc.md` JSON envelope / "Per-step bead mapping (resolves Goal
  6)" (new subsection): table mapping phase value → dog action plus
  CLI-side aggregation rules for each phase. Anchors the `escalate` set
  to the existing Error Class Taxonomy table so the `phases` block is a
  faithful aggregate, not a new contract.
- `design-doc.md` Implementation Plan / Phase 2 step 10: "envelope
  shape (including the `phases` aggregation rules)" added to unit test
  coverage.
- `design-doc.md` Data Model / Envelope schema: notes the
  `phases.{inspect,compact,verify,report}` and `databases[].pre_hash`
  fields explicitly.

### S2 — Acceptance Criteria 2 lacked automated coverage

**Finding (Leg 1 S2).** PRD Acceptance 2 binds the `mol-dog-compactor`
cycle running end-to-end (dog claims, shells out, compaction occurs,
integrity verified, `dolt_gc` runs, report sent). Design's only e2e
check was R-9's manual smoke. The 24h cooldown leg can't be tested in
CI, but the formula→CLI→envelope→step-close path can.

**Applied.**
- `design-doc.md` Implementation Plan / Phase 2 step 11: added a
  formula-driven integration test that loads
  `mol-dog-compactor.toml`, resolves the step's `executor`, execs it
  with `--json`, and asserts the parsed `phases` block plus per-DB
  outcomes drive correct step-closure decisions. Also added the
  rollback-injection integration tests (mismatch → flatten reverts to
  `pre_hash`; surgical halts before swap).
- `design-doc.md` Risks and Mitigations / R-9: extended to point at
  the new formula-driven integration test as the automated discharge,
  with manual smoke remaining for the 24h-cooldown leg only.

### S3 / S6 — single-source-of-truth drift not enforced

**Finding (Leg 1 S3, Leg 2 S6).** Goal 2 binds "no hardcoded values
that could drift from the formula." Design relied on convention; no
mechanism prevents a future PR from updating one side and not the
other.

**Applied.**
- `design-doc.md` Implementation Plan / Phase 2 step 10: added a
  formula/CLI defaults consistency unit test that loads
  `mol-dog-compactor.toml`, extracts the `commit_threshold`,
  `keep_recent`, `mode`, and `databases` formula vars, and asserts the
  CLI flag defaults match.
- `design-doc.md` Risks and Mitigations / R-11 (new): formula vars
  drift from CLI defaults — the consistency test fails CI on drift;
  named test with explicit failure message.
- `design-doc.md` Executive Summary: added one sentence summarising
  the drift-detection discharge alongside the e2e test discharge.

### S4 — Story 3 deferral not explicitly called out

**Finding (Leg 1 S4).** PRD Story 3 (`--inspect-only` operator
debugging flow) was deferred to v2 by TD-10, but the design doc never
said so out loud — a reader following the Story 3 trail hits a dead
end.

**Applied.**
- `design-doc.md` TD-10: appended a paragraph stating Story 3 is
  partially deferred along with `--inspect-only`, and pointing v1
  operators to the existing inspector-phase output (`inspect:` lines in
  human output, per-DB `outcome`/`pre_commits` in the JSON envelope) for
  the equivalent visibility.

## Items rejected

None.

## Verification

- Read the resulting design doc end-to-end (sections changed: Executive
  Summary, Algorithms / Flatten, Algorithms / Surgical, Threshold
  Semantics (new), Key Components / 5. Verifier, JSON envelope including
  Per-step bead mapping (new), Default human output, Data Model /
  Envelope schema, TD-10, Implementation Plan / Phase 2 / steps 10 & 11,
  Risks R-3 / R-9 / R-10 (new) / R-11 (new), Open Questions prelude).
- Section-heading audit (`grep -n '^## \|^### '`) confirms structure is
  intact; no broken cross-references.
- All findings either applied or explicitly justified. No items left
  open.

## Next round

PRD-alignment round 2 (bead `gc-fm7r62`, formula step
`mol-idea-to-plan.prd-align-2`). Per the bead description, that round
runs `constraints-compliance` and `non-goals-enforcement` against the
now-updated design doc, this round-1 log, and the PRD draft.
