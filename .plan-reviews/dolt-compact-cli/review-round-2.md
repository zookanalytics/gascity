# Plan Self-Review Round 2 — Round Log

Round: plan-review-2 (mol-idea-to-plan.plan-review-2, bead `gc-s6epvv`)
Date: 2026-05-04
Coordinator: gascity/gastown.furiosa

## Overview

Two legs (`risk`, `scope-creep`) executed inline. Reports captured
in `review-round-2-legs.md`; bead manifest in
`review-round-2-beads.tsv` (matches the
prd-align-round-{1,2,3} + plan-review-1 pattern of inline legs with
TSV manifest).

The legs surfaced **1 must-fix** and **5 should-fix** items (M1
cache-write authority ambiguity from leg 1; S1 `dolt_gc` failure
unenumerated, S2 dog idempotence on restart, S3 `database-not-found`
unenumerated, S4 R-12 wording correction from leg 1; S5
`concurrent-write-collision` semantically empty from leg 2). Per
the step description ("Apply every must-fix and should-fix item
unless it is objectively wrong, and if you reject one, record why
in the round log"), all six were applied. None rejected.

This round shifts focus from completeness/sequencing (round 1) to
**risk and scope-creep**. Round 1 made the design structurally
implementable; round 2 pressure-tested the design as carrying
mitigated risk and disciplined scope. The most consequential
finding (M1 cache-write authority) closes a contract gap that
round 1's M1 binding ("envelope cache lives on molecule metadata
under `gc.formula.envelope`") introduced but left ambiguous as to
**which agent writes the metadata**. Resolution: dog writes (per
the existing Step-execution contract / Dog-runtime mechanism
prose); the CLI's responsibility ends at stdout. The remaining
findings are taxonomy clarifications (S1, S3, S5), an idempotence
guard (S2), and a wording correction (S4).

## Findings → Applied changes

### M1 — Cache-write authority is ambiguous between "CLI writes" and "dog writes"

**Finding (Leg 1).** The Step-execution contract / Dog-runtime
mechanism subsection binds the cache write to the dog: "The dog
updates the molecule via `gc bd update <molecule-id>
--set-metadata gc.formula.envelope='<json>'` immediately after
parsing the CLI output and before closing the `compact` step
bead." But two artifacts elsewhere read as if the CLI writes:
(1) Phase 2 step 10's "cache-write happy path" unit test
description ("the compact-step orchestration writes
`gc.formula.envelope` to the molecule's metadata before
exiting — discharges M1's contract, R-12's CLI-side detection")
and (2) R-12's last sentence ("CI catches a missing-write
regression on the CLI side"). A naïve implementer following
Phase 2 numerically may add a CLI-side `--molecule-id` flag
and bd-shell-out to satisfy the step 10 test, duplicating the
dog-side write and creating belt-and-suspenders the design does
not contemplate.

**Applied.** Bind option **A** from the leg report — keep the
"dog writes" model and reframe Phase 2 step 10 + R-12 to match.
Rationale: smallest delta to round 1's M1 binding; matches the
existing Step-execution contract / Dog-runtime mechanism prose;
no new CLI dependency on bd substrate.

- Phase 2 step 10's "cache-write happy path" entry rewritten as
  "envelope-stdout shape and parseability" — the CLI's
  responsibility is to emit a parseable `gc.dolt.compact.v1`
  envelope to stdout that the dog can shell-pipe through `gc bd
  update --set-metadata` without further transformation. The
  test exercises only the CLI's contribution.
- R-12's last sentence rewritten — the formula-driven integration
  test (Phase 2 step 11) is now identified as the regression
  catcher for missing-write in the formula step description prose;
  the CLI-side test (Phase 2 step 10's envelope-stdout shape) is
  identified as the independent CLI-side regression catcher.
- Dog-runtime mechanism subsection's reference to the unit test
  updated: "Phase 2 step 10's envelope-stdout shape unit test pins
  the CLI's contribution; Phase 2 step 11's formula-driven
  integration test pins the dog-side write."

### S1 — `dolt_gc` failure not enumerated in the error taxonomy

**Finding (Leg 1).** The Algorithms / Flatten and Algorithms /
Surgical pseudocode both end with `CALL dolt_gc()` immediately
before releasing the lock. The 9-class error taxonomy covered
`database-deadline-exceeded` (gc hung past `--per-db-timeout`)
and `internal-error` (catch-all) but did not enumerate non-deadline
gc failures (out of disk, gc-internal error, gc-aborted-by-another-
connection). The compaction itself succeeded at this point — the
commit was made, row counts matched. What is the per-DB outcome?

**Applied.** Extend `internal-error` semantics — the per-DB
`outcome = internal-error` with `error_message` carrying the
gc-failure detail — and add an explicit operational note in the
Error Class Taxonomy paragraph spelling this out. R-4 also
extends to mention the non-deadline gc-failure path:
"Non-deadline `dolt_gc` failures (out of disk, gc-internal
error, gc-aborted-by-another-connection) follow the same
disk-reclaim-only-incomplete failure mode and the same
mitigation … the next cycle's gc reclaims what this one
missed." Phase 2 step 10 unit-test list extended: "the
`internal-error` carries gc-failure detail" case (per-DB
`error_message` carries the gc failure verbatim).

### S2 — Dog idempotence on restart loses first-invocation observability

**Finding (Leg 1).** If the dog crashes after CLI exit but before
the `gc bd update --set-metadata` write, metadata is empty when
the next dog claims the (still-open) compact step bead. The next
dog's instruction reads metadata empty and re-invokes the CLI.
The re-invocation is safe at the DB level (commit count is now
below threshold; second invocation skips), but the second
invocation's no-op envelope clobbers the first invocation's
canonical envelope via the metadata write — losing observability
for the cycle.

**Applied.** Extend the compact step's formula description (per
PR2 step 11 / Phase 2 step 12) with a leading **read-first
idempotence guard**:

- Step-execution contract / Dog-runtime mechanism subsection
  appended a new "Read-first idempotence guard" paragraph: the
  compact step's formula description leads with `gc bd show
  <molecule-id> --json | jq -r '.[0].metadata."gc.formula.envelope"
  // empty'`; if non-empty and parseable, skip the CLI invocation
  and the metadata write — proceed directly to closing the step
  bead from the cached envelope per the Per-step bead mapping
  table.
- PR2 step 11 updated: the `compact` step's description leads
  with the guard, then the existing flag-substituted CLI
  invocation, then the metadata write.
- Phase 2 step 12 updated identically (matches PR2 step 11
  wording).
- Phase 2 step 11 integration test scope extended: a
  cache-hit / read-first-idempotence test (simulate populated
  metadata, drive the compact step, assert the CLI is **not**
  invoked a second time) and a dog-side write test (assert the
  happy path populates metadata post-cycle).
- New R-14: "Dog crash between CLI exit and cache-write loses
  canonical envelope" — captures the failure mode, the read-first
  guard mitigation, and Phase 2 step 11's regression-catch role.

### S3 — `database-not-found` not enumerated in the error taxonomy

**Finding (Leg 1).** A typo in a positional DB arg or a stale
rig-registry entry results in `gc dolt-compact nonexistent_db`
reaching SQL execution with `USE nonexistent_db`. The validator
passes (charset OK); `USE` fails with the Dolt server's "Unknown
database" error. The 9-class taxonomy mapped this ambiguously to
`database-unreachable` (misleading — the server is up, only this
DB is missing) or `internal-error` (alarmist).

**Applied.** Add a 10th class `database-not-found` to the Error
Class Taxonomy table:

- Class string: stable; pre-PR2 envelopes never emitted it.
- Dog action: close `failed` (operator error); deacon nudge.
- Per-DB record's `error_message` carries the Dolt error
  verbatim ("Unknown database 'foo'").
- An operational note added under the table distinguishes
  `database-not-found` (reachable server, missing DB) from
  `database-unreachable` (connection refused, TCP timeout, port
  unresolved).
- Phase 2 step 10 unit-test list extended: "the `database-not-
  found` path" case (mocked `*sql.DB` returning the Dolt server's
  "Unknown database" error surfaces as `outcome =
  database-not-found`).
- Error-class count temporarily becomes 10 with this add and
  returns to 9 once leg 2's S5 fold removes
  `concurrent-write-collision` (net unchanged at 9 error
  classes plus `ok` for success).

### S4 — R-12's "CLI side" detection wording is incorrect

**Finding (Leg 1).** R-12's last sentence: "Additionally, the
integration test asserts the cache write happens; CI catches a
missing-write regression on the CLI side (independent of the
bd-unavailable runtime case)." If cache-write authority is
dog-side (per M1's resolution), "CLI side" detection is the wrong
framing. The integration test that asserts the cache write is
Phase 2 step 11's formula-driven leg, which exercises the dog
claim path (the prose-instructed write), not a CLI-side `gc bd
update` call.

**Applied.** Rewrite R-12's last sentence:

> "Additionally, the formula-driven integration test (Phase 2
> step 11) loads `mol-dog-compactor.toml`, exercises the compact
> step's executor invocation, and asserts the molecule's
> `gc.formula.envelope` metadata is populated post-cycle. CI
> catches a missing-write regression in the formula step
> description prose (independent of the bd-unavailable runtime
> case); the CLI's own contribution (a parseable envelope on
> stdout that the dog's shell pipeline can consume) is
> independently exercised by Phase 2 step 10's envelope-stdout
> shape unit test."

The wording aligns the regression-detection surface (formula
description prose) with the actual write authority (dog) and
clarifies that the CLI-side and dog-side regressions have separate
test catches.

### S5 — `concurrent-write-collision` error class is semantically empty

**Finding (Leg 2).** The taxonomy listed `concurrent-write-
collision` with `Dog action: observability only; final outcome
may be ok`. Walking the surgical algorithm pseudocode: per-DB
`outcome` is set once at end of the algorithm body; a successful
retry's `outcome = ok`, a failed retry's `outcome =
concurrent-write-fatal`. The `concurrent-write-collision` value
is never written to the per-DB `outcome` field — making the
class semantically empty as a terminal outcome. Retry
observability is provided by the per-DB `attempts` field.

**Applied.** Remove `concurrent-write-collision` from the Error
Class Taxonomy table:

- Table updated: row removed; combined with the
  `database-not-found` add from S3, the table stays at 10 rows
  (9 error classes plus `ok`).
- New paragraph below the table explaining retry observability
  via the `attempts` field: `attempts == 1` and `outcome == ok`
  → first try succeeded; `attempts > 1` and `outcome == ok` →
  retry succeeded; `attempts > 1` and `outcome ==
  concurrent-write-fatal` → retry failed.
- Explicit one-paragraph rationale: "A standalone `concurrent-
  write-collision` outcome class would have no terminal-state
  semantics — the per-DB `outcome` field is set once at end of
  the algorithm body, never to a transient mid-retry value."
- TD-6 updated to capture the fold: "Round-2 plan-review folded
  the original placeholder `concurrent-write-collision` into the
  per-DB `attempts` field … Round-2 also added `database-not-found`
  (round-2 S3) so operator-typo and stale-rig-registry cases
  surface a precise dog action distinct from server-
  unreachability. Net class count is unchanged at 9." (TD-6's
  "9 classes" framing counts error classes only; the table
  includes `ok` for a total of 10 rows — pre-existing
  convention preserved.)

## Items rejected

None.

## Verification

- Read the resulting design doc end-to-end (sections changed:
  Executive Summary; Proposed Design / Step-execution contract /
  Dog-runtime mechanism (read-first idempotence guard paragraph
  added); Two-PR Shipping Order PR2 step 11 (compact step
  description leads with read-first guard); Error Class Taxonomy
  table (`concurrent-write-collision` removed; `database-not-
  found` added; operational notes paragraph + retry-observability
  paragraph appended); Trade-offs / TD-6 (round-2 fold paragraph
  appended); Risks and Mitigations / R-4 (non-deadline gc-failure
  paragraph appended) / R-12 (last sentence rewritten) / R-14
  (new); Implementation Plan / Phase 2 step 10 (cache-write entry
  reframed as envelope-stdout shape; gc-failure-detail entry
  added; database-not-found entry added) / Phase 2 step 11
  (cache-hit + dog-side write integration tests added) / Phase 2
  step 12 (compact step description leads with read-first guard,
  matches PR2 step 11)).
- Section-heading audit (`grep -n '^## \|^### '`) confirms
  structure intact:
  - 11 top-level `##` sections (unchanged from round 1).
  - Risks R-1 through R-14 sequential and 1:1 with risk types
    discussed (R-14 added).
  - Operations subsections: Manual Recovery (4 MR sub-cases) +
    PR Rollback (3 sub-cases + sequencing) — total 2
    subsections (unchanged).
  - Implementation Plan: Phase 1 has 7 steps (unchanged); Phase
    2 has 15 steps (unchanged); Phase 3 has 5 steps (unchanged).
  - Open Questions: 4 Resolutions (OQ-1, OQ-4, OQ-5, OQ-7) + 3
    Remaining (OQ-2, OQ-3, OQ-6) = 7 total (unchanged).
  - Key Components 1-10 sequential and 1:1 with the PR2 module
    list (unchanged).
- Cross-reference audit:
  - PR2 step 11 references "Step-execution contract / Dog-runtime
    mechanism / Read-first idempotence guard" — present.
  - Phase 2 step 12 references "Step-execution contract / Formula
    `needs`-chain alignment" (round 1) and "read-first idempotence
    guard, mitigates R-14" — both present.
  - Phase 2 step 11 cache-hit and dog-side write entries
    cross-reference R-14 — present.
  - R-12's rewritten sentence references Phase 2 step 11 + Phase
    2 step 10 — both present.
  - R-14 references Step-execution contract / Dog-runtime
    mechanism / Read-first idempotence guard — present.
  - R-4's non-deadline gc-failure paragraph references the Error
    Class Taxonomy paragraph — present.
  - Error Class Taxonomy `database-not-found` row references the
    operational notes paragraph below the table — present.
  - TD-6's round-2 paragraph cross-references S3 (database-not-
    found) and S5 (concurrent-write-collision fold) — present.
  - Executive Summary's round-2 paragraph cross-references all 6
    findings (M1, S1, S2, S3, S4, S5) — present.
- Taxonomy class count audit:
  - Before round 2: 9 error classes plus `ok` for success
    (`below-threshold`, `concurrent-write-collision`,
    `concurrent-write-fatal`, `integrity-mismatch`,
    `database-locked`, `database-unreachable`,
    `invalid-identifier`, `database-deadline-exceeded`,
    `internal-error`); 10 rows in the taxonomy table.
  - After round 2: 9 error classes plus `ok` (removed
    `concurrent-write-collision`; added `database-not-found`);
    10 rows in the taxonomy table — net change zero.
  - TD-6's "9 classes" prose framing intact (the framing
    counts error classes only; the table includes `ok` for
    a total of 10 rows — pre-existing convention from the
    PRD-align-round-1 binding of TD-6, not a round-2
    introduction).
- All findings either applied or explicitly justified. No items
  left open.

## Round 2 fix counts (live-conversation summary)

- **Must-fix applied:** 1
  - M1 — Cache-write authority bound dog-side (the CLI emits
    envelope on stdout; the dog's prose-instructed shell pipeline
    is the writer). Phase 2 step 10's "cache-write happy path"
    entry reframed as envelope-stdout shape; R-12's "CLI side"
    wording rewritten; Dog-runtime mechanism subsection updated.
- **Should-fix applied:** 5
  - S1 — `internal-error` semantics extended for non-deadline
    `dolt_gc` failure (R-4 paragraph extended; Error Class
    Taxonomy operational notes added; Phase 2 step 10 unit-test
    list extended).
  - S2 — Read-first idempotence guard added to the compact
    step's formula description (Step-execution contract /
    Dog-runtime mechanism subsection extended; PR2 step 11 +
    Phase 2 step 12 + Phase 2 step 11 integration tests
    extended; R-14 added).
  - S3 — `database-not-found` added to the Error Class
    Taxonomy table (operational notes added distinguishing
    from `database-unreachable`; Phase 2 step 10 unit-test list
    extended).
  - S4 — R-12's last sentence rewritten to point at Phase 2
    step 11 (formula-driven integration test) as the
    regression catcher for missing-write in formula step
    description prose; Phase 2 step 10 (envelope-stdout shape)
    is the independent CLI-side regression catcher.
  - S5 — `concurrent-write-collision` removed from the Error
    Class Taxonomy; per-DB `attempts` field becomes the
    sole observability handle for retries (retry-observability
    paragraph + rationale paragraph appended; TD-6 fold
    paragraph appended).

- **Items rejected:** 0
- **Plan-review-2 scope this round:** technical / dependency /
  rollback / unknown-unknown risks (Leg 1) plus
  gold-plating / over-engineering / premature optimization /
  defer candidates (Leg 2). The dual-leg pressure-test surfaced
  the cache-write authority ambiguity as the consequential
  finding; the remaining items are taxonomy clarifications,
  an idempotence guard, and a wording correction.

## Next round

Plan self-review round 3 (bead `gc-ahiv4c`, formula step
`mol-idea-to-plan.plan-review-3`). Per the bead description, that
round shifts focus from risk and scope-creep to **testability and
coherence** — acceptance criteria, missing tests, vague
verification, phase gates (testability); contradictions, naming
drift, missing glue, final completeness pass (coherence). The
design is now risk-mitigated and scope-disciplined; round 3's
pressure-test is "is each acceptance criterion testably verifiable
and is the design self-consistent end-to-end."
