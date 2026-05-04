# PRD Alignment Round 2 — Leg Reports

Round: prd-align-2 (mol-idea-to-plan.prd-align-2, bead `gc-fm7r62`)
Date: 2026-05-04
Coordinator: gascity/gastown.nux

Two legs (`constraints-compliance`, `non-goals-enforcement`) executed
inline, matching round 1's pattern. Inputs read end-to-end:

- `.prd-reviews/dolt-compact-cli/prd-draft.md`
- `.designs/dolt-compact-cli/design-doc.md` (post round 1)
- `.plan-reviews/dolt-compact-cli/prd-align-round-1.md`

Each leg produces `must-fix` and `should-fix` findings with concrete
pointers into the design doc. Per the step description, all are applied
to the design doc unless objectively wrong; rejections are recorded in
the round log.

---

## Leg 1 — constraints-compliance

**Charter.** Walk every Constraint in PRD §Constraints (lines 163-208)
and verify the design as written delivers it concretely. Flag any
constraint that is partial, deferred, or not discharged.

The PRD Constraints + Q5/Q11 clarification text together yield 12
binding constraints. Eleven are concretely discharged by the design
(round 1 closed the M1/M3 pre-compaction-state gap on Constraint 5).
One remains: Constraint 11's clarification mandates manual recovery
documentation in the design doc, and the design defers it to a yet-
to-be-written engdocs file. Two structural / consistency gaps surface
alongside the main finding.

### Constraint coverage table

| # | Constraint | Coverage | Verdict |
|---|------------|----------|---------|
| 1 | Existing formula vars (`--mode`, `--threshold`, `--keep-recent`, `--databases`) honored exactly | Interface §CLI surface (lines 463-470); defaults match formula (`commit_threshold`, `keep_recent`, `mode`, `databases`); Phase 2 step 10 consistency test enforces parity | OK |
| 2 | CLI naming: `gc dolt-compact` (Go) + `gc dolt compact` (pack delegate) | Shape §, Component 1, Component 8 | OK |
| 3 | Auto-discovery via `gc rig list --json` only; never `SHOW DATABASES` | Trust and Discovery (lines 354-360); integration test (Phase 2 step 11) asserts no `SHOW DATABASES` issued | OK |
| 4 | Concurrent-write safety: flatten safe; surgical retries once with 2s pause | Algorithms / Surgical (line 271-275); R-3 | OK |
| 5 | Integrity verify pre/post user tables; mismatch → leave db pre-compaction | Algorithms / Flatten (line 235-241), Surgical (line 277-282); Verifier returns `pre_hash` (line 397-402); R-3, R-10 | OK (closed by round 1) |
| 6 | `dolt_gc` runs post-compaction | Flatten (line 240); Surgical (line 286); TD-8 | OK |
| 7 | Test coverage unit + integration; integration mirrors `examples/dolt/*_test.go` | Phase 2 steps 10-11; R-8 | OK |
| 8 | No SQL injection: charset `[A-Za-z0-9_-]`, leading `[A-Za-z0-9_]` | Trust and Discovery (line 362-364); R-6; PR2 module 7 (`dolt_compact_validate.go`) | OK with structural gap (S1) |
| 9 | No agent judgment in Go (ZFC) | All decisions are flag- and reality-driven; OQ-2 heuristic is a TEST heuristic, not runtime | OK |
| 10 | Origin/upstream model: origin/main only | Implicit; Phase 1/2 land local PRs only | OK |
| 11 | Surgical-mode lock-then-cleanup; reuse `dolt_lifecycle_lock.go`; **document manual recovery procedure for operators in the design doc** (Q5 clarification) | Auto-recovery covered (Algorithms / Surgical line 261-262, R-3); manual-recovery fragments in R-10; full procedure deferred to `engdocs/contributors/dolt-compact.md` (Phase 2 step 13) | **Partial — manual recovery missing from design doc (M1)** |
| 12 | Default principle: mirror `gc dolt-cleanup`; diverge only with stated cause | Executive Summary (line 41-42); Shape; Trust and Discovery; TD-2; TD-7; divergences (per-DB timeout, 9-class taxonomy, `phases`, `pre_hash`) each documented in their respective TDs | OK |

### M1 (must-fix) — Manual recovery procedure missing from design doc

**Constraint.** PRD Constraint 11 + Q5 clarification (PRD lines 432-440):

> Reuse `dolt_lifecycle_lock.go`. Crash recovery is implicit in the
> "next run cleans up its own orphans" model — but only after acquiring
> the lock. **Document the manual recovery procedure for operators in
> the design doc.**

The wording is explicit: "in the design doc," not in a downstream
operator guide.

**Where the design covers what:**

- **Auto-recovery (next-run cleanup):** Algorithms / Surgical line
  261-262 sketches the lock-then-cleanup ordering. Sufficient for the
  no-operator-action path.
- **Failed-rollback for flatten:** R-10 documents the operator
  procedure (`gc dolt sql` + `DOLT_RESET --hard <pre_hash>`) and
  notes `dolt_gc` is skipped on the mismatch path so chunks remain
  recoverable. Sufficient for that sub-case.
- **Surgical crash recovery (operator-driven):** Not documented in the
  design doc. The PR2 deliverable list mentions
  `engdocs/contributors/dolt-compact.md` will cover it (line 215), but
  that file is not part of the design doc — it lands in PR2 alongside
  the CLI itself.
- **Stuck advisory lock (pathological):** R-7 mentions kernel releases
  on process exit and proposes a back-to-back integration test, but
  doesn't tell an operator what to do if the lock is somehow stuck.

**Why this matters.** Plan-review rounds cannot audit a manual recovery
procedure that doesn't exist yet. The PRD requirement is to make the
recovery procedure auditable as part of the design — not to defer to a
later artifact.

**Recommended fix.** Add a "Manual Recovery" subsection at the end of
the Proposed Design section (or as a top-level §Operations subsection
between Risks and Implementation Plan) that consolidates the three
recovery sub-cases:

1. **Crash mid-flatten with successful in-CLI rollback.** No operator
   action; envelope's `outcome = integrity-mismatch` plus surfaced
   `pre_hash` give context. `dolt_gc` skipped → chunks remain.
2. **Crash mid-flatten with failed in-CLI rollback (R-10).** Operator
   reads `pre_hash` from the last envelope, runs `gc dolt sql` against
   the affected DB, issues `CALL DOLT_RESET('--hard', '<pre_hash>')`.
   Verifies HEAD is restored.
3. **Crash mid-surgical (any phase before swap).** `compact-base` and
   `compact-work` branches may be left behind; `main` is untouched.
   Next CLI invocation drops the orphans automatically (lock-then-
   cleanup ordering). If a manual cleanup is preferred, operator runs
   `gc dolt sql` and issues `CALL DOLT_BRANCH('--delete', '--force',
   'compact-base')` and the same for `compact-work` after acquiring
   the lifecycle lock.
4. **Stuck advisory lock.** Standard flock semantics release on
   process exit. If a pathological case persists, document the
   investigation step: `gc dolt sql -q "SELECT
   IS_FREE_LOCK('dolt_lifecycle')"` followed by
   `RELEASE_LOCK('dolt_lifecycle')` from a connection that holds the
   lock. Note that this should not be needed in practice and operators
   should escalate to mayor before manually releasing.

The engdocs file (`engdocs/contributors/dolt-compact.md`) can still
exist as the long-form operator guide — but the design doc should
contain the auditable summary that plan-review rounds can verify.

### S1 (should-fix) — Validator missing from Key Components section

**Constraint.** PRD Constraint 8 (no SQL injection):

> Database identifiers must be validated (charset identical to existing
> pack scripts: `[A-Za-z0-9_-]`, leading `[A-Za-z0-9_]`).

**Coverage in design.** The validator is mentioned in three places:

- Trust and Discovery (line 362-364): "Database identifiers go through
  the cleanup-style charset validator"
- Phase 2 module list (line 191-193): "`cmd/gc/dolt_compact_validate.go`
  — identifier charset validation (factored shared helper if cleanup's
  validator hasn't been exported; otherwise reuse)"
- Phase 2 step 3 (line 882): "Validator. Identifier charset (factor or
  copy from cleanup)"
- R-6: Charset validator drift mitigation

**Gap.** The Key Components section (lines 366-449) lists 9 components
in a numbered structure that mirrors the PR2 module ordering — but
omits the Validator entry. A reader scanning Key Components for "where
does identifier validation live?" hits no match. The numbering also
diverges from the Implementation Plan's PR2 module list (Component 7
in Key Components is "Executor-binding regression test (PR1)" while
PR2 module 7 in the Implementation Plan is `dolt_compact_validate.go`).

**Recommended fix.** Insert a new component "Validator —
`cmd/gc/dolt_compact_validate.go`" between current Component 5
(Verifier) and current Component 6 (Envelope), matching the PR2 module
ordering. Cross-reference the cleanup validator (R-6's "factor into a
single shared helper" target). Renumber subsequent Key Components
accordingly. The Implementation Plan's PR2 module list is unchanged;
the Key Components numbering becomes a faithful 1:1 with the file
ordering.

This also surfaces the validator → `invalid-identifier` outcome path
in the Error Class Taxonomy explicitly (which currently lists
`invalid-identifier` without a clear "where in the design does this
get raised" anchor).

---

## Leg 2 — non-goals-enforcement

**Charter.** Walk every Non-Goal in PRD §Non-Goals (lines 92-108)
and check whether any design content extends scope past it. Flag
anything that drifts.

Seven Non-Goals. Six are cleanly respected. One has an unstated
reading that an attentive reviewer would flag: TD-5 introduces
`--per-db-timeout` as a 1h-default flag, which a strict reading of
Non-Goal 7 ("no parameter tuning") could challenge. The fix is a
clarifying sentence in TD-5 (and an aligned scope note for the Phase 2
step 10 consistency test); the parameter itself is sound.

### Non-Goal coverage table

| # | Non-Goal | Coverage | Verdict |
|---|----------|----------|---------|
| 1 | No compactor daemon | TD-9 (line 729): "Daemon coexistence is rejected as a Non-Goal"; design has no daemon, no supervisor, no event loop | OK |
| 2 | No formula split | Shape §: single CLI process, 4 phases as one envelope; orders unchanged (Phase 2 step 12) | OK |
| 3 | No event-driven scheduling; 24h cooldown stays | Phase 2 step 12: "examples/dolt/orders/mol-dog-compactor.toml — unchanged (24h cooldown, dog pool)" | OK |
| 4 | No new compaction targets beyond what the formula already covers | Trust and Discovery: auto-discovery rig-registry-only; positional args are operator-only escape hatch (Constraint 3) and don't extend the formula's automatic target set | OK (relationship implicit but correct) |
| 5 | No upstream PR submission | No upstream PR mentioned anywhere in the design | OK |
| 6 | No backwards-compatibility shim for safely-skip | Shipping Order: PR1 leaves safely-skip path intact (still ZFC-exempt). PR2 drops `zfc_exempt` from `mol-dog-compactor` and the dog's safely-skip path becomes dead for that formula. No shim retains old behavior. | OK |
| 7 | No changes to underlying compaction algorithms; flatten and surgical ported verbatim; **no new modes, no parameter tuning** | Algorithms section ports formula prose. Round 1 added `pre_hash` capture-and-revert (flatten) and halt-before-swap (surgical) — these are constraint-discharge corrections, not algorithm changes. **TD-5 introduces `--per-db-timeout` (new parameter, 1h default).** | **Partial — TD-5 framing leaves a Non-Goal 7 reading open (S2)** |

### S2 (should-fix) — `--per-db-timeout` introduces a new parameter; defend against Non-Goal 7

**Non-Goal text.** PRD Non-Goal 7 (line 107-108):

> **No changes to the underlying compaction algorithms.** Flatten and
> surgical semantics are ported verbatim; no new modes, no parameter
> tuning.

**What the design adds.** TD-5 (lines 696-705):

> Compactor cycles can run for hours when `dolt_gc` is doing real
> work. A hung DB shouldn't block the cycle indefinitely. … Decision:
> 1h default, exposed as `--per-db-timeout`. Documented as "tune up if
> your DB legitimately needs longer." Not exposed as a formula var
> until a city actually hits the limit.

**The reading that fires.** A strict reader of Non-Goal 7 sees
`--per-db-timeout` as a NEW PARAMETER with a tunable default. The
phrase "no parameter tuning" arguably forbids this.

**Why the design is actually fine.** The parameter is a **wrapper-
level execution budget** around the algorithm, not an algorithm
parameter. The flatten and surgical algorithms still consume the four
formula-bound vars (`commit_threshold`, `keep_recent`, `mode`,
`databases`) and run them verbatim. The deadline is enforced at the
Cobra-orchestration layer via `context.WithTimeout`; on expiry the
algorithm is interrupted and the per-DB outcome is
`database-deadline-exceeded`. The algorithms themselves are unmodified.

**Why TD-5 doesn't say this clearly enough.** TD-5 frames the
parameter as a deadline for `dolt_gc` and a per-DB safety valve, but
does not draw the algorithm-vs-wrapper distinction. A reviewer asked
"does this violate Non-Goal 7?" finds no defense.

**Recommended fix.** Add a paragraph to TD-5 (after "Decision: 1h
default…"):

> `--per-db-timeout` is a **wrapper-level execution budget** enforced
> by `context.WithTimeout` around each per-DB invocation; it is **not
> a parameter of the flatten or surgical algorithms**, which continue
> to consume the four formula-bound vars (`commit_threshold`,
> `keep_recent`, `mode`, `databases`) verbatim. The PRD Non-Goal "no
> parameter tuning" addresses algorithm internals; the deadline is
> orthogonal — it bounds wall-clock cost in the orchestration layer.
> If a city later wants the deadline to live in the formula (e.g., to
> tune per-formula instead of per-binary), the addition is additive
> (new formula var, new CLI flag default sourced from the var, no
> algorithm change).

Also align the consistency-test scope language (Phase 2 step 10).
Currently it reads:

> a formula/CLI defaults consistency test that loads
> `examples/dolt/formulas/mol-dog-compactor.toml`, extracts the
> `commit_threshold`, `keep_recent`, `mode`, and `databases` formula
> vars, and asserts the CLI flag defaults match.

Add one explanatory clause:

> … vars, and asserts the CLI flag defaults match. (`--per-db-timeout`
> is intentionally not in scope; per TD-5 it is a wrapper-level
> deadline, not a formula-bound algorithm parameter.)

This closes the reader's loop: TD-5 explains why the parameter
exists; Phase 2 step 10's scope note tells a future test author why
they shouldn't add `--per-db-timeout` to the consistency test until
it becomes formula-bound.

### Items not flagged

A few candidates I considered and dropped:

- **Positional-arg DBs vs Non-Goal 4 ("no new compaction targets").**
  Constraint 3 explicitly authorizes positional args for unregistered
  DBs ("Operators wanting unregistered DBs must pass them as
  positional args"). The dog formula passes `--databases={{databases}}`
  (formula var); when empty, auto-discovery via rig-registry fires.
  The dog never compacts a non-registry DB. Non-Goal 4 governs the
  formula's automatic target set; positional args are an operator
  escape hatch. The design respects both. The relationship is implicit
  but correct — making it explicit is a clarity nit, not a finding.

- **Round 1's pre_hash and halt-before-swap additions vs Non-Goal 7.**
  These are **constraint-discharge corrections** to make the formula's
  prose actually deliver Constraint 5 ("mismatch → leave db in pre-
  compaction state"). The original formula prose committed before
  verifying with no rollback — the algorithm as written did not meet
  the constraint. Adding `pre_hash` capture-and-revert is correcting
  an algorithm-vs-constraint contradiction inherited from the formula,
  not introducing a new mode or tuning a parameter. This was the
  round-1 finding M1/M3.

- **9-class error taxonomy vs cleanup parity.** Cleanup may have a
  different number of error classes; the design diverges and TD-6
  documents the cause. Constraint 12 ("diverge only with stated
  cause") is satisfied.

- **`phases` block vs cleanup parity.** Cleanup's envelope likely
  doesn't have `phases`; the design adds it to discharge Goal 6.
  Round 1 added it; cause is documented in the round-1 log + Per-step
  bead mapping subsection. Constraint 12 satisfied.

---

## Summary

**Leg 1 — constraints-compliance:**

- Must-fix: 1 (M1: manual recovery procedure missing from design doc)
- Should-fix: 1 (S1: Validator missing from Key Components)

**Leg 2 — non-goals-enforcement:**

- Must-fix: 0
- Should-fix: 1 (S2: `--per-db-timeout` Non-Goal 7 defense missing)

Total: 1 must-fix, 2 should-fix. All three are concrete, fixable in
the design doc, and rooted in PRD text. Per the round-1 standard,
all three should be applied unless objectively wrong.

Findings to apply, in order of impact:

1. **M1** — Add a "Manual Recovery" subsection (PR2 §Operations or
   end of Proposed Design) consolidating flatten-failed-rollback,
   surgical-crash, and stuck-lock procedures.
2. **S1** — Insert "Validator" as a Key Components entry between
   Verifier and Envelope, matching the PR2 module ordering and
   cross-referencing R-6.
3. **S2** — Extend TD-5 with a paragraph explicitly framing
   `--per-db-timeout` as a wrapper-level execution budget (not an
   algorithm parameter), and align Phase 2 step 10's consistency-test
   scope language to note the intentional exclusion.
