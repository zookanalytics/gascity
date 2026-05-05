# Plan Self-Review Round 1 — Leg Reports

Round: plan-review-1 (mol-idea-to-plan.plan-review-1, bead `gc-imyo2c`)
Date: 2026-05-04
Coordinator: gascity/gastown.furiosa

Two legs (`completeness`, `sequencing`) executed inline, matching the
prd-align-round-{1,2,3} pattern. Inputs read end-to-end:

- `.designs/dolt-compact-cli/design-doc.md` (post round 3)
- `.prd-reviews/dolt-compact-cli/prd-draft.md`
- `.plan-reviews/dolt-compact-cli/prd-align-round-{1,2,3}.md`
- `.plan-reviews/dolt-compact-cli/human-clarifications.md`
- `examples/dolt/formulas/mol-dog-compactor.toml` (current formula state)
- `examples/gastown/packs/maintenance/agents/dog/prompt.template.md`
  (dog runtime model — agent-driven, prompt-templated)

Each leg produces `must-fix` and `should-fix` findings with concrete
pointers into the design doc. Per the step description, all are applied
to the design doc unless objectively wrong; rejections are recorded in
the round log.

This round shifts focus from PRD-vs-design contract gaps (rounds 1-3)
to **plan self-review against implementability** — whether the plan,
treated as a contract for a future implementer, is complete and
sequenced correctly.

---

## Leg 1 — completeness

**Charter.** Walk the plan looking for missing pieces — setup,
migrations, tests, docs, rollback, hidden dependencies. Flag any item
that the plan would need but the implementer cannot derive from the
plan alone.

The plan covers the algorithmic content fully (rounds 1-2 closed those
gaps) and the operator-facing recovery cases (round 2). What this leg
finds is two missing bindings at the **dog ↔ CLI ↔ envelope** seam
introduced by round 3's Step-execution contract, plus three smaller
documentation/rollback omissions.

### M1 (must-fix) — Envelope cache mechanism unspecified

**Finding.** The Step-execution contract (`design-doc.md` lines 173-221)
says:

- "the dog parses the resulting envelope and uses it to close `compact`
  immediately and the other three step beads as they are claimed (or
  atomically, depending on the dog's formula-execution model — the
  design constrains the outcome, not the dispatch order)" (lines
  198-203)
- "cache the envelope keyed by molecule id; consume from cache when a
  non-`compact` step bead is claimed. … This is a PR2 implementation
  detail bounded by the contract above; the design doc does not specify
  the cache shape." (lines 217-221)

The "design doc does not specify the cache shape" deferral is too
aggressive for the implementation plan to be implementable. PR2's
implementer needs concrete answers to:

1. **Where** the cached envelope lives.
2. **How** the bookkeeping steps' formula descriptions instruct the dog
   to retrieve it (these are the artifacts Phase 2 step 12 produces).
3. **Where** in the dog runtime/prompt the lookup logic lives. The dog
   is an LLM agent driven by `examples/gastown/packs/maintenance/agents/
   dog/prompt.template.md` and the formula step descriptions. There is
   no Go-side dog runtime — `grep -rn "safely-skip" internal/ cmd/`
   returns zero matches, and the formula's `Step` type has no current
   `Executor`/`ZfcExempt` fields (those are PR1's additions). So
   "consume from cache" is an instruction that lands in either the
   dog prompt template, the formula step description, or both.

The design's three concrete options that the plan must pick between:

| Option | Cache location | Pro | Con |
|--------|---------------|-----|-----|
| **A** — bead metadata on the molecule (root bead): `metadata.gc.formula.envelope = <json>` | bd substrate; persistent; queryable; survives session boundaries; same store the formula contract already uses | The envelope is small (sub-kilobyte for typical 4-DB cycles); bd metadata handles strings of this size today | None material |
| **B** — file in the rig directory: `<rig>/.gc/state/compactor-cache/<molecule-id>.json` | filesystem is fast; large blobs are fine | Filesystem state is per-host; multi-host dog pools (future) would lose the cache; bd is the design's other persistence substrate, not files |
| **C** — notes string on the compact step bead (`bd update --notes "<json>"`) | single substrate (bd) | Notes are textual prose by convention; structured data in notes is a misuse of the field |

Without a binding, PR2's Phase 2 step 12 cannot write the inspect /
verify / report step descriptions: the dog needs to know, in prose,
which substrate to query.

**Applied.** Bind option A as the v1 mechanism in the design doc.

- New paragraph appended to **Step-execution contract / Dog-runtime
  mechanism** (after line 221 in `design-doc.md`):
  - The compact step's CLI invocation result is cached as JSON-encoded
    envelope text on the molecule's (root bead's) metadata under the
    reserved key `gc.formula.envelope`. The dog updates the molecule
    via `gc bd update <molecule-id> --set-metadata
    gc.formula.envelope='<json>'` immediately after parsing the CLI
    output.
  - The bookkeeping steps' formula descriptions instruct the dog to:
    (1) read the molecule metadata via `gc bd show <molecule-id>
    --json | jq -r '.[0].metadata."gc.formula.envelope"'`, (2) parse
    the JSON envelope, (3) consult `phases.<this-step>` per the
    Per-step bead mapping table, and (4) close the step bead with the
    mapped status.
  - Rationale: bd is the design's persistence substrate and the same
    store the formula contract uses; the envelope is small (typical
    sub-kilobyte for 4-DB cycles) so metadata is appropriate; the
    cache survives session boundaries (dogs may rotate across step
    claims).
  - Future-compat: option B or C may be revisited if a multi-host
    dog pool emerges and metadata-blob size becomes a constraint.
    The migration is additive — bookkeeping step descriptions update
    independently of the CLI-side envelope shape.
- Two-PR Shipping Order PR2 step 11 (`design-doc.md` lines 288-300)
  extended: the inspect / verify / report step description bullet now
  reads "reference the cached-envelope closure path produced by
  `compact`: read `metadata.gc.formula.envelope` from the molecule
  via `gc bd show`, parse, and close per `phases.<this-step>`." The
  compact step's description still carries the full CLI invocation +
  the molecule-metadata write.
- Implementation Plan / Phase 2 step 12 (`design-doc.md` lines
  1144-1150) similarly extended.
- New unit-test entry in Phase 2 step 10: cache-write happy-path
  (the compact-step orchestration writes `gc.formula.envelope` to
  the molecule's metadata before exiting).
- Risks and Mitigations / R-12 (new): cache-write failure (bd
  unavailable mid-cycle) — symptom: the bookkeeping steps cannot find
  the cache and fall back to closing as `closed` (current safely-skip
  behavior), preserving forward progress at the cost of phases.<step>
  visibility for that cycle. Operator action: re-trigger the cycle
  if escalation visibility was needed.

### M2 (must-fix) — `needs` chain forces inspect to close before the envelope exists

**Finding.** The current `mol-dog-compactor.toml` (`examples/dolt/
formulas/mol-dog-compactor.toml`) has the step dependency chain:

```
inspect:  (no deps)
compact:  needs = ["inspect"]
verify:   needs = ["compact"]
report:   needs = ["verify"]
```

This means the inspect step bead must be **closed** before the dog can
claim the compact step bead. The compact step bead's CLI invocation is
what produces the envelope (per Step-execution contract). Therefore:

- inspect closes **before** the envelope exists.
- The dog cannot read `phases.inspect` from a not-yet-existing cache
  to drive inspect's closure.

But the Per-step bead mapping table (`design-doc.md` lines 661-685)
maps `phases.inspect` values to dog actions:

| `phases.inspect` value | Dog action |
|-----------------------|------------|
| `ok` | close step `closed` |
| `partial` | close step `closed` |
| `escalate` | close step `escalated` |
| `failed` | close step `failed`; deacon nudge |

These four mapping rules **cannot be exercised** under the current
`needs` chain — inspect is always closed before the cache exists.

The aggregation rules at lines 670-674 also imply `phases.inspect` is
computed from CLI-side state ("`ok` if every candidate's commit-count
probe succeeded; `partial` if all DBs were `below-threshold` (no
candidates); `failed` if rig-list / port resolution / the probe itself
errored before any per-DB outcome could be recorded"). All of these
states are only known after the CLI runs — i.e., after inspect would
already be closed.

This is a structural inconsistency between the design's "compact is
the trigger" model and the existing formula's `needs` chain.

The three resolution options:

| Option | Change | Trade-off |
|--------|--------|-----------|
| **A** — Reorder the formula `needs` chain: `compact` becomes the dependency-free root; `inspect`, `verify`, `report` all `needs = ["compact"]` | Cleanly aligns formula with trigger model; per-step bead mapping table is fully expressive | Changes the formula's visual step ordering; reviewers used to inspect→compact→verify→report read order have to adjust; the per-step bead's `phases.inspect` value is now meaningfully consumed |
| **B** — Keep the `needs` chain; remove the `phases.inspect` row from the per-step bead mapping table; clarify that inspect closes unconditionally as `closed` | Smaller change | Leaves `phases.inspect` partially decorative; envelope readers see a value the dog doesn't act on; operator/dog parity weakens |
| **C** — Move the CLI trigger to the inspect step | Preserves `needs` chain order | Contradicts the round-3 Step-execution contract (compact is the trigger); reverts that work |

Option A is the cleanest match to the design's intent. The CLI runs
once; the bookkeeping steps all consult the cache; phases.<every-step>
is meaningful. The visual reorder is one TOML edit and is captured by
the per-step bead mapping table's existing description (which already
treats all four steps symmetrically).

**Applied.** Bind option A.

- Two-PR Shipping Order PR2 step 11 (`design-doc.md` lines 288-300)
  extended: explicit instruction to drop `needs = ["inspect"]` from
  the `compact` step and add `needs = ["compact"]` to the `inspect`,
  `verify`, and `report` steps. Rationale paragraph anchors the
  reorder to the Step-execution contract (compact is the trigger; the
  three bookkeeping steps depend on compact's envelope).
- Implementation Plan / Phase 2 step 12 (`design-doc.md` lines
  1144-1150) similarly extended.
- Step-execution contract (`design-doc.md` lines 196-203) — appended
  a clarifying sentence: "The formula's `needs` chain is reordered in
  PR2 (PR2 step 11) so that the `compact` step has no dependencies
  and the bookkeeping steps depend on `compact`. This makes the dog's
  claim order match the trigger model — `compact` is claimable first,
  produces the envelope, and the bookkeeping steps then become
  claimable in any order (parallel-safe)."
- New Risks and Mitigations / R-13: formula `needs`-chain reorder
  could surprise reviewers reading the formula linearly. Mitigation:
  the formula's step `description` fields preserve the conceptual
  inspect→compact→verify→report flow even though `needs` reflects the
  trigger-then-bookkeeping execution order.

### S1 (should-fix) — CHANGELOG.md update missing from implementation plan

**Finding.** The repo has a Keep-a-Changelog–style `CHANGELOG.md` at
the project root (`CHANGELOG.md` lines 1-10), with an `## [Unreleased]`
section and `### Fixed`, `### Changed` subsections. Recent entries
(e.g., `proxy_process` services, `[[orders.overrides]]` rig matching)
describe user-facing changes. Adding `gc dolt-compact` (Go-side) and
`gc dolt compact` (pack delegate) is a user-facing CLI surface that
warrants a CHANGELOG entry. Adding `executor` and `zfc_exempt` formula
fields is a contributor-facing schema change that also warrants an
entry.

The plan does not have a step for this in either Phase 1 or Phase 2.

**Applied.**

- New step inserted in Phase 1 between current step 5 and step 6:
  - "**Update CHANGELOG.md.** Add `### Added` entries to the
    `## [Unreleased]` section: `executor` and `zfc_exempt` optional
    fields on `formula.Step`; executor-binding regression test."
  - Subsequent step (Open PR1) renumbers to step 7.
- New step inserted in Phase 2 between current step 13 and step 14:
  - "**Update CHANGELOG.md.** Add `### Added` entries to the
    `## [Unreleased]` section: `gc dolt-compact` Go-side command;
    `gc dolt compact` pack delegate; `gc.dolt.compact.v1` JSON
    envelope. Mention the migration of `mol-dog-compactor` off
    safely-skip."
  - Subsequent step (Open PR2) renumbers to step 15.

### S2 (should-fix) — PR rollback semantics not documented

**Finding.** The plan covers algorithm-level rollback (flatten reverts
to `pre_hash`; surgical halts before swap — Algorithms section) and
operator-facing manual recovery (Operations / MR-1 through MR-4). It
does not cover **PR-level rollback** — what happens if PR1 or PR2 lands
and a regression surfaces post-deploy.

PR1: purely additive (new optional fields, new test, formula migrations
that are zero-value-safe). Reverting the PR removes the contract test
and the new field accessors; embedded formula files lose their tags;
mol-dog-compactor returns to prose-only ZFC exemption. The dog's
behavior is unchanged because PR1 introduced no behavioral changes —
it's a safe revert.

PR2: behavioral change. Reverting restores `zfc_exempt = true` on the
compact step and removes `executor`. The dog goes back to safely-skip.
The CLI binary still ships the new `gc dolt-compact` command, which is
inert without the formula update — operators can still invoke it
ad-hoc. Acceptable degradation: the silent-failure mode returns
(safely-skip), but the operator-facing CLI is preserved for ad-hoc
runs.

This is auditable information that plan-review and ops-review need;
the design doc's Operations section is the natural home.

**Applied.**

- New `### PR Rollback` subsection appended to `## Operations`
  (after `### Manual Recovery`):
  - **PR1 rollback.** Additive-only PR; revert is mechanical
    (`git revert` of the PR's merge commit). No data migration is
    affected. Embedded formulas lose their `executor` / `zfc_exempt`
    tags but parse cleanly under the pre-PR1 schema (the fields were
    optional with `omitempty` on add). Effect: the executor-binding
    contract is no longer enforced; the silent-failure regression
    class re-opens until PR1 is re-landed.
  - **PR2 rollback.** Reverting restores `zfc_exempt = true` on the
    compact step and drops `executor` from all four steps. The dog
    returns to safely-skip behavior — silent failure resumes but the
    pre-PR2 baseline behavior is recovered exactly. The `gc
    dolt-compact` CLI binary remains shipped (operators can still
    invoke ad-hoc); only the formula's bind to it is removed. The
    `mol-dog-compactor` order remains unchanged.
  - **Sequencing of reverts.** PR2 must be reverted before PR1 can
    be reverted (PR1 is the prerequisite of PR2; reverting PR1 with
    PR2 still landed leaves the formula referencing fields the
    schema no longer supports — a parse-time validation failure on
    the next dog claim cycle).

### S3 (should-fix) — Auto-discovery error path absent from exit-code table

**Finding.** The Error Class Taxonomy / exit-code table
(`design-doc.md` lines 415-427) lists four examples of "invocation
error before per-DB execution begins" → exit `2`:

> bad flag, port unresolved, identifier failed charset check, explicit
> DB list with zero matches in rig-registry

It does **not** list `gc rig list --json` itself failing (registry
file missing or corrupt). This is also "invocation error before per-DB
execution begins" by spirit, but a reader scanning the example list
might think it falls under `internal-error` (escalate set, exit `1`)
or worse leave it as undefined behavior.

The integration tests (Phase 2 step 11) include "verify no `SHOW
DATABASES` is issued" — but no positive test for "rig-registry
unreachable / unparseable" → expected exit and envelope shape.

**Applied.**

- Exit-code table row 3 (`design-doc.md` line 423-425) extended:
  example list now reads "bad flag, port unresolved, identifier
  failed charset check, explicit DB list with zero matches in
  rig-registry, **`gc rig list --json` invocation failure (registry
  file missing or unparseable)**".
- Phase 2 step 10 unit-test list extended: "rig-list invocation
  failure path — assert exit code 2 and a well-formed envelope with
  `errors[].kind = "rig-registry"`, `inspect.value = "failed"`,
  `compact.value = "failed"`, no per-DB records."

### S4 (should-fix) — `--help` text not specified

**Finding.** The CLI surface section (`design-doc.md` lines 568-587)
defines flags but not the command's `Long` / `Short` Cobra strings.
The pack delegate's `command.toml` will have its own description.
Cleanup's CLI is the reference; its `Long` text is several paragraphs
covering discovery, removal strategy, dual-surface caveats. Compact
should match the depth — operators encounter `gc dolt-compact --help`
as the primary self-serve doc surface before reaching the engdocs.

The Phase 2 implementation steps don't explicitly bind `--help` text
content; an implementer might ship a one-liner.

**Applied.**

- Phase 2 step 8 (Cobra command body, `design-doc.md` lines
  1112-1113) extended: explicit requirement to land Cobra `Long` text
  covering: dual-surface invocation pattern (Go-side vs pack
  delegate), discovery rule (rig-registry-only, no SHOW DATABASES),
  threshold semantics (uniform across explicit and auto-discovered
  DBs per Q14), exit-code summary (3-row table), pointer to the
  engdocs operator guide for full recovery procedures. Brevity-vs-
  completeness reference: `cmd_dolt_cleanup.go` Cobra strings are
  the depth-of-coverage benchmark.

---

## Leg 2 — sequencing

**Charter.** Walk the implementation plan in order, looking for steps
in the wrong sequence, hidden dependencies (step X assumes Y is done
but Y is later or absent), unnecessary serialization (steps marked
sequential that could be parallel), and circularity.

**No circularity.** Phase 1 → Phase 2 → Phase 3 are acyclic; within
each phase, the step dependencies form a DAG (no back-edges).

**No problematic unnecessary serialization.** Phase 2 steps 2-7
(building blocks) are listed numerically but are independent and
parallel-safe across multiple developers; the plan does not mandate
serial execution. The numeric ordering is a checklist, not a critical
path.

What this leg does find: two TDD-violating sequencing patterns and one
forward-reference pattern that creates a momentary red state inside
PR2's iteration.

### M3 (must-fix) — PR2 step 11 (formula update) lands AFTER step 11 (integration tests) — inverted order

(Applying M2 above resolves this finding indirectly. Recording for
audit completeness.)

After M2's `needs`-chain reorder is applied, the PR2 step 11 (formula
update) and step 11 (integration tests) interplay needs to be
re-examined:

- The formula-driven integration test in step 11 ("load
  `mol-dog-compactor.toml`, resolve the step's `executor`, exec it
  with `--json`, …") is written against the **new** formula structure
  (compact step has `executor = "gc dolt-compact"`).
- If the implementer follows the numeric step order strictly, step 11
  is written before step 12 (formula update). The test cannot pass
  until step 12 lands.

This is acceptable under TDD — write the test (red), make it pass by
landing step 12 (green). But the plan does not say "step 11's tests
will be red until step 12 is applied." A naïve implementer might write
step 11 expecting it to pass immediately and conclude the test
infrastructure is broken.

**Applied.**

- Phase 2 step 11 (`design-doc.md` lines 1129-1143) — appended a
  sentence at the end: "**Sequencing note.** The formula-driven
  integration test will fail until step 12 (formula update) lands —
  this is the expected red→green TDD rhythm. The other integration
  tests (real-Dolt-server populate/flatten/surgical, concurrent-write
  injection, lock back-to-back, no-`SHOW DATABASES`, row-count
  divergence) are independent of the formula update and pass against
  the new CLI alone."

### S5 (should-fix) — Phase 1 sequencing violates TDD ordering

**Finding.** AGENTS.md mandates TDD: "Write the test first, watch it
fail, make it pass." Phase 1 steps in the current plan
(`design-doc.md` lines 1083-1097):

1. Add fields to `formula.Step`.
2. Wire fields through cooking.
3. Audit embedded formulas (apply tags).
4. Land the regression test.
5. Migrate `mol-dog-compactor.toml`.
6. Open PR1.

Step 4 (test) follows step 3 (apply tags). The test will pass on
first run because tags are already applied. This skips the red-state
TDD discipline.

The TDD-correct order interleaves: write the test first (red, every
formula fails), then apply tags (formula by formula, watching each
go green), then merge.

**Applied.**

- Phase 1 step ordering reorganized (`design-doc.md` lines
  1083-1097):
  1. **Add fields to `formula.Step`.** (unchanged)
  2. **Wire fields through cooking.** (unchanged)
  3. **Land the regression test (red).** Walk all embedded formulas;
     assert each step is either resolvable or annotated. **Initial
     run is expected to fail on every formula needing annotation.**
     Test failure names the exact formula+step.
  4. **Audit embedded formulas (green).** Walk
     `examples/*/formulas/*.toml`; for each step flagged by step 3,
     either confirm it resolves to a Cobra command / pack script
     today or apply `executor = "..."` / `zfc_exempt = true` as
     appropriate. After every formula is annotated, the regression
     test goes green.
  5. **Migrate `mol-dog-compactor.toml`** to `zfc_exempt = true`
     matching its current prose. **No forward references to
     `gc dolt-compact`** — the CLI doesn't exist yet.
  6. **Update CHANGELOG.md.** (per S1 above)
  7. **Open PR1.** Quality gates: `make test-fast-parallel`,
     `go vet`, dashboard check (if API surface touched — likely
     not), `make test-cmd-gc-process-parallel`.
- Reorder rationale prose appended to the Phase 1 header
  paragraph.

### S6 (should-fix) — Phase 2 unit tests grouped at end (step 10) reads as anti-TDD

**Finding.** Phase 2 step 10 (`design-doc.md` lines 1116-1128) is a
single step labeled "Unit tests" containing the entire unit-test
checklist:

> charset validator, envelope shape (including the `phases`
> aggregation rules), mode selection, error-class mapping, retry
> semantics (mocked SQL), threshold-on-explicit-DB skip (Q14), and a
> formula/CLI defaults consistency test …

This positions all unit tests **after** all implementation work
(steps 1-9). A literal reading is "implement everything, then write
tests" — which is anti-TDD per AGENTS.md ("write the tests first;
the implementation code isn't done until the tests pass").

**Applied.**

- Phase 2 step 10 reframed as a test-checklist anchor (not a single
  trailing test-writing pass). New introductory paragraph:
  - "Unit tests are written **alongside each module** in steps 2-7
    per the project's TDD principle (AGENTS.md). This step is the
    consolidated checklist of unit-test coverage that PR2 must
    contain by the time it opens — not a separate trailing
    test-writing pass. As each module lands, the implementer writes
    its tests in parallel; this step's only standalone work is the
    formula/CLI defaults consistency test (which spans modules and
    is naturally written last)."
- Phase 2 individual implementation steps 2-7 — appended a "test
  alongside" reminder to each: e.g., step 2 (Inspector) → "Includes
  unit tests for the rig-list parse + commit-count probe (mocked
  SQL); positional-merge logic."

### S7 (should-fix) — Phase 3 has no enforcement / acceptance criterion

**Finding.** Phase 3 ("Post-deploy verification", `design-doc.md`
lines 1155-1162) lists four steps:

1. Watch the next 24h cycle (or trigger via operator-side dispatch).
2. Verify the report bead shows non-zero compactions.
3. Verify HQ commit count drops.
4. Confirm the deacon report no longer flags compactor cycles as
   safely-skip.

These are operator-gated. The plan does not say **who** runs them,
**when**, or what bead/test enforces completion. PRD Acceptance
Criterion 2 ("`mol-dog-compactor` cycle runs end-to-end") is the
contract these steps would discharge — but without an enforcement
mechanism, PR2 can land and Phase 3 can be silently skipped, with
the work bead closing as merged but Acceptance 2 unverified.

R-9 (`design-doc.md` lines 957-970) addresses this partially: the
formula-driven integration test in Phase 2 step 11 discharges the
formula→CLI→envelope→step-close path automatically; the 24h-cooldown
leg remains operator-gated.

**Applied.**

- Phase 3 reframed (`design-doc.md` lines 1155-1162):
  - Pre-pended a paragraph: "**Owner.** The PR2 author (or a
    designated operator) runs Phase 3 within 48h of PR2 merging.
    Phase 3 results are recorded as comments on the PR2 work bead;
    no separate verification bead is created."
  - Step 1 reworded: "**Trigger a cycle.** Either wait for the next
    24h cycle or trigger immediately via `gc sling
    mol-dog-compactor` (operator-side dispatch). Recommended:
    trigger immediately to compress the verification window."
  - Steps 2-4 unchanged.
  - New step 5: "**Record verification result.** Comment on the PR2
    work bead with the report-bead ID, the post-cycle commit count
    delta, and the deacon report excerpt confirming non-skip."
- R-9 cross-reference appended: "Phase 3 step 5 records the
  manual-smoke result on the PR2 work bead so the 48h verification
  window is auditable."

### S8 (should-fix) — Hidden dependency: `dolt_lifecycle_lock.go` reuse path

**Finding.** Algorithms / Flatten and Algorithms / Surgical both call
"acquire advisory lock (city-wide flock for MVP)" — referring to
`dolt_lifecycle_lock.go` (PRD Rough Approach line 320). TD-4 also
references this. But:

- The design does not bind whether `dolt_lifecycle_lock.go` is
  currently exported / reusable from `cmd/gc/`, or if a refactor is
  needed to expose its acquire/release functions to the new compact
  modules.
- Implementation Plan / Phase 2 step 1 ("Skeleton") doesn't mention
  the lock-package import or any factoring work.
- If the lock function is currently unexported (or lives behind a
  package-private API), Phase 2 step 1 has a hidden setup task:
  refactor or re-expose it.

**Applied.**

- Phase 2 step 1 (Skeleton, `design-doc.md` lines 1101-1102)
  extended: "If `dolt_lifecycle_lock.go`'s acquire/release functions
  are not currently exported in a way the new compact modules can
  consume, factor them in this step. The factoring is mechanical
  (rename to exported identifiers; move to a shared sub-package if
  necessary); cleanup's existing call site must be updated to the
  new API in the same commit to avoid two-step churn."
- Phase 2 step 5 (Surgical executor) appended: "Surgical's
  lock-then-cleanup ordering depends on the lock helper from step 1
  being callable from this module."

---

## Synthesis (next-round handoff)

This round closes the seam between round 3's Step-execution contract
and the formula-on-disk by:

- Binding the envelope cache mechanism (M1).
- Reordering the formula `needs` chain to match the trigger model
  (M2).
- Recording PR2-internal red→green expectations (M3 indirect, S5,
  S6).
- Closing implementation-plan completeness gaps for CHANGELOG (S1),
  PR rollback (S2), help text (S4), and a previously-implicit lock
  refactor (S8).
- Documenting the auto-discovery error path (S3) and Phase 3
  enforcement (S7).

The next round (`gc-s6epvv`, `mol-idea-to-plan.plan-review-2`) will
review **risk** and **scope-creep** — technical/dependency/rollback/
unknown-unknown risks; gold-plating, over-engineering, premature
optimization, and defer candidates. The design is now structurally
implementable end-to-end; round 2's pressure-test is "is this design
trying to do too much, or carrying risk that needs an explicit
mitigation."
