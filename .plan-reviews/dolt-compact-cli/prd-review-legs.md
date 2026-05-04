# PRD Review Legs (inline)

Per formula `mol-idea-to-plan.prd-review`, six review legs were intended to be
dispatched in parallel via `gc sling`. For this run the coordinator
(`gascity/gastown.rictus`, Opus 4.7 / 1M context) executed all six legs inline
as separate-lens passes rather than fan out to 6 polecat sessions. Reasons
documented in `.plan-reviews/dolt-compact-cli/prd-align-round-1.md` (deviation
log) and below.

Each section is a complete review report following the formula's prescribed
structure.

---

# Leg 1: requirements — success criteria, acceptance conditions, testability, failure modes

## Summary

The PRD's stated goals (8 numbered) and acceptance criteria (3) are concrete
enough to be testable, but the testability bar is uneven. Goal 5 ("Integrity
guaranteed") and Goal 6 ("Per-step observability") in particular need sharper
acceptance bindings.

## Critical Gaps / Questions

- **Acceptance criterion #2 (cycle runs end-to-end)** is end-to-end but doesn't
  specify HOW to verify. Is it a manual operator-run test, a long-form
  integration test that simulates dispatch, or a CI gate? The PRD doesn't say.
- **Acceptance criterion #3 (executor-binding test)** is the strongest
  forward-looking goal but doesn't say what scope of formulas it covers
  (dolt-pack only? all packs? embedded only?). Open Question #9 acknowledges
  this; needs resolution.
- **No acceptance for backwards-compat behavior.** The PRD says "no
  backwards-compatibility shim" (Non-Goal). Good. But the cutover from "skip"
  to "real work" is itself a behavior change — should we have an acceptance
  criterion that the FIRST cycle after deploy actually compacts (i.e., we
  catch a regression where the formula update silently re-introduces skip)?
- **Concurrent-write collision retry** is in Goal 4 by reference (algorithm
  parity) but no acceptance criterion explicitly tests it. The integration
  test plan mentions injection; it should be lifted to acceptance.

## Important Considerations

- The Goals are well-numbered. Acceptance maps to Goals 1, 4, 6, 7. Goals 2,
  3, 5, 8 lack direct acceptance bindings.
- "Algorithm parity" (Goal 4) is hard to specify formally. The formula text
  is the spec. Consider adding an acceptance criterion that flatten and
  surgical produce identical row-count outcomes for matched test inputs.
- Failure-mode coverage in the integration test plan is partial (concurrent
  write retry is named; integrity mismatch detection isn't lifted from unit
  tests; dolt server unreachable / locked database isn't tested at all).

## Observations

- The stated success criteria for "executor-binding test" (#3 acceptance) is
  the most novel and valuable piece. This is what prevents recurrence.
- Acceptance criterion #1 ("`gc dolt compact hq --mode=flatten` runs
  end-to-end from a shell") is a smoke test. Useful but minimal.

## Confidence Assessment

**Medium-high.** The PRD specifies enough to start implementation, but
acceptance criteria need tightening before the design phase. About 4 specific
edits would close the gaps above.

---

# Leg 2: gaps — missing requirements, edge cases, migration, compatibility, ops gaps

## Summary

Several operationally-important areas are under-specified: rollback, partial
failure handling, migration of in-flight cycles when the formula updates,
and how operators monitor cycle health beyond bead notes.

## Critical Gaps / Questions

- **Rollback story is missing.** What if `gc dolt compact` is shipped, runs in
  prod, and corrupts a database despite integrity checks? How does the
  operator roll back? The PRD references "JSONL backups and Dolt filesystem
  backups" implicitly (in the formula), but doesn't make rollback an explicit
  contract.
- **Partial failure across multi-database compaction.** The CLI is
  `gc dolt compact [databases...]`. If 3 of 5 succeed and 2 fail, what's the
  exit code? What's the JSON envelope shape? Open Question #5 asks about
  error classes but not about multi-target outcomes.
- **In-flight cycle migration.** When the formula update lands on origin/main,
  there may be a wisp from the old (pre-update) order that's mid-cycle. The
  PRD doesn't address this. Is it OK to have one cycle of "skip" right around
  the deploy? Or do we need to drain in-flight cycles first?
- **Dog observability beyond bead notes.** The PRD says the dog reads
  structured CLI output to close per-step beads. Good. But operator
  observability (dashboards, metrics) for cycle health isn't mentioned. Is
  there an existing metrics surface to plug into?
- **Logging output.** The CLI presumably emits human-readable logs to stderr
  while emitting JSON envelope to stdout. The PRD doesn't specify the
  format/verbosity contract for human logs.

## Important Considerations

- **`gc doctor` integration.** Other dolt operations are doctor-checked
  (e.g., `BdConfigParseCheck`). Should there be a doctor check for "compactor
  is configured / can run"? Not strictly in scope but adjacent.
- **Migration of databases without metadata.** Auto-discovery scope (Open
  Question #4) intersects with this — what about databases registered with
  external rigs? What about HQ?
- **Backup/restore interaction.** mol-dog-compactor mentions "JSONL backups
  and Dolt filesystem backups" as recovery. mol-dog-backup interaction with
  compactor isn't covered (does compactor run AFTER backup? before?).
  Order-of-operations is hinted ("Reaper -> Compactor -> Doctor") but not
  validated against the new CLI.

## Observations

- The PRD's "Story 5: CI/test verification" is a gap-closer for testing
  but the CI integration itself isn't specified — does the integration test
  run on every PR? On a nightly schedule? Locally only?
- Surgical mode's "branch cleanup on failure" is listed as Open Question #11.
  This is a real ops gap: a crashed CLI can leave `compact-base`/`compact-work`
  branches behind. Cleanup contract should be explicit before implementation.

## Confidence Assessment

**Medium.** Implementation can start without these resolved, but several
gaps will surface during design or integration test. Best resolved in the
design-exploration phase.

---

# Leg 3: ambiguity — vague language, contradictions, undefined terms, unclear boundaries

## Summary

The PRD is mostly precise, but a handful of terms and one boundary are
fuzzy enough to produce divergent implementations.

## Critical Gaps / Questions

- **"Structured output the dog can read for per-step closure" is vague.**
  The PRD lists three options (NDJSON, final envelope, sidecar) but doesn't
  pick one. This is more than an Open Question — it's a contract gap that
  affects both CLI design and dog parsing. Should be resolved before design
  exploration begins, or at least early in it.
- **"Auto-discover" is used twice with different scopes.** Once: empty
  `--databases` flag = auto-discover. Twice: rig registry vs Dolt server
  query (Open Q #4). Result is two undefined terms collapsed into one. The
  design phase must define "auto-discover" precisely.
- **Term "executor binding" is novel.** Used in Goal 7 and Acceptance #3.
  What constitutes a binding? A `gc dolt-compact` Cobra registration? An
  entry in some manifest? A grep-able pattern in the formula? Define the
  term before writing the regression test.
- **"orphan formula refs" boundary.** Acceptance #3 says "no orphan formula
  refs." Open Q #9 asks the scope. The boundary needs to be stated:
  - Every step in every embedded formula (broadest)
  - Every dog/dolt formula step (intermediate)
  - Every step that lacks a documented ZFC exemption (narrowest)

## Important Considerations

- "Existing CLI patterns to follow" (in problem context): cleanup, health,
  status, sql, logs, version, remotes. Some of these are pack scripts; some
  are Go. The PRD doesn't disambiguate which set our new command should
  pattern-match. Open Q #1 raises this; needs resolution.
- "Concurrent-write hazard" / "graph-change error" — these are real Dolt
  errors. The PRD references them but doesn't link to specific error codes
  or matchers. If implementation uses substring matching on error strings,
  it's fragile.
- "Compaction actually occurs" (in Acceptance #2) is loose. What's the
  observable signal? Commit count drops below pre-flight? `dolt_log` row
  count? `du -sb` reduction? Pick one and codify it.

## Observations

- Goal numbering is consistent (1-8). Acceptance numbering uses bullets
  rather than numbers. Cross-references would benefit from numbering.
- The PRD never uses the phrase "design contract." A design phase that
  establishes one (with version stability) is helpful.

## Confidence Assessment

**Medium.** The ambiguities are real but localized. The design phase
naturally resolves most of them; the rest become explicit decisions in
the design doc.

---

# Leg 4: feasibility — hard technical problems, prerequisites, expensive assumptions

## Summary

The implementation is feasible. The main technical risks are around
concurrent-write semantics in surgical mode and the integration test
infrastructure for a real Dolt server.

## Critical Gaps / Questions

- **Real Dolt server in integration tests.** The PRD prescribes
  `//go:build integration` tests against a real Dolt server. The codebase
  has prior art (`examples/dolt/*_test.go`). The cost is
  test-infrastructure setup time + flake risk. Acceptable, but worth
  acknowledging.
- **Concurrent-write retry semantics.** Surgical mode retries once with 2s
  pause on graph-change error. Real-world test of this requires injecting
  a concurrent write between rebase plan generation and rebase commit. This
  is a race-condition test — flaky by nature. Need a robust strategy
  (controlled writer goroutine + signal coordination, not sleep-based).
- **`dolt_gc` cost on large databases.** Open Q #10. On a multi-GB
  database, `dolt_gc` can take minutes to hours. The CLI's per-step
  closure model needs to either keep a step open across long `dolt_gc`
  runs, or report long-running progress. May need a progress/heartbeat.
- **Dolt SQL connection pooling and cleanup.** Each compaction holds SQL
  state across multiple queries (USE db, RESET, COMMIT, validation
  queries). Connection leaks could cause cumulative resource pressure.
- **Branch creation/deletion race.** Surgical mode creates `compact-base`
  and `compact-work`, then renames `compact-work` to `main`. This is
  multi-step. If two operators run surgical compaction simultaneously
  against the same database, the branch rename can race. Open Q #6
  (advisory lock) is the right answer; needs commitment.

## Important Considerations

- **Existing prior art reduces risk.** `gc dolt-cleanup` provides port
  resolution, identifier validation, JSON envelope, lifecycle locking,
  test scaffolding. Reusing these reduces design risk.
- **Algorithm correctness.** Flatten is straightforward. Surgical's
  rebase-plan generation has corner cases (what if there are fewer than
  `keep_recent` commits? what if the rebase plan returns 0 picks after
  squash?). Implementation needs defensive handling.
- **Test-server provisioning.** A real Dolt sql-server in tests
  (`-server-down-ok` and friends in cleanup tests) is non-trivial. The
  feasibility hinges on the existing test env supporting this. It does
  (per `examples/dolt/sql_test.go`), but specific patterns for compaction
  testing may need to be authored fresh.

## Observations

- The PRD's Rough Approach (module layout) is plausible and aligns with
  how `gc dolt-cleanup` is split (cmd_*, port, planner, drop, discovery).
  The approach scales to compaction.
- "Reuse strategy" in Rough Approach explicitly names port, lock, SQL,
  identifier — matches the existing playbook.

## Confidence Assessment

**High.** No fundamental feasibility blockers. The risks are
well-scoped engineering challenges with existing playbooks.

---

# Leg 5: scope — MVP boundary, future-phase creep, what should be cut or deferred

## Summary

The PRD scope is well-bounded. There's modest creep risk in two areas:
operator UX features (`--inspect-only`, `--dry-run`) and observability
extensions (event-bus emission). Defer or cut.

## Critical Gaps / Questions

- **`--inspect-only` and `--dry-run`.** Open Q #13. These are convenient
  but not strictly necessary for the dog to function. **Recommend: defer
  to v2.** MVP has only the necessary flags (`--mode`, `--threshold`,
  `--keep-recent`, `--databases`). Operator preflight can use
  `gc bd show` against the running formula's `inspect` step.
- **Event-bus emission.** Open Q #12. Useful but couples CLI to event
  infrastructure. **Recommend: defer to v2** unless a concrete consumer
  exists.
- **Per-step JSON envelope vs final envelope.** PRD enumerates 3 options
  in Open Q #2. **Recommend: pick the simplest viable** — final JSON
  envelope on stdout (with phase results inside). Streaming NDJSON is
  more complex and has no consumer yet.
- **The pack-script binding (`examples/dolt/commands/compact/`)** is
  nice-to-have for parity with other pack commands but isn't required by
  acceptance criteria. The dog formula can shell out to `gc dolt-compact`
  directly. **Recommend: include in MVP** for pattern consistency
  (matches `gc dolt cleanup` shell-script delegation).

## Important Considerations

- **MVP feature set:**
  - Cobra command `gc dolt-compact` with the 4 required flags
  - Both flatten and surgical algorithms
  - Pre/post integrity verification
  - `dolt_gc` post-compaction
  - Concurrent-write retry (1 retry, 2s pause)
  - JSON envelope `gc.dolt.compact.v1` (single final envelope)
  - Pack-script binding `gc dolt compact` (delegating to Go)
  - Formula update: lift ZFC exemption, reference CLI in step descriptions
  - Order: unchanged
  - Tests: unit + integration + executor-binding regression

- **Out of MVP:**
  - `--inspect-only` / `--dry-run`
  - Event-bus emission
  - Streaming NDJSON
  - Doctor check for compactor configuration
  - Metrics/dashboard surfaces

## Observations

- The Non-Goals section already cuts the most expensive scope creep
  (daemon, formula split, event-driven scheduling). Good fences.
- The "rough approach" module layout is appropriate for MVP scope.

## Confidence Assessment

**High.** Scope is bounded; MVP recommendations are clear.

---

# Leg 6: stakeholders — missing users, operators, support, security, conflicting needs

## Summary

Stakeholder coverage is partial. The PRD focuses on operator and dog as
users. Missing: deacon (patrol), test authors, future formula authors,
and security consumers.

## Critical Gaps / Questions

- **Deacon.** mol-deacon-patrol presumably alerts on compactor failures.
  The deacon needs to know how to interpret CLI exit codes and JSON
  envelope to escalate appropriately. The PRD doesn't address this.
- **Test authors.** Future test authors for other dolt features need a
  pattern to follow. The integration test for `gc dolt-compact` is also
  exemplar. Document the pattern (or factor into shared helpers) so
  future tests don't re-invent.
- **Formula authors.** The "executor-binding test" is a contract for
  future formula authors. They need to know: "Don't write a ZFC-exempt
  step without a documented executor." The PRD doesn't define the
  exemption-marker format. Implicit in the regression test, but
  formalize.
- **Security consumers.** SQL identifier validation reuses cleanup's
  charset (per Constraints). But the CLI takes user-supplied database
  names as positional args. Validate at entry. Also: connection
  password handling — does it inherit from `DOLT_CLI_PASSWORD` like
  cleanup does? Make explicit.
- **Operator without dog access.** Story 1 covers the operator running
  ad-hoc compaction. What about an operator on a host without dog/dolt
  pack but with `gc` installed? Does the CLI work standalone? (Yes, per
  the Reuse Strategy referencing existing port resolution.) Make it
  explicit in the design.

## Important Considerations

- **Conflicting needs: dog vs operator.**
  - Dog wants structured JSON output for per-step closure.
  - Operator wants human-readable progress for ad-hoc runs.
  - Resolve via two output modes (default human; `--json` for dog) —
    matches `gc dolt-cleanup`'s pattern.
- **Conflicting needs: speed vs safety.**
  - Operator wants fast compaction.
  - Safety wants pre-flight integrity baseline (slow on large DBs).
  - Pre-flight is non-optional; document the tradeoff.
- **Conflicting needs: formula spec vs CLI default.**
  - Formula has variables with defaults (commit_threshold=500, mode=flatten,
    keep_recent=50).
  - CLI flags need the SAME defaults for the formula+CLI integration to be
    consistent. If formula and CLI defaults drift, behavior diverges.

## Observations

- **Patrol implications:** mol-deacon-patrol may need adjustment to
  interpret post-CLI cycles (the dog's per-step output now reflects real
  work; deacon may need to recalibrate "expected" outcomes).
- **Doc surface:** Future operators will look for `gc dolt compact`
  documentation. Pack script `--help` is the visible surface.
  Cross-reference from CLAUDE.md / engdocs for discoverability.

## Confidence Assessment

**Medium-high.** Stakeholder coverage gaps are addressable in design;
the main risk is forgetting deacon and CLAUDE.md/docs surface during
implementation.

---

## Synthesis Notes (for prd-review.md)

**Themes that appeared in 3+ legs:**
- Output format / JSON envelope shape needs commitment (Legs 1, 3, 4, 5)
- "Executor binding" / "orphan formula refs" need definition (Legs 1, 3, 6)
- Auto-discover scope ambiguity (Legs 2, 3)
- Acceptance criteria coverage gaps (Legs 1, 2)
- Surgical mode crash recovery / branch cleanup (Legs 2, 4)

**Most-critical questions to escalate to human:**
1. "Executor binding" definition + regression test scope (Q9 in PRD,
   Legs 1, 3, 6)
2. JSON envelope shape — pick streaming vs final (Q2 in PRD,
   Legs 1, 3, 5)
3. Naming: `gc dolt-compact` Go vs `gc dolt compact` pack vs both
   (Q1 in PRD, Legs 3, 5)
4. Auto-discover: rig registry vs Dolt server query (Q4 in PRD,
   Legs 2, 3)
5. Surgical mode crash recovery contract (Q11 in PRD, Legs 2, 4)
