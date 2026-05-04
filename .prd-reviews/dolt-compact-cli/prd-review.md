# PRD Review: Implement `gc dolt compact` CLI subcommand to provide an executor for mol-dog-compactor

## Executive Summary

The PRD is implementable as written, with the design phase needing to
resolve five concrete contracts before writing code:

1. JSON envelope shape (final vs streaming)
2. "Executor binding" definition + regression-test scope
3. CLI naming: `gc dolt-compact` (Go) + `gc dolt compact` (pack delegate)
4. Auto-discover scope (rig registry vs Dolt server)
5. Surgical mode crash recovery / orphan branch cleanup

Confidence in feasibility is high. No fundamental blockers. Existing prior
art (`gc dolt-cleanup`) provides reusable infrastructure (port resolution,
identifier validation, JSON envelope, lifecycle locking, integration test
scaffolding). MVP scope is appropriately tight; defer `--dry-run`,
`--inspect-only`, event-bus emission, streaming NDJSON to v2.

The most novel piece is the executor-binding regression test (Acceptance
#3). It prevents recurrence of the orphan-formula failure mode that produced
the silent-failure incident. Investing in a precise definition of "binding"
and "exemption marker" is critical.

## Before You Build: Critical Questions

These questions must be answered before the design phase concludes. Each
maps to one or more Open Questions in the PRD draft.

### Q1: Pick the JSON envelope shape (PRD Q2)

The dog parses CLI output to close per-step beads. Three options:
- (a) NDJSON streamed to stdout (one event per phase)
- (b) Single final JSON envelope on stdout (all phase results inside)
- (c) Plain stdout + machine-readable sidecar file

Recommendation: **(b)**. Matches `gc dolt-cleanup`'s `gc.dolt.cleanup.v1`
pattern. Streaming is more complex without a current consumer; sidecar
adds I/O coupling. Mint `gc.dolt.compact.v1` and pin the schema from day 1.

### Q2: Define "executor binding" and regression-test scope (PRD Q9)

Acceptance #3 requires "Test verifies the formula's executor binding (no
orphan formula refs)." Required decisions:
- What constitutes an "executor binding"? (Cobra command? Pack script?
  Dog daemon entry?)
- What scope of formulas does the test cover? (All embedded formulas? Dog
  formulas only? Step-level vs formula-level?)
- What's the "ZFC exemption" marker shape that the regression test
  recognizes? (TOML field? Comment? Magic string?)

Recommendation: define an explicit `[ZFC]` exemption block in formula
TOMLs (e.g., `zfc_exempt = true` with `executor = "gc dolt-compact"`),
and write the regression test to walk every embedded formula and assert
that every step either has an executable binding (Cobra command exists,
pack script exists) or carries an `executor` reference plus exemption.

### Q3: CLI naming convention (PRD Q1)

`gc dolt-cleanup` (Go) lives at top level with a `gc dolt cleanup` pack
script that can delegate. The acceptance criterion phrases the new
command as `gc dolt compact hq --mode=flatten`. Two naming surfaces will
exist:
- `gc dolt-compact` — Go-side (implementation)
- `gc dolt compact` — pack-script delegate (operator-facing)

Recommendation: **build both**. The pack-script binding is the
operator-facing surface (matches `gc dolt cleanup`); the Go-side
implementation is invoked by the dog formula and by the pack script.

### Q4: Auto-discover scope (PRD Q4, Q7)

Empty `--databases` should discover... what?
- (a) Rig registry (`gc rig list --json`) — same as `gc dolt cleanup`
- (b) Dolt server (`SHOW DATABASES`) — different scope; includes test DBs
- (c) Both, with filtering (registered rigs minus excluded namespaces)

Recommendation: **(a)** for safety. Compaction acts on production
databases; auto-discovery should include only registered rigs.
Test/orphan DBs should be excluded as a class. If an operator wants
to compact unregistered DBs, they pass them explicitly.

### Q5: Surgical-mode crash recovery and branch cleanup (PRD Q11)

If the CLI crashes mid-rebase, `compact-base` and `compact-work`
branches may persist. Required:
- Pre-flight cleanup: drop leftover branches before starting (already
  in formula).
- Crash-recovery cleanup: on next CLI invocation, detect orphaned
  branches and clean them up before proceeding.
- Manual recovery: document the recovery procedure for operators.

Recommendation: cleanup phase as the FIRST step of compact for surgical
mode. Pre-flight lock prevents two surgical runs from racing. Crash
recovery is implicit in "next run cleans up its own orphans."

## Important But Non-Blocking

These should be resolved during design but don't block decision to build.

- **Multi-database partial-failure semantics.** When the CLI is invoked
  with multiple databases and some succeed/some fail, define exit code
  and JSON envelope shape (Leg 2). Suggest: exit 0 only on full success;
  envelope reports per-database outcome.
- **Concurrent-write retry observability.** Surgical mode retries once
  internally. Both attempts should be logged in JSON envelope so the dog
  / operator can see "tried, collided, retried, succeeded" (Legs 1, 3).
- **Lifecycle lock vs concurrency.** Need an advisory lock to prevent
  two `gc dolt-compact` processes against the same database racing
  (Leg 4, PRD Q6). Reuse `dolt_lifecycle_lock.go` pattern.
- **`dolt_gc` cost on large databases** can take minutes-to-hours
  (Leg 4, PRD Q10). Document the expected duration in the report;
  consider a heartbeat for very long runs.
- **Acceptance criteria coverage gaps.** Goals 2, 3, 5, 8 lack direct
  acceptance bindings (Leg 1). Add acceptance criteria for: integrity
  mismatch detection, formula+CLI default consistency, executor-binding
  regression scope.
- **Backwards-compat cutover.** No shim is right (Non-Goal). But the
  cutover from "skip" to "real work" needs a verified-compaction signal
  on the first cycle after deploy (Leg 1).

## Observations and Suggestions

- **Reuse strategy is strong.** PRD's Rough Approach correctly leverages
  `gc dolt-cleanup` patterns (port resolution, lock, JSON envelope,
  identifier validation). Stay disciplined about this in design.
- **Two output modes.** Default human-readable (operator) + `--json`
  (dog parsing). Matches existing pattern (Leg 6).
- **Pack-script binding for ergonomic parity.** Even though dog can
  shell out to Go-side directly, providing
  `examples/dolt/commands/compact/{command.toml,run.sh}` keeps the
  operator UX consistent (Leg 5).
- **Defer features:** `--inspect-only`, `--dry-run`, event-bus emission,
  streaming NDJSON to v2 unless concrete consumers exist (Leg 5).
- **Test infrastructure reuse.** `examples/dolt/sql_test.go` already has
  patterns for spinning up a real Dolt sql-server in tests. Reuse, don't
  reinvent.
- **Patrol/deacon recalibration.** mol-deacon-patrol may need adjustments
  once cycles emit real "compacted N databases" instead of "skipped 4"
  (Leg 6).
- **Document the ZFC exemption marker format** in `engdocs/` once
  defined. Future formula authors need this contract.
- **Charset validation at CLI entry.** Database names from positional
  args should be validated with the same charset as cleanup (Leg 6).

## Confidence Assessment

| Dimension | Confidence | Notes |
|-----------|-----------|-------|
| Feasibility | **High** | Strong prior art; no fundamental blockers |
| Scope clarity | **Medium-High** | Non-goals well-fenced; few creep risks |
| Acceptance testability | **Medium** | Needs 3-4 acceptance edits before design |
| Implementation cost | **Medium** | Dolt SQL + retry semantics + tests = real work |
| Regression-prevention value | **Very High** | Executor-binding test is the durable win |
| Risk of cycle re-failing post-deploy | **Low** | If formula update + CLI lands together |

Overall: **build it.** Resolve the 5 critical questions during design,
not before.

## Next Steps

The next pipeline step is `human-clarify` (PRD bead `gc-oqzq9w`). The
critical questions above (Q1-Q5) are the consolidated questions to
present to the human in the live conversation. Each is structured as
a recommendation + alternatives, so the human can confirm with a
short yes/no answer.

After human resolution:
1. `design-exploration` produces the baseline design doc
   (`.designs/dolt-compact-cli/design-doc.md`)
2. Three PRD-alignment rounds verify design honors PRD
3. Three plan-self-review rounds catch implementation gaps
4. `create-beads` emits the convoy + task DAG for an implementer to pick up

---

*Inline review note:* Per coordinator decision, six review legs were
executed inline rather than via `gc sling` fan-out. Full leg reports
in `.plan-reviews/dolt-compact-cli/prd-review-legs.md`. Synthesis
above incorporates findings from all six legs. Deviation logged in
the round logs for transparency.
