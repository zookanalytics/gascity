# Beads Created — dolt-compact-cli

Final output of the `mol-idea-to-plan.create-beads` step
(bead `gc-8zdml1`). Converts the refined design doc at
`.designs/dolt-compact-cli/design-doc.md` into an executable bead
DAG owned by an integration-branch convoy.

- **Convoy:** `gc-i7e8p` — `dolt-compact-cli`
  - Lifecycle: `owned` (manual lifecycle, no auto-close)
  - Target branch: `integration/gc-i7e8p`
- **Beads:** 11 total (2 PR1 + 9 PR2)
- **First bead to dispatch:** `gc-jevbo`
  (PR1 schema fields — the only ready bead; everything else is
  blocked downstream)

## Convoy create commands replayed

```
bd create --type convoy "dolt-compact-cli-test-bd"   # → gc-i7e8p
bd update gc-i7e8p --title "dolt-compact-cli"
bd update gc-i7e8p --add-label owned
gc convoy target gc-i7e8p integration/gc-i7e8p
```

(Aside: `gc convoy create --owned` was tried first but lands the
convoy in city scope with `lx-` prefix, while child work beads
created via `gc bd create` land in the rig store with `gc-` prefix.
`gc convoy add` then refuses with "bead not found" because it does
not cross-store. Workaround applied: create the convoy in the rig
store via `bd create --type convoy`, mark it `owned` and set its
target via the standard `gc convoy` commands.)

## Bead DAG

```
PR1 (executor-binding contract):
  gc-jevbo  PR1: schema fields (Executor + ZfcExempt on formula.Step)
    └─ gc-34ia5  PR1: regression test + formula audit + mol-dog-compactor migration

PR2 (CLI + envelope + formula update):
  gc-4a68s  PR2: foundation (Cobra skeleton + envelope + validator + lock helper factor)
    └─ gc-uuz1r  PR2: Inspector + Verifier
         ├─ gc-gfggu  PR2: Flatten executor
         └─ gc-tt8or  PR2: Surgical executor (also depends on gc-4a68s for lock helper)
              └─ gc-hjeg8  PR2: Cobra orchestration body + --help + remaining unit tests
                   └─ gc-71vsy  PR2: pack delegate + arg-forwarding regression test
                        └─ gc-7cupa  PR2: integration tests (CLI-alone scenarios)
                             └─ gc-gonrd  PR2: mol-dog-compactor formula update + formula-driven integration test
                                  └─ gc-w9gh9  PR2: engdocs operator/dev guide + CHANGELOG entries

Inter-phase edge:
  gc-4a68s depends on gc-34ia5  (PR2 foundation requires PR1 contract)
```

`gc-tt8or` (surgical) carries an extra dependency on `gc-4a68s`
beyond the inspect+verify chain because surgical's lock-then-cleanup
ordering depends on the lock-helper API factored in the foundation
bead (R-7).

## Bead inventory (linear order)

1. **gc-jevbo** — `PR1: schema fields — Executor + ZfcExempt on formula.Step`
   - Files: `internal/formula/types.go`, `internal/formula/recipe.go`,
     `internal/formula/compile.go`.
   - Acceptance: existing tests stay green; round-trip parse-cook
     preserves both fields; doc comments spell out ZFC and pairing
     rule.
   - Design refs: §Two-PR Shipping Order / PR1 step 1-2; §Data Model
     / Formula schema additions; OQ-1, OQ-7 (resolved).

2. **gc-34ia5** — `PR1: executor-binding regression test + formula audit + mol-dog-compactor migration`
   - Depends on: `gc-jevbo`.
   - Files: `cmd/gc/embedded_formula_executor_test.go` (new),
     `examples/*/formulas/*.toml` (annotation sweep),
     `examples/dolt/formulas/mol-dog-compactor.toml` (zfc_exempt
     migration matching prose), `CHANGELOG.md`.
   - Acceptance: regression test goes red (pre-audit) →
     green (post-audit); orphan-formula failure mode unrepeatable.
   - Design refs: §Key Components / 8; §Implementation Plan / Phase
     1 (TDD red→green); R-1, R-2; OQ-2, OQ-3.

3. **gc-4a68s** — `PR2: foundation — Cobra skeleton + envelope structs + shared validator + lock helper factor`
   - Depends on: `gc-34ia5`.
   - Files: `cmd/gc/cmd_dolt_compact.go` (skeleton),
     `cmd/gc/dolt_compact_envelope.go`,
     `cmd/gc/dolt_compact_validate.go`, `cmd/gc/main.go` (Cobra
     registration), `dolt_lifecycle_lock.go` (lock helper factor),
     cleanup call site rewired to shared helpers.
   - Acceptance: `gc dolt-compact --help` runs; envelope + validator
     unit tests green; cleanup rewires land in same commit.
   - Design refs: §Implementation Plan / Phase 2 steps 1, 3, 7;
     §Key Components / 1, 6, 7; R-6, R-7.

4. **gc-uuz1r** — `PR2: Inspector + Verifier modules`
   - Depends on: `gc-4a68s`.
   - Files: `cmd/gc/dolt_compact_inspect.go`,
     `cmd/gc/dolt_compact_verify.go`, unit tests.
   - Acceptance: rig-list parse + commit-count probe + uniform
     threshold gating (Q14); `pre_hash` propagation on mismatch.
   - Design refs: §Key Components / 2, 5; §Trust and Discovery;
     §Threshold Semantics; TD-3 / Q4.

5. **gc-gfggu** — `PR2: Flatten executor (algorithm + per-DB timeout + rollback path)`
   - Depends on: `gc-uuz1r`.
   - Files: `cmd/gc/dolt_compact_flatten.go`, unit tests.
   - Acceptance: pseudocode-verbatim port; rollback-on-mismatch via
     `DOLT_RESET --hard pre_hash`; `dolt_gc` skip on mismatch;
     `database-not-found` and `internal-error`-from-gc-failure
     covered.
   - Design refs: §Algorithms / Flatten; §Error Class Taxonomy;
     TD-5; R-4, R-10.

6. **gc-tt8or** — `PR2: Surgical executor (lock-then-cleanup + retry + halt-before-swap)`
   - Depends on: `gc-uuz1r`, `gc-4a68s` (lock helper).
   - Files: `cmd/gc/dolt_compact_surgical.go`, unit tests.
   - Acceptance: pseudocode-verbatim port; lock-then-cleanup
     ordering; one-retry-on-graph-change with 2s pause;
     halt-before-swap on every escalate-class outcome; `attempts`
     populated.
   - Design refs: §Algorithms / Surgical; TD-5, TD-6; R-3.

7. **gc-hjeg8** — `PR2: Cobra orchestration body + --help text + remaining unit tests`
   - Depends on: `gc-gfggu`, `gc-tt8or`.
   - Files: `cmd/gc/cmd_dolt_compact.go` (body fill), unit tests.
   - Acceptance: 5-bucket `--help` content (dual-surface, discovery
     rule, threshold semantics, exit-code summary, engdocs pointer);
     OQ-5 3-value exit codes; PRD Story 1 human output; envelope-
     stdout shape test; rig-list-failure path test; formula/CLI
     defaults consistency test.
   - Design refs: §Implementation Plan / Phase 2 steps 8 + 10;
     §Default human output; §JSON envelope; OQ-5; R-11.

8. **gc-71vsy** — `PR2: pack delegate (gc dolt compact) + arg-forwarding regression test`
   - Depends on: `gc-hjeg8`.
   - Files: `examples/dolt/commands/compact/{command.toml,run.sh,
     compact_test.go}`.
   - Acceptance: arg forwarding regression test for #1485 class;
     `GC_DOLT_PORT` → `--port` per OQ-6.
   - Design refs: §Key Components / 9; R-5; OQ-6.

9. **gc-7cupa** — `PR2: integration tests (CLI-alone — real Dolt sql-server scenarios)`
   - Depends on: `gc-71vsy`.
   - Files: `cmd/gc/cmd_dolt_compact_integration_test.go`
     (`//go:build integration`).
   - Acceptance: 7 CLI-alone scenarios (populate/flatten,
     populate/surgical, concurrent-write injection, lock back-to-
     back, no `SHOW DATABASES`, row-count divergence rollback for
     both modes).
   - Design refs: §Implementation Plan / Phase 2 step 11; R-3, R-7,
     R-8, R-10; TD-3 / Q4.

10. **gc-gonrd** — `PR2: mol-dog-compactor formula update + formula-driven integration test`
    - Depends on: `gc-7cupa`.
    - Files: `examples/dolt/formulas/mol-dog-compactor.toml`
      (executor on every step; zfc_exempt only on bookkeeping;
      `needs` chain reorder; read-first guard + CLI invocation +
      metadata write in compact step description; cached-envelope
      closure paths in bookkeeping step descriptions); integration
      test extension (formula-driven happy path; cache-hit
      idempotence; dog-side write).
    - Acceptance: PR1's executor-binding test still green;
      formula-driven happy-path test asserts step closures + cache
      population; cache-hit test asserts CLI not re-invoked.
    - Design refs: §Step-execution contract (Designated trigger
      step, Bookkeeping steps, Dog-runtime mechanism, Read-first
      idempotence guard, Formula `needs`-chain alignment);
      §Implementation Plan / Phase 2 step 12 + formula leg of step
      11; R-9, R-12, R-13, R-14; TD-9.

11. **gc-w9gh9** — `PR2: engdocs operator/dev guide + CHANGELOG entries`
    - Depends on: `gc-gonrd`.
    - Files: `engdocs/contributors/dolt-compact.md` (new),
      `CHANGELOG.md`.
    - Acceptance: MR-1..MR-4 at full operator depth; flag reference;
      dog-cycle walkthrough; error class table at operator depth;
      CHANGELOG `### Added` (CLI, pack delegate, envelope) +
      `### Changed` (mol-dog-compactor migration).
    - Design refs: §Operations / Manual Recovery; §Implementation
      Plan / Phase 2 steps 13-14; R-10.

## Dispatch order

The convoy graduates to `main` once all 11 beads land on
`integration/gc-i7e8p`. Internal dispatch order is the linear
chain above; at any step, all currently-ready beads can be
dispatched in parallel (per the DAG), with the natural fanout
points being:

- After `gc-uuz1r` (inspect+verify): `gc-gfggu` (flatten) and
  `gc-tt8or` (surgical) can run in parallel.
- All other edges are linear.

**First dispatch:** `gc-jevbo` (PR1 schema fields). This is the only
ready bead in the convoy; everything else is blocked behind it.

## Artifact locations

- PRD draft: `.prd-reviews/dolt-compact-cli/prd-draft.md`
- PRD review: `.prd-reviews/dolt-compact-cli/prd-review.md`
- Design doc (load-bearing): `.designs/dolt-compact-cli/design-doc.md`
- Plan-review logs:
  - `.plan-reviews/dolt-compact-cli/manifest.md`
  - `.plan-reviews/dolt-compact-cli/state.env`
  - `.plan-reviews/dolt-compact-cli/prd-align-round-{1,2,3}.md`
  - `.plan-reviews/dolt-compact-cli/review-round-{1,2}.md`
  - `.plan-reviews/dolt-compact-cli/human-clarifications.md`
- This file: `.plan-reviews/dolt-compact-cli/beads-created.md`

(plan-review-3 / `gc-ahiv4c` was trimmed per the option-2 decision
recorded on its close — see manifest.md.)
