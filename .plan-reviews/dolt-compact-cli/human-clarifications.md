# Human Clarifications — dolt-compact-cli

Round: 1 (post-PRD-review synthesis)
Date: 2026-05-04
Step bead: `gc-oqzq9w` (`mol-idea-to-plan.human-clarify`)
Coordinator: gascity/gastown.nux

The PRD review (`.prd-reviews/dolt-compact-cli/prd-review.md`) surfaced
five "Critical Questions" Q1-Q5. All five were answered; the binding
text was appended to `.prd-reviews/dolt-compact-cli/prd-draft.md` under
**Clarifications from Human Review** and threaded into Goals,
Constraints, and Open Questions.

## Decisions (one-line summaries)

| ID | Topic                       | Binding decision                                                                            |
|----|-----------------------------|---------------------------------------------------------------------------------------------|
| Q1 | JSON envelope shape         | (b) single final envelope; mint `gc.dolt.compact.v1`, mirroring `gc.dolt.cleanup.v1`        |
| Q2 | Executor-binding contract   | TOML fields `executor` + `zfc_exempt`; regression test scope = embedded formulas; PR1/PR2 split |
| Q3 | CLI naming                  | (c) both — `gc dolt-compact` (Go) + `gc dolt compact` (pack delegate)                       |
| Q4 | Auto-discover scope         | (a) `gc rig list --json` only; **never `SHOW DATABASES`**                                   |
| Q5 | Surgical lock + cleanup     | Acquire advisory lock **first**, then drop leftover `compact-*` branches                    |

## Material scope changes locked in

1. **Two-PR shipping order** (from Q2 clarification):
   - PR1 = schema fields + parser + migration sweep + regression test
     (broad scope, no new CLI). `mol-dog-compactor` migrates to
     `zfc_exempt = true` matching its current prose. **No forward refs to
     `gc dolt-compact`** — it doesn't exist yet.
   - PR2 = `gc dolt-compact` Go CLI + pack delegate +
     `gc.dolt.compact.v1` envelope + `mol-dog-compactor` formula update
     (drop `zfc_exempt`, set `executor = "gc dolt-compact"`).
   - Bead DAG must reflect: PR1 work bead is a `blocks` prerequisite of
     PR2, not bundled.

2. **Default principle** (overarching): mirror `gc dolt-cleanup` patterns
   wherever possible — same envelope shape, same dual CLI surface, same
   discovery path, same identifier validation, same lifecycle locking.
   Diverge only with stated cause documented in the design doc.

## Design-exploration follow-ups (added to Open Questions as Q15, Q16)

- **Q15:** Does shipping the CLI flip `mol-dog-compactor` from
  `zfc_exempt = true` to `executor = "gc dolt-compact"` (resolving the
  existing ZFC contract mismatch), or do daemon and CLI coexist? Default
  assumption: flip (the daemon path was rejected as a Non-Goal). Confirm
  in the design doc.
- **Q16:** Confirm "ZFC" expansion (Zero Framework Cognition) for
  unambiguous developer-facing docs covering the new `zfc_exempt` schema
  field.

## Outcome

The PRD draft is now ready for the design-exploration phase. Six design
legs may proceed against an updated draft with locked Q1-Q5 contracts
and the two follow-up questions explicitly framed for the design phase
to resolve.
