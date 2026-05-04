# Planning Manifest: dolt-compact-cli

- **Review ID:** `dolt-compact-cli`
- **Repo Root:** `/home/zook/loomington/.gc/worktrees/gascity/polecats/gastown.rictus`
- **Coordinator:** `gascity/gastown.rictus`
- **Review Target:** `gascity/gastown.polecat`
- **Root Bead:** `gc-2189al`
- **Workflow Branch:** `polecat/gc-1piaau-rictus`

## Problem Statement

Implement `gc dolt compact` CLI subcommand to provide an executor for
`mol-dog-compactor`.

The compactor formula declares itself ZFC-exempt (daemon-only executor) but
is dispatched to the gastown.dog pool every 24h. Dogs read the exemption and
safely-skip — no compaction occurs. Upstream issue
`gastownhall/gascity#1557` enumerates three fix options; option 3 (CLI
subcommand) is the decided long-term answer. Implement it.

Scope: a new `gc dolt compact [databases...]` subcommand that:

- Honors existing formula variables: `--mode` (flatten|surgical),
  `--threshold` (commit count, default 500), `--keep-recent` (surgical,
  default 50), `--databases` (comma list, empty = auto-discover)
- Implements both flatten and surgical algorithms exactly as described in
  the existing formula
- Performs integrity verification (pre/post row counts, dolt_gc post-step,
  error classification)
- Emits structured output the dog can read for per-step closure
  (inspect/compact/verify/report)
- Has unit + integration tests
- Updates `mol-dog-compactor` formula and order to invoke the CLI and lift
  the ZFC exemption
- Adds an executor-binding test so this orphan-formula regression cannot
  recur

## Acceptance

- `gc dolt compact hq --mode=flatten` runs end-to-end from a shell
- `mol-dog-compactor` cycle runs end-to-end: dog claims, shells out,
  compaction actually occurs, integrity verified, dolt_gc runs, report sent
- Test verifies the formula's executor binding (no orphan formula refs)

## Pipeline State

- [x] init-run (bead: `gc-1piaau`, closed)
- [x] draft-prd (bead: `gc-k1gxux`, closed)
- [x] prd-review (bead: `gc-y1aztr`, closed)
- [x] human-clarify (bead: `gc-oqzq9w`, closed)
- [x] design-exploration (bead: `gc-oick69`, in_progress -> handing off)
- [ ] prd-align-1 (bead: `gc-qn5p3r`)
- [ ] prd-align-2 (bead: `gc-fm7r62`)
- [ ] prd-align-3 (bead: `gc-lygvkw`)
- [ ] plan-review-1 (bead: `gc-imyo2c`)
- [ ] plan-review-2 (bead: `gc-s6epvv`)
- [ ] plan-review-3 (bead: `gc-ahiv4c`)
- [ ] create-beads (bead: `gc-8zdml1`)

## Artifact Map

- `.prd-reviews/dolt-compact-cli/prd-draft.md` — draft PRD (this run)
- `.prd-reviews/dolt-compact-cli/prd-review.md` — synthesized PRD review (after prd-review)
- `.designs/dolt-compact-cli/design-doc.md` — design doc (after design-exploration)
- `.plan-reviews/dolt-compact-cli/state.env` — workflow state
- `.plan-reviews/dolt-compact-cli/manifest.md` — this file
- `.plan-reviews/dolt-compact-cli/prd-review-beads.tsv` — review leg bead IDs
- `.plan-reviews/dolt-compact-cli/design-review-beads.tsv` — design leg bead IDs
- `.plan-reviews/dolt-compact-cli/prd-align-round-{1,2,3}.md` — alignment logs
- `.plan-reviews/dolt-compact-cli/review-round-{1,2,3}.md` — plan review logs
- `.plan-reviews/dolt-compact-cli/human-clarifications.md` — human gate log
- `.plan-reviews/dolt-compact-cli/beads-created.md` — final bead DAG
