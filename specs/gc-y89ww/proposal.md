---
name: gc sling route-parent — design proposal
description: Research + design proposal for the engine-level fix to gc sling's
  inability to route a mol-attached parent (keeper rebase re-pour case).
  Informs an upstream gascity PR.
---

# `gc sling` re-pour — design proposal

Bead: `gc-y89ww` (gascity rig)
Coordinator: `gc-toolkit.mechanik`
Downstream PR target: upstream gascity

## 1. Provenance

Every claim below traces to a row in this table. SHAs are pinned to the
working trees inspected on 2026-05-14.

| Doc-type / artifact | Producer | Source location | Surveyed at |
| --- | --- | --- | --- |
| `gc sling` CLI surface | gascity CLI layer | `cmd/gc/cmd_sling.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Sling engine core | gascity domain | `internal/sling/sling_core.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Sling engine dispatch | gascity domain | `internal/sling/sling.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Molecule attachment helpers | gascity domain | `internal/sling/sling_attachment.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Sling unit tests | gascity test | `internal/sling/sling_test.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Sling CLI tests | gascity test | `cmd/gc/cmd_sling_test.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Bead store predicates | gascity domain | `internal/beads/beads.go`, `internal/beads/bdstore.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Agent config + `default_sling_formula` | gascity domain | `internal/config/config.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Default scale-check generator | gascity domain | `internal/config/config.go:2293-2300` @ gascity `d8c95dc5` | 2026-05-14 |
| Controller pool demand counter | gascity controller | `cmd/gc/build_desired_state.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Pool desired state computation | gascity controller | `cmd/gc/pool_desired_state.go` @ gascity `d8c95dc5` | 2026-05-14 |
| Gastown pack polecat agent | gascity examples | `examples/gastown/packs/gastown/agents/polecat/agent.toml` @ gascity `d8c95dc5` | 2026-05-14 |
| Rebase formula (v7) | gc-toolkit pack | `formulas/mol-upstream-gc-rebase.toml` @ gc-toolkit `a4e1d35` | 2026-05-14 |
| Rebase rework formula | gc-toolkit pack | `formulas/mol-upstream-gc-rebase-rework.toml` @ gc-toolkit `a4e1d35` | 2026-05-14 |
| PR-prep formula | gc-toolkit pack | `formulas/mol-upstream-gc-pr-prep.toml` @ gc-toolkit `a4e1d35` | 2026-05-14 |
| Polecat-work formula | gc-toolkit pack | `formulas/mol-polecat-work.toml` @ gc-toolkit `a4e1d35` | 2026-05-14 |
| Keeper prompt (current HEAD) | gc-toolkit pack | `agents/gascity-keeper/prompt.template.md` @ gc-toolkit `a4e1d35` | 2026-05-14 |
| Keeper "bare sling" fix commit | gc-toolkit git history | gc-toolkit commit `847ec50cee5d050608801c60372853aa8065fccd` "fix(keeper): re-pour example uses bare sling + clear assignee (tk-rb6ac)" | 2026-05-14 |
| Keeper "direct metadata" fix commit (superseded) | gc-toolkit git history | gc-toolkit commit `99b2d76` "fix(keeper): re-pour uses direct metadata update, not gc sling (tk-z6tyf)" | 2026-05-14 |
| Codex review of PR #18 | gc-toolkit issue tracker | bead `tk-r0213` notes | 2026-05-14 |
| `gc-toolkit` pack patches | gc-toolkit pack | `pack.toml` @ gc-toolkit `a4e1d35` | 2026-05-14 |

## 2. Surface-area survey

### 2a. `gc sling` — routing decision tree

The CLI entry point `cmdSling` (`cmd/gc/cmd_sling.go:179`) parses flags and
calls `doSlingBatch` (`cmd/gc/cmd_sling.go:391`, `784`). That CLI wrapper
splits on whether a formula is in play:

```go
// cmd/gc/cmd_sling.go:799
if opts.IsFormula || opts.OnFormula != "" || (!opts.NoFormula && opts.Target.EffectiveDefaultSlingFormula() != "") {
    result, err = sling.DoSlingBatch(opts, deps, querier)
} else {
    result, err = sl.ExpandConvoy(context.Background(), opts.BeadOrFormula, opts.Target, ...)
}
```

`Sling.ExpandConvoy` (`internal/sling/sling.go:273-287`) is a thin wrapper
that calls `DoSlingBatch`. Both paths converge in
`internal/sling/sling_core.go:828`:

```go
// internal/sling/sling_core.go:858-875
if b.Type == "epic" || beads.IsContainerType(b.Type) { ... }
if b.Type == "epic" {
    return SlingResult{}, fmt.Errorf("bead %s is an epic; first-class support is for convoys only", b.ID)
}
if !beads.IsContainerType(b.Type) {
    singleOpts := opts
    singleOpts.IsFormula = false
    singleDeps := deps
    singleDeps.ValidationQuerier = containerQuerier
    return DoSling(singleOpts, singleDeps, querier)
}
// container -> expand children below
```

`beads.IsContainerType` (`internal/beads/beads.go:62-70`) returns true
**only for `convoy`**. So the engine never expands a `task` bead's
children, even when the task has open child beads — single-bead routing
is the only path for tasks.

`DoSling` (`internal/sling/sling_core.go:43-67`) then branches on flags:

```go
switch {
case opts.IsFormula:
    return slingFormula(opts, deps)              // --formula
case opts.OnFormula != "":
    return slingOnFormula(opts, deps, querier, beadID, result)  // --on
case !opts.NoFormula && a.EffectiveDefaultSlingFormula() != "":
    return slingDefaultFormula(opts, deps, querier, beadID, result)  // pool has default formula
default:
    return slingPlainBead(opts, deps, beadID, result)  // bare bead route
}
```

The **plain-bead route** (`slingPlainBead`, line 332-335) calls `finalize`,
which ultimately calls `cliBeadRouter.Route` (`cmd/gc/cmd_sling.go:585-606`):

```go
if err := r.deps.Store.SetMetadata(req.BeadID, "gc.routed_to", req.Target); err != nil {
    return fmt.Errorf("setting gc.routed_to on %s: %w", req.BeadID, err)
}
```

So bare `gc sling <pool> <task-bead>` against a polecat-pool agent with
**no default formula** writes a single metadata key:
`gc.routed_to=<pool-template>`. It does not touch `assignee`, `status`, or
`molecule_id`. Test coverage:
`TestDoSlingBatchRoutesNonContainerFoundInQuerierStore`
(`internal/sling/sling_test.go:1195-1220`) asserts `result.Method == "bead"`
for a `Type: "task"` source.

There is **no flag today** that forces routing of a bead's parent over
its children — the type-based decision is hard-coded — but the question is
moot for our case because the rebase parent is a `task`, not a container,
so the engine already routes it as a single bead. The "expands children"
symptom seen in codex's dry-run is a CLI display bug (see 2f).

### 2b. Bead-type handling at routing time

`internal/beads/beads.go:62-70` registers exactly one container type:
`"convoy"`. `epic` is rejected at `sling_core.go:865-866`. All other types
(`task`, `molecule`, `step`, `wisp`, `message`) flow through `DoSling`
unmodified. Per-type branching in the routing code is otherwise minimal.

The polecat agent in the gastown pack (`examples/gastown/packs/gastown/agents/polecat/agent.toml`)
declares no `default_sling_formula` and no custom `sling_query`, so bare
`gc sling <polecat-pool> <bead>` reaches `slingPlainBead`.

### 2c. The `--force` flag — actual override surface

`--force` is consulted at five call sites; it is **asymmetric**:

| Call site | What `--force` permits |
| --- | --- |
| `sling_core.go:77-82` (preflight) | Suppresses `AgentSuspended` and `PoolEmpty` warnings. |
| `sling_core.go:139-144` (`shouldValidateExistingBead`) | Skips local-bead-exists check for plain bead routing. |
| `sling_core.go:151` (`shouldGuardCrossRig`) | Allows cross-rig routing. |
| `sling_core.go:222-224` (`slingOnFormula`) | If formula is graph.v2 workflow, switches to `CheckNoMoleculeChildrenAllowLiveWorkflow`. |
| `sling_core.go:286-288` (`slingDefaultFormula`) | Same as above for default-formula path. |

`--force` does **not** bypass `CheckNoMoleculeChildren` for non-graph
(legacy) molecules. The error at `sling_attachment.go:212` —
`bead %s already has attached %s %s` — is unconditional on the non-graph
path. The only escape valve is the auto-burn at
`sling_attachment.go:203-210`, which fires only when the parent is
**unassigned** at the moment of the check; assigned parents always error.

This asymmetry is the structural seam this proposal targets. Graph
workflows accept `--force` as "supersede the existing attachment under a
source-workflow lock". Legacy molecules accept no such bypass at all, so
the keeper cannot use `--on` for re-pour.

### 2d. The `--var` flag

`BuildSlingFormulaVars` (`internal/sling/sling.go:917-955`) parses
`--var k=v` pairs into a map used by formula compilation
(`InstantiateSlingFormula`, `internal/sling/sling.go:1124`). The map is
threaded into `molecule.Options.Vars` and applied to `text/template`
substitutions in step bodies.

The plain-bead route (`slingPlainBead`) **never calls
`BuildSlingFormulaVars`**. So `gc sling polecat <bead> --var requesting_keeper=X`
on a polecat pool (no default formula) **silently drops** the `--var`
value. The current keeper prompt at gc-toolkit `a4e1d35` line 517-518
shows exactly this footgun:

```bash
gc bd update <bead> --assignee ""
gc sling gascity/gc-toolkit.polecat <bead> \
  --var requesting_keeper="$GC_AGENT"   # ← ignored on the plain-bead route
```

`requesting_keeper` is already set on the bead by the rebase formula's
handback step (see 2g); the `--var` here is a no-op carried over from the
first dispatch's `--on mol-upstream-gc-rebase` form. The mechanik boot
prompt notes `--var base_branch=...` overrides `metadata.target` at sling
time. That is also formula-path-only: the convoy-ancestor walk in
`internal/sling/sling.go` reads `metadata.target` directly; the override
lands only when a formula compiles and the formula then writes the value
into a step's metadata or the routed bead.

### 2e. `--on <molecule-name>` — attachment lifecycle

`slingOnFormula` (`sling_core.go:205-266`):

1. Builds formula vars and compiles the formula to decide
   `isGraph` (`sling_core.go:209-214`).
2. Picks the attachment check function:
   ```go
   // sling_core.go:221-224
   checkAttachments := CheckNoMoleculeChildren
   if isGraph && opts.Force {
       checkAttachments = CheckNoMoleculeChildrenAllowLiveWorkflow
   }
   ```
3. Runs the check (`sling_core.go:225-227`). For non-graph formulas this
   walks the bead's metadata (`molecule_id`, `workflow_id`) and any
   attached-root children. If any open attachment is found and the parent
   is **assigned**, returns the "already has attached" error.
4. On success, instantiates the wisp formula, sets
   `metadata.molecule_id = wispRootID` on the parent
   (`sling_core.go:243`), and finalizes routing.

The attachment marker `molecule_id` is **never cleared on normal
completion**. Test:
`TestOnFormulaMetadataAttachmentSkipsIdempotentRetry`
(`cmd_sling_test.go:4570-4621`) asserts `metadata.molecule_id` persists
across a successful idempotent retry. The only clearing path is
`clearAttachmentMetadata` (`sling_attachment.go:155-170`), called only
from the auto-burn flow on unassigned parents.

Test `TestOnFormulaExistingMoleculeErrors`
(`cmd_sling_test.go:4438-4466`) confirms the failure mode end-to-end: a
bead with an open molecule child plus a non-empty assignee gets the
"already has attached molecule" error from `gc sling --on <fmla> <bead>`
with no routing side effects.

### 2f. CLI dry-run misleading output for task parents with children

`doSlingBatch` (`cmd/gc/cmd_sling.go:850-866`) decides between
`dryRunBatch` and `dryRunSingle` purely on whether the bead has children,
not on the engine's actual routing decision:

```go
if result.DryRun {
    if querier != nil {
        if b, getErr := querier.Get(opts.BeadOrFormula); getErr == nil {
            children, _ := querier.List(beads.ListQuery{
                ParentID: b.ID, IncludeClosed: true, Sort: beads.SortCreatedAsc,
            })
            var open []beads.Bead
            for _, c := range children { if c.Status == "open" { open = append(open, c) } }
            return dryRunBatch(opts, deps, stdout, stderr, b, children, open, querier)
        }
    }
    return dryRunSingle(opts, deps, querier, stdout, stderr)
}
```

The `dryRunBatch` template (`cmd/gc/cmd_sling.go:1601-1700`) hard-codes
the language "A <type> is a container bead that groups related work.
Sling expands it and routes each open child individually." For a `task`
bead with any children (open or closed), this preview is rendered even
though the engine would actually route the parent. Codex's PR #18 review
(bead `tk-r0213`) was misled by this exact display when dry-running
`gc sling polecat <task-bead>` against the recurrence beads `gc-j8e7j`
and `gc-q5q3u`; both reports show the "Children (N total, M open)"
batch banner with no parent-route command. The engine path (per 2a and
the `TestDoSlingBatchRoutesNonContainerFoundInQuerierStore` test) would
route the parent without expanding the children. **The actual behavior
of bare `gc sling <pool> <task-bead>` is correct; only the dry-run is
wrong.**

This dry-run defect propagated to a `REQUEST_CHANGES` verdict on PR #18,
caused the keeper prompt to flip between three workaround shapes across
commits `99b2d76` → `a6242bc` → `847ec50`, and is the most direct cause
of the structural confusion this proposal documents.

### 2g. The rebase formula's handback step

`formulas/mol-upstream-gc-rebase.toml` at gc-toolkit `a4e1d35`, the
"dispatch-rework" step (lines 700-712), writes this state on the parent
bead when handing off to a rework polecat and draining:

```toml
gc bd update {{issue}} \
    --set-metadata rebase_in_progress=true \
    --set-metadata pending_rework="$REWORK_BEAD" \
    --set-metadata dispatch_count="$DISPATCHES" \
    --status=open \
    --assignee="$REQUESTING_KEEPER" \
    --set-metadata gc.routed_to="$REQUESTING_KEEPER"

gc session nudge "$REQUESTING_KEEPER" "rebase {{issue}}: dispatched rework polecat on $REWORK_BEAD for $SHORT_SHA ($COMMIT_SUBJECT); re-pour when rework closes"

gc runtime drain-ack
exit 0
```

Post-handback state (verified against keeper-prompt lines 40, 368-420
which describe how the keeper reads this state on prime):

| Field | Value after handback |
| --- | --- |
| `metadata.molecule_id` | Still set to the rebase wisp's root ID (never cleared). |
| `metadata.rebase_in_progress` | `true` |
| `metadata.pending_rework` | Child rework bead ID |
| `metadata.requesting_keeper` | `$REQUESTING_KEEPER` |
| `metadata.gc.routed_to` | `$REQUESTING_KEEPER` (not the polecat pool) |
| `status` | `open` |
| `assignee` | `$REQUESTING_KEEPER` |
| Worktree | Mid-rebase (`.git/rebase-merge/` preserved) |

The keeper now owns the bead. When the rework child closes, the keeper
must re-route the parent back to the polecat pool to continue the
state machine.

### 2h. Pool scale-check predicate

The default scale-check generator is `Agent.EffectiveScaleCheck`
(`internal/config/config.go:2293-2300`):

```go
func (a *Agent) EffectiveScaleCheck() string {
    if a.ScaleCheck != "" { return a.ScaleCheck }
    template := a.QualifiedName()
    return `ready_json=$(bd ready --metadata-field gc.routed_to=` + template +
        ` --unassigned --limit 0 --json) && printf '%s\n' "$ready_json" | jq 'length'`
}
```

The controller side runs this and collates results in
`defaultScaleCheckCounts` (`cmd/gc/build_desired_state.go:820-882`),
specifically lines 871-878:

```go
for _, b := range ready {
    if strings.TrimSpace(b.Assignee) != "" { continue }
    template := strings.TrimSpace(b.Metadata["gc.routed_to"])
    if _, ok := group.templates[template]; ok {
        counts[template]++
    }
}
```

`bd ready` (`internal/beads/bdstore.go:564-576` and the type-exclusion
list at `internal/beads/beads.go:85-99`) admits beads with status in
the "ready" family (`open`, `blocked`, `review`, `testing`) and excludes
infrastructure types. The full predicate the controller demands for
counting a bead as pool-spawn demand:

```
status ∈ {open, blocked, review, testing}    (i.e., not in_progress, not closed)
∧ assignee == ""
∧ metadata.gc.routed_to == <pool template name>
∧ type ∉ {merge-request, gate, molecule, step, message, session, agent, role, rig}
```

The keeper-prompt rationale at gc-toolkit `a4e1d35` lines 528-544 cites
exactly the first three conjuncts. The fourth is implicit and not
violated by `task`-type beads.

A bead with `status=in_progress` is excluded from the count even if its
assignee is cleared — relevant for crash-recovery cases where the prior
polecat died mid-claim. The keeper's `--status open` clear is therefore
load-bearing in some handback paths (though for the rebase formula's
handback, the formula already sets `--status=open`).

### 2i. Sling's actual routing effect vs the scale-check predicate

The plain-bead route writes only `gc.routed_to` (per 2a). It does **not**
clear `assignee` or change `status`. When the keeper's handback state has
`assignee=$REQUESTING_KEEPER` and `status=open`, a bare
`gc sling polecat <bead>` rewrites `gc.routed_to` to the polecat pool
**but leaves the assignee on the keeper**, suppressing the
`assignee == ""` conjunct in the scale-check.

`--reassign` exists to close this gap (`cmd/gc/cmd_sling.go:129`,
`internal/sling/sling_core.go:113-117`, `clearHumanAssignee` at line
1098). `TestDoSling_Reassign_ClearsHumanAssignee`
(`internal/sling/sling_test.go:3014`) and the surrounding three tests
prove the preflight clear works for plain-bead routing. A single command
`gc sling --reassign polecat <bead>` therefore produces the full
controller-visible shape with no manual `gc bd update` required.

**No prompt, doc, or formula in the gascity or gc-toolkit trees
references `--reassign`** (`grep -rn '\-\-reassign' rigs/gc-toolkit/
docs/ examples/` returns no hits outside test files). The flag exists
but is undiscoverable at the prompt layer.

## 3. Cases survey

Patterns surveyed across both repos:

```bash
git -C rigs/gascity grep -nE 'set-metadata.*gc\.routed_to' -- '*.md' '*.toml'
git -C rigs/gascity grep -nE 'assignee.*=.*""' -- '*.md' '*.toml'
git -C rigs/gc-toolkit grep -nE 'requesting_keeper' -- '*.md' '*.toml'
git -C rigs/gc-toolkit grep -nE 'pending_rework|pending_review|re-pour' -- '*.md' '*.toml'
```

Workflows with handback-shaped lifecycle (sets `gc.routed_to=<other-agent>`
on the parent bead, drains, expects a future agent to pick the bead back
up):

| Formula | Re-enters same pool? | Multiple cycles? | Needs the bypass? |
| --- | --- | --- | --- |
| `mol-upstream-gc-rebase` | Yes (polecat → keeper → polecat → ...) | Yes (one cycle per conflict commit) | **Yes** |
| `mol-upstream-gc-pr-prep` | No (polecat → keeper, terminal) | No | No |
| `mol-polecat-work` | No (polecat → refinery, terminal) | No | No |
| `mol-upstream-gc-sync` | No (polecat → keeper, terminal) | No | No |
| Witness/Deacon patrols | N/A (routes other beads, doesn't re-pour) | No | No |

The `mol-upstream-gc-rebase-rework` formula (`formulas/mol-upstream-gc-rebase-rework.toml`)
is the child wisp dispatched by the rebase parent; it terminates when the
rework polecat closes its leg. The parent rebase loop is the re-pour
case.

**Only `mol-upstream-gc-rebase` exhibits the multi-cycle handback
pattern.** Three other formulas hand off, but each is a one-way pipeline
stage with a terminal recipient (keeper for conversation, refinery for
merge). They never need to re-enter a polecat pool after handback. This
is consistent with the survey rationale in keeper prompt lines 528-544.

So the structural gap appears in exactly one production workflow today.
A second case is on the horizon — any future "supervisor inspects, then
returns to worker" pattern (e.g., a deacon-supervised batch-rework
flow, or human-in-the-loop code review with iterative refinement) would
hit the same gap. But shipping today the survey shows **N=1**.

## 4. Design alternatives

Each option below is evaluated on: API surface, implementation surface,
ecosystem fit (does it match `gc sling`'s existing CLI shape), other
workflows affected, routing-surface coherence, backward compatibility,
misuse surface, and what the upstream PR would look like.

### A. `gc sling --route-self <pool> <bead>` (force bare-bead routing, ignore type)

**Out of scope.** The engine already routes the parent for non-convoy
types (per 2a, confirmed by
`TestDoSlingBatchRoutesNonContainerFoundInQuerierStore`). The "child
expansion" symptom is a CLI dry-run display defect (2f), not an engine
routing defect. A `--route-self` flag would either be a no-op for tasks
(adds surface without behavior change) or would have to do something
new (e.g., refuse expansion even for convoys), which is unrelated to the
re-pour case. Rejected.

### B. `gc sling --re-pour <pool> <bead>` (purpose-built flag)

**API surface**: One new boolean flag. Implied semantics: clear assignee,
preserve existing attached molecule, set `gc.routed_to`. Mutually
exclusive with `--on`.

**Implementation surface**: `cmd/gc/cmd_sling.go` (flag definition,
plumbing through `slingOpts`), `internal/sling/sling.go` (opt field),
`internal/sling/sling_core.go` (preflight branch: clear assignee like
`--reassign`; skip `CheckNoMoleculeChildren` even if a default formula
is configured). ~80 lines + tests.

**Ecosystem fit**: New verb-like flag. `gc sling`'s existing flags are
**modifiers**, not modes (`--force`, `--reassign`, `--nudge`,
`--no-convoy`, `--owned`, `--dry-run`, `--no-formula`, `--var`,
`--scope-kind`, `--scope-ref`). `--re-pour` would be the first
mode-flag. Slight cohesion loss.

**Other workflows affected**: None today (N=1 per cases survey). Future
multi-cycle handback workflows would get a documented seam.

**Routing-surface coherence**: Preserves "everything routes through
sling." Strong.

**Backward compatibility**: New flag, no break.

**Misuse surface**: `--re-pour` invoked on a bead that has *no* attached
molecule degenerates into `--reassign` semantically — confusing. Need
either an error ("nothing to re-pour") or silent degeneration.

**Upstream PR**: Tightly scoped, narrowly motivated. Reviewers may
push back: "why is this a flag and not just a doc fix?"

### C. Make `--on <same-attached-mol>` idempotent (non-graph parity with workflow `--force`)

**API surface**: No new flag. Existing `--on <mol>` semantics change:
when the bead has an attached molecule **of the same formula**, the call
becomes a no-op on attachment and proceeds with routing. Different-formula
attempts still error.

**Implementation surface**: `internal/sling/sling_core.go:225-227` —
the `checkAttachments` call gets a same-formula short-circuit before
returning the error. ~20 lines + tests.

```go
// sketch
checkAttachments := CheckNoMoleculeChildren
if isGraph && opts.Force {
    checkAttachments = CheckNoMoleculeChildrenAllowLiveWorkflow
}
if err := checkAttachments(querier, beadID, deps.Store, &result); err != nil {
    if attached, ok := SameFormulaAttachment(deps.Store, beadID, opts.OnFormula); ok {
        result.WispRootID = attached.ID
        result.Reattached = true
        // skip InstantiateSlingFormula and SetMetadata; reuse existing wisp
        return finalize(opts, deps, beadID, "on-formula", result)
    }
    return result, fmt.Errorf("%w", err)
}
```

Same-formula detection: compare the attached wisp's recorded formula
name (stored on the root step bead) against `opts.OnFormula`.

**Ecosystem fit**: Existing `gc sling X Y` is already idempotent on the
"already routed" case (`sling_core.go:97-107`: `CheckBeadState` →
`result.Idempotent`). Treating same-formula `--on` as idempotent on the
attachment marker is the symmetric move. Strong fit.

**Other workflows affected**: Any flow that re-attaches the same formula
becomes legal. Different-formula attaches still error (preserves the
guarantee that two distinct workflows don't fight over a bead).

**Routing-surface coherence**: Preserves "everything routes through
sling." Best fit among options.

**Backward compatibility**: Strict relaxation. Today's error case becomes
tomorrow's success case. No prior caller depended on the error to detect
"already attached." (Verified: no test in the gascity or gc-toolkit tree
asserts `--on` errors for re-attach as a feature; the only such test
exists in `TestOnFormulaExistingMoleculeErrors` as a guard against
silent corruption, which the same-formula short-circuit doesn't risk.)

**Misuse surface**: An operator who wants to **replace** an attached
molecule with the same formula (e.g., to re-instantiate with new vars)
would now silently re-route the old wisp instead. Mitigated by emitting a
clear `result.Reattached` warning ("re-attached existing wisp X; pass
--force to burn and re-instantiate" — once we've decided what `--force`
should mean here).

**Upstream PR**: Clear scope, motivated by visible asymmetry with the
graph workflow path, fits existing flag surface. Strong PR candidate.

### D. Make `--force` bypass non-graph "already attached" (full symmetry with workflow)

**API surface**: Same `--force` flag, broader effect: for non-graph
molecules, `--force` switches to a permissive check (`burn and
re-attach` or `treat as no-op`).

**Implementation surface**: `internal/sling/sling_core.go:221-224` —
drop the `isGraph &&` guard on the permissive path. ~5 lines change,
plus careful test coverage for the burn/re-attach semantic.

**Ecosystem fit**: Makes `--force` symmetric between graph workflows
and legacy molecules. The asymmetry today is a Chesterton's-fence
question (was non-graph deliberately not forceable, or just incidental?).
The keeper prompt explicitly notes the asymmetry as a bug:
"`bead <id> already has attached molecule <mol-id>` (also rejected with
`--force`)" (gc-toolkit `847ec50` commit message).

**Other workflows affected**: Any non-graph `--on` invocation gains a
new escape hatch.

**Routing-surface coherence**: Strong — `--force` is already the
"override safety checks" lever.

**Backward compatibility**: Strict relaxation, no prior caller depended
on `--force` failing here.

**Misuse surface**: Higher than C, lower than B. `--force` is already
the "I know what I'm doing" flag; broadening its scope is in keeping
with that. Auto-burn of an in-flight molecule could lose audit data —
needs explicit policy: error if attached molecule has open step
children with state, allow only if quiescent.

**Upstream PR**: Smaller diff than C, but requires answering "what does
re-attach mean — replace or re-pour?" The replace semantic loses
the rebase formula's accumulated `conflict_resolutions` audit log,
which is exactly what the keeper needs to preserve.

### E. Formula handback writes the pool-routed shape directly (no engine change)

**API surface**: No CLI change. The rebase formula's handback step writes
`assignee=""` and `gc.routed_to=<polecat-pool>` directly, bypassing the
keeper. The keeper becomes purely a notifier / inspector with no routing
responsibility.

**Implementation surface**: Single-file TOML edit in
`formulas/mol-upstream-gc-rebase.toml`. Zero gascity source change.

**Ecosystem fit**: Pushes state-machine responsibility into the formula
instead of an agent prompt. Aligns with "ZFC" principle (no judgment in
Go, no judgment in prompts either — formula expresses the state machine).

**Other workflows affected**: None directly; pattern can be reused by
future multi-cycle workflows. But this is a per-formula fix, not a
primitive.

**Routing-surface coherence**: Breaks "everything routes through sling"
— the formula writes routing metadata directly, just as the keeper
workaround does today. Same structural objection.

**Backward compatibility**: Trivial; only the gc-toolkit pack changes.

**Misuse surface**: Now every formula author who needs multi-cycle
handback has to know the controller demand predicate by heart and
hand-roll the metadata. Bakes the same implementation knowledge into
every formula author's vocabulary instead of into the keeper prompt.
Worse than the status quo, not better.

**Upstream PR**: N/A (this is a gc-toolkit-internal fix, not a gascity
PR). Listed for completeness; recommended against.

### F. `gc bd repour <bead>` (first-class verb)

**API surface**: New `gc bd repour` command. Inputs: bead ID; optional
target pool override. Behavior: read `metadata.molecule_id`, look up the
attached wisp's recorded target pool, clear assignee, set
`gc.routed_to`. Optionally bump a `repour_count`.

**Implementation surface**: New CLI command (`cmd/gc/cmd_bd_repour.go`),
new domain function (`internal/sling/repour.go`). ~150 lines + tests.

**Ecosystem fit**: Adds a new verb to the routing surface. `gc bd` is
the bead-CRUD layer; `gc sling` is the routing layer. Re-pour is a
routing operation (it writes `gc.routed_to`), so philosophically it
belongs under `gc sling`, not `gc bd`. Cohesion loss.

**Other workflows affected**: Any future multi-cycle workflow gets a
clean verb. But the verb is mostly sugar over what option B or C
already give us.

**Routing-surface coherence**: Splits routing across two verbs (`sling`
and `repour`), weakening "everything routes through sling."

**Backward compatibility**: New verb, no break.

**Misuse surface**: Lower than B (intent is explicit in the verb name)
but introduces a second routing entry point. Two ways to do the same
thing.

**Upstream PR**: Larger diff, more surface, less coherent. Weak PR
candidate.

### G. Documentation-only: teach the keeper to use `--reassign`

**API surface**: None.

**Implementation surface**: gc-toolkit pack only. Replace the keeper
prompt's "bare sling + `gc bd update --assignee ""`" with the
one-liner `gc sling --reassign polecat <bead>`. Drop the dropped
`--var requesting_keeper` (already no-op).

**Ecosystem fit**: Maximal — uses existing flags, no engine change.

**Other workflows affected**: None.

**Routing-surface coherence**: Strong — all routing remains in sling.

**Backward compatibility**: N/A.

**Misuse surface**: The keeper still owns rebase state-machine
knowledge. Future multi-cycle workflows still need to know `--reassign`
is the canonical lever. The dry-run misleading output (2f) still
misleads anyone who tries to verify the pattern. Solves the surface
symptom, leaves the underlying defect untouched.

**Upstream PR**: N/A (gc-toolkit-only doc edit).

## 5. Recommendation

**Hybrid: ship two upstream gascity PRs and one gc-toolkit prompt PR.**

### Recommended upstream change 1 (gascity PR, blocking)

**Fix the dry-run display defect (2f)**. `cmd/gc/cmd_sling.go:850-866`
should consult the engine's actual routing decision before rendering
the batch preview. For non-container beads, render `dryRunSingle` even
if children exist.

**Proposed implementation**:

```go
// cmd/gc/cmd_sling.go (replace existing dry-run block in doSlingBatch)
if result.DryRun {
    if querier != nil {
        if b, getErr := querier.Get(opts.BeadOrFormula); getErr == nil && beads.IsContainerType(b.Type) {
            children, _ := querier.List(beads.ListQuery{
                ParentID: b.ID, IncludeClosed: true, Sort: beads.SortCreatedAsc,
            })
            var open []beads.Bead
            for _, c := range children {
                if c.Status == "open" { open = append(open, c) }
            }
            return dryRunBatch(opts, deps, stdout, stderr, b, children, open, querier)
        }
    }
    return dryRunSingle(opts, deps, querier, stdout, stderr)
}
```

The `beads.IsContainerType(b.Type)` gate matches the engine's
decision at `internal/sling/sling_core.go:858-875`. Non-container beads
render `dryRunSingle` regardless of whether they have child beads.

**Test plan**:
- Unit: extend `TestDoSlingDryRun` (`internal/sling/sling_test.go:2797`)
  with a `Type: "task"` bead that has open child beads. Assert the
  dry-run output contains the parent-route command line and does not
  contain "Expanding container".
- CLI: `cmd/gc/cmd_sling_test.go` — add a CLI-level dry-run test for
  the same task-with-children case, asserting stdout matches the
  `dryRunSingle` template.

**Why this matters**: The codex review of PR #18 was correct *given the
dry-run output it saw*, but the dry-run output was lying. Fix the lie
and the supposedly-broken workaround is revealed to work. The
keeper-prompt churn across `99b2d76`, `a6242bc`, and `847ec50` is
traceable directly to this defect.

### Recommended upstream change 2 (gascity PR, follow-on)

**Make `--on <same-attached-formula>` idempotent for non-graph
molecules** — design Option C above.

**Proposed implementation**:

```go
// internal/sling/sling_core.go (inside slingOnFormula, after current
// checkAttachments error path at lines 225-227):
if err := checkAttachments(querier, beadID, deps.Store, &result); err != nil {
    if attached, ok := sameFormulaAttachment(deps.Store, beadID, opts.OnFormula); ok {
        result.WispRootID = attached.ID
        result.Reattached = true
        result.FormulaName = opts.OnFormula
        return finalize(opts, deps, beadID, "on-formula", result)
    }
    return result, fmt.Errorf("%w", err)
}
```

Where `sameFormulaAttachment` (new helper in `sling_attachment.go`)
looks up `metadata.molecule_id` on the parent, reads the recorded
formula name from the wisp's root step bead, and returns the wisp
record if it matches `opts.OnFormula`. Returns `(_, false)` for any
other shape, preserving today's error path.

Add a `Reattached bool` field to `SlingResult` and a
`Re-attached existing wisp %s (formula %q) on %s` line to
`printSlingResult` (`cmd/gc/cmd_sling.go:644-650`) so the operator
sees the re-pour explicitly.

**Test plan**:
- New unit: `TestOnFormulaSameAttachedFormulaIsIdempotent`
  (`internal/sling/sling_test.go`) — create a bead with an attached
  wisp of formula F, call `--on F` again, assert
  `result.Reattached == true`, `result.WispRootID == original`,
  bead metadata unchanged except for routing.
- New unit: `TestOnFormulaDifferentAttachedFormulaErrors`
  — create with formula F, call `--on G`, assert error path
  preserved.
- CLI: `TestOnFormulaReattachDisplayMessage`
  (`cmd/gc/cmd_sling_test.go`) — assert stdout matches the
  re-attached message.

**Why this matters**: Removes the asymmetry between graph workflows
(which accept `--force` to supersede an existing attachment) and
non-graph molecules (which do not). Lets the keeper invoke
`gc sling --reassign polecat <bead> --on mol-upstream-gc-rebase` as a
single, semantically-explicit command. The rebase formula's
accumulated state (`metadata.conflict_resolutions`, `pending_rework`,
etc.) is preserved because the existing wisp is reused, not
re-instantiated.

### Recommended downstream change (gc-toolkit prompt PR)

After both upstream PRs land, replace `agents/gascity-keeper/prompt.template.md`
lines 514-544 with:

```bash
gc sling --reassign gascity/gc-toolkit.polecat <bead> \
  --on mol-upstream-gc-rebase
```

The single line replaces the current two-command form plus its rationale
block. The mol stays attached and is re-poured idempotently; assignee is
cleared in preflight; `gc.routed_to` is set in `finalize`. No
implementation knowledge of the controller demand predicate remains in
the prompt.

### Migration story

| Consumer | Before | After |
| --- | --- | --- |
| `gascity-keeper` prompt | Two commands + rationale block | One line; rationale removed |
| Operator running `gc sling --dry-run` on task | Misleading "Expanding container" | Accurate single-route preview |
| Future multi-cycle handback formulas | Must hand-roll the demand-predicate shape | Use `--reassign --on <fmla>` directly |
| Existing `--on` callers (non-graph, attached) | Error | Error if formula differs; idempotent re-pour if same |

### Documentation updates needed in upstream gascity

- `docs/reference/cli.md` (or wherever `gc sling` is documented) —
  document `--reassign` semantics, the new "re-attach is idempotent on
  same formula" behavior, and the corrected dry-run output.
- A short example in `engdocs/architecture/` (or equivalent) covering
  the multi-cycle handback pattern: drain → external trigger → re-pour
  with `--reassign --on <fmla>`.

## 6. Upstream-PR readiness assessment

**Change 1 (dry-run fix): READY FOR UPSTREAM.**

Self-contained bug fix in `cmd/gc/cmd_sling.go`. The defect, the fix,
and the test coverage are all clear. The engine behavior the dry-run
should reflect is already enforced by
`TestDoSlingBatchRoutesNonContainerFoundInQuerierStore`. Reviewer
discussion would focus on style (whether to add a regression test that
exercises the exact reported `gc-j8e7j` / `gc-q5q3u` shape).

**Change 2 (idempotent re-attach): NEEDS DESIGN REVIEW.**

The scope is clear; the open question for maintainers is whether the
symmetric move is Option C (no flag, same-formula auto-idempotent) or
Option D (`--force` bypasses non-graph "already attached", with auto-burn
on different-formula). The choice depends on a Gas City policy decision
about what a non-graph molecule re-attach **means** in general — re-pour
or replace. The keeper's case is unambiguously re-pour (preserve audit
log), but the maintainer may prefer the more explicit Option D for the
"replace" case and reserve Option C for explicit re-pour intent.

Specific question to ask the maintainer:

> Today, `gc sling --on <fmla> <bead>` against a bead with an open
> non-graph attachment errors unconditionally. `--force` bypasses the
> same check for graph workflows (under a source-workflow lock).
> Should non-graph parity be (a) silent same-formula re-pour with
> different-formula still erroring, or (b) `--force` enables
> replace-style auto-burn + re-attach uniformly?

The cases survey (N=1, rebase only) suggests Option C is the smaller
move with adequate generality. If the maintainer wants both behaviors,
they compose: ship C, plus broaden `--force` later if a `mol-replace`
use case shows up.

## 7. Open questions

1. **Wisp's recorded formula name.** Option C requires a stable read of
   "what formula instantiated this wisp." The instantiation path stores
   it on the wisp root, but the exact metadata key needs confirmation
   from `internal/molecule/` — out of scope for this proposal.
2. **`--var` on plain-bead route.** Separately from re-pour, the silent
   drop of `--var` values on plain-bead routes is a footgun
   (2d). Should `gc sling` warn or error when `--var` is passed but no
   formula path will consume it? Worth a follow-on bead.
3. **Dry-run for `--on` re-pour.** After Change 2, the dry-run should
   show "would re-attach existing wisp X" instead of the current
   "would error (already attached)". The dry-run preview path in
   `dryRunSingle` and `dryRunReportBlockingMolecule`
   (`cmd/gc/cmd_sling.go:1465-1599`) needs a corresponding update.
4. **Multi-cycle handback in other workflows.** N=1 today, but if
   gc-toolkit lands an inspector/reviewer-supervised batch-rework
   formula or human-in-the-loop refinement, the re-pour pattern will
   become N=2+. Worth a periodic survey to confirm the structural fix
   stays load-bearing.
5. **Crash-recovery interaction.** A polecat that died mid-claim could
   leave the bead at `status=in_progress` with assignee set. The
   scale-check excludes `in_progress` beads. Today the rebase formula's
   handback resets `status=open`, so this is not in scope. But a future
   re-pour primitive should document its interaction with crash-recovery
   explicitly.
