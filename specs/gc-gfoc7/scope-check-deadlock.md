---
name: The graph.v2 submit scope-check strand is an ordering cycle, not a discovery failure (gc-gfoc7)
description: The final step's scope-check bead is simultaneously the bead that closes the scope body and the body's last remaining blocker. processScopeCheck converged the scope before closing itself, so the body close was refused, the control bead never closed, and the dispatcher retried forever — ~240k logged failures across two workflows. Records the evidence trail, why only the submit step showed it, why CI could not catch it, and the two framings this disproves.
---

# gc-gfoc7: the scope-check that blocks itself

## Verdict

**An ordering cycle inside `processScopeCheck`.** Not a discovery failure, not
a slow dispatcher tick, and not "blocked-by-design" residue.

The scope body carries a `blocks` dependency on *every* step's scope-check
control bead. The final scope-check is therefore both the bead that closes the
body and the last blocker standing in the body's way. `processScopeCheck`
converged the scope first and closed itself second, so the body close was
refused by the store and the error returned *before* the control bead was ever
closed.

## The evidence trail

Two hypotheses were ruled out before the real one was found. Both are recorded
because both are the natural first guess.

### Ruled out 1: discovery

The control-ready query (`workflowServeControlReadyQueryForBeads` in
`cmd/gc/dispatch_runtime.go`) finds control beads two ways: `bd ready
--assignee=<control identity>`, or `bd ready --metadata-field
gc.routed_to=<route> --unassigned`. The stranded beads satisfy the second:

    tk-q7qnv  status=open  assignee=null  gc.routed_to=gc-toolkit/core.control-dispatcher

`bd ready` genuinely did not return them, which looked like proof. It was a
red herring — bare `bd` in the rig directory resolves a different database than
`gc bd` does, so that probe was reading the wrong store. The dispatcher was
finding these beads the whole time.

### Ruled out 2: metadata shape

A stranded submit scope-check and its four closed siblings are byte-identical
in every field that routing or dispatch consults — same `gc.kind`, same
`gc.routed_to`, same `gc.scope_ref`, same unassigned state, same dependency
shape (one closed `blocks`, one `tracks` to an in-progress root). Nothing
distinguishes them structurally.

### What actually settled it: the trace

`gc-toolkit--core.control-dispatcher-trace.log` carried **1,115,351** lines
mentioning the two stranded beads, and **239,034** occurrences of `cannot close
blocked issue`. The dispatcher was processing the bead on every tick and failing
identically every time:

    scope-check bead=tk-q7qnv phase=close-body err=updating bead "tk-smof8":
      cannot close blocked issue: tk-smof8 is blocked by [tk-q7qnv]
    serve transient-error-pending bead=tk-q7qnv kind=scope-check

`tk-smof8` is the body scope. Its blockers:

    tk-15gq0 blocks closed     tk-d31f9 blocks closed
    tk-7trft blocks closed     tk-58ugq blocks closed
    tk-q7qnv blocks OPEN   <-- the bead trying to close tk-smof8

The error was classified transient, so it retried forever. Confirmed on a
second workflow: `tk-yak8x is blocked by [tk-gey83]`.

## Why only the `submit` step

For every step before the last, `hasOpenScopeMembers` still reports open
members, which takes the `Action: "continue"` branch — that branch closes the
control bead and never touches the body. Only the final step reaches
`closeScopeAsPassed`, and only that call hits the cycle.

This is exactly the distribution recorded in the filing: 6 closed each for
`implement`, `preflight-tests`, `self-review`, `workspace-setup`, against 4 for
`submit`. The skew was the fingerprint of the mechanism, not a sampling
artifact.

## Why CI could not catch it

`internal/dispatch` tests run against `beads.MemStore`, which closes a blocked
bead happily. The production guard lives in `bd`.

`TestProcessScopeCheckClosesScopeOnSuccess` already builds the exact production
graph, including `mustDepAdd(t, store, body.ID, control.ID, "blocks")`. It
passed because the fake store is more permissive than production.

A `strictCloseStore` mirroring bd's guard already existed in the test file — and
was referenced by **zero tests**. It also only guarded `Close()`, while
`updateMetadataAndClose` closes via `Update(Status: "closed")` and only falls
back to `Close()` if the bead is still open. So even wired up, it would have
missed this.

Both gaps are now closed: `strictCloseStore` guards the update path, and
`TestProcessScopeCheckClosesScopeWhenBodyBlocksOnControl` runs the existing
success graph against it. On the pre-fix tree that test fails with the exact
production error string.

## The fix

Close the control bead *before* calling `closeScopeAsPassed` / `abortScope`, in
all three terminal branches of `processScopeCheck`.

This is not a new pattern — it is the one every other terminal path in the
dispatcher already uses (`processRetryControl`, `recordControllerSpawnError`):
close your own bead, then reconcile the enclosing scope. `processScopeCheck`
was the sole outlier.

## Two framings this disproves

**"Control stays open so the dispatcher can retry body closeout."** This was
asserted by `TestProcessScopeCheckKeepsControlOpenIfBodyCloseoutFails`. The
invariant is unsatisfiable: holding the control bead open is precisely what
makes every retry fail. The test is renamed to
`TestProcessScopeCheckSurfacesBodyCloseoutFailure` and now pins that a genuine
closeout failure is surfaced rather than retried forever.

**"Blocked-by-design residue — do not recycle the dispatcher"** (gc-spa04).
Half right. Recycling the dispatcher does fix nothing, but not because the
residue is expected — because the dispatcher is correctly reporting a close it
cannot perform. The residue is a real, growing defect: open `Finalize workflow`
beads went 19 → 20 between 04:02Z and 15:55Z on 2026-08-23.

## Upstream

`upstream/main` carries the identical ordering in both scope-pass branches. This
is an upstream defect, not fork-local; `gc-gfoc7` is tagged
`upstream_pr_candidate`.

## Not fixed here

The fix helps **new** workflows only. Recovering the ~20 already-stranded ones,
and adding a durable convergence sweep for the case where body close fails
*after* the control bead closes, is filed as **gc-oqcxw**.

## Counting trap

`scope-check` beads do not appear in `bd list` at all — filtering a full listing
for them returns a **false zero** while `gc bd show` resolves them fine. Size
this class by walking workflow roots, never by listing.
