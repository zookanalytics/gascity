---
name: Pool polecat orphan drain — the guard fires; the deadlock is a store-blind release (gc-rxqp7)
description: The reported "assigned-work guard does not fire" is not reproducible on current main — the guard is present, fail-closed, and fans out over every rig store. The unrecoverable half of the incident has a different, localized root cause: the close-release path is single-store, and its documented fallback skips unrouted work.
---

# gc-rxqp7: the guard fires — the stranding is a store-blind release

## Verdict

The bug report's stated mechanism ("the assigned-work guard at
`session_reconciler.go:2106` is not firing for pool polecats") **is not
reproducible against current `main` (`41230df1e`).** Two regression tests
covering the exact acceptance criteria — including the cross-rig naming case
the report flags — pass on unmodified code.

What *is* localized here is the second half of the incident: why the drained
work became **unrecoverable** rather than merely retried. That has a concrete
root cause, and it is not the guard.

## What was verified about the guard

The orphan drain at `cmd/gc/session_reconciler.go:2106` calls
`sessionHasOpenAssignedWorkForConfigInfo(store, rigStores, infoByID[id], cfg)`.
Every link in that chain was read and checked:

- **Identifiers** — `sessionAssignmentIdentifiersForConfigInfo`
  (`cmd/gc/session_beads.go:837`) returns
  `{Info.ID, Info.SessionNameMetadata, Info.ConfiguredNamedIdentity}`. The
  reconciler's own `name` (the string in the `Draining session '...'` log) is
  `strings.TrimSpace(info.SessionNameMetadata)` (`session_reconciler.go:1618`) —
  the *same field*. So whatever name appears in the drain log is by
  construction in the identifier set, and the incident's work bead carried
  exactly that assignee (`gc-toolkit__polecat-lx-7vqe`).
- **Store fan-out** — `sessionHasOpenAssignedWorkInStores`
  (`session_reconciler.go:4675`) probes the primary store **and every entry in
  `rigStores`**, so a cross-rig pool session is covered. `rigStores` is the
  same map the desired-state builder uses (`city_runtime.go:2240`, passed to
  both `ComputeDesiredState` and the reconciler at `city_runtime.go:2388`).
- **Tier** — both tiers are probed: `TierIssues` via
  `sessionHasOpenAssignedWorkForTier`, then `TierWisps` via
  `sessionHasOpenAssignedWispWork` (`session_reconciler.go:4753`), whose cache
  fast-path is positive-only and correctly falls through to a live query.
  Confirmed empirically that graph.v2 step beads are issue-tier
  (`ephemeral` unset), so the issues probe alone suffices.
- **`Live: true`** is only a `CachingStore` bypass (`internal/beads/query.go:94`),
  not a lease or readiness filter.
- **Fail-closed on error** — a store error `continue`s past the drain rather
  than draining (`session_reconciler.go:2107-2110`), and a partial store query
  skips the drain outright (`session_reconciler.go:2071`).
- **Deployed binary** — `/home/zook/go/bin/gc` contains both
  `live assigned work found` and `pool session within spawn-claim grace`, so
  the guard and the gc-yi1ig grace were both present in the running controller.

Two new tests in `cmd/gc/session_reconciler_test.go` pin this behavior:

| Test | Arm |
|---|---|
| `TestReconcileSessionBeads_WorkingPoolWorkerSurvivesOrphanDrainPastGrace` | pool worker holding `in_progress` work, 10 min past the grace, not in the desired set → must not drain |
| `TestReconcileSessionBeads_WorkingCrossRigPoolWorkerSurvivesOrphanDrainPastGrace` | same, but the work bead lives in a **rig** store while the session bead lives in the city store |

The "holds nothing still drains" arm is already covered by the second half of
`TestReconcileSessionBeads_FreshPoolWorkerSurvivesOrphanDrainWithinGrace`.

Both new tests pass on unmodified `main`. **They are regression guards, not a
reproduction.**

## The localized defect: why the work became unrecoverable

The incident's lasting damage was not the drain — pool work is meant to be
retried — it was that `sl-6q2h` was left `status=in_progress`, assignee
cleared, `gc.routed_to` unset, generating no demand. Two independent gaps
combine to produce exactly that terminal state:

1. **The close-release path is single-store.**
   `closeBead` (`cmd/gc/session_beads.go:2956`) calls
   `releaseWorkFromClosedSessionBead(store, snapshot, stderr)`
   (`session_beads.go:3004`) with **one** store and no `rigStores`. That
   function is the thing that clears the assignee, resets `in_progress` → `open`,
   and applies `retiredSessionFallbackRoute` so the work stays discoverable
   (the ga-n2d.2 fix, `session_beads.go:3062-3081`). For a pool session whose
   session bead lives in the city store and whose work lives in a **rig**
   store, it queries the wrong store, finds nothing, and none of that recovery
   runs.

2. **The documented fallback skips unrouted work.**
   `releaseWorkFromClosedSessionBead`'s own doc comment names
   `releaseOrphanedPoolAssignments` as "our idempotent fallback". That fallback
   bails at `cmd/gc/pool_session_name.go:160-163`:

   ```go
   template := routedToOrLegacyWorkflowTarget(wb)
   if template == "" {
       continue
   }
   ```

   graph.v2 step beads carry an **empty `gc.routed_to`** — the report notes this
   itself ("Note the step beads carry an EMPTY gc.routed_to throughout") — so
   the fallback skips them. It needs the route to resolve the agent config and
   check `SupportsGenericEphemeralSessions()`, and with no route it cannot.

Net effect: primary recovery can't see the bead (wrong store), fallback
recovery won't touch it (no route). The work is stranded permanently, and the
parent workflow stays `in_progress` forever — precisely the reported deadlock.

## Recommended fix (not applied here)

The minimal, asymmetry-closing change is to give the release path the same
store breadth the *guard* already has: thread `rigStores` into `closeBead` →
`releaseWorkFromClosedSessionBead` and iterate `workAssignmentStores(store,
rigStores)`. The recovery logic itself already exists and is already correct —
it is simply pointed at the wrong store. That the read-side guard fans out
while the write-side release does not is an oversight, not a design choice.

It was deliberately **not** applied on this pass: `closeBead` has nine
non-test call sites, several without `rigStores` in scope, and it sits on a
hot fail-closed session-lifecycle path. It is filed as **gc-d9qnh** so it gets
the full process/integration tier run rather than a rushed tail-end diff.

The alternative — relaxing the `template == ""` skip in
`releaseOrphanedPoolAssignments` — is **not** recommended: without a route
there is no way to confirm the bead belongs to an ephemeral pool agent, so a
blanket release risks reopening work held by a live named session, which is a
worse failure than the one being fixed.

## What the drain half still needs

The guard is sound in-process, so if the 08:02–08:08 drain is real, the false
negative came from an input the unit harness cannot supply — most plausibly a
successful-but-empty assigned-work read from the live store (a partial read
would have been caught by the `storeQueryPartial` skip). The report's own
timeline hints at this: the `assignedWorkBeads` diagnostic line is described as
repeating "through the last one **before** the drain", i.e. the bead was
absent from the collection on the drain tick itself.

To settle it, catch it live rather than by inspection:

```bash
gc trace start <rig>/<polecat-template> --detail
# then, in a controlled window, revert session.startup_timeout 30m -> 5m
gc trace show --reason orphaned --json
```

The orphan drain arm records `TraceSiteReconcilerOrphaned` with
`live_assigned_work` / `spawn_claim_grace` in its payload, so a single captured
cycle distinguishes "guard evaluated false" from "guard never reached".

**Until that trace exists, keep the `startup_timeout` 30m tourniquet in
place.** Nothing in this pass justifies reverting it to 5m.
