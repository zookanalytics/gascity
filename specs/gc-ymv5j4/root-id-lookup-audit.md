---
name: Root-id lookups that read closed history, caller audit and live measurements (gc-ymv5j4)
description: Which gascity callers list a workflow's beads by gc.root_bead_id with closed beads included, which of them use the closed rows, what gc-ymv5j4 changed (the order open-work guard and the spec-sidecar close now exclude closed beads), and the live Dolt measurements behind it. Read before changing a gc.root_bead_id lookup or re-profiling metadata-scan cost.
---

# gc-ymv5j4: root-id lookups that read closed history

## Verdict

A Dolt-backed store answers `ListQuery{Metadata: {gc.root_bead_id: X}}` with a
`JSON_EXTRACT(metadata, ...)` predicate. When the query includes closed beads
there is no status predicate, so Dolt reads the metadata of every row. When it
excludes them, the `status NOT IN ('closed')` predicate limits that read to the
non-closed rows. On the tk store (42,334 issues, 41,158 of them closed) the
lookup took 3 to 17 seconds with closed beads included and under 2 seconds
without them, timed side by side on the same roots.

Two callers fetched closed rows only to discard them in Go, on recurring
paths. Both now exclude closed beads, and their results are unchanged:

- `storeOpenDescendantIDs` (`cmd/gc/order_dispatch.go`). It answers the order
  open-work gate, which has an 8 second budget per order. It is also the guard
  in front of every single-bead workflow delete, so the order-tracking
  retention sweep runs it once for each closed tracking bead it deletes. The
  core `order-tracking-sweep` order runs that sweep every minute, and the
  controller's retention watchdog runs it every 15 minutes.
- `sourceworkflow.CloseSpecSidecarsForRoot`. It runs on every workflow
  finalize, on source-bead and wisp autoclose, and in the wisp GC's sidecar
  repair.

Excluding closed beads in the query returns the same beads to both callers on
any store that excludes exactly the `closed` status. The native Dolt store
does exactly that, and `gc` opens it for a bd-provider store by default
(`beads.native_transport` unset or `auto`, once the store passes preflight).
The `bd` CLI store passes no `--all`, and `bd list` then also omits pinned
beads and custom statuses in the done or frozen categories. None of the six
live stores had a pinned workflow member or any custom status on 2026-10-09,
so the results match there as well.

Every other all-status root-id lookup feeds a caller that uses the closed rows:
it emits completion facts for them, deletes them, walks through them, or shows
them. Excluding closed beads there would change behavior. The largest live one
is the completion backstop, filed as gc-0mue2i. The control dispatcher's
`DirectMembers` callers are filed as gc-ho0oag. The schema-level remedy, a
functional index on hot metadata keys such as `gc.root_bead_id`, is filed as
gc-a75mwa.

## Measurements

All measurements ran read-only against the live city Dolt server on
2026-10-09, through the `dolt` client, with its connection overhead (the time of
`select 1`, 0.10 to 0.24 seconds) subtracted. Host load was 40 to 75 throughout.

### Query time

The query is the predicate the store emits for `gc.root_bead_id = <root>`, run
with and without `status NOT IN ('closed')`.

| When | Store and table | Roots, runs | Closed included | Closed excluded |
|---|---|---|---|---|
| 15:58Z | tk issues | 1 in-progress root, 3 runs | 13.5 to 17.2 s | 1.1 to 1.9 s |
| 16:16Z | tk issues | 3 roots, 9 runs | median 4.46 s, max 7.42 s | median 0.07 s, max 0.46 s |
| 16:30Z | tk issues | 2 roots, 6 runs | median 3.00 s | median 0.11 s |
| 16:30Z | tk wisps (66 rows) | 2 roots, 6 runs | median 0.03 s | median 0.05 s |
| 16:30Z | lx issues (7,116 rows) | 1 root, 3 runs | median 0.17 s | median 0.00 s |
| 16:30Z | lx wisps (3,614 rows) | 1 root, 3 runs | median 0.07 s | median 0.11 s |

The 15:58Z row is raw wall time, before overhead subtraction. For that
in-progress root both shapes returned the same 9 members, all of them open. A
both-tier lookup reads the issues and the wisps table, so on tk the issues leg
is nearly the whole cost.

### Share of in-flight queries

`information_schema.processlist` was sampled every 3 seconds, keeping every
non-sleeping query. A query that runs across several samples is counted once
per sample, so the share approximates the fraction of Dolt's busy time.

| Window | Samples | In-flight queries | Root-id, closed included | Root-id, closed excluded |
|---|---|---|---|---|
| A: 15:58 to 16:01Z | 60 | 508 | 118 (23.2%) | 1 (0.2%) |
| B: 16:22 to 16:33Z | 200 | 1,390 | 25 (1.8%) | 0 |

Each sampled lookup carries its root id as a literal, so it can be attributed
by what that root is.

- Window B read each root at sample time. 19 of the 25 were closed order-run
  beads without a `gc.kind` (12 on lx, 4 on tk, 2 on gc, 1 on sl), and none of
  the 19 existed five minutes later. That is the retention sweep's guard
  followed by its delete. 3 were two closed lx order wisps, 1 was an
  in-progress lx bead, and 2 were ids with another store's prefix, looked up
  in lx and absent from it.
- Window A was attributed afterward. 63 of the 118 were keyed by order-run
  beads: 21 by one closed tk wisp titled
  `order:refinery-reconcile:rig:gc-toolkit`, and 42 by 21 lx wisp ids. The
  four lx wisps checked a minute after the window were already deleted, and
  none of the 22 beads exists now, the same guard-then-delete pattern as
  window B. The remaining 55 were keyed by graph.v2 workflow roots without the
  completion-convergence stamp: three tk roots (one in progress, two closed,
  one of them since June) and one in-progress gc root.

The order-run lookups are the guard `storeOpenDescendantIDs` runs before each
retention delete, and this change moves them to the closed-excluded shape. The
unconverged workflow-root lookups come from callers that need closed rows: the
completion backstop (`currentStepRows`) and the control dispatcher's
`DirectMembers`. The samples cannot tell those two apart, because both send the
same SQL. A separate sample on the sl store caught one long-lived process
walking closed, unstamped workflow roots in ascending id order, which is the
order the backstop visits them in.

Workflow roots without the convergence stamp on 2026-10-09: tk 295 closed and
42 open or in progress, gc 234 and 16, su 112 closed, sl 97 and 1.

### The open-work gate

The supervisor log recorded six "open-work gate ... timed out after 8s"
events on 2026-10-09: `feedback-distiller` and `feedback-miner` twice each on
the gc-toolkit rig, and `deferred-dispatch` and `feedback-distiller` once each
on signal-loom. A timed-out gate skips an order that is not marked idempotent
and backs it off for 24 seconds. The membership lookup alone took longer than
8 seconds on tk at 15:58Z.

### Not measured

The processlist share after the change. The controller and the core order run
the installed `gc` binary, which is built from main, so the new query shape
appears in live samples only after this change lands and the city binary is
rebuilt. gc-52sf3i tracks that re-measurement, and gc-ofr2iz ranks the hot
query shapes.

What the attribution predicts, as a projection and not a measurement: the
lookups attributed to the retention sweep's guard (63 of 118 in window A, and
in window B the 19 of 25 keyed by tracking beads deleted right after) switch to
the closed-excluded shape, which took 0.07 to 0.11 seconds
median on tk instead of 3.0 to 4.5 seconds. The all-status root-id lookups left
are the ones this change does not touch: 55 of 118 in window A and 6 of 25 in
window B.

## Caller audit

Every non-test lookup of `gc.root_bead_id` through `ListQuery` or
`ListByMetadata` in `cmd/` and `internal/`, as of `65490dcf7`. "Discards" means
the caller drops every closed row in Go, so excluding closed beads in the query
returns the same result.

### Changed here

| Caller | Paths | Closed rows |
|---|---|---|
| `storeOpenDescendantIDs`, `cmd/gc/order_dispatch.go` | order open-work gate, retention delete guard, workflow delete guard, stale-wisp sweep | discards |
| `CloseSpecSidecarsForRoot`, `internal/sourceworkflow/sourceworkflow.go` | workflow finalize, source-bead and wisp autoclose, wisp GC | discards |

### Uses closed rows, unchanged

| Caller | Paths | What the closed rows are for |
|---|---|---|
| `currentStepRows`, `internal/executionevent/projector.go` | completion backstop, completions tick, `EmitCurrent` after each control bead, graph order dispatch | emits completion facts for closed steps (gc-0mue2i) |
| `molecule.ListSubtree`, `internal/molecule/cleanup.go` | molecule, wisp and source-bead autoclose, wisp GC, finalize, run cancel | seeds the parent walk and counts finished descendants |
| `collectExpiredBeadClosure`, `cmd/gc/wisp_gc.go` | wisp GC purge, city store only | deletes them |
| `orderWispMetadataDescendants`, `cmd/gc/order_dispatch.go` | `gc order sweep-tracking --include-wisps` only | seeds the subtree walk |
| `findWorkflowBeads`, `findWorkflowBeadsFromRoot`, `cmd/gc/cmd_convoy_dispatch.go` | `gc workflow delete`, `delete-source`, `reopen-source` | counts and deletes whole workflows |
| `ListWorkflowBeads`, `internal/sourceworkflow/sourceworkflow.go` | drain, wisp autoclose, run cancel, sling | orders open beads by depth through closed parents (its sling consumer, `SnapshotOpenWorkflowBeads`, discards them) |
| `listByWorkflowRootAndScope`, `internal/dispatch/runtime.go` | dispatcher scope check | copies closed members' metadata |
| `DirectMembers` in `findSpecBead`, `findLatestAttempt`, `processFanout`, `appendRalphRetry`, `resolveRetryRunSubject`, `resolveScopeBodyOnce`, `terminalAbortScopeFailureMember` | control dispatcher | each resolves a bead that may be closed |
| `collectBeadGraph`, `snapshotFromStore` and `convoy_sql.go`, `buildWorkflowRunProjections`, `internal/api` | HTTP bead graph, convoy and workflow views, orders feed | shown or aggregated in the response |
| `humaHandleWorkflowDelete`, `internal/api/huma_handlers_convoys.go` | HTTP workflow delete | deleted when the request asks for deletion |

### Discards closed rows, left for a follow-up

| Caller | Paths | Why not here |
|---|---|---|
| `DirectMembers` in `closeGeneratedSpecBeadsForAttempt`, `closeSpecBeadsByRefs`, `skipOpenScopeMembers` | control dispatcher | needs an open-only `DirectMembers`; not measured hot (gc-ho0oag) |
| `humaDeleteWorkflow`, `internal/api/huma_handlers_convoys.go` | HTTP convoy delete | one request per operator action |

`graphHasOpenDescendants` (`internal/storebinding/beads_adapter.go`) also
discards closed rows, and `collectRalphAttemptBeads`
(`internal/dispatch/ralph.go`) also lists them. Neither has a caller outside
tests.

### Not determined

`DirectMembers` in `ensureDrainWorkflowBlocksOn` and
`repairDrainWorkflowSourceMemberDeps` act on closed members as well, and a drain
pass calls them once per row and once per blocker (gc-ho0oag).
`resolveLogicalBeadID`'s fallback and `appendRetryAttempt` have no status
filter, and whether they ever need a closed match is not determined.

### Already excluding closed beads

`assertWorkflowDeleteSetLeavesNothingStranded`, the hook-claim continuation
lookups (`Status: "open"`), `listActiveByWorkflowRootAndScope`, the first pass
of `resolveScopeBodyByRole`, `failedAttemptAttachRootID`, `findExistingAttach`,
`existingAttachIDMapping` and `existingLogicalBeadIDIndex`.

## How the samples were taken

The processlist sample, run against the server with no database selected:

```sql
select id, db, time, info from information_schema.processlist
where command <> 'Sleep' and info is not null
  and info not like '%information_schema.processlist%';
```

A row counts as a root-id lookup when its `info` contains `root_bead_id`. It
counts as closed-excluded when its `WHERE` clause carries a status predicate.
The root id is the literal compared against
`JSON_EXTRACT(metadata, '$."gc.root_bead_id"')`. Window B classified each root
in the same pass by reading it from `issues` and `wisps` in its database, with
its `gc.kind`, `gc.formula_contract`, `gc.completion_facts_converged` and any
`order-run:` label.

The timing query, with and without the status predicate:

```sql
select id from issues
where status NOT IN ('closed')
  and JSON_UNQUOTE(JSON_EXTRACT(metadata, '$."gc.root_bead_id"')) = '<root>';
```
