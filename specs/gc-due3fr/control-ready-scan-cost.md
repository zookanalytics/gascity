---
name: What a control-dispatcher readiness scan reads, and what wakes it (gc-due3fr)
description: Records why a control dispatcher on a bd store now answers each readiness scan with one `bd ready` call instead of priming its open and in-progress beads, why that call reads one page and re-reads the whole ready set only when the page comes back full, and why bead events from stores it does not read no longer wake it. Holds the before measurements from loomington (scans per hour, scan time, per-hop pickup latency, per-scan read cost), the cost of a bounded against an unbounded `bd ready`, the two designs the bead proposed and why neither was built as proposed, and the method for re-measuring once the change is deployed.
---

# gc-due3fr: what a control-dispatcher scan reads

## What changed

- Every readiness scan reads the ledger as it is when the scan runs. Nothing is
  reused from an earlier scan. This is upstream gastownhall/gascity#7404
  (`8a52426d61`), carried here unchanged.
- On a store that is a bd workspace, a scan reads the store with one call:
  `bd --readonly --sandbox ready --json --exclude-type=epic --limit=5000`, plus
  `--brief` when bd is 1.2.1 or newer. That page holds the store's whole ready
  set unless it comes back full, and then the scan reads the whole set again
  with `--limit=0`. The scan picks out the dispatcher's own beads only after the
  read, so a page cut short could leave them out. The graph binding that a rig
  scope also reads is read whole for the same reason. The scan this replaces
  primed an in-process snapshot. That prime listed every open and every
  in-progress bead in both tiers through four `bd` calls, then read their
  dependencies and the blocked projection.
- Bead events from the city's other stores no longer wake the serve loop. The
  loop still wakes on every bead event from a store it reads, and it still
  sweeps on its idle timer, which backs off from one second to five.
- The single read runs with the Dolt host and port that the generated work
  query carries from the dispatcher's own environment. Before this change only
  the fallback took that read, and it ran without them.

A store opened through a file or exec provider, and a relocated city scope,
keep the snapshot primed for each scan.

## The bead's two designs

**A snapshot fed by events for the dispatcher's lifetime.** Upstream #7404
measured that a worker's raw `bd update` close does not reliably publish a city
event. A snapshot kept current only by events would miss such a close until
something else refreshed it, so it cannot keep the pickup latency the
three-second TTL was there to protect. #7404 also showed that any snapshot
reused across scans re-offers the control bead the drain just closed.

**A read of only the beads routed to the dispatcher.** In bd 1.3.1, `bd ready`
accepts one `--assignee` and ANDs its `--metadata-field` filters. `bd query`
supports OR, but evaluates it in memory over everything the base filter
returns. The routed question therefore takes one call per assignee candidate
and one per route and key. For the gc-toolkit dispatcher that is ten calls:
six assignee candidates, and two routes for each of `gc.run_target` and
`gc.routed_to`. Measured on the live gc-toolkit store (17:48Z, load 58 to 66,
three rounds):

| Read | Wall time |
|---|---|
| The ten routed calls, hold labels excluded in SQL as the old shell query did | 8.8, 8.4, 5.1 s |
| The ten routed calls, hold labels excluded in Go | 3.9, 4.4, 3.4 s |
| One `bd ready --brief` of the whole ready set | 1.25, 1.40, 1.24 s |

A metadata filter makes each call read the metadata JSON of the open rows it
scans, and the hold label exclusion adds an `id NOT IN (SELECT …)` subquery. One read
of the whole ready set is cheaper than asking ten narrow questions.
`BdStore.Ready` was not used either. When bd projects dependencies inline, it
follows its `bd ready` with a `bd list` of every closed blocker of every ready
bead, to apply the `gc.work_outcome` veto. The snapshot path never applied
that veto to closed blockers, because a prime of the open and in-progress
sets holds none.

## Why the first read is a page

The scan must see the whole ready set, because it filters for the dispatcher's
own beads in Go after the read. `--limit=0` alone would give it that, but bd
1.3.1 answers a bounded `bd ready --json` and an unbounded one differently
(`runReadyCountsInTx`, `internal/storage/issueops/ready_work_counts.go`). A
bounded read first selects one page of ready ids with an indexed query, then
hydrates the counts for only those ids. An unbounded read runs the counts query
over every candidate, which bd's comment there describes as
O(candidates × blockers). Both were measured on the live stores with `--brief`,
as the scan reads them (2026-10-10, 04:58Z, load 56 to 64). In two rounds that
compared the outputs, both reads returned the same ids and the same bytes.
Eight more rounds, with the two reads in alternating order, gave these medians:

| Store | Ready rows | `--limit=5000`, median | `--limit=0`, median |
|---|---|---|---|
| gascity | 128 | 0.22 s | 0.29 s |
| gc-toolkit | 744 to 745 | 0.50 s | 0.61 s |
| signal-loom | 67 to 70 | 0.16 s | 0.23 s |

So the scan reads one page and pays for an unbounded read only when the page
comes back full, which is the one case where the page can be short.

## Before: loomington, 2026-10-09, installed `gc` at 1a3bda7a6

**Scans and scan time, 16:48 to 17:48Z.** From each dispatcher's trace log. A
scan here is one drain, from the wake to its `idle-exit` or `pending-queue`
line.

| Dispatcher | Scans | Seconds spent scanning | Median scan | p90 scan | Scans over 1 s |
|---|---|---|---|---|---|
| gc-toolkit | 688 | 2,693 | 0.16 s | 10.9 s | 211 |
| gascity | 1,119 | 2,176 | 0.16 s | 4.4 s | 400 |
| signal-loom | 1,359 | 1,855 | 0.18 s | 4.0 s | 483 |

The short scans reused a snapshot younger than three seconds. The long ones
re-primed it.

**What woke the loops.** The city event log held 1,311 bead events in the same
hour: 746 from the city store (`lx`), 320 from gascity (`gc`), 180 from
gc-toolkit (`tk`), and 65 from the other three rigs. Every one of them could
wake every dispatcher. Each dispatcher reads one store, so 86% of these events
could not change the gc-toolkit dispatcher's answer, and 99% could not change
signal-loom's.

**Per-hop pickup latency.** For each control bead a dispatcher processed, the
time from the close of its last blocking dependency (`closed_at`) to the
dispatcher's `serve process` trace line.

| Dispatcher | Window | Hops | Median | p90 | Max |
|---|---|---|---|---|---|
| gascity | 15:00 to 18:10Z | 33 | 6.4 s | 13.5 s | 44.3 s |
| gc-toolkit | 15:38 to 17:53Z | 12 | 12.9 s | 27.0 s | 38.0 s |
| gc-toolkit | 12:00 to 17:53Z | 35 | 26.8 s | 505 s | 1,752 s |

signal-loom processed no control beads in these windows.

**Per-scan read cost on the live stores.** 18:12Z, load 44 to 46, three
interleaved rounds. "Prime" is the four `bd` calls of the prime: `bd list` and
the ephemeral `bd query`, for open and for in-progress. It leaves out the
dependency and blocked-projection reads that followed them.

| Store | Prime (four calls) | One `bd ready --brief` |
|---|---|---|
| gascity | 1.64, 5.37, 1.89 s | 0.53, 1.11, 0.39 s |
| gc-toolkit | 14.41, 14.31, 8.08 s | 4.94, 3.16, 2.68 s |
| signal-loom | 3.59, 2.20, 2.15 s | 1.28, 0.32, 0.42 s |

**Why `--brief`.** On gc-toolkit the ready set was about 680 rows: 24.5 MB of
JSON in full and 0.9 MB with `--brief`. A Go decode of the same row fields
took 1.13 s for the full output and 4 ms for the brief one. `--brief` drops only free-form text
(description, design, acceptance criteria, notes, payload, waiters), which the
scan never reads.

The processlist share of the prime's queries, 14.2% of in-flight Dolt time
over 60 snapshots from 16:14 to 16:36Z, is from the gc-ofr2iz analysis
(`specs/gc-ofr2iz/query-cost-analysis.md` on branch `polecat/gc-ofr2iz`).

## After: what to expect, and how to measure it

Not measured yet: the change has to reach the city's installed `gc` first.
The re-measurement is tracked in gc-3r0anv, which this bead blocks.

Expected, from the numbers above:

- **Per-scan cost** falls to one `bd ready` read: 3 to 5 times cheaper than the
  prime's four list calls alone, with no hydration of the open set.
- **Scans per hour** follow the dispatcher's own store. With the other stores'
  events filtered, a loop whose store is quiet backs off to one sweep every
  five seconds, about 3,600 / (5 s + scan time) scans an hour. For
  signal-loom that is roughly 650, against 1,359 before. A busier store's own
  events reset the backoff, so its count sits above that floor.
- **Per-hop latency** for a close that publishes an event is whatever remains
  of a scan already running, one debounce (250 ms), and one scan. For a raw
  `bd` write that publishes none, the debounce becomes at most one sweep
  interval (5 s).

Method, re-run with the same windows as above:

1. Scans and scan time: the trace logs at
   `.gc/runtime/<rig>--core.control-dispatcher-trace.log`. Pair each drain's
   start (`serve wake-sweep`, `serve wake-coalesce`, or `serve wake-event`
   plus the 250 ms debounce) with the next `serve idle-exit` or
   `serve pending-queue`. The new scan also logs one
   `control-ready scan source=ready ready=N queue=N dur=…` line.
2. Per-hop latency: for each `serve process bead=<id>`, read the bead's
   blocking dependencies with `gc bd --rig <rig> show <id> --json`, take the
   latest `closed_at` among them, and subtract it from the trace timestamp.
3. Processlist share: the gc-ofr2iz sampling method. The prime's shapes are
   `bd list --json --status=open|in_progress … --limit 0` from
   `gc convoy control`. The new shape is the ready read's ID-page query.
