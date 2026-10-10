---
name: Why bead queries are slow on Dolt (gc-ofr2iz)
description: Ranks the live Dolt query load of 2026-10-09 by caller and cost, classifies each source as gc-toolkit usage, gascity or beads structure, or a missing index, and records the measurements behind the accepted remedies and the beads filed for them. Corrects the premise that the metadata driver scan is the whole cost. Every non-empty `bd list --json` also pays a whole-table aggregate. bd's 10-second read deadline turns any read slower than that into a re-issued failure.
---

# gc-ofr2iz: why bead queries are slow on Dolt

## Verdict

It is a mix of all three classes, and the metadata filter is not the largest
single cost.

1. **Structural in beads: a fixed cost under every `bd list --json`.** bd
   hydrates list rows with a counts query whose aggregate subqueries read whole
   side tables on every call, however narrow the filter. On a copy of the tk
   store, a list that matches one bead takes 3.5 to 4.4 seconds. The
   `deps_json` aggregate over all 56,318 dependency rows accounts for 2.7
   seconds of that. Status scoping and indexes cannot remove it. Upstream
   already tracks the fix (gastownhall/beads#6701, proposal #7291).
2. **Structural in gascity, often driven by gc-toolkit cadence: pollers and
   walkers that re-read whole working sets.** Each of the three control
   dispatchers re-reads every open and in-progress bead about every three
   seconds. Together they processed 10 control beads in an hour. `gc convoy
   list --json` runs from every rig's reconcile pass and reads each open
   convoy's legacy children in a separate query. A session enumeration that
   includes closed sessions reads all 6,491 closed session beads on the city
   store, and one supervisor connection re-issues it about every ten seconds.
3. **A missing index, hit by closed-inclusive callers in both repos.** A
   metadata filter reads the JSON of every row it considers. Over all statuses
   that is about 3 seconds on the copy, against 0.25 seconds for the non-closed
   rows. The heaviest callers are gc-toolkit's `anchor_bead` lookups over all
   statuses and gascity's `gc.root_bead_id` lookups with `IncludeClosed`. A
   functional index on the key turns bd's unmodified SQL into an index lookup.

One amplifier ties these together. bd 1.3.1 abandons any read that receives no
data for 10 seconds, then retries it from scratch until 30 seconds have passed.
A read that needs more than ten seconds never completes. It runs up to three
times and fails. In gc-toolkit's refinery-reconcile trace, 160 of 311
closed-inclusive `anchor_bead` lookups failed, 159 of them after more than 30
seconds.

The profile behind the bead said the metadata predicate's driver scan was the
whole problem and the aggregate joins were not. Even for lookups over all
statuses the scan is under half the cost: 2.9 of 6.4 seconds for a root-id
lookup and 3.3 of 9.3 seconds for an `anchor_bead` lookup on the copy. For a
status-scoped lookup the aggregates are nearly all of it.

## Ranked load

The share column comes from 60 snapshots of the live server's processlist,
taken 16:14 to 16:36Z on 2026-10-09. They caught 359 in-flight queries, a mean of
6.0 at a time. A snapshot catches a query in proportion to how long it runs, so
the share is cost times frequency on the Dolt server. Costs per call are
medians of three runs against a copy of the tk tables on a scratch Dolt 2.4.2
server (method below).

| # | Query shape | Share | Cost per call | Originating caller | Class |
|---|---|---|---|---|---|
| 1 | Children of one parent: `id IN (SELECT issue_id FROM dependencies WHERE type='parent-child' AND COALESCE(…) = ?) OR (id LIKE '<id>.%' AND id NOT IN (…))` | 20.9% | 10.8 s | `gc convoy list --json`. The installed binary reads each open convoy's children separately. gc-toolkit's `convoy-graduate.sh` runs it once per reconcile pass on every rig, and about three are running at any moment. Every sampled tk parent found in the copy is a convoy. | 2, invoked by 1 |
| 2 | Metadata lookups that include closed rows | 14.8% | 3 s driver scan, plus 3.5 to 6 s more in a full `bd list --json` | gascity `IncludeClosed` root-id lookups (5.0%), gc-toolkit `lane-state.sh` `anchor_bead` over all statuses (3.6%), the helm-svc board's 7-day closed pass on `gc.takeaway`, `gc.routed_to` and `merge_result` (3.3%), and `step-close.sh` `gc.session_id` over closed rows (1.4%) | 3, called from 1 and 2 |
| 3 | Control-dispatcher re-prime: `bd list --json --status=open` and `--status=in_progress` with `--limit 0`, plus the blocked projection | 14.2% of SQL time, 45% of live `bd` processes | 4.6 s (open) and 3.9 s (in progress) on tk | gascity `gc convoy control --serve`, one per rig, three rigs | 2 |
| 4 | Session beads by label with no status filter: `JOIN labels … label = 'gc:session'` on the city store | 7.8% | up to 9 s live, reading 6,496 rows to find 5 open | gascity `session.ListAllSessionBeads` with `IncludeClosed`, run in the supervisor. One pooled supervisor connection re-issued it about every 10 seconds while observed. The code path that matches the SQL and the timing is mail recipient resolution's fallback, which enumerates every session bead (`beadmail` `cachedSessionBeads`, refreshed every 30 s). The hourly detached-orphan index and doctor checks issue it too. | 2 |
| 5 | Metadata lookups on non-closed rows | 3.9% | about 3 s in `bd list --json`, nearly all aggregate | many | 2 |
| – | `information_schema` probes | 22.6% | about 0 s each | Every `bd` process start runs several. Their share follows the number of `bd` processes, so cutting rows 3 and 1 cuts them too. | 2 |

The remaining 16% is batched dependency and label reads by id (4.2%), writes
(2.5%) and small one-off shapes (9.2%).

The external profile counted root-id lookups at about 23% of samples and
`anchor_bead` lookups at about 10%. In this window they were 5.0% and 4.4%. The
mix moves with activity, because a wisp GC run or a burst of control-bead
processing multiplies root-id lookups. The per-call costs and mechanisms below
do not depend on the window.

## Mechanism 1: the counts query aggregates whole tables (beads)

`bd list --json` runs `SearchCountsSQL` (beads v1.3.1,
`internal/storage/sqlbuild/counts.go`) in its predicate form. The WHERE clause
filters the driver table before the joins. Each LEFT JOIN subquery (labels,
blocker counts, reverse-blocker counts, comment counts, parent, and
`deps_json`) still aggregates its whole table, because nothing restricts it to
the driver's rows. The parent and `deps_json` joins are unconditional, so
neither `--brief` nor `--skip-labels` removes them.

Measured on the tk copy, for a list whose driver matches one bead:

| Variant | Median |
|---|---|
| Full query as bd sends it | 3.5 s (4.4 s in an earlier run) |
| Without the `deps_json` join | 0.8 s |
| Without `deps_json` and the reverse-blocker join | 0.5 s |
| With `deps_json` restricted to the driver's id | 1.0 s |
| The `deps_json` aggregate alone | 2.4 s |
| Each of the other five aggregates alone | 0.15 to 0.26 s, about the 0.2 s client overhead |
| A list whose driver matches no rows | 0.26 s, because Dolt skips the joins |

`deps_json` builds a JSON object for every dependency row, including a
`CAST(metadata AS CHAR)`, before grouping. A grouped subquery has to finish
before the join can return a row, so the query sends nothing until the
aggregates are done. That is also why these reads run into the 10-second
deadline below.

Owner: beads. gastownhall/beads#6701 describes the problem and #7291 proposes
the fix, which is to hydrate a predicate-form search by an ID page as ready work
already does. The by-IDs form already exists in `SearchCountsSQL`. `counts.go`
is unchanged on beads main since 2026-08-10, before v1.3.1.

## Mechanism 2: a metadata filter reads every row's JSON (missing index)

`AppendMetadataClauses` emits `JSON_UNQUOTE(JSON_EXTRACT(metadata, '$."<key>"')) = ?`,
and no index covers it. On the tk copy, the driver alone takes 2.85 seconds for a
`gc.root_bead_id` lookup over all statuses and 3.30 seconds for an `anchor_bead`
lookup whose status list includes `closed`. The same lookups over non-closed
rows take 0.23 and 0.27 seconds, because the status index narrows the rows
whose JSON is read.

Two index forms were tested on Dolt 2.4.2:

- **An indexed virtual generated column is not used** for the expression. The
  planner matches only a predicate that names the column, which would need a
  beads change to the SQL builder and one schema column per key.
- **A functional index is used for bd's SQL exactly as bd sends it.**
  `CREATE INDEX … ON issues ((JSON_UNQUOTE(JSON_EXTRACT(metadata, '$."<key>"'))))`
  matches because bd interpolates parameters client-side
  (`internal/storage/doltutil/dsn.go:58`, `InterpolateParams: true`), so the
  server sees the literal path. The live text, `'$.\"anchor_bead\"'`, matches
  the index. EXPLAIN shows an indexed lookup inside the counts query's derived
  driver, in `COUNT(*)`, and next to a status `IN` list.

With the two indexes on the copy:

| Lookup | No index | Indexed |
|---|---|---|
| `gc.root_bead_id`, all statuses, driver only | 2.85 s | 0.27 s |
| `anchor_bead` including closed, driver only | 3.30 s | 0.13 s |
| Same `gc.root_bead_id` lookup as a full `bd list --json` | 6.4 s | 3.3 s |
| Same `anchor_bead` lookup as a full `bd list --json` | 9.3 s | 3.6 s |

The indexed full queries still pay mechanism 1. Reads that skip the counts
query, such as the native store's, keep the whole gain. The API's SQL fast path
(`internal/api/convoy_sql.go:142, 395`) does not: its `id = ? OR <metadata
predicate>` form still scans the table, so it would need a UNION of the two
lookups to use the index. Building each index on 42,334 rows took 19 and 28 seconds.
Inserting and updating rows whose indexed value is 5,000 characters long, a
number, or a nested object all succeeded, and the index answered them
correctly.

gascity already creates query indexes on beads tables
(`cmd/gc/dolt_wisp_query_index.go`), once per controller, on the city database
only. The city store `lx` has those indexes. The rig stores, including tk, do
not.

## Mechanism 3: pollers and walkers re-read whole working sets (gascity)

**Control dispatchers.** `gc convoy control --serve` answers readiness from a
snapshot that lives three seconds (`controlReadyCacheTTL`,
`cmd/gc/dispatch_control_ready.go:70`). Re-priming it runs
`CachingStore.PrimeActive`. That lists every open and every in-progress bead in
both tiers through `bd`, then fetches their dependencies and the blocked
projection. The serve loop wakes on every `bead.updated` event in the city,
including other rigs' stores, and on idle sweeps every one to five seconds. In
the 62 minutes from 15:40 to 16:42Z, 2,413 readiness scans across the three
dispatchers found nothing to do, and 10 control beads were processed. The
gc-toolkit dispatcher's cycle averaged 8.3 seconds, consistent with the cost of
a tk prime.

**`gc convoy list`.** The installed binary (built from 1a3bda7a6) reads each open
convoy's legacy children in a separate query. One such read takes 10.8 seconds
on the tk copy. EXPLAIN shows why: the predicate ORs the parent-edge arm with
the dotted-id arm, so Dolt scans all 42,334 issue rows and tests each against
both dependency subqueries. Main replaced the loop with `convoy.MembersBatch`
(65490dcf7), which asks for all convoys' children through
`ListQuery.ParentIDs`. Neither the native store nor BdStore pushes `ParentIDs`
down (`internal/beads/native_dolt_store.go:3247-3253` translates `ParentID` and
`IDs` only), and the query contract says a store that cannot push it returns a
superset. With `IncludeClosed` set, that superset is every bead in the store.
The full-store read the new path issues takes 22.6 seconds and 228 MB on the tk
copy, once per call. That beats per-convoy reads, but it is still the whole
store.

**Session enumeration.** `session.ListAllSessionBeads` with `IncludeClosed`
reads every session bead, once by label and once by type. The city store holds
6,491 closed session beads and 5 open ones. While observed at about 17:05Z, one
pooled connection belonging to the supervisor re-issued the label scan about
every ten seconds. Each run reached 9 seconds and was replaced by a fresh one,
the pattern of a read cut off at the beads library's 10-second deadline. The
path that matches
the SQL and the timing is mail recipient resolution: when a recipient matches no
open or closed session address, `beadmail` enumerates every session bead
(`cachedSessionBeads`), and its cache refreshes every 30 seconds. A cache that
never fills re-lists on the next call. The hourly detached-orphan route index,
doctor checks, and the startup name-claim sweep issue the same scan.
`gc session list` is not a source; the API serves it from the open-session
cache. Separately, the gc-toolkit witness patrol reads every closed session each
cycle on every rig through `gc bd list --type=session --label=gc:session --all`
(`formulas/mol-witness-patrol.toml:299`).

## The amplifier: a 10-second read deadline with retries (beads)

bd's pooled connections give every read a 10-second I/O deadline
(`defaultPoolReadTimeout`, beads `internal/storage/dolt/store.go:521`). A Go `i/o
timeout` counts as a retryable connection error (`:673`), and `withReadTx`
(`:1033`) wraps each read in a backoff that retries for up to 30 seconds
(`:607`). A list query that produces nothing for 10 seconds is abandoned and
issued again, up to about three times, and then bd exits 1.

- No query in the 359 live samples had been running longer than 9 seconds.
- gascity's trace of gc-toolkit's refinery-reconcile
  (`~/.gc/bd-trace/refinery-reconcile.gc-toolkit.jsonl`, 05:55 to 16:39Z,
  5,338 calls, 14,501 seconds of `bd` time) shows the closed-inclusive
  `anchor_bead` lookup as the order's largest cost. It made 276 calls with a
  median of 31.3 seconds, 44% of the order's `bd` time. Of 311
  closed-inclusive `anchor_bead` lookups of any form, 160 exited 1, 159 of
  them after more than 30 seconds.
- The deadline cannot be raised for bd 1.3.1. `BEADS_DOLT_POOL_READ_TIMEOUT` is
  inert for every CLI command in server mode (gastownhall/beads#6144). Measured
  here, `bd sql "SELECT SLEEP(15)"` fails at 10.3 seconds with the variable
  unset and at 10.5 seconds with it set to 60s. The fix is on beads main but in
  neither v1.3.1 nor v1.3.2-rc.1.

Until queries get cheaper, the deadline turns contention into failure. It also
turns failure into more contention, because each abandoned read is issued again.

## Remedies and the beads filed for them

| Remedy | Removes | Owner | Bead |
|---|---|---|---|
| Restrict the counts query's aggregates to the driver's rows (ID-page hydration) | Mechanism 1: about 3 s from every non-empty `bd list --json` on tk | beads, upstream #6701 and #7291 | gc-m6zgpa, our measurements parked as a comment on #7291 for the operator to send |
| Replace the dispatchers' three-second full re-prime with an event-fed long-lived snapshot, or a read of control-dispatcher-routed beads only, and ignore other stores' events | Row 3 | gascity | gc-due3fr |
| Push `ListQuery.ParentIDs` down in the native store and BdStore so `MembersBatch` reads children, not the store, then deploy main | Row 1 | gascity | gc-d43dp4 |
| Create functional indexes for hot metadata keys on every managed store: gascity's own keys by default, a pack's keys by pack declaration (`anchor_bead` first) | Mechanism 2, rows 2 and 5 | gascity, with a gc-toolkit declaration | gc-a75mwa |
| Scope gascity's `IncludeClosed` root-id lookups | Part of row 2 | gascity | gc-ymv5j4 (already filed; evidence appended) |
| Stop enumerating every session bead, closed ones included: resolve mail recipients by indexed lookup, and bound or event-feed the remaining enumerations | Row 4 | gascity | gc-k0n2xi |
| Build the witness liveness map without reading every closed session each cycle | A per-cycle full read, share not measured | gc-toolkit | tk-4ujih0b |
| Share each anchor's closed-inclusive read across refinery-reconcile arms, invalidated by writes rather than arm boundaries, with one cache key per query | Row 2, and most reconcile timeouts | gc-toolkit | tk-a2j63cs |
| Pass `--root` from formula step loops so `step-close.sh` stops deriving the root by scanning `gc.session_id` over closed rows once per step | Part of row 2 | gc-toolkit | tk-o4lton4 |
| Skip the city-wide `gc convoy list` when the rig has no open integration convoy | Row 1 | gc-toolkit | tk-hngp2iq |
| Push metadata filters into BdStore's ephemeral-tier `bd query` leg, which drops them today | A smaller share; every ephemeral row is read and filtered in Go | gascity | gc-wtquzx |
| Raise the bd read deadline by projecting `BEADS_DOLT_POOL_READ_TIMEOUT` into bd subprocesses, once a beads release carries the #6144 fix | The amplifier | gascity, waiting on beads | gc-gnas26 |

## Candidates not taken

- **A generated column per key, with the SQL builder emitting a column
  predicate.** It needs a beads change and a schema column per key. The
  functional index reaches the same plan with no beads change.
- **A metadata side table maintained on write.** Every metadata write in beads
  would have to keep it current. The functional index gives the same lookup for
  the keys that are actually hot.
- **Deleting or archiving closed beads.** Out of scope by operator ruling. Every
  remedy above leaves history in place.

## Method

- **Live store sizes**, from `count(*)` by status: tk 42,334 issues (97.2%
  closed), 56,318 dependencies and 2,503 labels; gc 7,894 issues; lx 7,116
  issues and 3,629 wisps; sl 4,633; su 1,446; ss 212.
- **Live sampling.** A read-only sampler took 60 `information_schema.processlist`
  snapshots, and at the same instants recorded every `bd` process with five
  levels of parent processes. That gave 121 `bd` process samples. bd sends
  interpolated SQL, so each sample shows its literal filter values. `lsof` on the
  server port showed which processes hold connections. The controller holds
  seven, because the native store queries Dolt in-process. SQL from `gc`
  processes is attributed by query shape, literal values, and process sampling,
  not by connection, because Dolt's processlist does not show client ports.
- **Traces.** gascity's `bd` call trace for gc-toolkit's refinery-reconcile, and
  the three control-dispatcher trace logs in `.gc/runtime`.
- **Benchmarks.** The tk tables were copied with paged read-only `SELECT`s into
  a scratch Dolt 2.4.2 server on the same host, with stats off as on the live
  server. Each query ran three times and the median is reported. The host's load
  average was 43 to 51 throughout. The scratch server had no competing queries,
  so its timings understate live latency, which the profile put at 8 to 17
  seconds for these shapes.
- **Code read.** beads v1.3.1 from the module cache. gascity main at 65490dcf7,
  and the installed binary's revision 1a3bda7a6 where they differ. gc-toolkit
  as checked out in the city.
