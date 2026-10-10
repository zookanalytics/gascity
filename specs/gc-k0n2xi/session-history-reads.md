---
name: Who reads the whole session history on the city store, and what gc-k0n2xi changed
description: Attributes the closed-inclusive session-bead enumeration on the city store to its callers, using the controller's own trace and read-only processlist sampling on 2026-10-10. The supervisor's per-pass issuer is the demand pass's closed named-session index, not mail recipient resolution. The stream re-issued at the read deadline came from the hourly doctor sweep. A third, label-only stream is not attributed. Records the evidence, the changes, and the open work with the beads filed for it.
---

# gc-k0n2xi: who reads the whole session history

## Verdict

The bead blamed mail recipient resolution. Its evidence fits other callers,
and the mail path does not appear in the data.

1. **The supervisor reads the whole session history on every demand pass.**
   `readyAssignedWorkAssignees` builds the closed named-session index
   (`session.BuildClosedNamedSessionBeadIndex`) whenever an `on_demand` named
   session is configured. The build lists every session bead by type and by
   label, closed ones included. The city store holds 6,491 closed session beads
   in the issues table and 1,808 in the wisps table, and 4 of them carry a
   named-session identity. On 2026-10-09 the controller's trace recorded 2,554
   of these builds, 16,317 seconds of reading in one day. The median took
   3.6 s, the 90th percentile 11.2 s and the 99th 76.5 s. Twenty-two ran 95 to
   124 s and returned a partial index after the native read retry budget ran
   out. Each one runs inside the controller tick.
2. **The stream re-issued at the read deadline came from the hourly
   `gc doctor` sweep.** Its `pool-idle-routed-work` check called
   `session.Store.List("", template)` once per pool template per scope, and
   `List` read closed rows for every filter. The check overran its one-minute
   timeout and was abandoned, and its goroutine kept scanning until the doctor
   exited. Once the scans reached the ten-second read deadline, each one ran on
   a fresh connection and the doctor's stderr logged an `i/o timeout` about
   every ten seconds.
3. **Mail recipient resolution in the supervisor did not show up.** No
   `beadmail:` line appears in the 120 MB supervisor log. Of 5,316 mail API
   requests in that log, 3 took two seconds or more, and a cache-miss
   enumeration takes several seconds.
4. **A label-only, default-sort, closed-inclusive stream is not attributed.** It
   ran through every sampling window, both while the doctor ran and after it
   exited, on connections that changed every one or two scans. It is gc-oh4e2p.

## Evidence

### The controller trace

The session reconciler writes always-on trace segments under
`.gc/runtime/session-reconciler-trace/`. Each demand pass records its closed
index build as a `demand_snapshot.store_read` operation with
`op=closed_named_index`, `point=assigned` and a `duration_ms`. The record is
written when the read ends.

| Window | Builds | Durations |
|---|---|---|
| 2026-10-09, 00:00 to 23:59Z | 2,554 | median 3,558 ms, p90 11,212 ms, p99 76,542 ms, max 124,157 ms; 22 partial |
| 2026-10-09 16:14 to 16:36Z, the analysis window | 6 | 17.5 to 28.6 s each |
| 2026-10-10 00:25 to 00:36Z | one per pass, about every 25 s | 2.2 to 6.6 s |

A partial build of 119.9 s ended at 16:13:50Z, just before the analysis window.
Every partial build failed the same way: `native Dolt read retry budget
exhausted (context deadline exceeded)`.

### Processlist sampling

A read-only sampler polled `information_schema.processlist` on the city
database every one to one and a half seconds from 02:12 to 02:34Z on
2026-10-10, and every 0.4 s for two stretches inside that range. It kept the
queries that list session beads with no status filter. The native store renders
`SortDefault` as `ORDER BY priority ASC, created_at DESC, id ASC` and
`SortCreatedDesc` as `ORDER BY created_at DESC, id ASC`, so the sort tells the
callers' query shapes apart.

- **The index build** shows as a type leg (`issue_type = 'session'`,
  created-desc, about one second on issues) and then a label leg
  (`label = 'gc:session'`, created-desc, 4 to 7 s on issues and 1 to 2 s on
  wisps) on one pooled connection. The trace record ending 02:13:29Z (19.2 s)
  matches the legs sampled from 02:13:10Z, and a pass sampled from 02:28:48 to
  02:29:09Z ran 21 s.
- **The doctor's check** shows as created-desc label scans with no type leg,
  the shape of `session.Store.List`. One connection ran 28 of them back to back
  from 02:12:06 to 02:16:35Z, the last reaching nine seconds. From 02:16:37 to
  02:17:55Z each scan ran on a new connection, one about every eleven seconds,
  and reached eight or nine seconds. `lsof` on the server port showed
  `gc doctor --json` holding a connection. Its stderr logged `read tcp
  127.0.0.1:<port>->127.0.0.1:32831: i/o timeout` at 02:16:36, 02:16:47,
  02:16:57, 02:17:10, 02:17:21, 02:17:31, 02:17:44 and 02:17:54Z, each on a new
  local port and each a second before the next sampled scan began. The sweep ran
  from 02:04:12 to 02:22:45Z, and its payload reports
  `pool-idle-routed-work: timed out after 1m0s and was abandoned (outcome
  unknown)`.
- **The unattributed stream** shows as default-sort label scans with no type
  leg, 5 to 8 s on issues and about 1 s on wisps. It ran 25 times on 16
  connections from 02:12 to 02:19Z. From 02:20:02 to 02:27:38Z it gave 38
  samples before the doctor exited and 33 after. It continued in bursts at
  02:29:32 to 02:30:26 and 02:33:35 to 02:34:29Z, a scan about every fourteen
  seconds within a burst. Short-lived `gc mail check --inject`,
  `gc nudge drain --inject`, `gc session peek` and `gc session logs` processes
  held connections over the same period, next to the supervisor.

### What the analysis observed at 17:05Z

The analysis saw one connection re-issue the label scan about every ten
seconds, each run reaching nine seconds. The doctor's check produces exactly
that pattern once its scans reach the deadline. The trace also shows a 20.5 s
index build ending at 17:05:38Z. Which of the two the analysis sampled cannot be
recovered from what was kept.

## What changed

- **The controller keeps the closed named-session index between passes.**
  `closedNamedIndexCache` (`cmd/gc/allocator_demand_reads.go`) is created once
  per controller in `supervisorBuildAgentsFnWithSessionBeads` and
  `standaloneBuildAgentsFnWithSessionBeads`. The control-dispatcher tick keeps
  its own on the `CityRuntime`. That tick narrows the config to the dispatcher
  agents but keeps every named session, so an `on_demand` one sends each of its
  desired-state builds to the index as well. It builds without a trace, so its
  index builds are not among the ones the trace counted. Each cache answers
  from the last complete build until a named session it saw open leaves the
  pass's open-session snapshot, the store changes, the snapshot is degraded or
  absent, or the index is ten minutes old. A failed build reaches its pass
  unchanged and is not kept.
  The index only adds runtime-name assignees for on_demand identities, so a
  stale entry costs one extra ready probe, and the open-set check catches the
  change that would hide demand, a named session closing.
- **`session.Store.List` reads closed rows only when its filter can keep one**,
  for `all` or a list that names `closed`. Every other filter already dropped
  each closed row in memory, so results are unchanged, and the doctor's
  per-template listing reads open sessions only.
- **The cached beadmail provider holds an enumeration's outcome, a failure as
  well as a success, for its refresh interval after the enumeration returns.**
  A store too slow to answer is asked again thirty seconds after its last
  answer. It is not asked on every recipient lookup, and not again straight
  after an enumeration that used up the interval.

## What this does not change

- **The stateless beadmail provider** still enumerates on every alias-history
  lookup. `TestProvider_DefaultProviderSeesNewHistoricalAliasSessionAcrossCalls`
  pins that contract, and its one production use found, the store-maintenance
  alert mail, sends without resolving recipients through the history.
- **The hourly detached-orphan route index, the doctor's executor-identity
  residue check and the startup name-claim sweep** still read closed sessions.
  The first two need them, and none runs per pass.
- **The unattributed label-only stream** is gc-oh4e2p.
- **A doctor check abandoned at its timeout keeps running** and keeps reading
  the store until the doctor exits. That is gc-hvk4ub.

## Method

- Trace: `jq` over every 2026-10-09 segment file, keyed on
  `op=closed_named_index`.
- Processlist: the `dolt` CLI with `--host 127.0.0.1 --port 32831`, run under
  `env -i`, issuing `SELECT` statements only. Each sample kept the connection
  id, the running time, the table, the `WHERE` clause and the `ORDER BY` clause.
- Process attribution: `lsof -iTCP:32831 -sTCP:ESTABLISHED` and `ps` at sample
  time, and the doctor sweep's own `stderr.log`, `payload.json` and timestamps
  under `.gc/runtime/doctor-sweep/current/`.
- Mail latency: the supervisor's own `api:` request lines in
  `/Users/gc/.gc/supervisor.log`.
- Store sizes: `COUNT(*)` by status and tier on the city database.
