---
name: Who reads the whole session history on the city store, and what gc-k0n2xi and gc-oh4e2p changed
description: Attributes the closed-inclusive session-bead enumeration on the city store to its callers, using the controller's own trace, read-only processlist sampling and the usage sink on 2026-10-10 and 2026-10-11. The supervisor's per-pass issuer is the demand pass's closed named-session index, not mail recipient resolution. The stream re-issued at the read deadline came from the hourly doctor sweep. The third, label-only stream is the supervisor's usage lane retrying closed sessions whose model-usage sweep never settles (gc-oh4e2p). Records the evidence, the changes, and the open work with the beads filed for it.
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
4. **The label-only, default-sort, closed-inclusive stream is the supervisor's
   usage lane.** It ran through every sampling window, both while the doctor
   ran and after it exited, on connections that changed every one or two
   scans. The lane's catch-up for sessions that closed between passes kept a
   closed session whose model-usage sweep had not settled and retried it on
   every pass. Each retry ran transcript discovery. A Claude discovery that
   missed its keyed path checked whether another session shared the
   directory, and for a closed session that check listed every session bead
   by label, closed ones included. When the supervisor restarted at 04:08Z on
   2026-10-11, it was retrying 18 such sessions. gc-oh4e2p keys that check on
   the session's `work_dir` and bounds the retries.

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
- **The label-only stream** shows as default-sort label scans with no type
  leg, 5 to 8 s on issues and about 1 s on wisps. It ran 25 times on 16
  connections from 02:12 to 02:19Z. From 02:20:02 to 02:27:38Z it gave 38
  samples before the doctor exited and 33 after. It continued in bursts at
  02:29:32 to 02:30:26 and 02:33:35 to 02:34:29Z, a scan about every fourteen
  seconds within a burst. Short-lived `gc mail check --inject`,
  `gc nudge drain --inject`, `gc session peek` and `gc session logs` processes
  held connections over the same period, next to the supervisor.

### The label-only stream's caller (gc-oh4e2p)

The usage lane (`emitDueComputeFacts`, `cmd/gc/usage_compute.go`) runs inside
the controller's `bead_reconcile_tick`. Its catch-up for sessions that close
between passes (`accountVanishedIntervals`) kept, in memory, each closed
session whose accounting had not settled, and retried it on every pass. A
retry runs the terminal model-usage sweep, and for a Claude session the sweep
resolves the transcript through `TranscriptPathClassified`
(`internal/session/chat.go`). When the keyed lookup missed, the same-workdir
check (`sameWorkDirSessionBeads`) listed session beads by label with the
default sort, and included closed rows when the session was closed. That is
the sampled shape: label only, default sort, no status filter and no type
leg. An open session's check excludes closed rows, so only a closed session
produces this shape. `gc session logs` and the API's transcript handlers run
the same check for a closed session on demand; only the usage lane repeats it
on every pass.

- **The usage sink holds a copy per retry.** A retry whose model sweep does
  not settle records the interval's compute fact again, and the sink keeps
  every copy (`usage.ReadFacts` keeps the first). Between the supervisor's
  start at 05:31Z on 2026-10-08 and its restart at 04:08Z on 2026-10-11, 18
  closed sessions gathered 202 to 1,649 copies each, 16,323 in all. For the 17
  that record a `closed_at`, the first copy lands 25 s to 4 minutes after it.
  Every session's copies end between 04:04 and 04:07Z on 2026-10-11, at the
  restart that emptied the lane's retry set.
- **All 18 are closed Claude sessions that carry a `session_key` and still owe
  their interval.** Seventeen have their `<session_key>.jsonl` under a
  different folder of `/Users/gc/.claude/projects` from the one their
  `work_dir` names, most under the folder of a per-bead worktree, so the keyed
  lookup misses them. That miss is gc-ysf8tb. The eighteenth, lx-wisp-e9yi1,
  has no transcript under that root at all, and no `closed_at`. The sweep
  treats a keyed miss as transient and never settles it, so all 18 were
  retried on every pass.
- **The controller tick paid for it.** The usage lane has no trace phase of
  its own, so the part of `bead_reconcile_tick` that its traced sub-phases do
  not cover contains it, along with an asynchronous start that returns at once
  after its first call, an open-session snapshot load and in-memory
  bookkeeping. On 2026-10-10 that untraced part had a median of 43.8 s per
  tick (90th percentile 132.0 s), against a median of 62.0 s for the whole
  phase. From 00:00 to 04:08Z on 2026-10-11 its median was 41.0 s. After the
  restart, with the retry set empty, it fell to a median of 0.1 s (90th
  percentile 0.5 s), and the whole phase to 9.9 s. The restart also changed
  the binary, from 1a3bda7a6 to 254144dec, and none of the 19 commits between
  them touches the usage lane or the transcript lookup. Both builds report
  uncommitted changes, so that comparison does not cover everything the
  binaries differ by.
- **The stream is absent after the restart.** Samples from 05:21 to 05:33Z on
  2026-10-11 caught no default-sort label scan. The session-label scans they
  caught were created-desc, the shape of the closed named-session index
  build, which the running binary does not yet cache.

### What the analysis observed at 17:05Z

The analysis saw one connection re-issue the label scan about every ten
seconds, each run reaching nine seconds. The doctor's check produces exactly
that pattern once its scans reach the deadline. The trace also shows a 20.5 s
index build ending at 17:05:38Z. Which of the two the analysis sampled cannot be
recovered from what was kept.

## What changed

### gc-k0n2xi

- **The controller keeps the closed named-session index between passes.**
  One `closedNamedIndexCache` (`cmd/gc/allocator_demand_reads.go`) serves each
  `CityRuntime`. The entry point creates it and hands it to the desired-state
  build function and to the runtime, which reads through it in the
  control-dispatcher tick and in the v2 external-reads lane. The
  control-dispatcher tick narrows the config to the dispatcher agents but keeps
  every named session, so an `on_demand` one sends each of its desired-state
  builds to the index as well. It builds without a trace, so its index builds
  are not among the ones the trace counted.
  The cache answers from the last complete build until one of these happens: a
  bead event carries a closed named-session bead the index can find (a close as
  failed-create is not one), the event feed reports a gap, a named session the
  cache saw open leaves the reader's open-session snapshot, the store changes,
  the snapshot is degraded or absent, or the index is ten minutes old. The controller's bead event feed carries its own writes and the
  closes other gc processes announce, so it reports a named session that opened
  and closed between two passes, which no snapshot shows. One build runs at a
  time, outside the cache's lock, so an event never waits on a store read. A
  failed build reaches its reader unchanged and is not kept.
  The index only adds runtime-name assignees for on_demand identities, so a
  stale entry costs one extra ready probe. The change that would hide demand
  is a named session closing, and the event feed and the open-set check both
  report it.
- **`session.Store.List` reads closed rows only when its filter can keep one**,
  for `all` or a list that names `closed`. Every other filter already dropped
  each closed row in memory, so results are unchanged, and the doctor's
  per-template listing reads open sessions only.
- **The cached beadmail provider holds an enumeration's outcome, a failure as
  well as a success, for its refresh interval after the enumeration returns.**
  A store too slow to answer is asked again thirty seconds after its last
  answer. It is not asked on every recipient lookup, and not again straight
  after an enumeration that used up the interval. A failed refresh is logged,
  and alias-history routes keep using the last enumeration that succeeded; the
  failure reaches the lookup only while no enumeration has succeeded.

### gc-oh4e2p

- **The same-workdir check reads only the sessions that share the
  directory.** `sameWorkDirSessionBeads` puts a `work_dir` metadata filter
  beside the session label, so the store returns the sessions that share the
  target's directory, closed ones included for a closed target, and no others.
  The classification is unchanged. No index covers `work_dir`, so the server
  still reads each session-labeled row to test the filter, but the rows that
  fail it are no longer sorted, sent or hydrated with their labels and
  dependencies. Every path that resolves a transcript through the session
  manager reads less, among them the usage lane's sweeps, `gc session logs`
  and the API's transcript, stream and history handlers.
- **The usage lane retries a closed session whose accounting has not settled
  at most once every 10 minutes, and drops it 24 hours after its close**
  (`closedUsageRetryInterval`, `closedUsageRetryWindow`). The 24 hours run from
  `closed_at`, else from the bead's update time, else from the first retry that
  found the session unsettled. The city store's beads carry no update time, so
  lx-wisp-e9yi1 would take the last of these. Each attempt writes the compute
  fact whether or not the model sweep settles, so a drop gives up the
  model-usage tail of a transcript that never resolved, and the compute fact
  too only when the sink refused every write. The cadence and the horizon are
  those of the closed-session backstop in upstream gastownhall/gascity#6908,
  which reaches unsettled closed sessions through a periodic list of closed
  session beads rather than an in-memory set. Upstream main carries neither
  lane.

## What this does not change

- **The stateless beadmail provider** still enumerates on every alias-history
  lookup. `TestProvider_DefaultProviderSeesNewHistoricalAliasSessionAcrossCalls`
  pins that contract, and its one production use found, the store-maintenance
  alert mail, sends without resolving recipients through the history.
- **The hourly detached-orphan route index, the doctor's executor-identity
  residue check and the startup name-claim sweep** still read closed sessions.
  The first two need them, and none runs per pass.
- **The keyed Claude lookup still misses a transcript written outside the
  `work_dir` folder.** gc-ysf8tb fixes that lookup; its pull request, #215,
  was open on 2026-10-11. Until it lands, such a session's model-usage tail
  goes unbilled, and its retries stop after 24 hours.
- **An open session in a compute-terminal state whose sweep does not settle**
  is still retried on every pass by the lane's open-snapshot scan. Its check
  reads only open sessions that share its `work_dir`, and the retries end when
  the session wakes or closes.
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
- Retry copies: Python over `.gc/usage.jsonl`, counting compute facts per
  `idempotency_key` with the first and last `at` of each.
- Retried sessions: `gc bd show` on each session the sink names, and
  `ls /Users/gc/.claude/projects/*/<session_key>.jsonl` for its transcript.
- Untraced tick time: per `tick_id`, the `bead_reconcile_tick` duration minus
  the summed durations of its `bead_reconcile.*` sub-phases, over every
  2026-10-10 and 2026-10-11 segment file.
- Samples after the restart: the processlist sampler above, keeping the last
  700 characters of each session-label query so the `WHERE` and `ORDER BY`
  clauses survive.
