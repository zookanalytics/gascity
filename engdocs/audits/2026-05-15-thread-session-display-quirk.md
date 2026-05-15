# Audit: thread session display quirk — pane-alive + bead-asleep ghosts

**Bead:** `gc-a8pbb`
**Date:** 2026-05-15
**Auditor:** `gascity/gc-toolkit.polecat` (session `lx-qqd6rr`, alias
`gascity/gc-toolkit.nux`)
**Scope:** operator-facing session-listing surfaces in `cmd/gc/`,
`internal/api/`, `cmd/gc/dashboard/web/`
**Out of scope:** Applying fixes; fixing the underlying state-tracking
divergence that flips live sessions to `state=asleep`.

## TL;DR

The operator looked at a dashboard panel (Crew, Status, or the command
palette) while three mayor/mechanik thread sessions were alive in tmux,
and saw only one. The other two were not gone — they were classified
`state=asleep` in the bead metadata (despite live tmux panes), and
**all three dashboard panels that list sessions hardcode `?state=active`
on the API query**, so any asleep-but-actually-alive session is invisible
there. Surfaces that show all non-closed states (`gc session list`,
`gc session list --json`) did show all three; the operator confirmed
this on the JSON form.

The display quirk traces back to the state-tracking divergence
(`metadata.state` saying asleep while tmux says alive). Per the bead,
that's out of scope here — but the display surfaces that filter to
`state=active` are themselves architecturally fragile: when the
ground truth diverges, those panels silently lie. They have no
visual indicator for "X sessions hidden by state filter".

## Findings per investigation item

### 1. State-tracking divergence (referenced from bead description)

**Confirmed; root cause for the display gap; out of scope to fix.**

In `internal/session/manager.go:1298-1305`, the `state` returned to
callers is derived as:

```go
state := normalizeInfoState(State(b.Metadata["state"]))
if closed {
    state = ""
} else if m.sp != nil && state == StateActive && !m.sp.IsRunning(sessName) {
    // Surface stale "awake" / "active" beads as dormant immediately.
    state = StateAsleep
}
```

Two paths reach `state=asleep`:

- **Persistent**: `metadata.state` is already `asleep` in the bead — the
  reconciler wrote it (e.g., during a prior gap where `IsRunning` said
  the pane was gone). Nothing about a later nudge automatically heals
  this back to `active`; the pane responds, the bead stays asleep.
- **Transient**: `metadata.state` is `active`/`awake` but `IsRunning`
  returns false at query time. This is the `state_cache.go` line —
  default `cacheTTL=2s`, `staleTTL=30s`, and after `staleTTL` of
  failed `tmux list-panes -a` refreshes the cache returns `false` for
  **all** sessions.

`LastActive=0001-01-01T00:00:00Z` (zero-value) is set whenever
`state != StateActive` (after the flip). See
`manager.go:1333-1337` — `LastActive` is **only** populated for
`StateActive`. So a ghost-asleep session always has zero `LastActive`,
which is itself a signal a downstream surface could use to detect
"never had recorded activity".

The bead description's "2 of 3 ghost-asleep, 1 active" pattern is
consistent with the *persistent* path: the two ghosts were marked
asleep at some earlier moment of cache-staleness or controller
heal, and they never got promoted back; the one that was correctly
reported `active` happened to be queried while either its bead
metadata was already `active`/`awake` *and* the cache had a fresh
hit for its session name.

### 2. `gc session peek` mismatch ("session not found" while `tmux capture-pane` works)

**Confirmed; same root cause as #1.**

`cmd/gc/cmd_session.go:1498` (`cmdSessionPeek`) →
`internal/worker/handle_lifecycle.go:168-173` (`SessionHandle.Peek`) →
`internal/session/manager.go:1274` (`Manager.Peek`):

```go
func (m *Manager) Peek(id string, lines int) (string, error) {
    b, sessName, err := m.loadSessionBead(id, true)
    if err != nil { return "", err }
    if m.sp == nil { return "", fmt.Errorf("%w: %s", ErrSessionInactive, id) }
    if !m.sp.IsRunning(sessName) {
        return "", fmt.Errorf("%w: %s", ErrSessionInactive, sessName)
    }
    return m.sp.Peek(sessName, lines)
}
```

So `gc session peek` checks `IsRunning` (cached) **before** asking
tmux to capture the pane. If the cache says "not running", peek
fails with `ErrSessionInactive`. Raw `tmux capture-pane -t <name>`
talks directly to tmux and does not consult any cache, so it
succeeds whenever the pane actually exists.

The error string surfaced to the operator includes
`ErrSessionInactive` (`"session not active"`) rather than literal
"session not found"; the bead author may be paraphrasing, or there's
a separate not-found path (e.g., `ErrSessionNotFound` from
`cmd/gc/session_resolve.go:178`) hit if the operator passed an alias
form the resolver couldn't find. The behavior is consistent either
way: the cache is the gate, tmux is not consulted.

### 3. tmux window/session listing pagination or truncation

**No evidence this contributed.** The operator's description doesn't
mention raw `tmux ls` / `tmux list-windows` / `choose-tree`. They
referenced `gc session list --json` (which showed all 3), so the
operator-facing surface that lied was a downstream UI, not tmux
itself. The tmux `list-panes -a` call inside `FetchRunning`
(`internal/runtime/tmux/state_cache.go:162-197`) doesn't paginate
or truncate — it streams all panes from the server in one call —
so this branch is unlikely.

If we later learn the operator was using a TUI/status-line driven
by `tmux list-sessions` etc., this would need revisiting. As of
this audit, no in-tree `cockpit.sh` or TUI panel that lists
thread sessions exists (only test fixtures reference the name).

### 4. Cockpit / dashboard panels filter logic

**Confirmed; this is the proximate cause of the display gap.**

Three frontend surfaces call `/v0/city/{cityName}/sessions` with
`query: { state: "active", peek: true }` hardcoded — they will
never show `state=asleep`, `state=suspended`, or `state=""` (closed)
sessions:

- `cmd/gc/dashboard/web/src/panels/crew.ts:35-37` (Crew panel)
- `cmd/gc/dashboard/web/src/panels/status.ts:43-47` (Status panel /
  "stuck agents" / "dead sessions" counts derived from
  `sessions.filter(s => !s.running)` — but only over the
  state=active subset)
- `cmd/gc/dashboard/web/src/palette.ts:60-62` (command palette
  agent lookup)

The API itself (`internal/api/huma_handlers_sessions_query.go:20-32`
and the handler that backs `ListFullFromBeads` in
`internal/session/manager.go:1228-1271`) does not impose any state
filter when the query parameter is empty — it's the frontend that
sets the filter. The OpenAPI surface exposes `state` as a request
parameter; the frontends opt in to `state=active`.

The Status panel is the most operator-misleading: it derives the
running agents count from
`statusR.data?.agents.running ?? sessions.filter((session) => session.running).length`
(`status.ts:90`) and `deadSessions = sessions.filter(s => !s.running).length`
(`status.ts:87`). Both fall over the `state=active` pre-filter, so
ghost-asleep sessions are neither "running" nor counted as "dead" —
they're invisible.

The Crew panel additionally filters by `agent_kind === "crew"`
(`crew.ts:50`). Mayor and mechanik **threads** are
`adhoc`-style sessions whose `agent_kind` classification is not
guaranteed to be `crew`; whether they would appear on the Crew
panel at all depends on the server-side `agent_kind` derivation.
The pre-existing `state=active` filter masks this — by the time
ghost-asleep sessions are excluded, the `agent_kind` filter never
gets a chance to apply.

### 5. Multi-tmux-server case

**Plausible in general but unlikely to be the cause here.**

The runtime tmux provider supports per-city socket isolation via
`Config.SocketName` (`internal/runtime/tmux/tmux.go:53-56`). When
set, all gc-issued tmux commands use `tmux -L <socket> ...`. An
interactive operator shell that does not inherit `GC_TMUX_SOCKET`
will talk to the *default* tmux server, so raw `tmux ls` would not
see city-socket sessions.

However, the bead description says
`tmux capture-pane` on the same target name worked normally for the
"ghost" sessions, which means the operator's shell was in fact
talking to the same server gc uses. So a socket mismatch is not the
explanation in this incident.

Worth flagging for the future: if `IsRunning` cache failures coincide
with a wrong tmux socket, the surface symptoms (peek fails, dashboard
hides, `tmux capture-pane -t X` works from one shell and not another)
look very similar to today's incident. Future investigations should
verify `GC_TMUX_SOCKET` propagation early.

### 6. `gc session list` default filter

**No filter beyond closed-exclusion; both human and JSON forms agree.**

`internal/session/manager.go:1228-1271` (`ListFullFromBeads`) with an
empty `stateFilter` excludes only `b.Status == "closed"` (lines
1256-1261). All non-closed beads — `state=active`, `state=asleep`,
`state=suspended`, `state=creating`, `state=draining`, `state=awake`,
etc. — pass through.

The CLI handler `cmd/gc/cmd_session.go:650-754` calls the same path
for both table and `--json` output, with identical filter
semantics:

- JSON form (`sessionListJSONRows`, lines 779-809): all fields of
  `session.Info` including `LastActive` zero-value rendered as
  `"0001-01-01T00:00:00Z"`.
- Table form (lines 730-752): same set of sessions, with `LAST ACTIVE`
  column rendered as `"-"` when `LastActive.IsZero()`. The `REASON`
  column (computed by `sessionReason`, lines 919-960) does NOT
  exist in JSON.

Conclusion: `gc session list` (table) would also have shown all three
to the operator. The reason an operator could still believe "1-2
quit" while looking at the table form: STATE=asleep + LAST ACTIVE="-"
+ a REASON column showing something like a sleep reason all read as
"this thing isn't running right now", visually similar to a dead
session.

## Likely root-cause candidate (display side)

**The dashboard panels' `state: "active"` filter is the load-bearing
display bug.** When the underlying state-tracking diverges (item 1),
the dashboard has no recourse: it has already filtered out everything
not in active state. There is no visual indicator for "N sessions
hidden because they're in non-active states". The operator's display
surface therefore looks like the asleep sessions ceased to exist.

This is independent of the state-tracking root cause: even if state
tracking were perfect, the panels would still drop legitimately
`state=suspended` and `state=creating` sessions silently. A
secondary surface (e.g., a "Ghosts" or "All sessions" tab) or a
counter ("3 hidden non-active") would close this class of report
without requiring the deeper fix.

## Reproducer recipe

Goal: cause a session whose tmux pane is alive to appear missing from
operator dashboard surfaces, while `gc session list --json` still
shows it.

```bash
# 1. Pick or create a live session in a running city.
SESS=$(gc session list --json | jq -r '.[] | select(.State=="active") | .ID' | head -1)
test -n "$SESS"

# 2. Capture baseline.
gc session list --json | jq ".[] | select(.ID==\"$SESS\") | {ID, State, LastActive}"
# Expect: {State: "active", LastActive: "<recent timestamp>"}

# 3. Manually flip the bead's metadata.state to asleep (simulates what
#    the reconciler would do if it observed a brief no-pane gap).
gc bd update "$SESS" --set-metadata state=asleep

# 4. Verify the pane is still alive in tmux.
SESSNAME=$(gc bd show "$SESS" --json | jq -r '.[0].metadata.session_name')
tmux -L <city-socket> capture-pane -p -t "$SESSNAME" | head -5
# Expect: live pane output.

# 5. Verify the divergence shows on the CLI.
gc session list --json | jq ".[] | select(.ID==\"$SESS\") | {ID, State, LastActive}"
# Expect: {State: "asleep", LastActive: "0001-01-01T00:00:00Z"}

# 6. Observe the dashboard.
#    Open the dashboard web UI (gc dashboard / `make dashboard-check`),
#    inspect the Crew, Status, and command-palette panels. The session
#    is gone from all three.
#
#    Alternatively, hit the API directly with the same query the
#    frontend uses:
gc dashboard sessions --state=active --json 2>/dev/null \
  || curl -s "http://localhost:<port>/v0/city/<city>/sessions?state=active&peek=true" \
       | jq ".items[] | select(.id==\"$SESS\")"
# Expect: empty (session missing because state filter is active).

#    For contrast, query without the state filter:
curl -s "http://localhost:<port>/v0/city/<city>/sessions?peek=true" \
  | jq ".items[] | select(.id==\"$SESS\") | {id, state, last_active}"
# Expect: present with state=asleep.

# 7. Verify `gc session peek` lies too (because of cache + IsRunning gate).
gc session peek "$SESS"
# May succeed (cache says running) or fail with ErrSessionInactive.
# To force the failure path, evict the cache:
#   tmuxProvider.cache.Invalidate()   # not exposed via CLI; race the cache instead.

# 8. Cleanup: restore.
gc bd update "$SESS" --set-metadata state=active
```

Step 7's failure mode is the harder one to reproduce on demand
because the cache TTL (default 2s) refreshes quickly and there is no
public CLI to invalidate it. The persistent variant (step 3-6) is
fully deterministic and demonstrates the operator-facing display
gap with no race.

## References

- `internal/session/manager.go:1228-1340` — state derivation and
  Info enrichment.
- `internal/runtime/tmux/state_cache.go` — IsRunning cache TTL and
  staleness behavior.
- `cmd/gc/dashboard/web/src/panels/crew.ts`,
  `cmd/gc/dashboard/web/src/panels/status.ts`,
  `cmd/gc/dashboard/web/src/palette.ts` — frontend `state=active`
  filters.
- Memory: `project_ghost_sessions.md` — the reverse case
  (bead-active + no pane) from 2026-05-01.
