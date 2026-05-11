# Survey: gascity session/agent-lifecycle for `gc-h1gxg` decision

**Bead:** `gc-upid5` (research) under `gc-h1gxg` (decision)
**Branch:** `gc-upid5-session-survey`
**Surveyed at:** 2026-05-10
**Worktree:** `/home/zook/loomington/.gc/worktrees/gascity/polecats/gc-toolkit.slit`
**origin/main:** `d11ee0e1` (zookanalytics/gascity)
**upstream/main:** `0a2713dd` (gastownhall/gascity)

This survey is research only. It does not recommend a framing — that
decision sits with mechanik and the operator after this lands.

## Provenance

| Doc-type or artifact | Producer (skill / concept / workflow step) | Source location | Surveyed at |
|---|---|---|---|
| Filing bead | `gc-toolkit__mechanik` (dispatcher) | bd `gc-h1gxg`, parent of `gc-upid5` | 2026-05-10 |
| Survey bead | `gc-toolkit__mechanik` (filer) | bd `gc-upid5` | 2026-05-11 |
| `gc handoff` source | gascity cobra command | `cmd/gc/cmd_handoff.go` @ `d11ee0e1` (origin/main) | 2026-05-10 |
| `gc handoff` Reset path | gascity worker boundary | `internal/worker/handle_lifecycle.go:102` `SessionHandle.Reset` (calls `h.manager.RequestFreshRestart(id)` at `:111`; the manager method is at `internal/session/manager.go:787`) @ `d11ee0e1` | 2026-05-10 |
| `gc handoff` Kill path | gascity session manager | `internal/session/manager.go:877` `Manager.Kill` (calls `m.sp.Stop` at `:894`) @ `d11ee0e1` | 2026-05-10 |
| Final tmux kill primitive | gascity runtime/tmux | `internal/runtime/tmux/tmux.go:426` `KillSession` (executes `tmux kill-session -t <name>`) @ `d11ee0e1` | 2026-05-10 |
| Process-tree reaping wrapper | gascity runtime/tmux | `internal/runtime/tmux/tmux.go:452` `KillSessionWithProcesses` @ `d11ee0e1` | 2026-05-10 |
| `gc session reset` source | gascity cobra command | `cmd/gc/cmd_session_reset.go:69` @ `d11ee0e1` | 2026-05-10 |
| `gc session kill` source | gascity cobra command | `cmd/gc/cmd_session.go:1508` @ `d11ee0e1` | 2026-05-10 |
| Session reconciler restart detection | gascity controller | `cmd/gc/session_reconciler.go:1031-1085` (`restart_requested`, `continuation_reset_pending`) @ `d11ee0e1` | 2026-05-10 |
| Restart-flag patch builder | gascity session lifecycle | `internal/session/lifecycle_transition.go:283-296` `RestartRequestPatch` @ `d11ee0e1` | 2026-05-10 |
| Session-name derivation | gascity agent helper | `internal/agent/session_name.go:53-89` `SessionNameFor` @ `d11ee0e1` | 2026-05-10 |
| Pool instance naming | gascity supervisor | `internal/agentutil/pool.go:172` `PoolInstanceName`; `cmd/gc/build_desired_state.go:2203` `poolInstanceName` (and call site at `cmd/gc/pool.go:429`) @ `d11ee0e1` | 2026-05-10 |
| Pool-session tmux name | gascity supervisor | `cmd/gc/pool_session_name.go:22` `PoolSessionName` (returns `{basename(template)}-{beadID}`) @ `d11ee0e1` | 2026-05-10 |
| Pool expansion check | gascity config | `internal/config/config.go:2214` `SupportsInstanceExpansion`, `:613` `max_active_sessions` field tag @ `d11ee0e1` | 2026-05-10 |
| Mail ambiguity resolver | gascity mail | `cmd/gc/cmd_mail.go:643` `resolveLiveConfiguredNamedMailTargetCached` @ `d11ee0e1` | 2026-05-10 |
| Session bead schema | gascity session | `cmd/gc/session_beads.go:22-25` (type="session", label="gc:session"); metadata: `agent_name`, `session_name`, `alias`, `pool_slot`, `generation` @ `d11ee0e1` | 2026-05-10 |
| Runtime env injection | gascity session | `internal/session/lifecycle.go:31-78` (`GC_SESSION_ID`, `GC_SESSION_NAME`, `GC_ALIAS`, `GC_AGENT`, `GC_SESSION_ORIGIN`) @ `d11ee0e1` | 2026-05-10 |
| Salvage path (work-dir resolution) | gascity reconciler | `cmd/gc/session_reconciler.go:2206` `resolveTaskWorkDir` @ `d11ee0e1` | 2026-05-10 |
| `respawn-pane` definitions | gascity runtime/tmux | `internal/runtime/tmux/tmux.go:2838` `RespawnPane`, `:2846` `RespawnPaneWithWorkDir` @ `d11ee0e1` | 2026-05-10 |
| Session-model design (accepted) | gascity engdocs | `engdocs/design/session-model-unification.md` @ `d11ee0e1` | 2026-05-10 |
| Session-lifecycle cleanup plan | gascity engdocs | `engdocs/design/session-lifecycle-domain-cleanup-plan.md` @ `d11ee0e1` | 2026-05-10 |
| Agent-pools design (implemented) | gascity engdocs | `engdocs/design/agent-pools.md` @ `d11ee0e1` | 2026-05-10 |
| Named-configured-sessions design | gascity engdocs | `engdocs/design/named-configured-sessions.md` @ `d11ee0e1` | 2026-05-10 |
| Related-but-distinct bead | bd | `gc-ljkvi` "Named-session crash detection gap: dead panes sit silent" | 2026-05-10 |
| Scratch-clone overlay script | gc-toolkit pack (read-only cross-rig reference) | `/home/zook/loomington/rigs/gc-toolkit/assets/scripts/tmux-spawn-scratch.sh` lines 1-87 | 2026-05-10 |
| Scratch-clone guard fragment | gc-toolkit pack (read-only cross-rig reference) | `/home/zook/loomington/rigs/gc-toolkit/template-fragments/scratch-clone-guard.md` | 2026-05-10 |
| Prior overlay audit (tk-my4za) | gc-toolkit polecat | `/home/zook/loomington/rigs/gc-toolkit/specs/tk-my4za/reset-vs-handoff-audit.md` | 2026-05-10 |
| Handoff skill | gc-toolkit pack (loaded into this session) | `/home/zook/loomington/rigs/gc-toolkit/skills/handoff/SKILL.md` | 2026-05-10 |
| Handoff path at audit SHA | gascity historical | `cmd/gc/cmd_handoff.go` @ `c7cd79f9` (`feat(config): cache parsed pack tree…`) — `handle.Reset` and `workerKillSessionTargetWithConfig` already present; no `RespawnPane` call | 2026-05-10 |

## Reading guide

Everything in §1–§5 is gascity-native (the Go binary). §2 ("scratch-clone
model") is where the survey leaves gascity briefly to point at the overlay
that owns the concept — citations to `/home/zook/loomington/rigs/gc-toolkit/`
are read-only references to an external rig, included because the operator's
incident touches both layers and §6 ("upstream movement") cannot reason
about overlap without naming the overlay's in-flight work.

## 1. Restart code path

**Punchline.** Every restart entry point — `gc handoff` (self), `gc handoff
--target X`, `gc session reset`, `gc session kill` — converges on the same
final operation: `tmux kill-session -t <name>` at
`internal/runtime/tmux/tmux.go:426`, wrapped by `KillSessionWithProcesses`
(`:452`) which first SIGTERMs the pane's process group and descendants then
SIGKILLs after a 2 s grace period (`processKillGracePeriod`, `:433`).
**`tmux kill-session` is session-scoped**: all panes and windows in the
session die. There is no `kill-pane` invocation anywhere in the
`internal/runtime/tmux/` package (`grep -n "kill-session\|kill-pane\|kill-window"
internal/runtime/tmux/tmux.go` returns one hit, line 426). The operator's
claim is **confirmed** by code inspection.

**`gc handoff` (self):**
1. `cmd/gc/cmd_handoff.go:113` → `doHandoffWithOutcome`
2. `:204` → `dops.setRestartRequested(sessionName)` (in-memory runtime flag)
3. `:211` → `persistRestart()` → `sessionRestartPersister` (`:156-167`) →
   `workerHandleForSessionTargetWithConfig` → `handle.Reset(ctx)`
4. `internal/worker/handle_lifecycle.go:102` `SessionHandle.Reset` calls
   `h.manager.RequestFreshRestart(id)` at `:111`; the manager method
   (`internal/session/manager.go:787`) patches the session bead with
   `restart_requested=true` and `continuation_reset_pending=true`
   (lines `:793-794`). The patch builder used later on the reconciler
   side is `internal/session/lifecycle_transition.go:283`
   `RestartRequestPatch`
5. `cmd/gc/cmd_handoff.go:123` → `waitForControllerRestart` polls until
   the controller clears the flag (bounded by `controllerRestartTimeout(cfg)`)
6. Controller side: `cmd/gc/session_reconciler.go:1047` reads
   `Metadata["restart_requested"]=="true"`; at `:1050` calls
   `workerKillSessionTargetWithConfig` → `handle.Kill(ctx)` →
   `internal/worker/handle_lifecycle.go:129` → `internal/session/manager.go:894`
   `m.sp.Stop(sessName)` → `internal/runtime/tmux/adapter.go:197`
   `KillSessionWithProcesses(name)` → `internal/runtime/tmux/tmux.go:452` →
   call to `KillSession(name)` at `:508` → **`KillSession` body at `:426`
   runs `tmux kill-session -t <name>`**

**`gc handoff --target X` (remote):**
- `cmd/gc/cmd_handoff.go:153` → `doHandoffRemote` (`:332`) →
  `workerKillSessionTargetWithConfig("", store, sp, nil, sessionName)` →
  same final kill primitive at `internal/runtime/tmux/tmux.go:426`.

**`gc session reset`:**
- `cmd/gc/cmd_session_reset.go:69` → `workerHandleForSessionWithConfig` →
  `handle.Reset(ctx)` at `:88` → same `RequestFreshRestart` →
  reconciler-driven `kill-session` (same path as self-handoff).

**`gc session kill`:**
- `cmd/gc/cmd_session.go:1508` → `workerHandleForSessionWithConfig` →
  `handle.Kill(ctx)` at `:1514` → `internal/worker/handle_lifecycle.go:129` →
  `manager.Kill(id)` → `sp.Stop(name)` → same final kill primitive.
- Difference from `Reset`/handoff: `Kill` skips the restart-flag dance —
  the reconciler will *not* respawn the session, because no `restart_requested`
  metadata is set.

**`gc agent restart`:**
- Does not exist. `cmd_agent.go` defines only `add`, `suspend`, `resume`.
  Per the session-first migration committed in `dd90ac0a` (Mar 8 2026,
  recorded in CLAUDE.md "Active migrations"), restart is owned by
  `gc session reset` and `gc handoff`. `internal/agent/` is helper code,
  not a primitive.

**Reconciler restart semantics.** The reconciler does **not** run a paired
"stop-then-start" operation. It detects `restart_requested`, calls `Kill`,
rotates `session_key` (`session_reconciler.go:1063`), clears
`started_config_hash` (`:1059-1062`), bumps a continuation epoch
(`RestartRequestPatch`, `:1064`). The next reconcile tick observes the
dead session and re-wakes it via the standard "session bead says
`pool_desired ≥ 1` or `mode = always`, no live tmux session ⇒ create
session" path.

**A note on `tmux respawn-pane`.** `internal/runtime/tmux/tmux.go:2838`
defines `RespawnPane` and `:2846` defines `RespawnPaneWithWorkDir`. Both
are exported. Both have **no callers outside the `internal/runtime/tmux/`
package** in production code. `grep -rn "Respawn" --include="*.go" cmd/
internal/` outside `internal/runtime/tmux/` returns one hit
(`cmd/gc/session_reconciler.go:1413`), and it is a comment about the
*named-session respawn circuit breaker* — meaning "spawn a fresh session
after the old one crashed", not the tmux `respawn-pane` primitive. The
`Respawn*` methods exist in the tmux adapter (imported from gastown
upstream at `307e5a14`) but the gascity controller does not wire them
into the restart path.

### Summary table

| Entry point | First in-process call | Worker-boundary method | Final tmux operation | Pane vs session |
|---|---|---|---|---|
| `gc handoff` (self) | `cmd_handoff.go:113` `doHandoffWithOutcome` | `handle.Reset` → `RequestFreshRestart` (reconciler then calls `handle.Kill`) | `tmux kill-session -t <name>` (`tmux.go:426`) | **Session** (all panes die) |
| `gc handoff --target X` | `cmd_handoff.go:153` `doHandoffRemote` | `workerKillSessionTargetWithConfig` → `handle.Kill` | `tmux kill-session -t <name>` (`tmux.go:426`) | **Session** |
| `gc session reset` | `cmd_session_reset.go:69` | `handle.Reset` → `RequestFreshRestart` | `tmux kill-session -t <name>` (`tmux.go:426`) | **Session** |
| `gc session kill` | `cmd_session.go:1508` | `handle.Kill` (no restart flag) | `tmux kill-session -t <name>` (`tmux.go:426`) | **Session** (no respawn) |
| `gc agent restart` | n/a — command does not exist | n/a | n/a | n/a |
| (unused) `RespawnPane` | n/a — no caller in cmd/ or internal/ outside `internal/runtime/tmux/` | n/a | `tmux respawn-pane -k -t <pane>` (`tmux.go:2839`) | (would be pane-scoped if wired) |

## 2. Scratch-clone model

**Punchline.** The "scratch clone" concept is **not gascity-native.** Inside
`/home/zook/loomington/.gc/worktrees/gascity/polecats/gc-toolkit.slit`
(this repo, the gascity SDK), `grep -rn -i "scratch_clone\|scratchclone\|
registered.*scratch\|scratch.*registered" --include="*.go" --include="*.md"`
returns nothing. The string "scratch" appears in the Go source only as
"the agent's scratch space" — meaning the `<workDir>/.gc/tmp` filesystem
directory used for prompt files (`internal/runtime/tmux/adapter.go:992-998,
1008`). That is a *temp-file* scratch, not a *clone-agent* scratch.

**Where the concept actually lives** is the gc-toolkit overlay (loaded into
this session via the pack at `/home/zook/loomington/rigs/gc-toolkit/`):

- **`assets/scripts/tmux-spawn-scratch.sh`** — the script bound to
  `prefix + a` in tmux that opens a new window in the *same tmux session*
  as the canonical (registered) named-crew agent, running an unregistered
  `claude` with the same persona prompt and cwd (lines 30-87). The
  scratch is marked with `GC_SCRATCH=1` and is **not** added to the bead
  store: "no wisp, no mail/nudge delivery, no respawn lifecycle" (lines
  11-12 of the script).
- **`template-fragments/scratch-clone-guard.md`** — the prompt fragment
  appended to the scratch's persona: "You are a SCRATCH CLONE of
  <agent-name>, spawned for ad-hoc questions" (line 1).

**Problem it was designed to solve** (per the script's own header
comments, lines 1-23): give the operator a side-channel for ad-hoc
questions to a coord agent without polluting the registered agent's
transcript or pulling its mail/nudges. By co-locating the scratch in the
same tmux session, the operator can quickly switch with tmux window keys
and the scratch inherits the agent's cwd automatically.

**Deliberate or emergent.** Deliberate at the overlay layer. The
sibling-window model is an explicit choice documented in
`/home/zook/loomington/rigs/gc-toolkit/specs/tk-my4za/reset-vs-handoff-audit.md`
(authored 2026-05-07), which catalogs every gc-toolkit instructional surface
that mentions `gc session reset` vs `gc handoff`. **However**, that audit's
"decision rule" (lines 8-18) and the spawn-script's own comment (lines
12-19) both rest on the premise that `gc handoff` is **pane-scoped**
(uses `respawn-pane` on `:^.0` only). §1 of this survey shows that premise
is false today: `gc handoff` calls `kill-session`, not `respawn-pane`. The
audit cites `gascity/internal/runtime/tmux/tmux.go:2833` in its provenance
table, but I have not found any call site that wires `RespawnPane` to the
handoff path. Verifying against the SHA the audit pinned (`c7cd79f9`,
`feat(config): cache parsed pack tree…`), `cmd/gc/cmd_handoff.go` at that
revision already calls `handle.Reset` and `workerKillSessionTargetWithConfig`
— the same path as today. So the misaligned premise predates current main.

**Sibling bead.** `tk-mjvm9` ("Move scratch clones back into the agent's
main tmux session") is referenced from the tk-my4za audit (lines 115, 173,
208) as a planned follow-up. The bead text is in the gc-toolkit bead store
(not the gascity bead store, where this survey runs), so I have not read
it directly; it appears to acknowledge the sibling-session model is in
flux. That's a complementary motion, not a duplicate of `gc-h1gxg`.

## 3. Identity primitives

For each identifier: **what it is**, **where it lives**, **stability across
a session restart**, and **uniqueness scope**.

1. **Session name** (`internal/agent/session_name.go:53-89` `SessionNameFor`).
   tmux-safe string derived deterministically from the qualified agent name
   (`/` → `--`, `.` → `__`). Examples: `mayor`, `gascity--gc-toolkit.polecat`,
   `myrig--claude__1`. **Stable** across restart for the same template; only
   changes if the agent name or template changes. **City-wide** unique (per
   tmux socket).

2. **Tmux pane id.** Not a persisted identity. Resolved dynamically at
   runtime via `tmux list-panes` keyed by session name
   (`internal/runtime/tmux/tmux.go:1586` runs `list-panes -s -t <session>`;
   `:1708` `sessionPanesDead` does the same for dead-pane detection). The
   `StateCache` in `internal/runtime/tmux/state_cache.go` (functions from
   `:52` onward) memoizes the session-running set, not pane ids.
   **Ephemeral** — regenerated on each tmux server restart. Never written
   to a bead.

3. **Bead assignee.** The `Bead.Assignee` field
   (`internal/beads/beads.go:27`). Carries either the session bead ID, the
   alias, or the qualified agent name (e.g., `mayor`,
   `myrig/claude-1`), depending on who set it (sling, CLI `bd update
   --assignee=`, automatic claim). **Stable** across restart — the field
   persists; the destination resolves on each lookup.

4. **Environment variables** (`internal/session/lifecycle.go:31-78`):
   - `GC_SESSION_ID` — bead ID of the *current* session bead;
     regenerated per incarnation only on hard recreate. Carries
     `GC_INSTANCE_TOKEN` for fencing.
   - `GC_SESSION_NAME` — see (1); **stable**.
   - `GC_ALIAS` — public-facing alias (`:42-48`); mutable via
     `gc session alias`; can be empty.
   - `GC_AGENT` — derived from `GC_ALIAS` if present else
     `GC_SESSION_NAME` (`:62-64`).
   - `GC_RIG` — rig binding (`cmd/gc/work_query_probe.go:55-62`); empty
     for city-wide agents.
   - `GC_SESSION_ORIGIN` — `named` / `adhoc` / `ephemeral` (`:59`).

   Of these, only `GC_SESSION_ID` is allowed to change across restart in
   the common case; the others are derived from configuration and remain
   stable.

5. **Supervisor pool id.** `internal/agentutil/pool.go:172`
   `PoolInstanceName` (exported helper); `cmd/gc/build_desired_state.go:2203`
   `poolInstanceName` (the local helper used by the desired-state
   builder); call site at `cmd/gc/pool.go:429`. A pool member is
   identified by a concrete instance name — either a themed name from
   `namepool_names` (e.g., `furiosa`, `nux`, `slit`) or `{base}-{slot}`
   (e.g., `polecat-3`). The tmux session name is bead-derived:
   `cmd/gc/pool_session_name.go:22` `PoolSessionName` returns
   `{basename(template)}-{beadID}`. The session bead's metadata stores
   `pool_slot` (the slot index) and `agent_name` (the concrete name).
   **Stable** for the lifetime of the pool member; pool reconcile can
   replace a slot but the slot index is preserved.

6. **Session bead id.** Auto-generated UUID, type=`"session"`, label
   `gc:session` (`cmd/gc/session_beads.go:22-25`). Bead metadata holds
   `agent_name`, `session_name`, `alias`, `pool_slot`, `generation`.
   The `generation` counter (`GC_RUNTIME_EPOCH`) increments on each wake
   for NDI fencing. The bead ID itself **persists across normal
   restart**; hard-recreate replaces the bead.

7. **Agent bead.** A `type="agent"` bead exists in the data model
   (excluded from work queries at `internal/beads/beads.go:95`
   `readyExcludeTypes`). It is less central than the session bead and
   is not used for dispatch identity in the current code; primarily a
   role/configuration record.

8. **Alias.** See (4); stored both on the session bead and in the
   runtime env via `sp.SetMeta(sessionName, "GC_ALIAS", alias)`
   (`internal/session/lifecycle.go:78`). Conflict detection lives at
   `cmd/gc/session_beads.go` via `poolAliasConflictMetadataKey`.

### Canonical identity for mail/dispatch

The **session name** is the primary persistent identity for live lookup
(tmux target, env injection, reconciler matching). The **session bead
ID** is the persistent identity for bead store cross-references
(work-bead `assignee`, mail `recipient`, audit chain). The **alias**
(when present) is the public mail/dispatch target; the resolver in
`cmd/gc/cmd_mail.go:643`
(`resolveLiveConfiguredNamedMailTargetCached`) raises
`session.ErrAmbiguous` at `:688` when an alias matches multiple live
session beads — so
"who receives mail addressed to a role" is currently enforced to be
exactly one instance.

### What would have to change for N peer instances of a single role

- **Session name** already supports multiple instances of a template
  through the pool naming scheme (`PoolInstanceName`). No change needed.
- **Alias / mail routing** is the hard constraint. Today
  `cmd/gc/cmd_mail.go:643` `resolveLiveConfiguredNamedMailTargetCached`
  treats multiple live session-bead matches as an error
  (`ErrAmbiguous`). A peer-instance coord model would need either (a)
  one canonical instance to claim the role-level alias, or (b) a new
  resolver that picks an instance (round-robin / leader / latest) or
  fans out.
- **Bead assignee** — when work is filed for "the mayor" today, it goes
  to a specific bead/alias; under peer instances we need an analog of
  "pool work" (unassigned, routed via `metadata.gc.routed_to`, claimed by
  any available instance). The polecat-pool pull-claim pattern already
  works; the question is whether coord work can be pull-claimed or
  whether some coord state needs single-writer semantics.
- **`GC_AGENT` derivation** — if peer instances run, each instance's
  `GC_AGENT` will be `GC_ALIAS` (e.g., `mayor-1`), not the role name.
  Skills/prompts that read `GC_AGENT == "mayor"` to check role would
  need to read role from a different field (e.g., the agent-config
  template name).

## 4. Pre-existing peer-instance precedent

**Polecat pools are gascity's existing peer-instance precedent.**
`internal/config/config.go:613` declares `max_active_sessions` as a
field tag on the agent config struct. `:2214` `SupportsInstanceExpansion`
returns true when that value is `nil` (unbounded), `<0`, or `>1`, or
when `namepool_names` is set. The design rationale is in
`engdocs/design/agent-pools.md` (implemented per the design's status
note; commit history shows `agent-pools.md` and the session-model-
unification work in `0a79e9b8` "Session model unification phases 0-2
(#666)").

**How multiplicity is handled:**

- Pool members are spawned by the reconciler when the desired count
  (from `min_active_sessions` floor + `scale_check` script) exceeds the
  live count. The desired state is computed by
  `ComputePoolDesiredStates` (`cmd/gc/pool_desired_state.go:59`) and
  `computePoolDesiredStates` (`:78`); discovery for unbounded pools
  uses `discoverPoolInstances` (`cmd/gc/pool.go:421-475`).
- The tmux session name for a pool member is bead-derived:
  `cmd/gc/pool_session_name.go:22` `PoolSessionName` returns
  `{basename(template)}-{beadID}` (e.g., `polecat-mc-abc`). So *the
  session name is unique per session bead*, not per slot.
- Pool-instance human-facing identity (the `pool_slot`, `agent_name`,
  `alias`) lives in the session bead metadata, set by the supervisor
  when it creates the session bead.

**Dispatch into a pool is pull-based, not push-based.** Work routed to
`metadata.gc.routed_to=gascity/<role>.polecat` without a specific
instance is left for any pool member to claim via its `work_query`. The
reconciler does not pre-select an instance for the work. Mail to a pool
without an instance picker fails with `ErrAmbiguous` (see §3).

**Pool-member lifecycle.** Crash recovery is handled by
`selectRunningPoolSessionRefs` (`cmd/gc/pool.go:530`) which filters
desired pool sessions against running tmux sessions, plus
`releaseOrphanedPoolAssignments` (`cmd/gc/pool_session_name.go:71`)
which releases assigned work whose owning session disappeared. This
*does not* extend to named/coord sessions, which have `pool_desired=0`
and therefore no auto-respawn loop (see also bd `gc-ljkvi` for the
specific "named session crashed and nobody noticed" gap).

**Canonical-among-peers** machinery is **absent.** Searches for
`canonical`, `primary`, `leader`, `preferred`, `designated` in
`internal/config/`, `internal/agentutil/`, `internal/supervisor/`, and
`cmd/gc/` surface nothing pool-related — `internal/supervisor/registry.go:661`
is the lone hit and refers to canonical file paths. The
codebase treats pool members as fully homogeneous and uses an aliasing
conflict guard to enforce "if you address a named session, exactly one
instance answers."

**Could the pool pattern extend to coord agents (mayor/mechanik/deacon)
as-is?** *Spawn loop:* mechanically yes — the reconciler doesn't check
the role name. *Coordination:* not without new machinery. Mail routing
would break on the first peer-instance mayor because the alias resolver
returns ambiguous. Bead assignment for "the mayor" would need a
"route-to-any" semantic; today work claims are instance-specific. And
no precedent exists in code for "elect one of the peers as canonical."
The polecat pattern works for homogeneous workers because none of them
needs to be addressed singularly — coord agents do.

## 5. Operator-experience inventory

The inventory for a coordination agent being handoffed, reset, or
crashed:

| Surface | On `gc handoff` (self) | On `gc session reset` | On unexpected crash |
|---|---|---|---|
| tmux session | `tmux kill-session -t <name>` (`tmux.go:426`) via reconciler | `tmux kill-session -t <name>` via reconciler | reconciler emits `session.crashed` (`session_reconciler.go:828`); reconciler will re-wake `mode=always` sessions on the next tick (see also gc-ljkvi for the silent-death gap) |
| tmux sibling panes/windows (incl. scratches) | **Die** with the session | **Die** with the session | **Die** with the session |
| session bead | Same bead persists; metadata patched with `restart_requested=""` cleared, `continuation_reset_pending=""` cleared post-restart (`RestartRequestPatch`, `lifecycle_transition.go:283-296`) | Same bead persists; metadata patched as above; reset also clears the respawn circuit breaker (`cmd/gc/cmd_session_reset.go:21`) | Same bead persists; state advances to `crashed`/`stopped` (`session_reconciler.go:828, 709`) |
| work beads (`assignee = session name/alias`) | Persist untouched; on wake `resolveTaskWorkDir` (`session_reconciler.go:2206`) consults `metadata.work_dir` to resume | Persist untouched; same salvage path | Persist untouched; same salvage path |
| inbox mail (recipient bead) | Persists in bead store; new-life agent reads via `gc mail inbox` after `gc prime` | Persists | Persists |
| transcript / provider conversation | **Lost** (new process, new conversation) | **Lost** | **Lost** |
| subprocess children of the killed pane | Reaped explicitly by `KillSessionWithProcesses` (`tmux.go:452-514`; the `Excluding` variant at `:520-606` skips the caller's own PID): walk process group + descendants, SIGTERM, 2 s grace, SIGKILL | Same reaping (same code path) | Same — runtime stop path is shared |
| `<workDir>/.gc/tmp` (prompt scratch dir) | **Survives on disk** — gascity does not delete it (`internal/runtime/tmux/adapter.go:991-998` says "cleaned up with the agent's scratch space," meaning external/operator responsibility) | Survives | Survives |
| events emitted | `session.draining` (`session_reconciler.go:1174, 1208`), then `session.stopped` (`:709`) on the kill, then `session.woke`/`session.updated` on restart | `session.draining` → `session.stopped` → `session.woke` | `session.crashed` (`:828`) with scrollback capture; then `session.woke` if the reconciler decides to restart |

**The scratch problem in plain English.** The gc-toolkit overlay's
`tmux-spawn-scratch.sh` opens a *new window in the same tmux session* as
the canonical agent (line 85, `tmux new-window -t "$SESSION" ...`). When
the operator runs `gc handoff` to refresh that canonical agent's
transcript, gascity's reconciler picks up the `restart_requested` flag
and tells the worker boundary to kill the session via
`workerKillSessionTargetWithConfig` → `handle.Kill` → `sp.Stop` → `tmux
kill-session -t <name>`. tmux's semantics are that
`kill-session` terminates the entire session, including every window
inside it — so the scratch window dies along with the canonical pane.
The overlay's own header comment (line 12) describes the intended
behavior as "survive `gc handoff` (the routine restart path, which uses
`respawn-pane` on `:^.0` only)" — but no `respawn-pane` call is wired
into the gascity restart path at any commit I could check (verified at
both `d11ee0e1` and the audit-cited `c7cd79f9`). The overlay's mental
model and gascity's actual behavior have been misaligned since at least
2026-04-15 (when `84c0e19d`/`53f0c926` consolidated the
`handle.Reset` + `Kill` path).

**Salvage paths.** For *work* (i.e., bead-backed implementation tasks
in a polecat's worktree), gascity already supports recovery: a work
bead carries `metadata.work_dir` and `metadata.branch`
(per the polecat boot prompt's "Work Bead Metadata Contract"), and
`resolveTaskWorkDir` (`cmd/gc/session_lifecycle_parallel.go:648`)
re-opens the worktree on next wake. For *transcripts* (the scratch's
ad-hoc conversation), no gascity-side salvage exists — the
conversational state is in the provider process and dies with it. The
`<workDir>/.gc/tmp` directory contains prompt files but not transcript
state. The bead-side recovery hook
(`resolveTaskWorkDir`, `cmd/gc/session_reconciler.go:2206`) is what
makes work resumable; scratches do not have an analogous bead.

## 6. Upstream movement

**Open bead — same surface, distinct problem:**
- **`gc-ljkvi`** "Named-session crash detection gap: dead panes sit
  silent" (open, prio 2). Different problem (silent failure of a dead
  named-session pane vs. handoff-driven session destruction), same
  identity boundary (named-session restart path). Proposes a doctor
  check + a `session.crashed` consumer. Complementary to `gc-h1gxg`
  rather than overlapping. The `gc-h1gxg` decision should be aware
  that the named-session restart loop currently has no auto-recover
  layer.

**No other open or in-progress beads in the gascity bead store** match
the search "session OR pane OR handoff OR restart OR identity OR peer
OR multi-instance OR scratch OR canonical OR pool" with content that
addresses session-vs-pane coupling or peer-instance coord agents.
`gc-h1gxg` is the only open decision bead in this area in gascity.

**Recent commits on origin/main shaping the restart surface** (the ones
relevant to a decision conversation; full list is longer):

| SHA | Subject | Why it matters |
|---|---|---|
| `0a79e9b8` | Session model unification phases 0-2 (#666) | Foundational: lifecycle_projection, state_machine, named_config — unifies pool and non-pool session identity |
| `84c0e19d` | fix: complete named session handoff handling | Closed gaps in the named-session handoff path |
| `53f0c926` | fix: skip restart request for named sessions on self-handoff | Recognizes that named-session self-handoff has special semantics |
| `4c24172d` | fix: preserve named-session handoff guards | Same area — named-session safety |
| `f94c244a` | fix(reconciler): recognize failed-create as known pool session state (#1912) | Pool/lifecycle refinement |
| `3424878d` | fix(reconciler): suppress per-tick re-stamp on attached config-drift deferral (#1687) | Named-session attachment continuity |
| `ece15565` | fix(handoff): replace select{} with bounded poll loop in cmdHandoff (#1481) | Handoff reliability (does not change identity model) |

None of these commits introduce decoupled-identity / peer-instance coord
machinery. They refine the existing named-session + pool model.

**Design docs in `engdocs/design/`:**

- **`session-model-unification.md`** (accepted) — makes `[[agent]]` a
  pure config/factory; runtime identity lives on sessions. Foundational
  for any identity-model change. Phases 0-2 landed (`0a79e9b8`).
- **`session-lifecycle-domain-cleanup-plan.md`** (in progress) —
  introduces typed lifecycle projection to replace distributed metadata
  interpretation. The projection at
  `internal/session/lifecycle_projection.go` is the head of this work.
- **`agent-pools.md`** (implemented) — the design behind pool semantics
  documented in §4.
- **`named-configured-sessions.md`** (accepted) — introduced
  `[[named_session]]` as a first-class config object. Partially
  subsumed by session-model-unification.

No proposal addresses scratch-clone decoupling, peer-instance coord
models, or canonical-among-peers election. The decision space remains
open.

**Upstream divergence (`gastownhall/gascity`):**

- `git log upstream/main..origin/main -- internal/session/
  internal/runtime/ cmd/gc/cmd_handoff.go cmd/gc/cmd_session*.go
  cmd/gc/session_reconciler.go` shows fork-specific commits that the
  upstream does not have; none are identity-model changes.
- `git log origin/main..upstream/main -- <same paths>` shows no
  upstream-only commits in this area that the fork is missing.

**Cross-rig overlay activity** (gc-toolkit, the consumer that owns the
scratch concept):

- `/home/zook/loomington/rigs/gc-toolkit/specs/tk-my4za/reset-vs-handoff-audit.md`
  (2026-05-07) — audit of `gc session reset` vs `gc handoff` usage in
  gc-toolkit instructional surfaces. Useful prior reading. Note the
  caveat in §2 of this survey: the audit's decision rule rests on a
  premise about `gc handoff` using `respawn-pane` that does not match
  current (or audit-time) gascity behavior.
- bd `tk-mjvm9` ("Move scratch clones back into the agent's main tmux
  session") referenced from tk-my4za as a follow-up; not in the gascity
  bead store so I have not read it directly.

**Is `gc-h1gxg` a green field?** Not green, but not crowded. The
foundational lifecycle work (session-model unification, lifecycle
projection, agent-pools) gives the decision a stable footing. The
specific question — "what is the unit of restart, and can a role have
N instances?" — has no in-flight implementation that competes with it.
The decision should layer on top of session-model-unification, not
reopen it.

## Decision-relevant takeaways

These map findings to the three framings already named in `gc-h1gxg`,
plus any fourth option the survey surfaces. **Not recommendations.**

### Framing A — decouple agent identity from tmux session (restart = pane)

What the survey says about A:
- The Go primitive `RespawnPane` already exists
  (`internal/runtime/tmux/tmux.go:2838`) and is exported. The work to
  realize A in gascity is "wire `handle.Reset` to call `RespawnPane`
  on the pane :^.0 of the named session instead of going through
  `KillSessionWithProcesses`" — a localized change in the
  worker-boundary `Kill`/`Reset` implementations for `mode=always`
  named sessions. Not architectural — it's plumbing.
- It requires the reconciler to know which pane is the agent vs. sibling
  windows; today the reconciler tracks sessions, not panes. Verify the
  reconciler's session-aliveness check (`tmux list-panes` based, via
  `IsRunning` at `internal/runtime/tmux/adapter.go:235`) is
  sufficient when the agent's pane has been `respawn-pane`'d in place.
- It does **not** address §4: a single named session still hosts a
  singleton agent.
- It fixes the scratch problem with the smallest semantic change. It
  also aligns gascity's behavior with the gc-toolkit overlay's existing
  mental model (which already expected handoff to be pane-scoped, per
  tk-my4za and the spawn-script comments).
- Risk surface: the `KillSessionWithProcesses` reaper currently catches
  *all* descendants of the agent pane. A pane-scoped restart needs to
  preserve sibling panes' processes while still reaping the agent's
  process group. tmux's `respawn-pane -k` SIGHUPs the pane's process,
  but downstream `claude` orphans that reparent to init would no longer
  be reaped by the session-wide cleanup. Process-tree behavior needs
  explicit attention.

### Framing B — drop scratch concept; N peer instances per role, one canonical

What the survey says about B:
- The spawn-loop side is mostly free: §4 shows the pool reconciler is
  role-agnostic; setting `max_active_sessions > 1` on a coord agent
  config would mechanically work.
- The blocking issues are **mail/dispatch**, **bead assignment**, and
  **the absence of canonical election**:
  - `cmd/gc/cmd_mail.go:688` raises `session.ErrAmbiguous` for
    multi-match aliases. Without a resolver change, the first
    peer-instance mayor fails to receive role-addressed mail.
  - No "any mayor" routing exists; bead assignment is per-bead/per-alias.
  - No leader/canonical election machinery exists in `internal/config/`,
    `internal/agentutil/`, or `internal/supervisor/`.
- It does *not* directly need `RespawnPane` — each peer-instance lives
  in its own tmux session, so restarting one canonical does not touch
  the others (regardless of whether handoff is session-scoped or
  pane-scoped).
- It re-poses the scratch problem at a different layer: if scratches
  are spawned in the canonical's session, they still die when the
  canonical restarts. If scratches become their own peer instances
  (B in its purest form), the operator gains uniformity but loses the
  "scratch inherits the agent's cwd / persona conveniently" affordance
  unless that's re-built.

### Framing C — hybrid

What the survey says about C:
- The cheapest hybrid is "scratches move out of the agent's tmux
  session" — i.e., spawn each scratch in its own tmux session
  (overlay-side change to `tmux-spawn-scratch.sh`). This is closer to
  what `tk-mjvm9` apparently proposes from the other direction. It
  doesn't change gascity at all; it changes the overlay's spawn script.
  Side effects: scratches survive any gascity restart of the canonical;
  but they also detach from the canonical's tmux window layout (the
  tmux-window switching affordance is lost).
- A more ambitious hybrid is "decouple identity from session AND allow
  N peers" — pane-scoped restart for the canonical, peer instances for
  scale-out. The pieces are independent (A is plumbing; B is
  config + resolvers); they can land in either order.

### A possible fourth option — "scratches in their own sessions, gascity unchanged"

The survey surfaces this not as a recommendation but as a data point.
If the operator's experiential goal is "my scratch doesn't die when I
handoff the mayor," that goal is achievable by changing where the
overlay spawns the scratch — a one-script change in gc-toolkit — and
does not require any gascity-side decision. It does **not** address
the broader question gc-h1gxg is asking ("is agent identity coupled to
tmux session?"); it solves the symptom without resolving the structure.
Whether that's the right scope is a decision the
operator-plus-mechanik conversation should make.

### Cross-cutting facts the decision conversation should hold

1. `gc handoff`, `gc session reset`, and `gc session kill` all converge
   on `tmux kill-session` today (§1). The overlay's documentation
   assumes otherwise. Either gascity should be changed to match the
   assumption (framing A), or the overlay's documentation should be
   corrected — and that correction is a non-trivial UX change for the
   operator since it means "handoff loses scratches" becomes
   official rather than a known bug.
2. Session-model-unification (`engdocs/design/session-model-unification.md`,
   landed phases 0-2 in `0a79e9b8`) already separated agent-as-config
   from session-as-identity. A peer-instance coord model would be a
   natural next phase rather than a re-architecture.
3. `gc-ljkvi` is in the same neighborhood (named-session crash detection
   gap). Whichever framing wins, the silent-death problem is still
   there — and a peer-instance model arguably makes it worse (more
   silent-failure surfaces) unless `gc-ljkvi`'s tier 1/2 lands first.
4. `tk-mjvm9` in gc-toolkit appears to want to move scratches *back* to
   the canonical session, presumably under an assumption that gascity
   handoff is or will be pane-scoped. If gascity stays session-scoped,
   that motion makes the scratch problem worse, not better.
