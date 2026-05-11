# Survey Pass 2: multi-instance role-agent primitive for `gc-h1gxg`

**Bead:** `gc-64d5u` (research) under `gc-h1gxg` (decision)
**Branch:** `polecat/gc-64d5u`
**Surveyed at:** 2026-05-11
**Worktree:** `/home/zook/loomington/.gc/worktrees/gascity/polecat-codex/gc-toolkit.ripley`
**origin/main:** `e05da010` (zookanalytics/gascity)
**upstream/main:** `0a2713dd` (gastownhall/gascity)

This survey is research only. It does not recommend a framing. The design
call sits with mechanik and the operator after this lands.

## Provenance

| Doc-type or artifact | Producer (skill / concept / workflow step) | Source location | Surveyed at |
|---|---|---|---|
| Filing bead | `gc-toolkit__mechanik` (dispatcher) | bd `gc-h1gxg`, parent of `gc-64d5u` | 2026-05-11 |
| Survey bead | `gc-toolkit__mechanik` (filer) | bd `gc-64d5u` | 2026-05-11 |
| Pass 1 baseline | previous survey pass | `specs/gc-h1gxg/survey.md` @ `e05da010` | 2026-05-11 |
| PackV2 role layout | gascity pack docs | `docs/guides/shareable-packs.md:6-19`, `:21-95` @ `e05da010` | 2026-05-11 |
| Gastown pack binding | Gastown example pack | `examples/gastown/packs/gastown/pack.toml:1-12`, `examples/gastown/city.toml:1-22` @ `e05da010` | 2026-05-11 |
| Polecat-flavor agent config | Gastown pack agent | `examples/gastown/packs/gastown/agents/polecat/agent.toml:1-8` @ `e05da010` | 2026-05-11 |
| Polecat work formula | Gastown pack formula | `examples/gastown/packs/gastown/formulas/mol-polecat-work.toml:1-37` @ `e05da010` | 2026-05-11 |
| Pack agent discovery | gascity config loader | `internal/config/agent_discovery.go:18-58`, `:77-123` @ `e05da010` | 2026-05-11 |
| Pack expansion and rig stamping | gascity config loader | `internal/config/pack.go:1300-1308`, `:1339-1391` @ `e05da010` | 2026-05-11 |
| Agent config surface | gascity config model | `internal/config/config.go:1718-1981`, `:2009-2128`, `:2183-2233` @ `e05da010` | 2026-05-11 |
| Pool instance expansion | gascity agent utility | `internal/agentutil/pool.go:84-180` @ `e05da010` | 2026-05-11 |
| Controller pool demand | gascity controller | `cmd/gc/build_desired_state.go:222-365` @ `e05da010` | 2026-05-11 |
| Session environment and prompt rendering | gascity controller | `cmd/gc/template_resolve.go:232-306` @ `e05da010` | 2026-05-11 |
| Mail identity resolver | gascity mail command | `cmd/gc/cmd_mail.go:643-726` @ `e05da010` | 2026-05-11 |
| Operator session CLI | gascity session command | `cmd/gc/cmd_session.go:105-145`, `:1124-1265` @ `e05da010` | 2026-05-11 |
| Operator/session API surface | gascity API docs | `docs/reference/api.md:30-35` @ `e05da010` | 2026-05-11 |
| Operator session API | gascity HTTP API | `internal/api/handler_session_create.go:21-120`, `:151-228` @ `e05da010` | 2026-05-11 |
| Configured session materialization | gascity session command | `cmd/gc/session_template_start.go:97-242` @ `e05da010` | 2026-05-11 |
| Formula step model | gascity formula model | `internal/formula/types.go:196-273` @ `e05da010` | 2026-05-11 |
| Late-bound workflow attach | gascity molecule runtime | `internal/molecule/molecule.go:123-147`, `:196-215` @ `e05da010` | 2026-05-11 |
| Graph workflow routing | gascity graph router | `internal/graphroute/graphroute.go:116-184`, `:224-362` @ `e05da010` | 2026-05-11 |
| Session lifecycle state | gascity session model | `internal/session/state_machine.go:31-139`, `internal/session/lifecycle_transition.go:226-296` @ `e05da010` | 2026-05-11 |
| Runtime drain/restart | gascity runtime command | `cmd/gc/cmd_runtime_drain.go:327-450` @ `e05da010` | 2026-05-11 |
| Reconciler drain and idle handling | gascity controller | `cmd/gc/session_reconciler.go:863-979`, `:1283-1324` @ `e05da010` | 2026-05-11 |
| Convoy lifecycle | gascity convoy primitive | `internal/convoy/convoy.go:41-108`, `cmd/gc/cmd_convoy.go:988-1122` @ `e05da010` | 2026-05-11 |
| Wisp autoclose | gascity bd hook helper | `cmd/gc/wisp_autoclose.go:53-77` @ `e05da010` | 2026-05-11 |

## 1. Pack-defines-role / gascity-instantiates contract

The primitive under the polecat name is not polecat-specific. PackV2 defines
portable behavior as pack metadata plus directories for agents, prompt
templates, formulas, orders, overlays, skills, MCP files, and helper assets
(`docs/guides/shareable-packs.md:6-19`). The standard role shape is an
`agents/<name>/` directory with `prompt.template.md`; `agent.toml` supplies
fields that differ from pack defaults (`docs/guides/shareable-packs.md:21-95`).

Gastown demonstrates the split. Its `city.toml` says Gas Town is expressed as
Gas City config and that rig-scoped agents such as witness, refinery, and
polecat are stamped per rig (`examples/gastown/city.toml:1-22`). The pack file
declares role names and named-session modes at the pack layer, not in SDK code:
mayor, deacon, and boot are city-scoped named sessions; witness and refinery
are rig-scoped named sessions (`examples/gastown/packs/gastown/pack.toml:1-12`,
`:32-55`). Polecat is just one rig-scoped agent config with `wake_mode`,
`work_dir`, `nudge`, `pre_start`, `idle_timeout`, and min/max session counts
(`examples/gastown/packs/gastown/agents/polecat/agent.toml:1-8`).

The pack also supplies the work formula. `mol-polecat-work` is described as a
feature-branch work lifecycle where the agent receives work, follows steps,
pushes a branch, sets work bead metadata, assigns refinery, and exits
(`examples/gastown/packs/gastown/formulas/mol-polecat-work.toml:1-37`). Those
are pack-authored workflow semantics. Gas City only provides the config,
session, bead, formula, and controller machinery that makes them executable.

Layered view:

| Pack layer | Gas City primitive | Runtime artifact |
|---|---|---|
| `pack.toml`, imports, `[agent_defaults]` | PackV2 config loader and expansion | Loaded `config.City` with pack provenance |
| `agents/<role>/agent.toml` | `config.Agent` fields for provider, prompt, workdir, pool caps, work routing, lifecycle, attachments | One configured role template, optionally rig-scoped |
| `agents/<role>/prompt.template.md` | Prompt discovery and template rendering | Provider prompt with `{{.WorkQuery}}`, `{{.SlingQuery}}`, rig/workdir context |
| `formulas/<name>.toml` | Formula compile/cook/attach | Work beads, dependencies, molecule/workflow metadata |
| `scope = "rig"` or `scope = "city"` | Pack expansion and stamping | Qualified identities such as `rig/role` or `role` |
| `min_active_sessions`, `max_active_sessions`, `scale_check` | Pool demand and desired-state controller | Session beads and concrete runtime sessions |
| `work_query` default or override | Hook/query primitive | Assigned work recovery and routed pool pickup |
| `sling_query` default or override | Metadata routing primitive | `gc.routed_to=<qualified-name>` on work beads |
| `pre_start`, `overlay_dir`, `session_setup`, `session_live` | Runtime preparation and live-side effects | Worktree, overlay files, tmux setup, provider process environment |
| `named_session` entries | Configured named-session materialization | Canonical singleton or on-demand session bead |

The gascity-side interface a pack must implement is therefore a config
convention, not a Go interface:

- Define a PackV2 pack with agent directories and, if needed, formulas.
- Put each role flavor under `agents/<name>/` with a prompt and optional
  `agent.toml`.
- Choose scope, provider/session transport, workdir, nudge, startup hooks,
  pool capacity, and lifecycle fields through `config.Agent`.
- Use `named_session` only when the flavor needs a canonical identity rather
  than a generic multi-instance pool.
- Route work through the configured agent's qualified name, either by default
  `sling_query` or an explicit override.

The loader side is generic. `DiscoverPackAgents` scans `agents/`, reads
`agent.toml`, applies prompt/overlay/namepool/skills/MCP conventions, and
returns `config.Agent` values (`internal/config/agent_discovery.go:18-58`,
`:77-123`). Pack expansion appends discovered agents, applies inherited
defaults, stamps rig-scoped agents with `Dir=rigName`, resolves pack-relative
paths, and merges named sessions (`internal/config/pack.go:1300-1308`,
`:1339-1391`). The `Agent` struct carries the role definition surface:
provider, prompt, workdir, scope, pre-start hooks, pool caps, scale check,
work/sling queries, idle policy, overlays, default formulas, attach behavior,
dependencies, and wake mode (`internal/config/config.go:1718-1981`).

The runtime side is also generic. The default `EffectiveWorkQuery` checks work
assigned to `GC_SESSION_ID`, `GC_SESSION_NAME`, then `GC_ALIAS`, and only then
checks unassigned work routed to the template's `gc.routed_to` target when the
session origin is ephemeral or a controller probe (`internal/config/config.go:2009-2058`).
The default `EffectiveSlingQuery` stamps `gc.routed_to=<qualified-name>` and
leaves session creation to reconciliation and scale checks
(`internal/config/config.go:2109-2128`). Pool expansion turns multi-session
templates into concrete display instances or discovers unlimited running
instances (`internal/agentutil/pool.go:84-180`), while desired-state building
evaluates configured agents, collected assigned work, scale checks, and pool
demand before realizing sessions (`cmd/gc/build_desired_state.go:222-365`).
When a session is created, template resolution injects `GC_SESSION_NAME`,
`GC_SESSION_ID`, `GC_TEMPLATE`, `GC_SESSION_ORIGIN`, `GC_AGENT`, `GC_ALIAS`,
beads scope, rig scope, and rendered prompt context
(`cmd/gc/template_resolve.go:232-306`).

## 2. Mail-to-role-with-policy

Mail today resolves session/mailbox identity, not role demand. The live
configured-named resolver normalizes a bare identifier, rejects empty,
`human`, and slash-qualified strings, scans live session beads, and matches
the basename of `configured_named_identity` (`cmd/gc/cmd_mail.go:643-663`).
Zero matches fall through, one match returns that session's mailbox addresses,
and multiple matches produce `ErrAmbiguous` with the list of displays
(`cmd/gc/cmd_mail.go:681-688`). The higher-level resolver first tries the
ordinary session resolver, then the live configured-named resolver, then a
configured mailbox address fallback (`cmd/gc/cmd_mail.go:692-726`).

That means "name" currently means one of these identities:

- A concrete session bead ID.
- A concrete open session name.
- A concrete current alias.
- A live configured named-session identity, but only when given as a bare
  basename.
- A configured mailbox address fallback.

Role and identity are conflated at the configured-named boundary. A singleton
such as a named refinery has both a role-like human name and a canonical
session identity. A pool role is different: the role lives in `config.Agent`
and its routed queue, while concrete instances live as session beads or
expanded agent names. The current mail resolver does not ask "which role
template owns this candidate?" and does not inspect `gc.routed_to`,
`PoolName`, or pool membership. It only finds a concrete mailbox identity
after session/name/alias resolution or configured-named basename matching
(`cmd/gc/cmd_mail.go:643-726`).

The minimum missing surface for role-addressed mail is a role-target resolver
separate from the session identity resolver. It would need to enumerate
candidates from loaded config, named-session specs, open session beads, and
pool metadata, then apply an explicit router policy:

- `canonical-only`: deliver only to the configured canonical named session for
  that role; ambiguous or absent canonical identity is an error.
- `any-instance`: deliver to one live instance of a role, using a deterministic
  tie-breaker when several are live.
- `affinity`: select the instance already associated with the sender, thread,
  convoy, work bead, or configured affinity key.
- `load-balanced`: select among live instances by a load metric or explicit
  rotation state.

Without that separate role resolver, a phrase like "a mayor" cannot be
represented distinctly from "the live session named mayor". The existing path
has no policy input and no role-candidate set to apply policy to.

## 3. Operator-initiated spawn

The gascity primitive for operator-spawnable agent instances is "create a
session from an agent template". The CLI surface is `gc session new
<template>`, documented as creating a persistent conversation from an agent
template in loaded config and attaching the terminal by default
(`cmd/gc/cmd_session.go:105-145`). The API docs expose agent and session CRUD
surfaces (`docs/reference/api.md:30-35`), and the session-create body accepts
`kind` set to `agent` or `provider`, a `name`, optional alias, message, async
flag, options, project ID, and title
(`internal/api/handler_session_create.go:21-60`). For `kind = "agent"`, the
API resolves the configured template and transport, validates options, creates
a deferred worker session, stamps metadata, and pokes the reconciler
(`internal/api/handler_session_create.go:77-120`, `:151-228`).

Who can spawn today:

- CLI operators through `gc session new <template>`.
- API clients, including dashboard clients, through the HTTP session-create
  endpoint.
- Internal config materialization paths when a named session or template must
  exist.
- Workflows can create work routed to templates; they do not currently expose
  the same operator-attached "new chat session" primitive as a formula step.

What can be spawned today:

- Any loaded configured agent template whose provider and transport resolve
  successfully.
- Provider-only sessions through the API's `kind = "provider"` path.
- Configured named sessions through materialization: a closed canonical bead
  can be reopened instead of creating a new one, preserving references
  (`cmd/gc/session_template_start.go:97-115`).

The target of `gc session new` is the template, not a concrete pool member.
The concrete identity is created during session creation and reconciler start.
For configured named sessions, materialization builds metadata including
`configured_named_session`, `configured_named_identity`, mode, and
`session_origin=named`; it creates through the worker handle in deferred mode
when the managed controller is available (`cmd/gc/session_template_start.go:137-200`).
For ordinary configured agent templates, the materializer falls through to
`materializeSessionForAgentConfig` (`cmd/gc/session_template_start.go:242`).

Attachment is CLI-centric. `gc session new` defaults to terminal attach, with
`--no-attach` to skip it (`cmd/gc/cmd_session.go:121-137`). The agent config
also carries an `attach` field that controls whether interactive attachment is
supported (`internal/config/config.go:1943-1946`). API-created agent sessions
are explicitly asynchronous/deferred so the HTTP response does not block while
the agent boots (`internal/api/handler_session_create.go:178-228`).

The identity-spoofing question resolves to explicit session identity stamping,
not role-specific spoofing. Template resolution injects the session name, bead
ID, template, origin, agent value, alias, and `BEADS_ACTOR` into the runtime
environment (`cmd/gc/template_resolve.go:232-240`). The API stamps
`agent_name` and `session_origin=ephemeral` for agent sessions
(`internal/api/handler_session_create.go:159-168`). Named materialization
stamps the named identity and `session_origin=named`
(`cmd/gc/session_template_start.go:137-144`). The CLI "new" path is described
as Phase 2 bead creation plus controller reconciliation, with fallback direct
start when the controller is not running (`cmd/gc/cmd_session.go:141-145`).

Lifecycle today is session lifecycle, not operator-thread lifecycle. An
operator-created session persists as a session bead until suspended or closed.
`gc session suspend` records a user hold and lets the controller stop the
runtime, preserving the bead for wake (`cmd/gc/cmd_session.go:1124-1204`).
`gc session close` permanently stops the runtime and closes the session bead
(`cmd/gc/cmd_session.go:1206-1265`). Idle and drain behavior may also stop or
sleep sessions depending on config and controller state, but it is not scoped
to the operator's current task.

Limits visible from the current surface:

- There is no explicit `operator_spawnable` field. A loaded agent template is
  spawnable if it resolves and the caller names it.
- There is an `attach` capability knob, but no separate "visible in operator
  launcher" or "hidden infrastructure role" knob on `config.Agent`.
- A pack can define a flavor as a named singleton, a pool, or both by config,
  but the operator-spawn primitive is still template/session creation rather
  than a role policy such as "new consultant for this conversation".

## 4. Workflow-step role-instance spawn

Formula steps already create routable work. The step model includes title,
description, metadata, dependencies, `needs`, fanout wait behavior, assignee,
children, gates, loops, and on-complete hooks (`internal/formula/types.go:196-273`).
`molecule.Cook` and `CookOn` compile and instantiate formulas
(`internal/molecule/molecule.go:123-147`). `molecule.Attach` grafts a compiled
recipe as a sub-DAG onto an existing workflow bead, adds a blocking dependency
on the sub-DAG root, inherits workflow metadata, and provides idempotency and
epoch fencing (`internal/molecule/molecule.go:196-215`).

Graph routing can target either a concrete session or a config role queue. The
router reads a direct-session target from `assignee`; otherwise it reads
`gc.run_target` metadata (`internal/graphroute/graphroute.go:116-134`). When
the target comes from `assignee`, it must resolve to a concrete session or the
router errors and instructs the formula author to use `gc.run_target` for
config routing (`internal/graphroute/graphroute.go:336-344`). When the target
comes from `gc.run_target`, the resolver finds the configured agent. If the
agent is multi-session, it records a metadata-only binding; otherwise it
resolves the concrete session name (`internal/graphroute/graphroute.go:345-361`).
Applying the binding either sets a direct assignee or stamps
`gc.routed_to=<qualified-name>` (`internal/graphroute/graphroute.go:136-149`).

The answer is therefore split:

- Yes, a workflow can materialize a step as work routed to a role template,
  wait on dependencies or a sub-DAG root, and proceed when the work closes.
  That composes formula dependencies, `gc.run_target`, metadata routing, pool
  demand, and controller session materialization.
- No, the current formula language does not have a distinct step-local
  primitive that says: spawn a fresh role instance solely for this step, bind
  output to the step, join on that output, and dismiss that session when the
  step completes.

The smallest missing addition is not a new hardcoded flavor. It is a typed
workflow binding for step-scoped role instantiation: role/template target,
freshness/session reuse policy, lifecycle owner, output contract, and mailbox
or nudge routing policy. Today, those concerns are spread across formula
metadata (`gc.run_target`), generic work dependencies, controller demand, and
ordinary session lifecycle.

## 5. Lifecycle primitives

Current session lifecycle has explicit commands and states. The state machine
names create, ready, suspend, wake, sleep, quarantine, drain, archive, and
close, with close as terminal and archive as retained history
(`internal/session/state_machine.go:31-139`). Metadata patch helpers cover
begin drain, sleep, acknowledged drain, completed drain, and restart request
(`internal/session/lifecycle_transition.go:226-296`).

Current operational hooks:

- `gc runtime drain-ack` records that the current or named session has finished
  and asks the controller to stop it (`cmd/gc/cmd_runtime_drain.go:327-365`).
- `gc runtime request-restart` records restart intent and waits for the
  controller to kill and restart the session when restart is available
  (`cmd/gc/cmd_runtime_drain.go:372-450`).
- The reconciler honors drain ack, stops the runtime, records the event,
  re-queries assigned work, and sets either acknowledged-drain or completed-
  drain metadata (`cmd/gc/session_reconciler.go:863-979`).
- Idle timeout stops an alive idle session, records an idle-killed event, and
  applies a sleep patch before wake logic may re-wake it
  (`cmd/gc/session_reconciler.go:1283-1324`).
- `gc session suspend` records an explicit user hold for the controller or
  directly stops via the worker handle when unmanaged
  (`cmd/gc/cmd_session.go:1124-1204`).
- `gc session close` stops the runtime and closes the session bead
  (`cmd/gc/cmd_session.go:1206-1265`).
- Convoys can be created, linked to child work, and queried for progress
  (`internal/convoy/convoy.go:41-108`).
- Owned convoys can land only after children close unless forced; landing
  closes the convoy bead and records a convoy-closed event
  (`cmd/gc/cmd_convoy.go:988-1122`).
- Wisp autoclose closes attached molecule/workflow roots and descendants when
  a parent work bead closes (`cmd/gc/wisp_autoclose.go:53-77`).

Lifecycle needs from the prompt, mapped to current gaps:

**Operator dismiss.** Today the closest operations are `session close`
permanent termination, `session suspend` user hold, and archive in the state
machine. There is no separate "dismiss this operator-created thread" intent
with a policy for preserving history, freeing UI focus, selecting whether the
session can be resurrected, and withdrawing any pending nudges. The missing
primitive is a typed dismiss lifecycle reason distinct from close/suspend, or
an explicit mapping that makes dismiss a first-class policy over those states.

**Workflow-step complete.** Today work completion closes beads and can close
attached wisps, while sessions drain or sleep according to pool demand and
idle policy. There is no session owner edge from a workflow step to a concrete
fresh session and no controller rule that drains/dismisses that session when
the step closes. The missing primitive is a session ownership/lifetime binding
from workflow step to role instance.

**Convoy close.** Today convoys track child completion and can land. The
convoy `notify` field is printed on land but is not itself a session lifetime
or mail/nudge routing primitive (`cmd/gc/cmd_convoy.go:1114-1120`). There is
no steward session that is owned by a convoy and remains alive until the
convoy lands. The missing primitive is a convoy-owned session lifecycle edge
with an end condition of convoy close/land.

## 6. Pack-level role definition surface

A pack provides a role by supplying data, not SDK behavior. The surface already
covers the common "agent = vendor + prompt" interpretation:

- Provider and session transport: `provider`, `session`, `start_command`,
  `args`, prompt mode, and options (`internal/config/config.go:1747-1777`).
- Prompt and prompt composition: `prompt_template`, injected/appended
  fragments, private skills/MCP directories discovered by convention
  (`internal/config/config.go:1741-1746`, `:1902-1942`;
  `internal/config/agent_discovery.go:88-123`).
- Placement and runtime setup: `scope`, `dir`, `work_dir`, `pre_start`,
  `session_setup`, `session_setup_script`, `session_live`, `overlay_dir`
  (`internal/config/config.go:1724-1740`, `:1867-1889`).
- Work routing: `work_query`, `sling_query`, and `default_sling_formula`
  (`internal/config/config.go:1816-1840`, `:1913-1919`, `:2009-2128`).
- Multiplicity: `min_active_sessions`, `max_active_sessions`, `scale_check`,
  `namepool`, `wake_mode`, `SupportsMultipleSessions`, and
  `SupportsInstanceExpansion` (`internal/config/config.go:1778-1815`,
  `:1960-1970`, `:2183-2224`).
- Interaction and lifecycle hints: `nudge`, `idle_timeout`,
  `sleep_after_idle`, `drain_timeout`, `attach`, `depends_on`,
  `resume_command` (`internal/config/config.go:1744-1746`, `:1795-1809`,
  `:1841-1847`, `:1943-1963`).

The same gascity machinery is reusable for worker, steward, consultant, or
other role flavors as long as those flavors reduce to a configured agent
template plus prompt/formula/lifecycle/routing data. Different flavors do not
need different hardcoded SDK surfaces merely because their instructions differ.
The existing pack surface already expresses prompt, provider, pool capacity,
work routing, startup environment, formulas, and attachability.

The current surface is thinner where flavors need policy beyond "spawn
sessions for work routed to this template":

- Trigger policy is generic pool demand or named-session mode; there is no
  step-scoped or convoy-scoped trigger/lifetime field.
- Mail policy is session identity resolution; there is no role-addressed
  router policy.
- Lifecycle policy is session close/suspend/drain/idle; there is no operator
  dismiss, workflow-step-owned session, or convoy-owned steward lifecycle edge.
- Interaction mode has attach/nudge/provider transport, but no explicit
  operator-visible versus infrastructure-hidden spawnability flag.

## Primitives gascity already has

- PackV2 config discovery for agent directories, prompt templates, overlays,
  namepools, skills, MCP files, and formulas.
- Rig/city scoped expansion that turns pack roles into qualified configured
  agent templates and named sessions.
- Generic `config.Agent` fields for provider, prompt, workdir, routing,
  pool capacity, lifecycle hints, attach behavior, fragments, and formulas.
- Metadata-based work routing via `gc.routed_to` and default work/sling
  queries.
- Multi-session expansion and controller desired-state logic for pool demand,
  assigned-work recovery, scale checks, and runtime session creation.
- Session beads, concrete aliases/session names, runtime environment injection,
  and prompt rendering.
- Operator session creation from configured templates through CLI and API.
- Formula/molecule instantiation, sub-DAG attach, step dependencies, gates,
  loops, on-complete hooks, and graph routing through `gc.run_target`.
- Session lifecycle states and operations for suspend, wake, sleep, drain,
  archive, close, drain ack, restart request, idle timeout, and controller
  reconciliation.
- Convoy creation/progress/landing and wisp autoclose for attached workflows.

## Primitives gascity is missing

- A role-addressed mail resolver that distinguishes role/template targets from
  concrete session identities and applies explicit policy: canonical-only,
  any-instance, affinity, or load-balanced.
- A step-scoped role-instance binding for formulas: target role/template,
  fresh/reuse policy, output contract, join semantics, and lifecycle owner.
- A session ownership edge from workflow step to concrete role instance so the
  controller can drain or dismiss that instance when the step completes.
- A convoy-owned steward/session lifecycle edge so a steward can remain alive
  until convoy close/land and then drain or dismiss by policy.
- An operator dismiss lifecycle primitive distinct from permanent close and
  resumable suspend, or a typed policy that maps dismiss to those existing
  states while preserving history and UI semantics.
- A pack-level spawnability/visibility policy that can expose or hide a role
  from operator-initiated session creation independently from whether the
  controller may use it for routed work.
