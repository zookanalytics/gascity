---
title: Gas City Architecture
description: The architecture of the whole repository on one page. It states what Gas City is for, what it talks to, its domains, the rules every part follows, its settled decisions and its debt, and links each subsystem's own document for depth.
---

# Gas City Architecture

## Scope

**Mandate.** The architecture of the whole repository: what Gas City is for
and the quality goals a change is weighed against, the systems it talks to,
the domains it is built from, the rules every part follows, the decisions
that are settled, and where what is built falls short of that design.

**Boundaries.**

- How each subsystem works: its document under
  [engdocs/architecture/](architecture/index.md), which lists them in reading
  order.
- The model a user learns and how an operator runs a city: the published
  docs, starting at [how Gas City works](../docs/getting-started/how-gas-city-works.md).
- Where each package lives: [the codebase map](contributors/codebase-map.md).
- The config schema and the CLI surface: the generated references for
  [config](../docs/reference/config.md) and the [CLI](../docs/reference/cli.md).
- How a change is built, tested and contributed: [AGENTS.md](../AGENTS.md),
  [TESTING.md](../TESTING.md) and [CONTRIBUTING.md](../CONTRIBUTING.md).

## Purpose and quality goals

Gas City is an SDK for building multi-agent coding workflows. A city's packs
supply every role's behavior as configuration, and the SDK supplies only the
infrastructure that runs it. A formula is the method for a job, and the
controller runs it as a graph of beads across many agent sessions, outside
the user's own session. Its model is six primitives, Agent, Bead, Formula,
Rig, Pack and Event, and [the code-layering view](architecture/nine-concepts.md)
maps each part of the code onto them.

Quality goals, ranked:

1. **Roles are configuration.** No line of Go names a role, and every SDK
   operation works with only the controller running, whichever agents a city
   declares.
2. **Work converges.** Work persists in beads, so the system reaches the
   right outcome through crashes and restarts by re-reading the same state.
3. **Judgment stays with the agents.** Go moves work and makes no decision
   about it, so each primitive grows more useful as models improve.

## Context

| Party | What it is to Gas City |
|---|---|
| Operator | Runs `gc` against a city directory and follows the city's events. |
| Coding-agent providers | The agent CLIs a session runs, such as Claude Code, Codex and Gemini. Gas City shapes each one through its launch command, its environment and the files in its work dir. |
| Runtime substrates | Where a session runs, such as tmux, a subprocess, an exec script or Kubernetes, each behind a runtime provider. |
| Bead store | The beads store, backed by Dolt in production. The city and each of its rigs keep their work in a store of their own. Infrastructure beads, such as the execution graph and order tracking, are kept there too unless the city's `[storage]` binds their classes to a store of their own ([storage classes](../docs/runbooks/split-storage-classes.md)). |
| Git | A rig is a project directory, and when it is a Git repository, agents work in its checkouts and worktrees. |
| Packs | The user's configuration: `city.toml`, `pack.toml`, formulas, prompts and orders. Builtin packs ship inside the binary. |

The boundary is the `gc` binary: its CLI, the supervisor that hosts one
controller per registered city and serves the HTTP and SSE API, and the
builtin packs it embeds. Providers, runtimes, the bead store and the user's
packs lie outside it.

## Domains

| Domain | Responsibility | Data it owns | Must not |
|---|---|---|---|
| Bead store | Keeps beads and the dependencies between them, and answers queries over them, such as which beads are ready, through one interface, whichever backend holds them. | Each bead's record: its id, status, assignee, labels, metadata and dependencies. | Decide what a bead means: the parts that write and read a kind of bead decide that. |
| Event bus | Records each event of the city's activity in sequence and delivers it to every watcher. | The city's event log, and the payload shape registered for each event type. | Fail the operation that fires an event, or change an event once it is recorded. |
| Config | Composes `city.toml` and every pack imported at city scope or on a rig into the one resolved configuration every other part reads. | The declared configuration: the rigs, the imports at city scope and on each rig, and the layers each scope reads formulas and orders from. | Start a session, dispatch work or write a bead. |
| Sessions | Start, wake, sleep, stop and close each agent session, deliver its prompt, and decide each change to its lifecycle, whatever runtime it runs on. | Each session's record, a bead at city scope that only sessions write: its identity, its lifecycle state, the provider conversation it continues and the config it started under. | Reach a session's process except through its runtime provider. |
| Runtime providers | Run a session's process on one substrate: start and stop it, deliver input to it, and report its output, liveness and activity. | The live process, and the metadata the provider keeps under the session's name. | Write beads or config: a provider knows a session only by its name and the start config it is handed. |
| Prompt templates | Render an agent's prompt from the template its pack supplies, with the agent's city, rig, name and work queries filled in. | The variables and functions a template can use. | Deliver the prompt it renders. |
| Provider hooks | Install the files that have each coding-agent provider call `gc` at its lifecycle points. | The hook files `gc` manages, in the city and in each agent's work dir. | Act in a session `gc` did not start, such as one a person opens with the same provider in an agent's work dir. |
| Formulas and dispatch | Route work to agents and pools, turn a formula into a run of beads, and execute a v2 run's control steps until it finishes. | Each run's root, step and control beads, with their dependencies and routes, and the convoys that track routed work. | Start or wake a session. |
| Orders | Discovers the order set, binding each order city-wide or to one rig by where it is defined and the scope it declares. | The order set: each order's definition, trigger, action and binding. | Dispatch an order or write a bead. |
| Health patrol | Keeps the sessions config wants alive: restarts one that dies or drifts from its config, quarantines one that keeps failing, and stops one idle past its timeout. | No durable data of its own. | Restart, stop or close a session on an unanswered probe: a runtime read that fails or times out defers the action. |
| Controller | Runs one city: it reloads config when the city's files change, reconciles the city's sessions, and dispatches each due order. | The order runs it starts, each recorded as a tracking bead. | Decide which orders exist or where one is bound: it dispatches the order set as discovered. |
| CLI and API | Project the parts to people, agents and programs: the `gc` commands, and the HTTP and SSE API the supervisor serves. | No domain data: each answer comes from the part that owns it. | Re-implement domain logic, or carry an untyped shape on the wire ([invariants](architecture/invariants.md)). |

How they work together:

- To reconcile sessions, the controller gathers what config wants and what
  each runtime reports, sessions decide each session's next lifecycle
  change, and the controller carries it out. Health patrol's restarts,
  quarantines and idle stops are among those changes.
- A session starts from a start config built from its agent's resolved
  configuration, and its runtime provider launches the process from it. The
  config carries the prompt that prompt templates render, by a route the
  agent's provider takes, such as the system prompt or the first message.
- A session's provider hooks call `gc` at the session's lifecycle points.
  At its start, the call brings in the prompt when the launch did not carry
  it. Where the provider wires them, a call before a compaction hands off,
  and a call at each submitted prompt brings in the session's mail and
  queued nudges.
- The controller starts or wakes a session for the work routed to it, and
  sizes each pool to the ready work routed to that pool. An agent claims its
  work through `gc hook`, which finds pool work by the predicate the
  controller counts it with.
- Coding agents run each formula run's work steps. A v2 run's control steps
  go to the control dispatcher that `gc` provides at the scope whose store
  holds the run, and no coding agent runs them.
- The CLI and API serve the event bus to its watchers, through `gc events`
  and the API's event stream.
- While a supervisor serves the city, the CLI sends a mutation that must
  hold the supervisor's locks through the API instead of writing the stores
  itself.
- Orders scans the layers config resolves, the city's and then each rig's
  own, and applies the city's order overrides and skip list.
- The controller discovers the order set again whenever it reloads config.
  `gc order`, the API and doctor discover it through the same discovery, so
  each shows the orders the controller dispatches.
- To find an order due, the controller reads its past runs from the bead
  store and, for an event trigger, the event bus. A due exec order runs its
  script on the controller's host, with no agent. A due formula order becomes
  a wisp of its formula.
- A run executes in its order's scope: the city for a city-wide order, and
  the rig for a rig-bound one. A rig-bound order whose pool is a city agent,
  not one of the rig's, runs at city scope, where that agent reads its work.
  Each record a run writes is kept in the store that holds the record's class
  at that scope: the tracking bead is order tracking, and the wisp is
  execution graph, except a v1 wisp poured with steps, which is work.

Each part's own document holds its depth: the [bead store](architecture/beads.md),
the [event bus](architecture/event-bus.md), [config](architecture/config.md),
[sessions and runtime providers](architecture/session.md),
[prompt templates](architecture/prompt-templates.md),
[provider hooks](../internal/hooks/config/README.md),
[formulas](architecture/formulas.md) and [dispatch](architecture/dispatch.md),
[orders](architecture/orders.md), [health patrol](architecture/health-patrol.md),
the [controller](architecture/controller.md), and the
[CLI and API](architecture/api-control-plane.md).

## Cross-cutting rules

- **No upward dependencies.** A lower layer never imports a higher one
  ([layering](architecture/nine-concepts.md)).
- **Beads hold domain state.** Events are the outbound notification of
  activity, fired so people and agents can watch, and config is how every
  capability activates.
- **Side effects stay in Layer 0.** I/O and process spawning happen only
  there.
- **Running state is queried live.** No PID, lock or state file tracks a
  running process; the system itself is asked.
- **On a rig, the rig's own definition wins.** Where a city-level formula or
  order applies on a rig, a definition of the same name in the rig's own
  layers takes its place there.
- **`scope = "rig"` binds a city-level definition to each rig.** An agent or
  named session in a pack the city imports, and any city-level order, that
  declares it is instantiated on each configured rig and never city-wide.

## Decisions

- A city is a directory holding `city.toml`, its `.gc/` runtime state and
  `rigs/`.
- Config is TOML: `pack.toml` defines a pack and `city.toml` deploys a city.
- `gc` is a fresh binary, not a fork of Gas Town.
- No interface exists until two implementations do.
- No package is exported as an SDK API. Every package is internal to the
  binary except `pkg/eventexport`, the public contract of a city's event
  export.
- Sessions and runtime providers carry a running agent's lifecycle. There is
  no agent-protocol interface.
- Gas City has no capability flags. A sentence in a prompt does their job.
- Gas City adds no framework around skills or tools. It places a pack's
  skills and MCP servers where an agent's provider reads them, and the model
  decides when to use one.

## Debt

- **A run's tracking bead does not show whether the run is still going.**
  The controller reads an order's open tracking bead as its run still going,
  and holds the order's next run back when it finds one. After a restart, a
  run that ended with the old controller can keep its order from running
  until the run's timeout passes. After a crash, the next controller can
  close the tracking bead of an exec run that is still going, and a second
  run can start beside it. Order dispatch does not yet meet Work converges
  across a restart. gc-xpnop0 and gc-8utvkm retire it.
- **Some session decisions still sit outside sessions.** The controller's
  reconciler and some `gc` commands decide some session lifecycle changes
  and patch the session's record themselves, so a change to how sessions
  behave must be checked there too. The session refactor's sequence in
  [internal/session/PLAN.md](../internal/session/PLAN.md) moves each
  decision into sessions.
