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
| Runtime substrates | Where a session runs: tmux, a subprocess, an exec script or Kubernetes, each behind a runtime provider. |
| Bead store | The beads store, backed by Dolt in production. The city and each of its rigs keep their beads in a store of their own. |
| Git | Each rig is a repository, and agents work in its checkouts and worktrees. |
| Packs | The user's configuration: `city.toml`, `pack.toml`, formulas, prompts and orders. Builtin packs ship inside the binary. |

The boundary is the `gc` binary: its CLI, the supervisor that hosts one
controller per registered city and serves the HTTP and SSE API, and the
builtin packs it embeds. Providers, runtimes, the bead store and the user's
packs lie outside it.

## Domains

Gas City's parts are the bead store, the event bus, config, sessions and
their runtime providers, prompt templates, provider hooks, formulas and
dispatch, orders, health patrol, the controller, and the CLI and API that
project them. The rows below are the parts that decide which orders a city
runs, where each one is bound, and which store its runs go to.

| Domain | Responsibility | Data it owns | Must not |
|---|---|---|---|
| Config | Composes `city.toml` and every pack imported at city scope or on a rig into the one resolved configuration every other part reads. | The declared configuration: the rigs, the imports at city scope and on each rig, and the layers each scope reads formulas and orders from. | Start a session, dispatch work or write a bead. |
| Orders | Discovers the order set, binding each order city-wide or to one rig by where it is defined and the scope it declares. | The order set: each order's definition, trigger, action and binding. | Dispatch an order or write a bead. |
| Controller | Runs one city: it reloads config when the city's files change, reconciles the city's sessions, and dispatches each due order. | The order runs it starts, each recorded as a tracking bead in the store the run goes to. | Decide which orders exist or where one is bound: it dispatches the order set as discovered. |

How they work together:

- Orders scans the layers config resolves, the city's and then each rig's
  own, and applies the city's order overrides and skip list.
- The controller discovers the order set again whenever it reloads config.
  `gc order`, the API and doctor discover it through the same discovery, so
  each shows the orders the controller dispatches.
- To find an order due, the controller reads its past runs from the bead
  store and, for an event trigger, the event bus. A due exec order runs its
  script on the controller's host, with no agent. A due formula order becomes
  a wisp of its formula, routed to the pool the order names, whose agents run
  it.
- A run goes to the store of its order's binding: the city's store for a
  city-wide order, and the rig's store for a rig-bound one. A rig-bound order
  whose pool is a city agent, not one of the rig's, runs in the city's store,
  where that agent reads its work.

The depth is in [config](architecture/config.md), [orders](architecture/orders.md)
and the [controller](architecture/controller.md).

## Cross-cutting rules

- **No upward dependencies.** A lower layer never imports a higher one
  ([layering](architecture/nine-concepts.md)).
- **Beads hold domain state.** Events are the outbound notification of
  activity, fired so people and agents can watch, and config is how every
  capability activates.
- **Side effects stay in Layer 0.** I/O and process spawning happen only
  there.
- **The controller drives SDK infrastructure.** Every infrastructure
  operation, such as gate evaluation, health patrol, the bead lifecycle and
  order dispatch, works with only the controller running.
- **The CLI and the API are projections.** Both call the domain packages,
  neither re-implements domain logic, and every wire shape is typed
  ([invariants](architecture/invariants.md)).
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
- Every package is internal to the binary; none is exported as an SDK API.
- Sessions and runtime providers carry a running agent's lifecycle. There is
  no agent-protocol interface.
- Gas City has no skills system, no capability flags and no MCP or tool
  registration. A sentence in a prompt, or the tool's own CLI, does each job.

## Debt

- **A run's tracking bead does not show whether the run is still going.**
  While an order has an open tracking bead, it starts no other run. After a
  restart, a run that ended with the old controller can keep its order from
  running until the run's timeout passes. After a crash, the next controller
  can close the tracking bead of an exec run that is still going, and a
  second run can start beside it. Order dispatch does not yet meet Work
  converges across a restart. gc-xpnop0 and gc-8utvkm retire it.
