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
| Bead store | The beads store, backed by Dolt in production, that holds the beads of the city and its rigs. |
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
project them. The rows below are the parts that deliver Gas City's hooks to
a coding agent.

| Domain | Responsibility | Data it owns | Must not |
|---|---|---|---|
| Config | Resolves an agent's provider and composes the command that launches it, including Gas City's hook registration for a provider that takes it at launch. | The city's declared configuration and the provider specs it resolves. | Start a session or write its work dir. |
| Provider hooks | Holds Gas City's managed hooks for each provider family and how that family receives them: the arguments a launch carries, or the files installed into a work dir. | The managed hook documents embedded in the binary, and the trust hash each Codex hook carries. | Write a provider's home or any host-wide hooks file. |
| Sessions | Starts, stops and observes a coding agent through a runtime provider, with the command config composed and the files staged into its work dir. | Each session's lifecycle record and the fingerprint of the configuration it started with. | Compose a provider's hook configuration itself. |

The depth is in [config](architecture/config.md), [session](architecture/session.md)
and the [provider hook configs](../internal/hooks/config/README.md).

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
- **A session start is never config drift.** The reconcile tick writes a
  session's overlay files, and strips Gas City's own entries from them,
  before it fingerprints the session's configuration, so the files session
  start stages never read as a change.

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
- Gas City's managed hooks travel with a session's launch command when its
  provider takes them there: Claude through the city's settings file, passed
  with `--settings`, and Codex through a `-c hooks=` override that also
  carries each hook's trust hash. A provider that reads hooks only from files
  gets them installed into the session's work dir when
  `install_agent_hooks` names it. Codex reads no hooks file from a linked
  worktree's own directory, so a work-dir file never reaches such a session.
- Registering hooks writes no provider home and no host-wide hooks file, so
  a second city, or a provider session the operator starts by hand, is
  unaffected.
- A work dir's Codex hooks file belongs to the pack or user that ships it.
  Gas City removes only its own managed entries from it and keeps every
  other entry, because Codex would run a copy there a second time beside the
  launch registration.

## Debt

- **A Codex provider's own `resume_command` resumes without the managed
  hooks.** That template carries no hook override, so a session it resumes
  runs without `gc prime`, nudge and mail delivery, or handoff. A reader who
  gives a Codex-family provider a `resume_command` must know it. No open bead
  retires it: gc-wpeoqs closed it as latent.
- **Session start can run a managed Codex hook twice.** It stages a pack's
  Codex hooks file into the work dir just before Codex starts. Outside a
  linked worktree Codex loads that copy, so a pack whose file holds Gas
  City's own commands runs each of them twice in that session; the next
  reconcile tick strips the file only after Codex has read it. gc-und66b
  retires it.
