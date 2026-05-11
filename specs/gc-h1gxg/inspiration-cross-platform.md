# Inspiration: cross-platform survey for operator-attached role agents (`gc-h1gxg`)

**Bead:** `gc-9qrf3` (inspiration pass 3) under decision bead `gc-h1gxg`
**Branch:** `polecat/inspiration-cross-platform-gc-9qrf3`
**Surveyed at:** 2026-05-11
**Worktree:** `/home/zook/loomington/.gc/worktrees/gascity/polecats/gc-toolkit.furiosa`
**Pairing:** pass 1 gascity-native survey at `specs/gc-h1gxg/survey.md`; pass 2 operator-UX inspiration at `specs/gc-h1gxg/inspiration-operator-ux.md` (`gc-kiy9q`, in flight)

This pass is **inspiration**, not synthesis. The point is breadth: scan
other systems for primitives that map onto — or contrast with — what
Gas City would need to support operator-spawnable, role-flavored,
ephemeral agents with ongoing conversation. No design recommendation
appears below. The payload is the closing section: **patterns Gas City
hasn't named yet.**

Lens used per source (where applicable):
- **Identity vs. role** — is an agent's identity the same as its role,
  or a separate handle that role-tags it?
- **Multi-instance posture** — can N peers of the same role coexist,
  and how is one of them addressed singularly?
- **Operator-attached lifecycle** — how is an instance spawned,
  resumed, dismissed; does its transcript survive its process?
- **Mail / dispatch addressing** — by name? by role? by some "any
  available" routing? broadcast?

A source that doesn't bear on the lens for an obvious reason is
included with a one-line note rather than padded with speculation.

## Provenance

Cited references reflect public documentation, source code, and papers
as of the surveyor's training/knowledge through Jan 2026. Where a
project moves quickly (LangGraph, Cursor, Anthropic Projects), specific
feature claims may have drifted; the survey flags areas where the
*pattern* matters more than the *current API*.

| Source | Type | Reference | Surveyed at |
|---|---|---|---|
| LangGraph | OSS framework | https://github.com/langchain-ai/langgraph | 2026-05-11 |
| LangChain (agents) | OSS framework | https://github.com/langchain-ai/langchain | 2026-05-11 |
| CrewAI | OSS framework | https://github.com/crewAIInc/crewAI | 2026-05-11 |
| AutoGen | OSS framework (MS Research) | https://github.com/microsoft/autogen | 2026-05-11 |
| Swarm / openai-agents-python | OSS framework (OpenAI) | https://github.com/openai/swarm , https://github.com/openai/openai-agents-python | 2026-05-11 |
| AgentScope | OSS framework (Alibaba) | https://github.com/modelscope/agentscope | 2026-05-11 |
| MetaGPT | OSS framework | https://github.com/geekan/MetaGPT | 2026-05-11 |
| Cursor (Agents / Background Agents) | Commercial editor | https://cursor.sh ; https://docs.cursor.com | 2026-05-11 |
| Cline (formerly Claude Dev) | OSS VSCode ext. | https://github.com/cline/cline | 2026-05-11 |
| Aider | OSS CLI | https://github.com/paul-gauthier/aider | 2026-05-11 |
| Continue.dev | OSS IDE ext. | https://github.com/continuedev/continue | 2026-05-11 |
| Sourcegraph Cody | Commercial assistant | https://sourcegraph.com/cody | 2026-05-11 |
| Claude Code (Anthropic) | Commercial CLI | https://docs.anthropic.com/claude-code | 2026-05-11 |
| Slack / Discord bot frameworks | Commercial platforms | https://api.slack.com/bot-users ; https://discord.com/developers/docs/intro | 2026-05-11 |
| Tupperbox / PluralKit (Discord) | OSS Discord webhooks | https://pluralkit.me ; https://tupperbox.app | 2026-05-11 |
| OpenAI Custom GPTs | Commercial product | https://platform.openai.com/docs/assistants/overview ; https://help.openai.com/en/articles/8554407 | 2026-05-11 |
| Anthropic Projects | Commercial product | https://www.anthropic.com/news/projects | 2026-05-11 |
| Generative Agents (Park et al.) | Research paper | arXiv:2304.03442 (UIST 2023) | 2026-05-11 |
| AI Town | OSS reference impl. | https://github.com/a16z-infra/ai-town | 2026-05-11 |
| Voyager (Wang et al.) | Research paper | arXiv:2305.16291 | 2026-05-11 |
| CAMEL | OSS framework / paper | https://github.com/camel-ai/camel ; arXiv:2303.17760 | 2026-05-11 |
| Erlang/OTP processes & supervisors | Language/runtime | https://www.erlang.org/doc/system/processes.html ; https://www.erlang.org/doc/design_principles/sup_princ.html | 2026-05-11 |
| Akka actors & routers | Toolkit (Scala/Java) | https://doc.akka.io/docs/akka/current/general/addressing.html ; https://doc.akka.io/docs/akka/current/routing.html | 2026-05-11 |
| Kubernetes (Pods / Deployments / Services) | Container runtime | https://kubernetes.io/docs/concepts/workloads/ ; https://kubernetes.io/docs/concepts/services-networking/service/ | 2026-05-11 |
| tmux sessions / windows / panes | Terminal multiplexer | https://github.com/tmux/tmux/wiki | 2026-05-11 |
| IRC operator semantics | Protocol | RFC 1459, RFC 2812 | 2026-05-11 |
| Raid composition (MMO design) | Design pattern | https://wowwiki-archive.fandom.com/wiki/Raid ; general MMO design literature | 2026-05-11 |
| Shadow of Mordor Nemesis system | Game design | https://en.wikipedia.org/wiki/Middle-earth:_Shadow_of_Mordor#Nemesis_System | 2026-05-11 |
| MUD/MMO GM "possess" puppeting | Design pattern | LambdaMOO / generic MUSH @possess; https://en.wikipedia.org/wiki/MUSH | 2026-05-11 |

## 1. AI orchestration frameworks

### LangGraph
**What it is.** Graph-runtime for stateful LLM apps: nodes are
callables, edges are transitions, state is a threaded dict
checkpointed by a saver (Memory / Postgres / SQLite).
**Primitive exposed.** **Thread** — a `thread_id` keys a checkpoint
chain that persists conversation state independent of the agent
*code* that runs it. Subgraph spawning (`Send`) lets one node fan out
to N parallel instances of the same subgraph, each with its own
state slice, joined back by a reducer.
**Borrow vs. contrast.** *Borrow:* the **thread-id-as-conversation-identity** abstraction is the
most natural analog of "this scratch conversation continues even if
the agent process is restarted." A polecat session bead is close, but
the *transcript* dies because the provider isn't asked to checkpoint
on it. *Contrast:* LangGraph has no operator-attached UI — threads
are addressed by code, not by a human in a terminal.

### LangChain (agents)
**What it is.** Library for assembling LLM + tools + memory; an Agent
is a callable, optionally with a memory backend.
**Primitive exposed.** **Agent-as-callable + decoupled memory
backend.** The "memory" abstraction (`ConversationBufferMemory`,
`VectorStoreRetrieverMemory`, etc.) is orthogonal to the agent
instance, so memory survives independent of process.
**Borrow vs. contrast.** *Borrow:* the **memory-store-as-separate-from-agent** decomposition is what makes
"restart the process, keep the conversation" possible. *Contrast:* no
multi-instance dispatch story; you just instantiate two callables.

### CrewAI
**What it is.** Crew = list of `Agent(role=..., goal=..., backstory=...)`
+ list of `Task(...)`. A "manager LLM" can run hierarchical mode.
**Primitive exposed.** **Role-as-prompt-bundle** — `role`, `goal`, and
`backstory` are pure strings on the Agent object, **separable from
the agent instance** (you can deepcopy or templatize them). Two
agents with identical role strings are distinct Python objects.
**Borrow vs. contrast.** *Contrast:* CrewAI uses Python references
("this Agent here") as identity. No addressing-by-role-when-N-exist
problem because the crew topology is hand-wired up front. Gas
City's mail-by-alias model has no equivalent.

### AutoGen
**What it is.** Multi-agent conversations: each participant has a
`name`; `GroupChatManager` orchestrates speaker selection;
`UserProxyAgent` represents the human.
**Primitive exposed.** **Named participant in a group chat** —
addressing is by name string; group is a shared conversation log.
**`UserProxyAgent`** is a first-class operator-seat at the table:
the operator is literally one of the named agents in the chat.
**Borrow vs. contrast.** *Borrow:* the **`UserProxyAgent` model** is
the cleanest framing of "operator is a participant, not an outsider"
seen in this scan. A scratch clone in Gas City could be reframed as a
`UserProxyAgent` that the operator drives, sharing a "group chat" log
with the canonical mayor. *Contrast:* AutoGen assumes a single group
chat; Gas City has many work streams.

### Swarm / openai-agents-python
**What it is.** Lightweight "handoff" pattern: an Agent's tool can
`return another_agent` and the conversation transfers to it,
carrying message history.
**Primitive exposed.** **Handoff** — explicit transfer of
*conversation* (not state) from one agent to another. The
conversation is the durable thing; the agent is the role-flavor
applied to it at any given moment.
**Borrow vs. contrast.** *Borrow:* the **conversation-as-portable-thing-that-agents-take-turns-owning**
inversion. Gas City currently treats the session/process as primary
and the conversation as a side-effect of it. Swarm flips that: the
conversation is the primary thing, agents are pluggable.
*Contrast:* no multi-instance story; handoff is one-at-a-time.

### AgentScope
**What it is.** Multi-agent framework (Alibaba) with a Studio web
UI; agents are named, organized by `Pipeline` / `Msghub` /
`Sequential`; Studio shows live message panes per agent.
**Primitive exposed.** **`Msghub`** — a structured message bus with
named publishers/subscribers and topic-style routing among agents.
**Studio** as an operator-attached observation surface.
**Borrow vs. contrast.** *Borrow:* the **studio-pane-per-agent** UX
is a direct visual analog of tmux-pane-per-agent — multiplied with
record/replay/breakpoint. Suggests Gas City's dashboard could go
deeper than "list" into "conversation-pane-per-agent."

### MetaGPT
**What it is.** "Software company" — fixed roles (ProductManager,
Architect, Engineer, QA) collaborating via a shared `Environment`
message log.
**Primitive exposed.** **Role-class + subscription filter.** Each
role-class declares which message types/from-whom it watches; the
shared log + filters route work without explicit addressing.
**Borrow vs. contrast.** *Borrow:* the **publish-to-shared-log, agents-self-select** pattern is the natural form of
"pool work" — no one addresses the engineer by name; whoever's
watching `code-needed` picks it up. Gas City's `gc.routed_to`
metadata is close but more push-shaped. *Contrast:* MetaGPT
hard-codes one-instance-per-role; doesn't address N peers.

## 2. Coding assistants with multi-instance modes

### Cursor (Modes / Agents / Background Agents)
**What it is.** AI-first editor. "Modes" (Ask, Edit, Composer,
Agent) are interaction shapes within one chat. **Background Agents**
(rolled out 2025) are async workers spawned from the chat that run a
task off-thread and report back into a sidebar list; multiple
parallel agents are explicitly supported.
**Primitive exposed.** **Agent-as-sidebar-entry** — every running
background agent is a row in a sidebar with status, name, and
attach action. Spawning is "fork a task from the current
conversation"; the agent then has its own conversation.
**Borrow vs. contrast.** *Borrow:* the **sidebar-of-live-agents UI**
plus **fork-from-current-conversation spawn** is a very close cousin
to "operator-spawned role agent." The fork-point matters: the new
agent inherits enough context to be useful without inheriting the
operator's entire transcript. *Contrast:* Cursor's agents aren't
role-flavored — each is task-flavored. The role-as-persona axis
isn't expressed.

### Cline (Claude Dev)
**What it is.** VSCode extension; one Cline session per workspace.
**Primitive exposed.** Workspace-scoped chat; no multi-instance
within a workspace. To get peers you open more VSCode windows.
**Borrow vs. contrast.** Not directly relevant to the multi-instance
question; instructive that "one agent per workspace" is the natural
fall-back UX for IDE assistants. Suggests the operator's mental
model may already be "one canonical per pane."

### Aider
**What it is.** CLI pair-programmer; one Aider per terminal; chat
history written to `.aider.chat.history.md` in the repo.
**Primitive exposed.** **Chat-history-as-file-in-repo.** Restart =
re-run aider; pass the history file to continue the conversation.
The *agent process* is fully disposable.
**Borrow vs. contrast.** *Borrow:* the **transcript-as-on-disk-artifact** pattern. Gas City's worktree is the analog of
the repo; a `.gc/scratch-transcripts/<name>.md` could give scratches
the same restartability that worktree work already enjoys. Today
the provider's conversation lives in the provider process only.

### Continue.dev
**What it is.** Multi-session IDE assistant; a sessions list lives
in the sidebar; switch between sessions in one click; sessions
persist locally under `.continue/sessions/`.
**Primitive exposed.** **Session-as-named-thread** with a
session-picker UI. Sessions are typed (`chat`, `agent`, etc.) and
named by the user.
**Borrow vs. contrast.** *Borrow:* the explicit **switcher across
many concurrent named conversations** is the operator-attached
multi-thread management that AutoGen's group-chat model lacks.
*Contrast:* still one-conversation-at-a-time visually; only one
"active" session.

### Sourcegraph Cody
**What it is.** Code-aware chat in IDE; multiple parallel chats via
tabs; chats persist server-side.
**Primitive exposed.** **Chat-tab UI** with server-side persistence.
**Borrow vs. contrast.** Same family as Continue.dev — the
multi-chat UI is the recurring shape. Notable in Cody: server-side
storage lets the operator resume from a different machine, which
suggests "scratch transcript on city storage, not pane-local
provider memory."

### Claude Code
**What it is.** Anthropic's CLI. Parent agent in the terminal; an
`Agent` tool spawns sub-agents (each is a fresh Claude with its own
context) that run to completion and return one message.
**Primitive exposed.** **Sub-agent-as-tool-call.** Sub-agents are
fire-and-forget; the operator only sees the parent's view, with
sub-agent results inlined into the parent's transcript.
**Borrow vs. contrast.** *Contrast:* the **operator never converses
with a sub-agent.** This is the inverse of what Gas City is
exploring — Gas City wants the operator to *converse with* an
ephemeral role-agent, not just delegate to one. Worth contrasting:
when does a delegate-and-return primitive suffice, and when does
ongoing-conversation justify a richer model?

## 3. Chatbots with persona threading

### Slack & Discord bot frameworks
**What they are.** Each bot has a single account-level identity per
workspace (user id / bot id). Channels = shared rooms, threads =
in-channel subconversations.
**Primitive exposed.** **Thread-as-ephemeral-subconversation.** A
thread under a message is a bounded scope the bot can address
specifically; reply-in-thread vs. reply-in-channel is a first-class
distinction. Some bots use private channels per user as
"workspaces."
**Borrow vs. contrast.** *Borrow:* the **thread-as-context-scope**
pattern. An operator-attached scratch could be modeled as "a
thread under the canonical agent's channel" — addressing routes to
the thread context, message history stays in the platform.

### Tupperbox / PluralKit (Discord)
**What they are.** Webhook-based "persona" bots that let one human
post as many distinct named/avatar'd personas, triggered by message
prefix or proxy tag.
**Primitive exposed.** **Persona-as-webhook.** One identity-slot in
the platform, many surface-level personas — the operator picks
which persona is speaking by message prefix; transcripts
interleave naturally.
**Borrow vs. contrast.** *Borrow:* the **one-operator-many-personas
proxy** model. Maps onto the operator-attached scratch: a scratch
is "mayor-persona, but it's me typing" — Tupperbox makes that
literal. The proxy gives the operator on-demand role-flavor
without a separate process at all.

### OpenAI Custom GPTs
**What they are.** Named GPTs (instructions + tools + knowledge)
published to a store; conversations are per-user-per-GPT.
**Primitive exposed.** **Published-persona-with-URL.** Identity =
GPT id (publisher-owned); each user spawns their own conversation
thread against the same GPT.
**Borrow vs. contrast.** *Borrow:* the
**persona-as-versioned-publishable-thing** pattern. Mayor /
mechanik / deacon prompts could be GPTs in this framing —
instructions versioned at the source, conversations per-user (or
per-operator-session). The Gas City `[[agent]]` config block is the
analog; the missing piece is "many parallel conversations against
the same role-config."

### Anthropic Projects
**What they are.** Persistent workspace with project-level
instructions and pinned context; many conversations per project.
**Primitive exposed.** **Project-as-context-namespace.** Project
pins context; conversations inherit and accumulate.
**Borrow vs. contrast.** *Borrow:* the **two-level
{persistent-project, ephemeral-conversation}** model. Maps to: "the
canonical mayor is the project; operator-attached scratches are
new conversations under that project that share its pinned
context."

## 4. Generative-agent research

### Park et al. — Generative Agents (UIST 2023, arXiv:2304.03442)
**What it is.** 25 named agents in Smallville with memory streams,
hierarchical reflection, daily planning; persisted state across
days.
**Primitive exposed.** **Memory stream + reflection ladder** —
agents stay coherent over long time horizons because retrievable
memory + periodic reflection keeps identity stable even after
restarts.
**Borrow vs. contrast.** *Borrow:* the **agent-identity-as-memory-stream**
framing — identity isn't the process; it's the durable memory.
*Contrast:* generative-agent research has no
operator-attached-conversation concept; the "operator" is the sim
designer who can inject events but doesn't chat 1:1 with an agent
mid-life.

### AI Town (a16z-infra/ai-town)
**What it is.** Open-source Smallville-style sim on Convex; named
agents synced reactively to a web view; operator can add/remove
agents.
**Primitive exposed.** **Persistent-agent-as-database-row** with
reactive sync; agent identity = row id, independent of any process.
**Borrow vs. contrast.** *Borrow:* the **agent-as-row-in-store, process-is-just-a-runner**
posture. Closely matches Gas City's bead model already; reinforces
that the bead store is the identity, and tmux is the runner.

### Voyager (Wang et al., 2023; arXiv:2305.16291)
**What it is.** Minecraft agent with a growing skill library
(JavaScript snippets written by the agent).
**Primitive exposed.** **Skill library as durable artifact
separate from agent instance.** A new Voyager run inherits the
prior library.
**Borrow vs. contrast.** *Borrow:* the **artifact-outside-the-agent
that survives restart** pattern. Suggests "what should survive a
scratch ending?" — maybe nothing, maybe a structured summary, maybe
distilled-into-skills. The Gas Town bead model already supports
"output bead," but a *skill artifact* (vs. a work artifact) isn't a
named pattern there yet.

### CAMEL (arXiv:2303.17760)
**What it is.** Role-playing two-agent system; an "AI user" and an
"AI assistant" cooperatively solve a task via dialogue.
**Primitive exposed.** **Role-playing duo with explicit role
contract.** Each agent's prompt commits to a role; conversation is
the substrate.
**Borrow vs. contrast.** *Borrow:* the **explicit-role-contract-as-prompt** pattern;
two-agent setups can make role asymmetry rigorous. The Gas City
mayor↔mechanik conversation has a similar shape implicitly; making
it explicit could help when introducing peer/scratch instances.

## 5. Game / sim design analogies

### Raid composition (WoW, FFXIV, etc.)
**What it is.** Encounter group of N players; each fills a role
(tank, healer, DPS); roles have multiplicity (e.g., 2 tanks).
**Primitive exposed.** **Orthogonal axes: name (instance) and role
(class).** "MT" (Main Tank) and "OT" (Off Tank) are *canonical
designations among role-peers* — informal but functionally
load-bearing.
**Borrow vs. contrast.** *Borrow:* the **canonical-vs-peer
distinction inside the same role**. "MT/OT" is the gaming-culture
analog of "canonical/scratch." It works because raid leaders call
plays by role-designation, not by character name; the designation
is reassignable on the fly. Suggests: "canonical" could be a
*designation* (mutable, transferable) rather than a *type* (fixed
on spawn).

### Party of N (classic JRPGs, BG3, FF Tactics)
**What it is.** Player controls a party; each character has unique
identity and a class/role; operator focus cycles between members.
**Primitive exposed.** **Operator-cyclable focus** — the operator's
attention is a single beam they can point at any party member;
non-focused members continue acting autonomously per their config.
**Borrow vs. contrast.** *Borrow:* the **focus-cursor** abstraction
— who is the operator currently "in" right now? The dashboard /
tmux switch-pane is the analog. Operator-attached scratches are
party members the operator can re-focus to without spawning anew.

### Shadow of Mordor Nemesis system
**What it is.** Game procedurally generates named NPCs that persist
across encounters, remember the player, and gain narrative
weight over time.
**Primitive exposed.** **Emergent-named-NPC-with-history** — the
system spawns identities; each accrues a transcript-of-encounters
that *is* its character.
**Borrow vs. contrast.** *Borrow:* the **history-as-identity-substrate**
pattern. A peer-instance mayor that has lived through 30 incidents
is meaningfully different from a fresh peer-instance mayor — even
if their config is identical. Suggests: in a peer-instance model,
"which peer should I keep?" might be answered by "which one has
the richer history."

### MUD/MMO GM "possess" puppeting
**What it is.** Game Masters in MUDs/MUSHes can spawn an NPC and
"possess" it — type commands and chat as that NPC, then release it
when done. The NPC has a name; players don't know a human is
behind it.
**Primitive exposed.** **GM-puppeted ephemeral named entity.** The
human operator drives an in-world identity for a bounded time,
then releases.
**Borrow vs. contrast.** *Borrow:* the **possess / release**
lifecycle as a literal operator-attached-agent model. This is the
closest cultural analog of the proposed Gas City primitive — a
named, role-flavored, ephemeral entity the operator inhabits for a
conversation and then dismisses. The MUSH `@possess` command (and
its release semantics) is worth re-reading as design grammar.

## 6. Programming-language and runtime patterns

### Erlang/OTP — processes, registered names, supervisors
**What it is.** Lightweight processes with unique PIDs; can be
`register`'d under a name for global lookup; supervisors restart
crashed children per strategy (one_for_one / one_for_all / rest_for_one).
**Primitive exposed.** **PID = instance ; registered name =
role-handle.** Two PIDs for the same role-handle is illegal in
`register`, but `global` and `gproc` extensions allow
*property-based* registration where many processes claim the same
role tag. Hot-code-reload preserves PID across code change.
**Borrow vs. contrast.** *Borrow:* the **registered-name-vs-PID
separation** is the canonical formulation of "role-name vs.
instance-id." `gproc` properties are the canonical
"role-with-N-instances" extension. *Contrast:* Erlang's
single-registry-per-name is exactly the constraint Gas City would
need to relax (see pass 1 survey §3 on `cmd/gc/cmd_mail.go:688`
`ErrAmbiguous`).

### Akka actors and routers
**What it is.** Actor model with addressable paths (`/user/mayor`)
and `Router` actors that fan a message to a pool of routees
(round-robin, broadcast, random, consistent-hash, smallest-mailbox).
**Primitive exposed.** **Actor-path** as a durable address;
**router-as-the-any-instance-abstraction**. Routers let you
address `/user/mayor-router` and the runtime picks a routee.
**Borrow vs. contrast.** *Borrow:* the **router** is the missing
piece for "send mail to any mayor" semantics. Gas City's existing
mail resolver is single-target; a router would be a new resolver
type ("alias resolves to a routing strategy across the matching
session beads"). *Contrast:* Akka assumes addressable, durable
actors; doesn't have an operator-attached-converse-with-an-actor
notion natively.

### Kubernetes — Pods, Deployments, Services
**What it is.** Pods are instance units; Deployments hold N
replicas of a Pod template; Services give a stable DNS name that
load-balances across replicas. `kubectl exec` attaches to a
specific Pod by name.
**Primitive exposed.** **Stable-name-fronting-pool** (Service) +
**direct-attach-to-instance** (`kubectl exec <pod>`). Two
addressing modes coexist: "I don't care which" (Service) and
"this exact one" (Pod name).
**Borrow vs. contrast.** *Borrow:* the **two-addressing-modes**
posture is exactly what an N-peer-instance role model needs.
"Mail mayor" should mean Service-semantics; "nudge
mayor-instance-3" should mean Pod-semantics. Gas City has the Pod
addressing (session bead ID) but not the Service-style indirection.

### tmux sessions / windows / panes
**What it is.** Sessions contain windows contain panes; each level
is named/addressable; clients attach to sessions.
**Primitive exposed.** **Hierarchical name scopes** — pane
identity is rooted in a session; killing the session takes the
panes; respawn-pane is a finer-grained operation that the
gascity-side path doesn't currently invoke (per pass 1 survey §1).
**Borrow vs. contrast.** *Constraint surfaced:* the inherited
hierarchy (session-owns-panes) is exactly what makes scratches
fragile. The pattern itself isn't novel — it's the substrate Gas
City already runs on. Gas City's choice is whether to fight the
substrate (pane-scoped restart) or relocate scratches outside the
session hierarchy.

### IRC operator semantics
**What it is.** Channels are shared rooms; nicks are user
identities; operators (`+o`) have channel-management rights;
private messages route nick-to-nick.
**Primitive exposed.** **Nick (instance) vs. channel (context)**;
**roles-as-channel-modes**. Bot frameworks built on this routinely
have one bot = one nick with channel-keyed behavior.
**Borrow vs. contrast.** Historic baseline for the chatroom shape.
The Slack/Discord patterns are the modern descendant; included
because the nick-vs-channel split is the simplest formulation of
"instance addressing vs. shared context."

## 7. Closing — patterns Gas City hasn't named yet

The point of this pass. Each named pattern is a concept the survey
surfaced that Gas City's vocabulary (`agent`, `session`, `alias`,
`pool`, `polecat`, `scratch`) does **not currently express**. They
are not recommendations — they are *vocabulary expansions* worth
having on the table for the operator conversation.

### 7.1 The "Service vs. Pod" distinction
Two distinct mail/dispatch addressing modes that K8s makes
explicit and Gas City currently merges:
- **Service-style:** "send mail to *a* mayor" — router picks any
  available instance; the alias is a pool handle.
- **Pod-style:** "send mail to *this specific* mayor instance" —
  by session-bead-id or pool-instance name.
Gas City's alias today is Pod-style with an ambiguity guard. A
Service-style alias is a *new resolver*, not a new primitive.

### 7.2 The "Router" abstraction (Akka)
A first-class object whose only job is to fan a message to a pool
by a *named strategy*: round-robin, broadcast, smallest-mailbox,
consistent-hash. Sits between "alias" and "agent." Could be a
Service variant or its own thing.

### 7.3 The "UserProxyAgent" seat at the table (AutoGen)
The operator as a *first-class participant in a multi-agent
conversation*, with the same addressing affordances as the
software agents. Gas City currently treats the operator as
"outside the system, types commands"; AutoGen treats them as one
of the named speakers.

### 7.4 The "Possess / Release" lifecycle (MUSH / MUDs)
Spawn a named role-flavored entity, drive it conversationally for
a bounded time, release it. Closer to Gas City's proposed
operator-attached agent than any modern coding-assistant analog.
The MUSH grammar (`@possess`, `@release`) is small and complete.

### 7.5 "Conversation-as-portable-thing-agents-take-turns-owning" (Swarm)
Inversion of Gas City's current "session is primary, conversation
is incidental." A scratch could be modeled as a *conversation that
gets handed from the canonical mayor to a scratch-flavor and back*,
rather than as a separate agent that has its own conversation.

### 7.6 "Thread-id as conversation identity" (LangGraph) /
**"Transcript-as-on-disk-artifact" (Aider)**
Two flavors of the same observation: the *conversation* survives a
process restart **if it's externalized**. Gas City externalizes
work state (beads, worktrees) but not provider-conversation state.
A scratch transcript-file in `.gc/scratches/<name>.md` would let
the operator resume any scratch from any session — exactly the
property pass 1 §5 lists as missing from the salvage inventory.

### 7.7 "Sidebar-of-live-agents" UX (Cursor / Continue.dev / Cody)
A persistent UI list of every live agent the operator has spawned,
with status and one-click attach. Gas City has `gc session list`
and the dashboard; neither yet shows *conversation-pane-per-agent*
the way Studio (AgentScope) or Cursor's agents panel does.

### 7.8 "Persona-as-webhook" / one operator, many personas (Tupperbox)
The operator drives multiple named personas without spawning
multiple processes. A scratch-equivalent that's *zero-process*: the
operator picks a role flavor for the next message and the system
applies the corresponding system prompt. Avoids the lifecycle
problem entirely by *not* creating a new instance.

### 7.9 "Canonical-as-designation, not type" (raid MT/OT)
"Canonical" as a *transferable role tag* assigned to one peer
among N, rather than a *spawn-time type* baked in. Lets the
canonical role move between peers — e.g., a scratch can be
promoted to canonical, or the canonical's role flag can flip to a
peer when it crashes. Erlang's `global:re_register_name/2` is the
runtime analog.

### 7.10 "History-as-identity-substrate" (Nemesis system, generative agents)
Two peer instances with identical configs differ by *what they've
been through*. If Gas City keeps peer instances around long-term,
their differentiating history (mail received, decisions made,
beads touched) becomes their identity. Suggests a "show me the
biographical chain of this instance" affordance worth more than a
session bead's metadata flat record.

### 7.11 "Publish-to-shared-log, agents-self-select" (MetaGPT)
Pure pull-based dispatch where roles subscribe to message types
and the framework doesn't pre-route. Gas City's `gc.routed_to`
metadata is a half-step in this direction (the work isn't claimed
yet, but it *is* tagged for a pool). MetaGPT's filter-on-subscribe
is more permissive: an agent picks up any message it's
configured to watch.

### 7.12 "Project-as-context-namespace, conversation-as-leaf"
(Anthropic Projects / Custom GPTs)
A persistent role/persona definition with N transient
conversations rooted in it, sharing pinned context. Maps to: "the
mayor role-config is the project; every scratch is a fresh
conversation pinned to that project."

### 7.13 "Skill library as artifact separate from agent" (Voyager)
Conversations end; *distilled artifacts* persist. If scratches end
without a work-bead, but a structured "what we figured out"
artifact lives somewhere, the value isn't lost. Distinct from a
work bead; closer to a *durable memo*.

### 7.14 "Two-level identity: stable-config + ephemeral-instance"
(K8s Deployment, OpenAI Custom GPTs, Anthropic Projects, Continue
sessions)
Recurring pattern across categories: a long-lived configuration
artifact (Deployment, GPT, Project, agent-config) plus a fleet of
ephemeral runtime instances that inherit from it. Gas City has
both (`[[agent]]` config + session bead) but the relationship is
1:1 rather than 1:N for coord roles.

### 7.15 "Focus cursor" (JRPG party UI / tmux)
A single operator-attention pointer that can move freely across
many running agents. The cursor *is* the operator-attached
affordance — every other agent is running unattended until the
cursor lands on them. Reframes "scratch agent" as "an agent the
cursor is currently on."

---

These patterns have been **named** but not **evaluated against
Gas City**. That evaluation is the next step (synthesis pass or
operator conversation); per the dispatch instructions, this pass
stops at "surface, don't fit-assess."
