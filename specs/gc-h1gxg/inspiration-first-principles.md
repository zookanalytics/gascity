# Inspiration pass: first-principles design for parallel role-agents (`gc-h1gxg`)

**Bead:** `gc-s7hlv` (inspiration, first-principles) under `gc-h1gxg` (decision)
**Branch:** `gc-s7hlv-first-principles`
**Surveyed at:** 2026-05-11
**Worktree:** `/home/zook/loomington/.gc/worktrees/gascity/polecats/gc-toolkit.slit`
**Base:** `origin/main` @ `e05da010` (`docs(specs): survey gascity session/agent-lifecycle for gc-h1gxg (gc-upid5)`)
**Sibling passes (open under `gc-h1gxg` at filing):**
- `gc-upid5` — gascity-native survey (Pass 1, closed; `specs/gc-h1gxg/survey.md` at `e05da010`)
- `gc-64d5u` — Pass 2 (gascity multi-instance primitive design)
- `gc-9qrf3` — cross-platform inspiration survey
- `gc-kiy9q` — operator UX flows
- `gc-s7hlv` — **this pass** (first-principles design)

This pass is an **inspiration-flavor design exercise**. It sketches what a
minimal system for "operator-attached, role-scoped, parallel thinking
partners" looks like if we wrote it from scratch today — and then audits
Pass 1 to identify which Gas City concepts would reappear in a first-
principles design (essential) versus which are artifacts of Gas City's
history (path-dependent). It does **not** recommend an implementation
direction; that synthesis is mechanik's after all three inspiration passes
land.

## Provenance

| Doc-type / artifact | Producer | Source | Surveyed at |
|---|---|---|---|
| Filing bead | `gc-toolkit__mechanik` (dispatcher) | bd `gc-h1gxg`, parent `gc-wa8cb` (convoy) | 2026-05-11 |
| Inspiration bead | `gc-toolkit__mechanik` (filer) | bd `gc-s7hlv` | 2026-05-11 |
| Decision bead (gc-h1gxg) | operator + mechanik | bd `gc-h1gxg` description | 2026-05-10 |
| Pass 1 (gascity-native survey) | `gc-toolkit__polecat` | `specs/gc-h1gxg/survey.md` @ `e05da010` | 2026-05-11 |
| Sibling pass 2 (in flight) | `gc-toolkit__polecat-codex-lx-5rvnui` | bd `gc-64d5u` | 2026-05-11 |
| Sibling cross-platform inspiration (in flight) | `gc-toolkit__polecat-lx-tfnmkh` | bd `gc-9qrf3` | 2026-05-11 |
| Sibling UX flows (in flight) | `gc-toolkit__polecat-lx-v7iv57` | bd `gc-kiy9q` | 2026-05-11 |

The path-dependence audit (§6) cites Pass 1 directly. Sibling passes 2 / 9qrf3 /
kiy9q are noted as parallel work but not synthesized — synthesis is
mechanik's job downstream.

---

## 0. Setup, restated

From the filing bead, the design constraints are:

1. An **operator** (single human) is the primary interface. They have many
   parallel work-threads.
2. Some agents have **role-knowledge** (architect, mayor, mechanik — system
   roles whose embodiment costs many tokens).
3. The operator needs **parallel conversational access** to role-knowledge
   without bottlenecking on a single instance per role.
4. Some role-instances are **operator-spawned** ("steward"); some are
   **workflow-spawned** ("consultant"); some are **work-driven** (no
   operator attachment).
5. **Mail routes between agents.** Routing addresses roles, with a policy
   (canonical / any-instance / affinity / load-balanced).
6. **"Agent = vendor + prompt"** — instantiating a new role is config, not
   new code.

The exercise is to design the minimum primitive set and the orthogonal
axes a role-instance varies along, then sketch the API.

## 1. The minimum primitive set

### Walk up from one primitive

**One primitive — `Thread`.** Each conversational instance carries its own
role definition inline: prompt, vendor, mail policy. Spawning means handing
a fresh blob of role config to the runtime.

This collapses: roles are reused across many conversations (think: ten
parallel mayors), so re-defining "mayor-ness" at every spawn site duplicates
the persona text and forces operators to know how to encode it. Roles also
*evolve* — when you change a prompt, every existing instance of that role
either has to be re-spawned or has to absorb the change. Inline definitions
make that "all instances pick up the update" semantics impossible without
introducing back-references.

So one primitive doesn't hold. Roles are class-like; instances are run-like;
the distinction needs to live somewhere.

**Two primitives — `Role` + `Thread`.** This is the floor.

- `Role` = a durable spec for a kind of agent.
  Fields: `id`, `vendor`, `prompt`, `mail_policy_default`, `cap_default`,
  optional knowledge attachments. Immutable from the runtime's perspective
  (mutation is a config reload, not a runtime API).
- `Thread` = a durable conversational instance that adopted a role.
  Fields: `id`, `role_id`, `transcript`, `attached_backing` (transient
  pointer), `properties` (overrides of the role's defaults: name shown to
  operator, mail mode, cap participation, lifetime, etc.).

Every other concept in the system either lives on `Role` (defaults), on
`Thread` (instance-level state), or is derived from the relationship
between them.

**Three primitives — add `Backing` as a first-class object.** A `Backing` is
the live process/connection serving a Thread right now. It has its own
identifier and lifecycle (it can crash, restart, be replaced).

Arguments for making it a primitive:
- It has its own failure modes (process death, transport breakage) that
  the runtime must observe and act on.
- A single Thread might re-bind to many successive Backings across its
  life.

Arguments against:
- The operator never names a Backing directly. Their mental model is
  "this is my mayor" (a Thread), not "this is process 42891" (a Backing).
- All Backing operations can be expressed as Thread-scoped operations:
  "restart Thread X" = "swap its Backing"; "what's running?" = "which
  Threads are currently bound to a live Backing?"

**Verdict.** Internally the runtime tracks Backings explicitly; externally
they are invisible. So Backing is an **implementation detail of Thread**,
not a primitive an operator or workflow consumer interacts with. It earns
a name in the implementation but no addressing surface.

**Walking up further?** Mail policies, queue state, workspace attachments,
event streams — all of these are state that lives on Role or Thread, not
new primitives.

### The answer

The minimum primitive set is **two**: `Role` and `Thread`. The operator
sees Threads. Workflows can address either (a Thread directly, or "a Role
under a policy"). Roles supply defaults; Threads carry the live state.

Backings are an implementation detail of Thread. Mail policy is a
property of Role with per-Thread overrides. The message log is a
property of Thread (its transcript, plus an inbox for incoming mail).

This is the load-bearing claim of the pass. The path-dependence audit in
§6 traces how Gas City's current concepts collapse onto this two-primitive
model — and which ones disappear when they do.

---

## 2. Axes a role-instance varies along

A Role provides defaults; a Thread can override most of them at spawn
time. The axes are orthogonal — every combination is valid (with one
constraint noted below).

| Axis | Values | Default-on (where) | Per-Thread override? |
|---|---|---|---|
| **Trigger** | `operator` / `workflow` / `work` / `auto-resume` | n/a (set at spawn) | n/a |
| **Interaction mode** | `attached` (operator-facing) / `detached` (no operator) | Role | Yes |
| **Lifetime** | `persistent` / `session` / `work-bound` / `ephemeral` | Role | Yes |
| **Discoverability** | `addressable` (role-routable) / `unlisted` (id-only) | Role | Yes |
| **Mail policy** | `canonical` / `any-instance` / `affinity` / `least-busy` / `none` | Role | Yes |
| **Cap participation** | `counts` / `excluded` | Role | Yes |
| **Vendor** | `claude` / `gpt` / `gemini` / … | Role | Yes (rare) |
| **Workspace binding** | `none` / `shared-rig` / `dedicated-worktree` / `read-only-overlay` | Role | Yes |

**One non-orthogonal constraint.** If a Thread is `unlisted`, its mail
policy is implicitly `none` (you can't role-route to a Thread that isn't
discoverable). Workflows can still mail an unlisted Thread by id.

**Axis semantics.**

- **Trigger** distinguishes how the Thread was born. `operator` = manual
  spawn from a CLI / UI. `workflow` = another agent spawned this Thread
  to consult it. `work` = a work item dispatched into a Thread (worker
  mode, no operator attachment). `auto-resume` = the runtime brought it
  back after a crash. The Trigger is recorded once and never changes.

- **Interaction mode** is whether the operator has any conversational
  attachment. `attached` = there's a terminal/UI window the operator can
  see and type into. `detached` = the Thread runs without a human
  attached; its outputs go to its mail outbox, transcript, and any
  configured event sinks. A Thread can transition `attached` ↔ `detached`
  freely.

- **Lifetime** governs when the runtime is allowed to delete the Thread.
  `persistent` = lives until explicitly destroyed (operator survives
  reboots). `session` = lives while at least one terminal is attached
  (auto-archives when last operator detaches). `work-bound` = lives until
  its driving work item closes. `ephemeral` = runtime-managed, dies on
  the next idle-sweep.

- **Discoverability** + **Mail policy** together make role addressing
  work. A workflow that mails `role:mayor` triggers the dispatcher to
  pick a Thread per the role's mail policy from the set of `addressable`
  Threads with role=mayor.

- **Cap participation.** Roles can advertise a cap ("no more than N
  mayors"). A Thread with `counts=true` consumes a slot. A Thread with
  `counts=false` (e.g., a side-channel ad-hoc Thread) doesn't.

- **Vendor** override exists because the same persona ought to work
  across providers; a single Role can spawn Threads on different vendors
  (cheap-fast vendor for side conversations, premium vendor for canonical).

- **Workspace binding** is what filesystem/repo state the Thread sees.
  Distinct from the role's *knowledge* (which is part of the prompt).

### Mode aliases (convenience over axes)

In practice you don't pick every axis manually. The Role supplies a set
of **modes** — named bundles of axis values — and the operator picks a
mode. Typical pack-supplied modes:

| Mode name | Trigger | Interaction | Lifetime | Discover. | Mail policy | Cap | Workspace |
|---|---|---|---|---|---|---|---|
| `canonical` | operator | attached | persistent | addressable | canonical | counts | shared-rig |
| `side` | operator | attached | session | unlisted | none | excluded | shared-rig (RO) |
| `consultant` | workflow | detached | session | unlisted | none | excluded | none |
| `worker` | work | detached | work-bound | unlisted | none | counts | dedicated-worktree |
| `peer` | operator | attached | persistent | addressable | any-instance | counts | shared-rig |

The "polecat" of Gas Town is the `worker` mode of any role; the "scratch
clone" is the `side` mode; the "mayor" is the `canonical` mode of role
`mayor`. None of these names are primitive — they're just convenient
labels for axis tuples.

The decision bead `gc-h1gxg` is, in this framing, asking "what mode
should our scratches use, and what mode should our canonical coord
agents use, given the operator wants parallel conversational access?"
The answer: scratches in `side` mode, plus the option to spawn
additional `peer` Threads of mayor when the operator wants two parallel
addressable mayors.

---

## 3. Minimum-viable design sketch

### 3.1 Pack config — registering a role

The pack contributes role definitions and (optionally) named mode
bundles for those roles. Roles are TOML; prompts are filesystem-loaded.

```toml
# pack.toml fragment

[role.mayor]
vendor = "claude"
prompt = "roles/mayor.md"
knowledge = ["roles/mayor.knowledge/"]
mail_policy_default = "canonical"
cap_default = 1                # default: only one canonical mayor

[role.mayor.mode.peer]           # additional "peer mayor" mode
mail_policy = "any-instance"
cap = "unbounded"

[role.mayor.mode.side]           # explicit side mode
interaction_mode = "attached"
lifetime = "session"
discoverability = "unlisted"
mail_policy = "none"
counts = false
workspace = "shared-rig:ro"

[role.architect]
vendor = "claude"
prompt = "roles/architect.md"
mail_policy_default = "canonical"
cap_default = 1

[role.worker.generic]            # generic worker (replaces "polecat")
vendor = "claude"
prompt = "roles/worker.md"
mail_policy_default = "any-instance"
cap_default = 8
[role.worker.generic.mode.work]
trigger = "work"
interaction_mode = "detached"
lifetime = "work-bound"
discoverability = "unlisted"
workspace = "dedicated-worktree"
counts = true
```

Compared to Gas Town's current `[[agent]]` table, three things change:

- Roles are *typed* (mayor / architect / worker), not bare agent records.
- Modes are *named* under the role and pre-mix axes for common cases.
- There is no "agent = polecat" entry. `polecat` is the `worker.generic`
  role in `work` mode. The role name is descriptive, not load-bearing.

### 3.2 Operator CLI

The operator never names a Backing. They name Threads (by id or by
role-modulo-policy) and Roles (by name).

```bash
# What's running?
$ rolesys ls
ID         ROLE                MODE       LIVE  CANON  ATTACHED
th-001     mayor               canonical  yes   ★     tmux:0.0
th-002     mayor               peer       yes         tmux:1.0
th-003     mayor               side       no          (resumable; transcript saved)
th-004     architect           canonical  yes   ★     tmux:2.0
th-005     worker.generic      work       yes         work:gc-s7hlv (detached)
th-006     mechanik            canonical  no    ★     (crashed; restart pending)

# Spawn a new Thread with the role's default mode (canonical)
$ rolesys spawn mayor
spawned th-007 (role=mayor, mode=canonical)
WARNING: role.mayor.cap_default=1 and th-001 is already canonical.
th-007 will be addressable but non-canonical (mode=peer auto-promoted).

# Spawn explicitly in a non-default mode
$ rolesys spawn mayor --mode=side
spawned th-008 (role=mayor, mode=side, unlisted)

# Attach to a Thread (open the terminal/UI window)
$ rolesys attach th-003
(restoring transcript; binding new backing; opening terminal)

# Detach from a Thread (close the window; Thread continues to exist)
$ rolesys detach th-003   # OR just close the terminal

# Send mail
$ rolesys mail role:mayor "PR-42 ready for review"
delivered to th-001 (canonical for role:mayor)

$ rolesys mail th-002 "side question"
delivered to th-002

$ rolesys mail role:worker.generic --any "build something" --work gc-xxxxx
spawned th-009 (role=worker.generic, mode=work) and delivered
  (no canonical, policy=any-instance, no live members available)

# Change canonical
$ rolesys promote th-002 --canonical
ok — role:mayor canonical is now th-002 (was th-001)
th-001 is now mode=peer

# Restart a Thread (re-bind backing; transcript preserved)
$ rolesys restart th-001
backing swapped; transcript preserved; one connected terminal re-bound

# Kill a Thread (terminate transcript, free cap slot)
$ rolesys kill th-008
killed th-008
```

Three principles in the CLI:

- The Thread id (`th-NNN`) is stable; that's what the operator references.
- The Backing is never visible. "Restart" is a Thread operation that swaps
  the Backing under the hood.
- `attach` / `detach` are reversible operations; they don't destroy the
  Thread. A `persistent` Thread with no attached terminal still lives.

### 3.3 Workflow / programmatic API

```python
# Send role-addressed mail (resolved via the role's mail policy)
mail.send("role:mayor", body, from="refinery#abc")

# Send Thread-addressed mail (bypasses policy)
mail.send("th-002", body, from="dispatch")

# Spawn a Thread for a workflow consultation
th = rolesys.spawn(role="architect", mode="consultant", attached=False)
mail.send(th.id, "design question: ...")
reply = mail.await_reply(th.id, timeout="5m")
rolesys.kill(th.id)

# Spawn a worker (no operator attachment, work-bound)
th = rolesys.spawn(role="worker.generic", mode="work", work_bead="gc-s7hlv")
# work bead now has assignee=th.id; worker pulls and executes; Thread
# self-cleans when work_bead closes
```

The workflow API and the operator CLI are two surfaces over the same
two primitives. There's no "operator vs system" partition in the data
model — only in who is allowed to do what (an authz concern outside
this pass).

### 3.4 Mail routing

The dispatcher resolves a target to one or more delivery destinations.

```
resolve(target):
  if target is "th-X":
    return [thread X]
  if target is "role:R":
    candidates = threads_where(role=R, addressable=true, alive=true)
    if candidates is empty:
      if role R has policy.spawn_on_demand:
        return [spawn_new_thread(role=R)]
      else:
        return error: no addressable instances of R
    return [ policy_for(R).choose(candidates) ]

deliver(thread, message):
  append message to thread.inbox
  if thread.attached_backing is live:
    poke backing  # wake-up signal; non-blocking
  # otherwise: message will be observed on next attach / next worker tick
```

Each role-level mail policy is a small choice function:

```
policy=canonical:    choose(C) = the one Thread in C marked canonical
                                 (error if zero; tie-break by recency
                                  if multiple, but role.cap_default=1
                                  should prevent that case)
policy=any-instance: choose(C) = round-robin / random
policy=affinity:     choose(C) = the Thread with prior correspondence
                                 from this sender (else round-robin)
policy=least-busy:   choose(C) = the Thread with smallest inbox depth
policy=none:         not role-routable; deliver-by-id only
```

A role can ship its own policy in code (a function plugged at register
time). The default set above covers the four named policies in the
filing bead, plus the natural `none`.

**Mail to a non-existent canonical.** A mail to `role:mayor` when there
are zero live mayors is an error by default. A role can opt into
`policy.spawn_on_demand=true`, in which case the dispatcher spawns a
fresh Thread in the role's default mode and delivers to it. This makes
"the mayor wakes up when someone needs them" a config-level concern
rather than a special-case in the routing code.

### 3.5 Runtime mechanics (sketch only, not implementation)

For a complete picture, three runtime jobs need to exist:

- **Reconciler.** Owns the Thread → Backing relationship. Notices when a
  Backing crashes or hangs and either replaces it (if the Thread is
  `persistent` or `work-bound` with the work still open) or marks the
  Thread `unbound`. Notices when caps are violated and refuses new
  spawns. Notices when `lifetime=session` Threads have no attached
  terminals and archives them. Owns the auto-resume trigger.

- **Dispatcher.** Resolves mail targets and performs delivery (above).

- **Workspace allocator.** When a Thread declares `workspace=dedicated-
  worktree`, allocates one (idempotent, name-derived from Thread id);
  cleans up when the Thread is killed. When a Thread declares
  `workspace=shared-rig`, simply records the path. None of this lives in
  the Thread primitive itself; it's a runtime side-table.

Each of these is internally implemented but doesn't add a primitive the
operator or pack author sees.

---

## 4. What this design buys

### Direct answers to the filing bead's concrete questions

- *"What is the minimum primitive set?"* — Two: `Role` and `Thread`. The
  proof is that every Gas City concept maps to one of these (or is
  implementation state). §6's audit demonstrates the mapping.

- *"What are the orthogonal axes?"* — Eight, listed in §2: trigger,
  interaction mode, lifetime, discoverability, mail policy, cap
  participation, vendor, workspace binding. Each is independent (with
  the one constraint that `unlisted ⇒ mail_policy=none`). Modes are
  named bundles for ergonomics; they aren't additional axes.

- *"What does mail look like?"* — Two address forms: `th-X` (Thread id)
  and `role:R` (role under policy). Policy is per-role state with five
  named choice functions, extensible per role.

- *"Operator CLI?"* — Verbs over Threads: `ls`, `spawn`, `attach`,
  `detach`, `mail`, `promote`, `restart`, `kill`. No Backing surface.

- *"Pack config?"* — `[role.NAME]` tables declaring vendor, prompt,
  defaults, and named modes. No `[[agent]]` records; the concept of a
  named agent collapses into a Thread + its role/mode.

### Properties that fall out

- **Restart doesn't take side-conversations down.** `attach`/`detach`
  are reversible; restarting Thread X swaps X's Backing without touching
  Thread Y's Backing — they were never coupled.

- **Peer-instance coord agents are free.** Spawn two `peer`-mode mayors
  and route role-mail with `any-instance`. The `canonical` mode is a
  special case (cardinality 1) of the same machinery.

- **No mail "ambiguous."** Every role has a policy. Multi-match is a
  *normal* outcome, not an error — the policy disambiguates.

- **Crash recovery without flags.** A crashed Backing leaves the Thread
  alive; the reconciler binds a new one. No "restart_requested"
  metadata patch dance.

- **Worker pools and side chats are the same mechanism.** Both are
  Threads of a role with cap and policy set differently. The pool/named
  split in Gas Town disappears.

---

## 5. What this design does *not* do

Honestly named limitations:

- **It does not say where transcripts live** beyond "in the Thread." The
  question of how transcripts are stored (event log? blob? messages-as-
  beads?) is an implementation choice. Pass 1 noted that Gas City's
  conversation state currently lives only in the provider process — a
  first-principles answer to durability is: the Thread owns its
  transcript, period. *How* the runtime materializes that is open.

- **It does not address authorization.** Which operator can spawn which
  role, who can promote canonical, whether workflows can spawn arbitrary
  roles — all left out. Authorization is a concern that wraps the API,
  not part of the primitive set.

- **It does not solve cross-runtime federation.** If two operators on
  two machines both run a city, do their Threads share a namespace?
  This pass assumes a single-runtime single-namespace world.

- **It does not enumerate failure modes.** Backing crashes, transcript
  corruption, network partitions, vendor outages — these are real and
  need answers, but they belong in an implementation pass not an
  inspiration pass.

- **It does not pick names for things.** "Thread" might be the wrong
  noun (overloaded with OS threads, with chat threads, with discussion
  threads). "Conversation," "instance," "lane," "stream" all candidates.
  The naming is downstream of the structural decision.

---

## 6. Path-dependence audit against Pass 1

Reading `specs/gc-h1gxg/survey.md`, this section catalogs every named
Gas City concept and labels it **essential** (would reappear in a
first-principles design) or **path-dependent** (an artifact of Gas
City's history). The audit cites Pass 1 sections / specific code
references where relevant.

### 6.1 Essential — would reappear

These concepts (or close analogs) are load-bearing in any first-
principles design that satisfies the filing-bead constraints.

| Gas City concept | First-principles analog | Why it's essential |
|---|---|---|
| `[[agent]]` config record / `internal/config/config.go` | `[role.NAME]` table | You need declarative role specs to honor "agent = vendor + prompt is config, not code." |
| `agent` bead (Pass 1 §3.7) | `Role` primitive | Roles need a durable identity for cross-references (work assignment, mail policy state). |
| Session bead (Pass 1 §3.6) | `Thread` primitive | A durable instance identity is needed for resume/restart, transcript ownership, and mail delivery across Backing replacement. |
| Reconciler crash detection (Pass 1 §1, `session_reconciler.go:828`) | Reconciler over Thread→Backing relation | Something has to notice when the live process dies and replace it; this work is unavoidable. |
| `gc.routed_to=role/mode` work routing + pull-claim (Pass 1 §4) | `mail_policy=any-instance` + pool-of-Threads | "Many homogeneous instances of a role compete for routed work" is exactly the polecat-pool semantic generalized. |
| `metadata.work_dir` + `resolveTaskWorkDir` (Pass 1 §5) | Thread `workspace` attribute + workspace allocator | Worker Threads need filesystem state that survives Backing churn. |
| `KillSessionWithProcesses` process-tree reap (Pass 1 §5 table) | Backing termination | Whatever the Backing is (process, pane, container), tearing it down cleanly is essential; whether it's pane-scoped or session-scoped is the path-dependent part. |
| Worker boundary / `worker.Handle` (Pass 1 active migrations note) | Thread API surface | Centralizing "lifecycle operations on a thing" behind one boundary is sound; the *shape* of the thing is what's being redesigned. |
| Eventing (`session.draining`, `session.stopped`, `session.crashed`, Pass 1 §5) | Thread lifecycle events | Observability of Thread/Backing transitions is essential; the specific event-type vocabulary follows the primitive set. |
| `bd` / Beads as the work-tracking substrate (Pass 1 §3.3 assignee) | Same | Work tracking is orthogonal to the role-instance question. Beads stay. |
| Pool config (`max_active_sessions`, `min_active_sessions`, Pass 1 §4) | Role `cap` + per-mode overrides | Bounding cardinality is essential; the current pool config is the right *idea* generalized to all roles. |
| Aliases for human-friendly addressing (Pass 1 §3.8) | Thread `name` property | Operators need readable handles ("mayor" not "th-001"). Aliases stay as a Thread property. |
| The runtime env block (`GC_AGENT`, `GC_SESSION_NAME`, Pass 1 §3.4) | Thread-injected env | The Backing must know which Thread/Role it's serving. The specific variable names are path-dependent; the injection pattern is essential. |
| Generations / instance tokens for NDI fencing (Pass 1 §3.6 mention of `generation`) | Backing generation counter | When a stale Backing tries to claim work after a Thread's "real" Backing is already a newer instance, fencing prevents corruption. Essential. |
| `session_reconciler` salvage path (Pass 1 §5, `resolveTaskWorkDir`) | Thread auto-resume | Reattaching a Thread to a new Backing while preserving its work context is essential. The specific salvage path is implementation. |

### 6.2 Path-dependent — would not reappear (or would look very different)

These concepts trace to specific Gas City decisions whose constraints
don't carry over to a first-principles design.

| Gas City concept | What's path-dependent | What a first-principles design would do |
|---|---|---|
| **tmux session as the unit of agent identity** (Pass 1 §1 punchline) | Choosing tmux + binding "one agent = one tmux session" couples identity to the transport. The session-vs-pane debate is downstream of this binding. | The Backing is an opaque implementation handle. Whether it's a tmux session, a tmux pane, a subprocess, a container, an ACP socket, doesn't change the Thread API. The decision "one tmux session per Thread" is just the simplest implementation; "one tmux pane per Thread within a shared session" is just as valid. |
| **`gc handoff` / `gc session reset` / `gc session kill` all converging on `tmux kill-session`** (Pass 1 §1 summary table) | The convergence is forced by the session=identity binding; once you bind that, every "restart" is "kill the session." | These verbs unify on `Thread.restart(swap backing, preserve transcript)` and `Thread.kill(terminate transcript)`. Backing-level operations aren't surfaced. The session-vs-pane distinction stops mattering. |
| **The `restart_requested` / `continuation_reset_pending` metadata flag dance** (Pass 1 §1 manager.go:787) | A workaround for not having a clean Thread-vs-Backing split: the reconciler can't simply "restart the Thread" because Thread and Backing are the same record. So it patches flags on the conflated record to signal intent. | `Thread.restart()` directly invokes the reconciler to swap Backings. No flags; no patch dance. |
| **`Manager.RequestFreshRestart` / `RestartRequestPatch`** (Pass 1 §1) | Same: signaling-via-bead-metadata is downstream of the conflation. | Direct API call; the metadata becomes structured Thread state, not free-form patch. |
| **`internal/agent/` having shrunk to "helper code, not a primitive"** (Pass 1 active-migrations) | A path-dependent outcome of the session-first migration. The migration moved live identity to sessions but left `agent` as a vestigial config record. | `Role` is a first-class durable primitive (the config schema with an id and pointer ownership). It's not vestigial; mail policy state and per-role caps live on it. |
| **`SessionNameFor` deriving identity from qualified agent name** (Pass 1 §3.1) | Coupling the tmux identifier to the agent config name means rename-the-config-rename-the-session and tightly binds the live thing to its definition. | Thread id is opaque (random). The Thread carries a *human name* property (overridable per-thread). Rename-the-role doesn't rename anything live. |
| **Pool tmux name as `{basename(template)}-{beadID}`** (Pass 1 §4) | Encoding both "what spawned this" and "which bead" in the *tmux name* is downstream of session=identity. | The tmux name (or any Backing identifier) is implementation; the operator never sees it. Thread id is the user-visible handle. |
| **`session_key` rotation on restart** (Pass 1 §1) | A workaround to invalidate stale references that pointed at the now-killed tmux session. | The Backing has its own short-lived id that rotates per binding; Thread id never rotates. Stale references to a Backing id are naturally invalidated; references to a Thread id remain valid across rebind. |
| **Mail ambiguity / `ErrAmbiguous` from `resolveLiveConfiguredNamedMailTargetCached`** (Pass 1 §3.8 + §6 framing-B analysis) | Treats multi-match as an error because the system has no concept of "this role has a policy for picking among matches." | Multi-match is the *normal* case for `any-instance` / `affinity` / `least-busy` policies. No `ErrAmbiguous` in the design vocabulary. Single-match-required is just `canonical` policy + `cap=1`. |
| **Scratch-clone as overlay-only sibling-window concept** (Pass 1 §2) | The scratch-clone exists *because* there's no first-class "side conversation of the same role" mode in Gas City. The overlay reaches outside the bead-store world to construct it. | `mode=side` is a first-class mode of any role: `unlisted`, `lifetime=session`, `mail_policy=none`. It lives in the same primitive system as canonical Threads; no separate spawn script, no GC_SCRATCH=1 magic, no out-of-band tmux window manipulation. |
| **GC_SCRATCH=1 environment marker** (Pass 1 §2) | A workaround to tell prompt code "you are a scratch, not a real agent." | The Thread's mode and properties are injected directly; the prompt template branches on them if it cares. The role.mode tuple IS the marker, addressable as data. |
| **`scratch-clone-guard.md` prompt fragment** (Pass 1 §2) | Required because scratches are second-class; the prompt has to explicitly tell the model "you are different, don't act like the canonical one." | If desired, role.mode can compose a prompt fragment, but most variation is data the model reads (you're in `side` mode → no mail expected → behave conversationally) rather than text. |
| **The audit `tk-my4za`'s premise that `gc handoff` is pane-scoped** (Pass 1 §2 caveat) | Path-dependent confusion: the *intended* mental model (handoff doesn't take siblings down) and the *actual* implementation (handoff kills the tmux session) diverged because the primitive model can't express what was intended. | The intended semantic ("restart the agent but don't kill the side conversations") *is* the design: restart Thread X swaps its Backing without touching Thread Y. There's nothing to "wire up"; it's the natural shape. |
| **Pool member naming themed names (furiosa, slit, nux)** (Pass 1 §3.5) | Cute, but downstream of needing human-readable handles for pool members in a system that didn't separate Thread id from human name cleanly. | Thread ids are short opaque strings; human names are optional Thread properties. Themed names are a flavor choice for `mode=worker`, not a structural feature. |
| **`pool_slot` metadata on session bead** (Pass 1 §3.5) | A workaround to give pool members a stable identity inside a pool when the tmux name is bead-derived (not slot-derived) and roles need to track which slot a member occupies. | Threads in a `cap>1` role have stable ids; slot accounting is the runtime's job (pool reconciler), not a per-Thread metadata field. |
| **Two parallel restart paths: `handle.Reset` (with restart flag) vs `handle.Kill` (without)** (Pass 1 §1) | The flag-vs-no-flag distinction exists *because* there's no Thread record to decide "should the reconciler bring this back?" The decision had to be encoded as a metadata flag. | Thread has a `lifetime` field. `kill(Thread)` terminates the Thread (and its Backing). `restart(Thread)` swaps the Backing. The reconciler reads the Thread's `lifetime` to decide auto-resume. No flag dance. |
| **`pool_desired ≥ 1` triggering reconciler respawn** (Pass 1 §1 reconciler semantics) | A workaround in the pool world: the "should I be running" signal is encoded in a count rather than as an explicit Thread state. | A Thread with `lifetime=persistent` has an explicit "should be live" property; the reconciler reads it. Pool-cardinality control is a separate role-level signal: "spawn N peers if fewer than M live." Two clean signals instead of one overloaded count. |
| **`gc-ljkvi` ("Named-session crash detection gap")** (Pass 1 §6) | Exists because named sessions live outside the pool reconcile loop; `pool_desired=0` means no auto-respawn. | A `persistent`-lifetime Thread is auto-respawned by definition. The named/pool split disappears; the gap doesn't exist. |
| **Convoy / molecule / formula vocabulary** (CLAUDE.md MEOW stack) | Not path-dependent in the bad sense — these are real abstractions for work composition. *However*, they are a separate concern from the role-instance question. | Same. The convoy/molecule machinery sits on top of the Thread primitive (a worker Thread executes a molecule's step) without needing to change shape. |
| **`internal/runtime/` providers (tmux, subprocess, exec, k8s, acp, hybrid)** (CLAUDE.md primitives) | Already structured around the right idea: "Backing is pluggable." | Confirms the FP analog: Backing is an opaque pluggable thing; the Thread API is provider-agnostic. The provider abstraction is essential; the *tmux-specific* identity coupling is what's path-dependent. |

### 6.3 Mid-status — partially essential

A few items don't cleanly fall into either bucket.

- **The decision between session-scoped and pane-scoped restart**
  (`gc-h1gxg` framing A vs B). In the FP design *this question
  evaporates*: it's an implementation detail of how the Backing is
  realized. tmux-pane Backings restart by `respawn-pane`; tmux-session
  Backings by `kill-session` + respawn; subprocess Backings by SIGTERM
  + relaunch. All are valid; the runtime picks one per provider.
  The framing-A vs framing-B debate in Gas City is essential only
  because the current primitive set forces a choice. The FP design
  doesn't have to choose.

- **The session-model-unification design** (`engdocs/design/session-
  model-unification.md`, accepted, phases 0-2 landed at `0a79e9b8`,
  Pass 1 §6). This is the *partial* migration toward the FP shape:
  "agent-as-config separated from session-as-identity." That separation
  is essential. The work left to do (per the design doc and Pass 1's
  reading) is the deeper split into Role + Thread, where Thread has
  its own first-class identity not just runtime. Session-model-
  unification is the right direction; its end-state lines up with this
  design's two-primitive answer.

- **Naming**: "session," "agent," "alias" all carry historical
  meaning that doesn't quite map. "Session" in Gas City means both
  "instance of an agent" and "tmux session" — these are different in
  the FP design. A first-principles rename to `Role` / `Thread` /
  `Backing` (or whatever) is path-dependent *as naming* but essential
  as a structural rebinding.

### 6.4 Concepts the FP design has that Gas City doesn't yet

For completeness, the FP design names a few things that don't appear
in Pass 1 directly:

- **Mode** (named axis-tuple). Gas City has implicit modes (a polecat
  is a worker-mode worker; a named-session mayor is a canonical-mode
  mayor) but they're not first-class. The FP design hoists them.

- **Mail policy as a per-role declared object.** Gas City has implicit
  per-target resolution (alias → live session) that errors on
  ambiguity. The FP design names the choice function.

- **Backing as a deliberately invisible internal primitive.** Gas City
  has Backings (tmux sessions, subprocess handles) but they leak into
  the operator's surface (session names, `gc session kill`). The FP
  design treats them as strictly internal.

- **Lifetime as a Thread property.** Gas City encodes "should I auto-
  respawn?" implicitly via `pool_desired` and metadata flags. The FP
  design names it directly.

- **`spawn_on_demand` mail policy option.** Gas City has no analog;
  routed mail to an unstaffed role today is just an error.

### 6.5 Summary table — concept-by-concept

For quick reference, a single combined view:

| Gas City concept | Bucket | First-principles disposition |
|---|---|---|
| `[[agent]]` table | path-dependent (shape) | becomes `[role.NAME]` table |
| `[[named_session]]` | path-dependent | absorbed into Thread + mode |
| Agent bead (type="agent") | essential | becomes Role primitive |
| Session bead (type="session") | essential | becomes Thread primitive |
| Pool / `max_active_sessions` | essential (concept) | becomes Role cap + per-mode override |
| Polecat-as-role | path-dependent | becomes `worker.generic` role |
| Scratch-clone (overlay) | path-dependent | becomes `side` mode of any role |
| GC_SCRATCH=1 marker | path-dependent | becomes Thread.mode injection |
| tmux session = identity | path-dependent | Backing is opaque |
| `gc handoff` / `reset` / `kill` | path-dependent (shape) | unifies to Thread.restart / Thread.kill |
| `restart_requested` flag | path-dependent | direct Thread.restart call |
| `session_key` rotation | path-dependent | Backing id rotates per bind; Thread id stable |
| `SessionNameFor` qualified-name derivation | path-dependent | Thread id opaque; human name optional property |
| Mail alias resolver `ErrAmbiguous` | path-dependent | mail policy disambiguates |
| `metadata.gc.routed_to` pull-claim | essential | becomes `any-instance` mail policy + work routing |
| `metadata.work_dir` / `resolveTaskWorkDir` | essential | Thread `workspace` + allocator |
| Process-tree reap (`KillSessionWithProcesses`) | essential (concept) | Backing termination (provider-specific) |
| Worker boundary (`worker.Handle`) | essential | Thread API surface |
| Reconciler (crash detect + respawn) | essential | reconciler over Thread→Backing |
| Generation / instance token (NDI fencing) | essential | Backing generation + Thread fencing |
| Event vocabulary (`session.draining`, etc.) | essential (concept) | Thread lifecycle events |
| `convoy` / `molecule` / `formula` | orthogonal | unchanged; sits over Thread |
| `internal/runtime/` providers | essential | confirms Backing-as-pluggable |
| `gc-ljkvi` named-session crash gap | path-dependent (a symptom) | dissolves: persistent Threads always auto-respawn |
| Session-model-unification (in flight) | essential direction | continues toward Role+Thread split |
| Alias / `GC_ALIAS` | essential | Thread.name property |
| Pool themed names | path-dependent (flavor) | Thread name is a free-form property |

### 6.6 What the audit says about `gc-h1gxg`'s framings

Pass 1 enumerated three framings (A: decouple identity from tmux
session, B: drop scratch + N peer instances, C: hybrid) plus a possible
fourth (move scratches to their own sessions, no gascity change). The
FP design's relation to each:

- **Framing A** (decouple identity from tmux session) becomes a
  *natural property of the design*, not a feature to add. The Backing
  is opaque; whether it's pane- or session-scoped is a provider choice.

- **Framing B** (drop scratch + N peers with one canonical) is a
  *strict subset* of the FP design. The FP design keeps "scratch" as a
  named `side` mode (because operators want a non-cap-counting,
  unlisted side channel; that's a real ergonomic need), and adds
  N-peer support as standard. Scratches survive restart because Thread
  ≠ Backing.

- **Framing C** (hybrid) is mostly what the FP design does, but as a
  consequence of the primitive choice rather than as a compromise.

- **Framing fourth** (scratches in their own tmux sessions; no gascity
  change) — would solve the immediate symptom (scratches survive
  handoff) but leaves the structural confusion (sessions = identity)
  in place. In FP terms it's "use a different Backing-provider
  configuration for `side`-mode Threads" — which is a legitimate
  thing to do, but doesn't address the larger question the decision
  bead names.

---

## 7. Open questions the design surfaces

Not asks to mechanik — just things this pass deliberately doesn't
resolve.

1. **Transcript storage shape.** Threads own their transcripts. *Where*
   transcripts live (event-sourced bead stream, per-Thread blob,
   provider-specific durable log) is open. Pass 1 noted that today
   they live only in the provider process and are lost on restart.
   First-principles answer: they should be durable, owned by the
   Thread record. *How* needs an implementation pass.

2. **Authz model.** Operator vs workflow-agent capabilities. Likely
   wraps the API surface rather than living in the primitives.

3. **Cross-runtime federation.** Two operators sharing Threads? Out of
   scope here.

4. **Thread metadata extensibility.** Pack-supplied roles may want to
   attach role-specific properties (e.g., a `mechanik` role wants to
   advertise a planning style). The Thread.properties bag absorbs this,
   but typed metadata vs free-form is a design choice.

5. **Backing-provider-specific operations.** "Take a screenshot of this
   pane" is a tmux-Backing operation that doesn't generalize. The
   provider surface needs to allow optional operations without polluting
   the universal Thread API. Standard interface-extension territory.

6. **Migration story.** This pass doesn't address how Gas City would
   move from current shape to FP shape. The session-model-unification
   doc is the closest in-flight effort; its next phases would be the
   natural carrier. Migration design is downstream of the decision.

---

## 8. Where this points

This pass deliberately doesn't recommend an implementation direction.
But the design surfaces a few signals worth carrying into the operator
conversation:

- The "scratch dies on handoff" failure is **structural, not
  superficial.** It exists because session-and-identity are bound. No
  amount of overlay scripting around the symptom resolves the
  structure.

- The N-peer-instance question (framing B) and the pane-scoped-restart
  question (framing A) are **the same question** in the FP framing.
  They both reduce to "separate Thread identity from Backing identity."
  Whichever framing the operator-mechanik conversation prefers,
  implementation-side the move is the same primitive split.

- Session-model-unification has already done **phases 0-2** of this
  separation (per `engdocs/design/session-model-unification.md` and
  Pass 1's reading of `0a79e9b8`). The next phases of that design,
  taken to completion, *are* the FP shape. This pass argues the
  decision is "do we finish that work?" rather than "do we adopt a new
  model?"

- The polecat-pool pattern (Pass 1 §4) is **the proof of concept that
  the FP design is achievable**. Polecat pools already do role-scoped
  routing, cap enforcement, pull-claim, and orphan recovery — for one
  mode (`worker`). Generalizing those mechanisms to *all* modes is the
  engineering work.

---

## 9. Reading guide / cross-references

This document is intentionally about structure, not vocabulary. The
names I used (`Role`, `Thread`, `Backing`, "mode," "canonical/peer/
side/consultant/worker") are working labels; mechanik and the operator
should rename freely. What matters is the two-primitive claim and the
axis enumeration.

For the operator-and-mechanik synthesis conversation, the relevant
material:

- **This pass** (`specs/gc-h1gxg/inspiration-first-principles.md`) —
  primitives and axes; what falls out; what's open.
- **Pass 1** (`specs/gc-h1gxg/survey.md`) — what's actually in
  gascity today; ground truth for the audit.
- **Pass 2** (bd `gc-64d5u`, when filed) — multi-instance role-agent
  primitive design grounded in gascity internals.
- **Cross-platform inspiration** (bd `gc-9qrf3`, when filed) — how
  other systems have shaped this problem; useful for naming and for
  spotting axes this pass missed.
- **Operator UX flows** (bd `gc-kiy9q`, when filed) — what the
  operator's day-to-day actually looks like; useful for stress-testing
  the CLI sketch in §3.

Three points the synthesis conversation should hold from this pass:

1. **The split is two primitives, not three or one.** Backing is an
   implementation detail of Thread, not a peer primitive.
2. **Modes are bundles of axes, not their own type.** That keeps
   "polecat / scratch / mayor" as descriptive labels over a uniform
   substrate.
3. **The decision bead's framings collapse.** Framings A and B are the
   same FP move; framing C is the natural consequence. Picking one
   framing in current Gas City terms is choosing what to *show first*
   from the same structural change.
