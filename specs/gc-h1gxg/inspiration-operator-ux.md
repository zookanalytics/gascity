# Inspiration: operator UX flows for parallel role-agent conversations (`gc-h1gxg`)

**Bead:** `gc-kiy9q` (inspiration, operator-UX flavor) under `gc-h1gxg` (decision)
**Branch:** `gc-kiy9q-inspiration-operator-ux`
**Drafted at:** 2026-05-11
**Worktree:** `/home/zook/loomington/.gc/worktrees/gascity/polecats/gc-toolkit.nux`
**origin/main:** `29565ba1` (zookanalytics/gascity)
**Pairs with:** `specs/gc-h1gxg/survey.md` (Pass 1, research, `gc-upid5`)

This is an *inspiration* document. It is not a specification, not a
recommendation, and not a roadmap. The goal is to make the abstract
decision in `gc-h1gxg` ("scratch-clone vs N-peer-instance vs hybrid")
land in the operator's body — to sketch what the same primitive choice
*feels like* from the seat in front of tmux + the web dashboard. The
six flows below are taken straight from the dispatch bead.

## Provenance

| Doc-type or artifact | Producer (skill / concept / workflow step) | Source location | Surveyed at |
|---|---|---|---|
| Filing bead | `gc-toolkit__mechanik` (dispatcher) | bd `gc-h1gxg`, parent of `gc-kiy9q` | 2026-05-11 |
| Inspiration bead | `gc-toolkit__mechanik` (filer) | bd `gc-kiy9q` | 2026-05-11 |
| Pass 1 (research survey) | gascity polecat `gc-toolkit.slit` | `specs/gc-h1gxg/survey.md` @ `e05da010` | 2026-05-11 |
| CLI top-level commands | gascity cobra | `cmd/gc/main.go` + `cmd_*.go` @ `29565ba1` | 2026-05-11 |
| `gc session` subcommands | gascity cobra | `cmd/gc/cmd_session.go` (`start`, `list`, `attach`, `nudge`, `alias`, `kill`, `reset`) @ `29565ba1` | 2026-05-11 |
| `gc mail` subcommands | gascity cobra | `cmd/gc/cmd_mail.go` (`send`, `inbox`, `peek`, `read`, `archive`, `reply`) @ `29565ba1` | 2026-05-11 |
| `gc handoff` semantics | gascity cobra | `cmd/gc/cmd_handoff.go` @ `29565ba1` | 2026-05-11 |
| `gc sling` semantics | gascity cobra + formula resolver | `cmd/gc/cmd_sling.go`, `internal/sling/` @ `29565ba1` | 2026-05-11 |
| Dashboard ttyd panel | gascity dashboard | `cmd/gc/dashboard/web/` (commit `c1ef98e6` "spike: embed ttyd iframe panel for mayor terminal") | 2026-05-11 |
| Scratch-spawn overlay | gc-toolkit pack (read-only cross-rig reference) | `/home/zook/loomington/rigs/gc-toolkit/assets/scripts/tmux-spawn-scratch.sh` lines 1-87 | 2026-05-11 |
| Scratch-clone guard | gc-toolkit pack (read-only cross-rig reference) | `/home/zook/loomington/rigs/gc-toolkit/template-fragments/scratch-clone-guard.md` | 2026-05-11 |
| Handoff skill | gc-toolkit pack (loaded into this session) | `/home/zook/loomington/rigs/gc-toolkit/skills/handoff/SKILL.md` | 2026-05-11 |
| Bead-comment / metadata UX | gascity dashboard + CLI | `cmd/gc/cmd_bd.go` `--set-metadata`, `--notes`; dashboard bead detail panel @ `29565ba1` | 2026-05-11 |
| Survey's session-restart inventory | survey.md §1, §5 | `specs/gc-h1gxg/survey.md` lines 57-150, 381-429 @ `e05da010` | 2026-05-11 |
| Survey's identity primitives | survey.md §3 | `specs/gc-h1gxg/survey.md` lines 207-318 @ `e05da010` | 2026-05-11 |

The survey already enumerates the live primitives and their stability;
this doc treats those facts as given and focuses on operator-visible
behavior. Where a sketch implies a primitive that does not exist
today, this doc flags it under "surface gaps" — but stays out of the
implementation-vs-decision aisle.

## Reading guide

The six flows below share a structure:

1. **Setting** — one paragraph painting where the operator is and
   what they want to do.
2. **Concrete sketch (today's primitives)** — the smallest change that
   could make the flow feel right *without* moving any structural
   pieces. Often: change the overlay, add a flag, add a thin command.
3. **Concrete sketch (peer-instance world)** — what the same flow
   could feel like if framing B from `gc-h1gxg` lands (N peer instances
   of a role, one designated canonical).
4. **Existing Gas City surfaces this flow touches** — the CLI / tmux /
   dashboard / bead UI / mail entry points already in the binary.
5. **Surface gaps** — UX affordances that don't exist today and would
   need to be invented (regardless of framing).
6. **Decision points** — the UX choices that, if locked in, force a
   particular primitive shape.

The "today" and "peer-instance" sketches are intentionally placed
side-by-side so the operator can compare *experiences*, not designs.
Both sketches assume the same vocabulary, defined immediately below.

## Vocabulary used in this document

These terms are scoped to this doc; they are not vocabulary the rest
of gascity has adopted.

- **Canonical instance** — the live agent that receives role-addressed
  mail and is treated as authoritative for a role (`mayor`,
  `gc-toolkit.mechanik`, etc.). Today: the singular named session per
  role. Future: one peer marked canonical.
- **Steward** — an operator-attached, role-flavored, *non-canonical*
  agent that exists for ad-hoc conversation. The successor concept to
  "scratch clone." Lifecycle anchored to the operator's intent, not to
  a piece of work.
- **Consultant** — a workflow-attached, role-flavored, *non-canonical*
  agent spawned by a mol formula step (e.g., "consult the architect on
  this design"). Lifecycle anchored to the formula step.
- **Thread** — a single conversation with a steward or consultant.
  Operator-visible noun.
- **Role** — the configuration template (`gc-toolkit.mechanik`,
  `mayor`). Both stewards and consultants are *flavors* of a role.

These three nouns — canonical, steward, consultant — let the rest of
this doc separate "who am I talking to?" from "who decides I'm done?"
without re-litigating identity primitives in every section.

---

## 1. Spawn a thinking partner

### Setting

The operator is debugging a wedged convoy. They want to think out
loud with a mechanik-flavored partner — not the canonical mechanik,
who has its own inbox and pool of work — so as not to pollute the
canonical's transcript and not to interrupt anything the canonical is
already in the middle of. The operator wants the partner now, wants
it cheap, and wants to be able to close their laptop without
ceremony in five minutes if the thought peters out.

### Concrete sketch (today's primitives)

```text
$ gc steward spawn mechanik --topic "convoy gc-xxx is wedged on rebase"
opened steward thread thr-abc12 (gc-toolkit.mechanik/steward, ephemeral)
  → tmux window: mechanik-steward-abc12 in session "mechanik"
  → switch: tmux next-window  (already attached if you were on mechanik)
```

Visual landing in tmux (today's overlay model, polished):

```
┌────────────────────────────────────────────────────────────────────┐
│ tmux session: mechanik                                              │
├──────────────────┬─────────────────────────────────────────────────┤
│ 0: canonical     │ 1: steward-abc12 (NEW, focused)                 │
│ ❯ mechanik       │ steward (mechanik, ephemeral) cwd: ../mechanik  │
│ inbox: 3 unread  │                                                  │
│ working: gc-yyy  │ > convoy gc-xxx is wedged on rebase, here's …   │
└──────────────────┴─────────────────────────────────────────────────┘
status bar: [mechanik] [w0:canonical w1*:steward-abc12]
```

The "today's primitives" sketch is essentially the existing overlay
behavior (a sibling tmux window under the canonical's tmux session),
but with a `gc`-level entry point so the operator doesn't have to
remember the keybinding and so the action is auditable in the bead
store. This shape inherits the survey's known weakness (§5): handoff
of the canonical also kills the steward's window.

### Concrete sketch (peer-instance world)

```text
$ gc steward spawn mechanik --topic "convoy gc-xxx is wedged on rebase"
opened steward thread thr-abc12 (gc-toolkit.mechanik/steward-1, ephemeral)
  → tmux session: gc-toolkit__mechanik--steward-abc12 (own session)
  → ttyd panel: https://dash/agent/steward-abc12  (or)
  → attach now? [Y/n] y                              # tmux attach
```

In this shape the steward is its own tmux session. The dashboard
ttyd panel (the surface added in `c1ef98e6`) is the natural always-on
view; tmux attach is for power users. Restarting the canonical does
not touch the steward — the steward is a peer instance, not a
sibling window. Closing the steward (flow 3) is decoupled from
closing the canonical.

Visual landing in the dashboard (peer-instance world):

```
┌────────────────────────────────────────────────────────────────────┐
│ Gas City — gascity rig                                              │
├─────────┬──────────────────────────────────────────────────────────┤
│ Sidebar │ Threads (3)                                              │
│ Agents  │  ▸ mechanik / steward-abc12   "convoy gc-xxx…"  • now    │
│ Threads │    spawned 2s ago · ephemeral · no bead binding          │
│ Beads   │  ▸ mayor    / steward-mn34    "perf budget Q3"  · 14m    │
│ Convoys │  ▸ deacon   / steward-q1bx    "rotation drift"  · 1d     │
└─────────┴──────────────────────────────────────────────────────────┘
[ttyd panel for steward-abc12 fills the remaining area]
```

### Existing Gas City surfaces this flow touches

- **CLI.** `gc session start` (named-session start), `gc session
  attach` (tmux attach), `gc session nudge` (ephemeral message into a
  pane). None of these natively express "spawn a *new* ephemeral
  instance of a configured role for an ad-hoc conversation"; the
  closest fit today is the gc-toolkit overlay's
  `tmux-spawn-scratch.sh` (survey §2).
- **tmux.** Named sessions, `new-window` (today's overlay), `new-session`
  (peer-instance world). Window/pane targeting via `-t`.
- **Dashboard.** ttyd iframe per agent (commit `c1ef98e6` is the
  current frontier here). No "threads" panel yet.
- **Bead UI.** Today nothing represents an in-flight steward
  conversation as a bead. The survey explicitly notes scratches have
  "no wisp, no mail/nudge delivery, no respawn lifecycle"
  (`tmux-spawn-scratch.sh:11-12`).
- **Mail.** Mail addressed to `gc-toolkit.mechanik` resolves to the
  canonical via `resolveLiveConfiguredNamedMailTargetCached` (survey
  §3); the steward should be unreachable via mail unless explicitly
  addressed as a peer.

### Surface gaps

- **No `gc steward` (or equivalent) command.** Spawning a non-canonical
  instance currently requires either the overlay tmux key or a manual
  `gc session start ... --alias ...` plus the right env vars.
- **No "topic" affordance.** The operator wants to declare the topic on
  spawn — both for navigation later (flow 2) and for the bead-comment
  summary at dismissal (flow 3). The existing dispatch flow takes a
  bead, not a topic string.
- **No way to express "ephemeral, do not respawn me on crash."** Pool
  sessions auto-respawn (survey §4); stewards must not, but the
  reconciler has no per-instance "no auto-respawn" knob distinct from
  `mode=adhoc` (which already exists but is tied to the session-origin
  taxonomy, not to operator intent).
- **No transcript persistence story.** A steward's conversation lives
  in the provider process; if the operator wants to refer back to "what
  did mechanik-steward say about that convoy three days ago?", there is
  no surface that returns it.

### Decision points

- **Where does the steward live in tmux?**
  - *Sibling window of the canonical* — operator gets fast tmux-window
    switching, but handoff of the canonical kills the steward (survey §5).
    Implies: scratches must move out, OR `gc handoff` becomes pane-scoped
    (framing A).
  - *Own tmux session* — survives canonical handoff, but the operator
    loses one-keystroke window switching. Implies: a higher-quality
    multi-thread navigator (flow 6) becomes mandatory rather than
    optional.
- **Does spawning create a bead?**
  - *Yes, always* — every steward thread is bead-backed, surfaces in
    dashboard, can carry a transcript-summary on dismiss (flow 3).
    Implies: stewards have a persistent identity, which couples them
    to mail/dispatch resolution (every spawn writes to Dolt).
  - *No, never* — stewards are pure-tmux affairs; cheap, lossy,
    invisible to the dashboard. Implies: flow 6's navigation must
    inspect tmux state, not bead state.
  - *Lazy* — no bead at spawn, a bead is created only at dismiss if
    the operator opts to summarize (flow 3). Implies: navigation
    works on tmux state during life, on bead state after.
- **Does `--topic` route to anything (an existing bead, a convoy)
  on spawn?**
  - *Yes* — the steward thread is linked to a work bead; mail in
    flow 5 can reference "the steward thread on bead X." Implies: a
    new edge type in the bead store (or reuse `parent`).
  - *No* — topic is a freeform string used only for display. Implies:
    nothing in the bead schema changes, but flow 6's navigation cannot
    cluster threads by topic without parsing.

---

## 2. Resume an ongoing conversation

### Setting

It is the next morning. The operator started a mechanik thread
yesterday about the wedged convoy, did something else for the rest of
the afternoon, slept, and now wants to pick that thread back up.
Maybe they also have a mayor steward about Q3 perf budget and a
deacon steward about rotation drift. The operator does not remember
the thread id. They remember "the mechanik one about convoys."

### Concrete sketch (today's primitives)

```text
$ gc steward list
ID         ROLE     TOPIC                                  AGE   STATE
thr-abc12  mechanik convoy gc-xxx is wedged on rebase      18h   detached
thr-mn34   mayor    perf budget Q3                         14h   detached
thr-q1bx   deacon   rotation drift                         1d    detached

$ gc steward attach thr-abc12          # or: gc steward attach mechanik
attaching tmux session mechanik, window steward-abc12 …
```

If we keep stewards as sibling tmux windows under the canonical's
session, "attach" is `tmux select-window -t mechanik:steward-abc12`
plus an attach if not already there. If the canonical was handoffed
overnight, the steward window is gone (survey §5); resume falls back
to "spawn fresh with the old topic" — i.e., this flow effectively
collapses into flow 1 every time a handoff happened. That's the
status-quo bug we are trying to surface.

### Concrete sketch (peer-instance world)

```text
$ gc steward list
ID         ROLE     TOPIC                                  AGE   STATE
thr-abc12  mechanik convoy gc-xxx is wedged on rebase      18h   asleep
thr-mn34   mayor    perf budget Q3                         14h   asleep
thr-q1bx   deacon   rotation drift                         1d    awake

$ gc steward attach thr-abc12
waking steward gc-toolkit__mechanik--steward-abc12 …
attached. ttyd: https://dash/agent/steward-abc12
last activity: 2026-05-10 17:22 — "ok so the rebase fails because…"
```

In the peer-instance world, the steward's tmux session can be
*asleep* (process exited cleanly, bead persists, conversation
transcript persists as a bead-attached artifact if the framing
chooses to persist transcripts) and *wake* on attach. The status-bar
"last activity" gives the operator a one-line anchor to context.
Alternatively, the steward stays running on a small idle budget and
attach is a no-op tmux attach.

Visual landing in the dashboard (peer-instance world):

```
Threads
  ▸ mechanik / steward-abc12   "convoy gc-xxx…"   18h   [Attach] [Dismiss]
  ▸ mayor    / steward-mn34    "perf budget Q3"   14h   [Attach] [Dismiss]
  ▸ deacon   / steward-q1bx    "rotation drift"   1d    [Attach] [Dismiss]

[Filter: role:mechanik]   [Filter: bead:gc-xxx]   [Search topic …]
```

### Existing Gas City surfaces this flow touches

- **CLI.** `gc session list`, `gc session attach`. Today `gc session
  list` filters to live sessions; a "threads" notion does not exist.
- **tmux.** `tmux list-windows`, `tmux select-window`, `tmux attach`.
  Pane state is ephemeral; nothing about "last activity" persists in
  tmux itself.
- **Dashboard.** Agent panel today lists configured agents and live
  sessions; there is no thread filter, no topic search.
- **Bead UI.** If threads are bead-backed (flow 1 decision), this
  becomes "list of `type=thread` beads filtered to assignee == me as
  operator."
- **Mail.** Not directly involved; mail-to-canonical is unaffected.

### Surface gaps

- **No persistent "thread" record at all.** Today's scratches do not
  produce any bead, so there is nothing to enumerate. Even a
  pure-tmux model needs a name registry (or topic registry) for
  navigation by topic instead of by id.
- **No "last activity" surface.** The provider's last-utterance timestamp
  is not exposed to gascity in any structured way. Closest existing
  approximation: the session bead's `updated_at`, but that fires on
  many other events.
- **No filter-by-bead.** "Show me the steward threads attached to
  convoy gc-xxx" requires either explicit bead binding at spawn
  (flow 1 decision) or topic parsing at list time.
- **No sleep/wake semantics for ephemeral agents.** Reconciler's
  `mode=always` / `mode=adhoc` (survey §3) does not support "asleep
  but resumable."

### Decision points

- **Are threads always-on or sleep-on-detach?**
  - *Always-on* — `gc steward attach` is a one-liner; cost is N idle
    provider processes for N stewards. Operator can have 5 going
    cheaply only if the provider's idle cost is near zero.
  - *Sleep-on-detach* — attach wakes; cheaper at rest but requires
    bead-attached transcript persistence to make wake meaningful (the
    operator wants context, not a blank fresh session).
- **What does "resume" actually restore?**
  - *The tmux pane only* — operator sees the scrollback. If the
    provider died, scrollback may be empty.
  - *The provider transcript* — operator sees the full conversation
    history regardless of pane lifecycle. Requires storing provider
    transcripts somewhere (bead artifact, on-disk file, dashboard
    asset blob).
  - *A model-generated summary plus the most recent N turns* — cheaper
    than full transcript, lossy.
- **How is a thread addressable?**
  - *By thread id* — exact, requires `list` first. Implies an opaque
    id namespace.
  - *By role + topic substring* — `gc steward attach mechanik convoy`
    matches. Implies the topic string is in a queryable index, not
    just free text.
  - *By attached bead* — `gc steward attach --bead gc-xxx`. Implies
    flow 1 has bead-binding turned on.

---

## 3. Dismiss a thinking partner

### Setting

The operator has thought through the wedged-convoy problem with the
mechanik steward and is satisfied. They want to close the thread now.
The questions: does the transcript survive? Is anything handed back to
the canonical mechanik? Does the dismissal write a comment on the
work bead the steward was reasoning about?

### Concrete sketch (today's primitives)

```text
$ gc steward dismiss thr-abc12
dismissing steward thr-abc12 (gc-toolkit.mechanik/steward, ephemeral) …
  → tmux window mechanik:steward-abc12 closed
  → no transcript persisted (default)
  → no canonical handoff
```

Cheapest possible dismiss: kill the tmux window. The conversation is
gone. The bead store has no record. This is the gc-toolkit overlay's
implicit current behavior (close the tmux window, the model process
SIGHUPs, that's the end of it).

A slightly richer "today's primitives" sketch adds an opt-in
summary:

```text
$ gc steward dismiss thr-abc12 --summary
asking mechanik-steward to summarize the conversation …
mechanik-steward: "convoy gc-xxx is wedged because the rebase of the
                   integration branch dropped the `target` metadata
                   on the work bead. recommended fix: reset metadata,
                   re-sling."
write summary to bead gc-xxx? [Y/n] y
  → comment added to gc-xxx (76 chars, author: gc-toolkit.mechanik/steward)
  → tmux window mechanik:steward-abc12 closed
```

The summary itself is a model action; the comment-creation is a
gascity CLI action against the existing `gc bd update --notes` path.

### Concrete sketch (peer-instance world)

In the peer-instance world the dismissal can be more deliberate:

```text
$ gc steward dismiss thr-abc12 --summary --notify mechanik
asking mechanik-steward to summarize …
mechanik-steward: "convoy gc-xxx wedged because rebase of integration
                   branch dropped `target` metadata. fix: reset metadata,
                   re-sling. low-confidence side-note: deacon may want
                   to know."
attach summary to bead gc-xxx? [Y/n] y
  → comment added to gc-xxx
  → mail sent to gc-toolkit.mechanik (canonical) [summary] [thread thr-abc12]
  → steward session gc-toolkit__mechanik--steward-abc12 stopping (clean)
  → bead thr-abc12 → status=closed, type=thread
```

Visual handoff:

```
Threads (after dismissal of thr-abc12)
  ▸ mechanik / steward-abc12   "convoy gc-xxx…"   closed 12s ago
    └─ summary delivered to gc-toolkit.mechanik · summary added to gc-xxx
  ▸ mayor    / steward-mn34    "perf budget Q3"   awake
  ▸ deacon   / steward-q1bx    "rotation drift"   asleep
```

Dismiss is now the *narrative beat* — the operator's "I'm done with
this thinking; here's the takeaway." The CLI is doing the routing
that today the operator does manually (writing a bead comment, sending
mail to the canonical, etc.).

### Existing Gas City surfaces this flow touches

- **CLI.** `gc session kill` (today, the only way to end a session
  without restart). `gc bd update --notes "…"` (writes a comment-ish
  field on a bead). `gc mail send` (delivers a summary to the canonical).
- **tmux.** `tmux kill-window` / `tmux kill-session` depending on the
  flow-1 decision.
- **Dashboard.** Bead detail panel already shows `notes` /
  metadata. No "thread closed with summary" timeline event today.
- **Bead UI.** `--set-metadata` and `--notes` are the existing
  attachment mechanisms; a new `type=thread` bead is the natural place
  for thread metadata.
- **Mail.** `gc mail send canonical -s "summary thr-abc12" -m "..."` is
  the existing way to deliver a summary; it costs a Dolt write per
  mail (survey §5; CLAUDE.md "Nudge First, Mail Rarely").

### Surface gaps

- **No "ask the model to summarize" primitive.** The provider can do
  this, but there is no `gc` command that issues "summarize your
  conversation in N words and emit it on stdout." Today this is done
  manually by typing into the steward.
- **No thread closure ceremony.** A scratch window today closes via
  the operator typing `exit` or killing the tmux window. There is no
  hook for "before the steward goes, do X."
- **No "transcript artifact" notion.** If the operator wants the full
  conversation (not just a summary), there is no on-disk or bead-side
  artifact to retain — the scrollback in tmux is the canonical record,
  and even that is bounded.
- **No "deliver this to the canonical" path that isn't mail.** Mail
  costs a bead write, has an inbox-management cost on the canonical
  side (archive, etc.). For routine end-of-thread notifications, a
  lighter path (nudge with a structured payload?) would fit.
- **No backpressure on dismiss-without-summary.** Operator can close
  a tmux window and lose 20 minutes of reasoning. The system has no
  way to even ask "do you want a summary first?"

### Decision points

- **What is the default behavior of `dismiss`?**
  - *Silent close* — no summary, no comment, no mail. Cheap. Operator
    must opt in to memory. Implies threads are *truly* ephemeral and
    the system makes no claim about reasoning persistence.
  - *Always summarize* — every dismiss writes a comment on the
    attached bead (if any) and mails the canonical. Costs N Dolt writes
    per spawn-dismiss cycle; risks "summary spam" on canonicals.
  - *Prompt the operator* — UX-heavy, easy to dismiss the prompt by
    reflex. Most likely outcome: operator clicks "no" out of habit
    and the system effectively defaults to silent close.
- **Does the dismissal handoff bind to a bead?**
  - *Required* — every steward must be spawned with `--bead`
    (or default `--bead=convoy-i'm-in`), and dismissal always writes a
    comment there. Implies flow 1 enforces bead binding.
  - *Optional* — `--bead` flag at dismiss time. Implies dismiss is a
    multi-step UX (ask for bead, ask for summary, write).
  - *Forbidden* — stewards are pure thinking partners, never touch
    work beads. Implies stewards' value is entirely in the operator's
    short-term memory, and the system invests nothing in capturing it.
- **What persists across dismissal?**
  - *Just the bead with status=closed* — minimal record, "the thread
    existed and ended."
  - *Bead plus summary text on the bead* — operator can review.
  - *Bead plus full transcript artifact* — operator can re-read every
    word. Requires storage strategy (artifact blob on the bead,
    on-disk file referenced by bead, dashboard-served URL).
- **Does dismissal influence the canonical?**
  - *No* — canonical never hears about steward threads.
  - *Notification only* — canonical gets a structured nudge: "your
    operator dismissed steward thr-abc12, summary attached to gc-xxx."
    Canonical chooses what to do.
  - *State transfer* — the steward's reasoning is replayed into the
    canonical's context. (Risky; the canonical has its own context
    budget. Probably out-of-scope for this decision.)

---

## 4. Workflow consultant

### Setting

A mol formula step (say, `consult-architect` inside `mol-polecat-work`)
needs an architect to weigh in on a design choice before the polecat
proceeds. The operator is not the trigger; the formula is. The
operator's question is: *what does this look like from my seat?*
Do I see the consultant? Can I attach to it? Can I redirect it? Does
it disappear after the step completes? Does its reasoning end up in
the bead?

### Concrete sketch (today's primitives)

The closest thing in today's gascity is a polecat dispatch with a
short-lived custom prompt. The operator does not naturally see it
unless they `tmux ls` or `gc session list`. It runs, writes its
output somewhere (likely a comment on the parent bead or a sub-bead),
and exits.

```text
# operator's view (passive, polling)
$ gc session list
NAME                                    STATE   ROLE                             AGE
mayor                                   alive   mayor                             4d
gc-toolkit__polecat-lx-v7iv57           alive   gc-toolkit.polecat (you)          12m
gc-toolkit__architect-lx-cz8801         alive   gc-toolkit.architect              3m

$ gc session attach gc-toolkit__architect-lx-cz8801   # if curious
attached. (consultant currently mid-thought on bead gc-yyy)
```

The consultant is *visible* because it's a real tmux session, but
nothing in the operator's flow surfaces it proactively. The
consultant exits when the formula step completes; the operator
notices, if at all, by the disappearance from `gc session list`.

### Concrete sketch (peer-instance world)

In the peer-instance world the consultant is a peer-instance of
the architect role, marked as a *consultant* (not canonical, not
operator-attached steward, but workflow-attached). The dashboard
shows it under a third bucket:

```
Active in your rig
  Canonical agents (4)
    ▸ mayor                          alive · inbox 0
    ▸ gc-toolkit.mechanik            alive · inbox 1
    ▸ gc-toolkit.witness             alive · inbox 0
    ▸ gc-toolkit.refinery            alive · inbox 0

  Stewards (you spawned)
    ▸ mechanik / steward-abc12       awake · "convoy gc-xxx…"

  Consultants (workflow spawned)
    ▸ architect / for-step consult-architect on gc-yyy
      step: 3/7 · age 3m · attach for live view
```

Operator-attachable, but not operator-driven. The consultant is
running a model conversation initiated by the formula step, not by a
prompt the operator typed.

```text
$ gc consultant attach gc-yyy:consult-architect
attached to architect/cons-cz88 (consulting on bead gc-yyy)
  current state: drafting decision
  step deadline: 5m
  intervene? (ctrl-shift-i for sidebar input)
```

The operator can read the unfolding reasoning, *can* intervene
(typing in the pane queues an operator-message that the consultant
receives), and detaches. The consultant continues regardless.

### Existing Gas City surfaces this flow touches

- **CLI.** `gc sling` dispatches work; consultants are a dispatch
  variant. `gc session list/attach`. `gc bd show` to inspect what the
  consultant is reasoning about.
- **Formula.** Mol formula steps in `internal/formula/` (per the
  survey, this is the orchestration layer). A `consult-step` step is
  the architectural fit.
- **tmux.** A consultant is a tmux session like any other peer
  instance.
- **Dashboard.** Agent listing today does not separate "active
  formula consultants" from canonical agents.
- **Bead UI.** The bead the consultant is reasoning about already
  exists (the work bead, the convoy). The consultant's output is a
  comment or sub-bead.
- **Mail.** Not usually involved on the inbound side — the consultant
  receives its prompt as a formula directive, not mail. On the
  outbound side, the consultant may mail the polecat or the canonical
  architect with a summary.

### Surface gaps

- **No "consultant" surface in the dashboard.** Workflow-spawned
  ephemeral agents look identical to operator-spawned stewards (and to
  polecats) in `gc session list`.
- **No operator-intervention channel.** A polecat today does not
  accept operator typing mid-flight as a structured input; the operator
  would have to `gc mail` and hope the consultant checks. For
  intervention to be a real UX affordance, the consultant needs an
  in-band input channel separate from work-bead state.
- **No "formula step deadline" surface.** The consultant might be
  on a timer (formula gate); operator has no visibility into "5 minutes
  left before this step times out."
- **No proactive notification to the operator.** Operator must know
  the consultant exists to attach. Today's flow has no notification
  surface for "a consultant just started on convoy you own."

### Decision points

- **Is the consultant visible to the operator at all?**
  - *No, never* — consultants are formula internals; operator sees
    only the bead-side outcome. Implies the dashboard's "Active in your
    rig" panel hides workflow-spawned peers.
  - *Yes, in a separate bucket* — sketch above. Implies a new
    dashboard concept (the "consultant" role-flavor) and a new
    field on session beads (`origin=consultant` or
    `flavor=consultant`).
  - *Yes, indistinguishable from any other peer* — minimum new
    surface, but operator cannot tell which sessions are theirs vs.
    workflow's.
- **Can the operator intervene mid-flight?**
  - *No, observe-only* — operator can attach, can read, cannot type
    input that the consultant will process. Implies attach uses a
    read-only ttyd panel.
  - *Yes, full read-write* — operator can send messages that the
    consultant treats as a side-input. Implies a structured "operator
    message" envelope; semantics ("does the consultant treat this as
    an instruction or as background?") need to be specified.
  - *Yes, but only structured (e.g., cancel)* — operator gets a panic
    button, not a chat input. Implies a new CLI verb (`gc consultant
    cancel <id>`) and a formula-side respect for the cancellation.
- **What is the consultant's lifecycle relative to the formula step?**
  - *Strictly bounded* — the consultant exists only while the step
    runs; on step-end the session dies, no resume. Implies dismiss is
    automatic; operator cannot "save the consultant for later."
  - *Promotable to steward* — operator can `gc consultant promote
    <id>` to convert it into a regular steward (operator-attached,
    surviving the formula step). Implies a state-transition vocabulary
    on the thread bead.
- **Where does the consultant's reasoning land?**
  - *Comment on the work bead* — same place polecat reasoning lands.
    Implies no new bead type.
  - *Sub-bead* (e.g., `gc-yyy.consult-architect`) — first-class record,
    queryable, searchable. Implies a new edge type or naming convention.

---

## 5. Outbound-only notifications

### Setting

The operator's mayor-steward has just figured out that a long-running
convoy is finally ready to graduate. The operator does not want to
type that conclusion into the canonical mayor's pane manually. They
want the steward to deliver the message — and they want a visible
record of who-told-whom-what, so if a discrepancy shows up later
("the canonical never got told about that convoy"), they can audit.

This is the "outbound-only" flow: the steward speaks on the operator's
behalf to the canonical, the canonical does not speak back into the
steward's conversation. (If the canonical needs to ask a question, it
mails the operator, not the steward.)

### Concrete sketch (today's primitives)

The cheapest version is the operator wraps the conversation up,
copy-pastes a summary into a `gc mail send` call, and credits the
steward in the body:

```text
$ gc mail send mayor -s "convoy gc-xxx ready to graduate" -m "$(cat <<'EOF'
[from operator, via mayor/steward thr-abc12]

Steward concluded the integration branch is at parity with main.
Recommend graduation tonight; tests are green; refinery queue empty.
EOF
)"
sent → mayor (mail bead mb-9xy12) · from=operator-direct
```

This works. It's manual. The audit trail is the mail bead. The
operator-as-author is the actor; the steward is mentioned in prose
but not structurally.

### Concrete sketch (peer-instance world)

In the peer-instance world the steward itself can issue the
notification, and the audit trail captures the chain:

```text
# operator types into the steward pane
[operator] looks good. tell canonical mayor to graduate the convoy tonight.

[mayor/steward-abc12] OK. Sending an outbound-only notification to
                       canonical mayor; this thread won't see a reply.

  → outbound notification queued: mayor/steward-abc12 → mayor (canonical)
  → subject: "convoy gc-xxx graduation recommended tonight"
  → audit edge: thr-abc12 → mb-9xy12 → mayor inbox
  → operator visibility: dashboard → Threads → thr-abc12 → Activity
```

The notification is a mail bead (mb-9xy12 in the sketch) authored by
the steward's session identity, with metadata pointing back at the
thread bead. The canonical mayor's reply (if any) goes to the
operator's own inbox, not to the steward — closing the loop without
re-engaging the now-quiescent thread.

Operator visibility panel:

```
Thread mechanik/steward-abc12 — Activity
  09:14 spawned · topic: "convoy gc-xxx is wedged…"
  09:18 operator: "looks good. tell canonical mayor…"
  09:18 outbound notification → mayor: mb-9xy12 (graduation recommended)
  09:18 thread auto-quiesce (1m)
  …
  (later, if canonical replies to operator)
  10:02 canonical mayor mailed operator (re: convoy graduation)
```

### Existing Gas City surfaces this flow touches

- **CLI.** `gc mail send`, `gc mail reply`, `gc mail inbox`. Today
  mail authorship is `GC_AGENT` (the sender's identity). A steward
  authoring mail today would use `GC_AGENT=gc-toolkit.mayor/steward`
  (or whatever the steward's GC_AGENT is); the canonical resolver
  picks up the recipient correctly.
- **Dashboard.** Mail inbox view per agent; bead detail view for
  individual mail beads. No "thread activity timeline" today.
- **Bead UI.** Mail beads are `type="message"` per the survey's
  reference; threading is via `gc mail reply` setting parent edges.
- **Mail routing.** Canonical resolver (survey §3) handles
  `mayor`-addressed mail; with peer instances, the resolver must learn
  to (a) route to canonical when no peer is explicitly named, (b)
  treat the steward's `from` as a peer-instance address that the
  canonical's reply-to can route back to operator-owned addresses.

### Surface gaps

- **No "thread activity timeline."** Outbound notifications from the
  steward are mail beads, but they are not grouped or visualized as
  "things this thread did." Operator audit requires manually walking
  `gc mail inbox --from steward-abc12`.
- **No "outbound-only" mail flag.** Today every mail is bidirectional
  by convention (the recipient can `gc mail reply` to the original
  sender). A steward-authored outbound notification ideally tells the
  canonical: "do not reply to me, reply to the operator." Today this
  is a prose convention, not a structural one.
- **No operator-as-reply-target.** If the steward is the structural
  sender, the canonical's `gc mail reply` will go to the steward
  address, not the operator. Today there is no "reply-to operator
  alias" envelope on mail beads.
- **No `gc steward speak` (or equivalent) verb.** Today the operator
  types into the steward, the steward generates output, and the
  operator copy-pastes that output into a `gc mail send`. A more
  cohesive flow would let the steward issue the outbound notification
  directly from within the conversation, with a structured prompt to
  the operator ("send this as outbound? [Y/n]").

### Decision points

- **Is the steward authorized to send mail in its own name?**
  - *Yes* — the steward has a real bead-store identity and authors
    mail directly. Implies peer-instance authentication: each peer
    instance has a distinct `GC_AGENT` that the bead store treats as a
    legitimate author.
  - *No, the operator authors everything* — the steward generates text,
    the operator runs `gc mail send`. Implies stewards remain
    "thinking aids" with no agency over the bead store; cleaner audit
    trail at the cost of manual ceremony.
- **Where does the canonical's reply go?**
  - *To the operator's inbox* — closes the loop without re-engaging
    the steward. Implies mail beads carry an explicit `reply_to` edge
    distinct from `from`.
  - *To the steward's inbox* — re-engages the steward (which may be
    dismissed already). Requires steward-as-mailbox semantics.
  - *To both* — costs two reads in the audit log per round-trip.
- **How visible is the chain?**
  - *Thread activity timeline in dashboard* — first-class concept.
  - *Just the mail beads, walk-yourself audit* — relies on
    metadata-search.
- **Does the canonical see the steward's reasoning that led to the
  notification?**
  - *No* — canonical sees only the summary the steward authored.
  - *Yes, on demand* — canonical can `gc thread peek thr-abc12` to
    pull the full reasoning. Implies cross-instance read access to
    steward transcripts.

---

## 6. Multi-thread management

### Setting

It is Wednesday afternoon. The operator has, simultaneously:

- 1 canonical mayor (always-on);
- 1 canonical mechanik (always-on);
- 1 canonical deacon (always-on);
- 1 mechanik-steward open about a wedged convoy (this morning);
- 1 mayor-steward open about Q3 perf budget (yesterday);
- 1 deacon-steward open about rotation drift (last week, dormant);
- 1 architect-steward open about a new dispatch shape (just started);
- 2 active polecats grinding in the rig;
- 1 architect-consultant just spawned by a formula step on a convoy.

That is 10 model-backed processes the operator might want to glance
at, attach to, dismiss, or hand off. The operator has *one* terminal
and *one* dashboard. The question: what is the navigation?

### Concrete sketch (today's primitives)

Today the navigation surface is the union of `tmux` and `gc session
list`:

```text
$ tmux ls
mayor: 1 windows (created Mon May  4 09:14:08 2026)
gc-toolkit.mechanik: 4 windows (created Mon May  4 09:14:11 2026)
gc-toolkit.deacon: 2 windows (created Mon May  4 09:14:14 2026)
gc-toolkit__polecat-lx-aa01: 1 windows (created Wed May 13 11:02:33 2026)
gc-toolkit__polecat-lx-bb02: 1 windows (created Wed May 13 11:04:17 2026)
```

That is a flat list. Scratches live as windows *inside* canonical
sessions (mechanik has 4 windows = canonical + 3 scratches), so the
top-level list undercounts threads. `gc session list` gives a similar
flat view with bead-side identity.

In tmux: `prefix s` lists sessions, `prefix w` lists windows across
sessions. None of this surface knows "topic" or "bead" or "this
steward is dormant from last week."

### Concrete sketch (peer-instance world)

In a peer-instance world, the dashboard's "Threads" panel is the
center of gravity:

```
┌──────────────────────────────────────────────────────────────────────┐
│ gascity rig — Wednesday May 13                                        │
├──────────────────────────────────────────────────────────────────────┤
│ Canonical agents (3)                                                  │
│  ▸ mayor                inbox 2  · idle 14m                           │
│  ▸ mechanik             inbox 0  · working on gc-zzz                  │
│  ▸ deacon               inbox 1  · idle 2h                            │
│                                                                       │
│ Stewards (you spawned, 4)                                             │
│  ▸ mechanik/steward-abc  "convoy gc-xxx wedged"   awake  · 4h     ✕   │
│  ▸ mayor/steward-mn3     "Q3 perf budget"         asleep · 1d     ✕   │
│  ▸ deacon/steward-q1b    "rotation drift"         asleep · 7d     ✕   │
│  ▸ architect/steward-zz  "new dispatch shape"     awake  · 2m     ✕   │
│                                                                       │
│ Consultants (workflow spawned, 1)                                     │
│  ▸ architect/cons-cz8    on gc-yyy step 3/7       awake  · 3m         │
│                                                                       │
│ Polecats (2)                                                          │
│  ▸ gc-toolkit/polecat-aa01   working on gc-pp01   awake  · 12m        │
│  ▸ gc-toolkit/polecat-bb02   working on gc-pp02   awake  · 8m         │
└──────────────────────────────────────────────────────────────────────┘
[Filter: bead] [Filter: role] [Search topic …] [Group by: role | bead]
```

`gc steward list` and `gc consultant list` are the CLI parallels.
Bead-attached threads (e.g., "all stewards reasoning about convoy
gc-xxx") cluster under the bead in a "Threads on gc-xxx" panel on
the bead's detail view:

```
Bead detail: gc-xxx  (convoy: integration/gc-xxx)
  Status: open       Owner: refinery
  ────────────────────────────────────────
  Threads (2)
    ▸ mechanik/steward-abc      operator-spawned, awake
    ▸ deacon/steward-q1b        operator-spawned, asleep
  Activity timeline:
    09:14 spawned mechanik/steward-abc
    09:18 outbound mb-9xy12 → mayor
    11:02 polecat-aa01 claimed gc-pp01 (child)
    …
```

In tmux, the navigation collapses back to `gc steward attach <id>`
or `gc consultant attach <id>` (which under the hood `tmux attach`'s
the right session). The dashboard's per-thread row has an "Attach"
button that emits a `gc steward attach <id>` invocation and an
"Dismiss" button (flow 3).

### Existing Gas City surfaces this flow touches

- **CLI.** `gc session list` (flat, current). `gc agent list`
  (config-side). `gc bd list --metadata-field …` (powerful but
  not topical).
- **tmux.** `tmux ls`, `tmux lsw -a` (list-windows across sessions),
  `prefix s` and `prefix w` choosers. Status-bar customization is
  out of gascity's scope (overlay-side concern).
- **Dashboard.** Today: agent panel, bead panel, mail panel, convoy
  panel. The future "Threads" panel is the new surface.
- **Bead UI.** Bead detail view today shows comments/notes; no
  "threads attached to this bead" affordance.

### Surface gaps

- **No "Threads" panel.** This is the largest surface gap in this
  whole exercise. Multi-thread navigation today requires the operator
  to memorize tmux session names and mentally tag them.
- **No grouping by bead.** `gc steward list --bead gc-xxx` requires
  threads to carry a bead binding (flow 1 decision).
- **No "asleep" state in the listing.** All `gc session list`
  entries are live tmux sessions; dormant threads either don't exist
  (today they don't) or need to be enumerated from bead store, not tmux.
- **No notification badges.** When a consultant finishes, the operator
  has no visual cue except checking the dashboard. A toast / badge /
  status-bar indicator is the standard UX answer.
- **No "favorite" or "pinned" threads.** Operator with 10 threads
  cannot prioritize the 2 they're actively thinking about.

### Decision points

- **What is the canonical navigation surface — tmux or dashboard?**
  - *tmux primary, dashboard auxiliary* — operator lives in tmux;
    dashboard is for occasional checks. Implies the CLI must be the
    surface where the affordance is best (richer `gc steward list`,
    fuzzy attach, status-bar integration via tmux hooks).
  - *Dashboard primary, tmux secondary* — operator lives in browser;
    tmux is the underlying executor. Implies ttyd panels per thread
    must be solid; tmux ergonomics matter less.
  - *Both equal* — CLI and dashboard maintain feature parity. Implies
    everything has two implementations.
- **Are threads attached to beads by default?**
  - *Yes, always* — every steward / consultant has a parent bead
    (work bead, convoy, or the role's canonical bead as a fallback).
    Implies the dashboard's bead detail view becomes the *natural*
    navigation root.
  - *No, optional* — flexibility at the cost of navigation cohesion.
  - *Only consultants* — workflow-spawned threads bind, operator-spawned
    ones float. Implies two navigation modes.
- **How is tmux laid out under peer instances?**
  - *One tmux session per peer instance* — uniform model. tmux session
    list grows linearly with thread count. Could be 30+ entries.
  - *One tmux session per role, peers as windows* — keeps tmux session
    list compact, but re-imports the survey §5 problem: handoff of the
    canonical takes down the peers in the same session.
  - *Hybrid — peers grouped by topical convoy* — tmux session per
    convoy, role-flavored windows inside. Implies tmux session names
    map to bead ids, not roles.
- **How is thread state observable from outside the dashboard?**
  - *`gc steward list --json` for scripting* — the operator pipes it
    to `jq` / `fzf` for custom navigation. Implies the CLI returns
    rich structured output.
  - *Status bar in tmux* — operator's tmux config polls `gc steward
    list` and renders a count. Implies a fast / cached path for the
    list query.

---

## Decision points for the operator conversation

Below is a roll-up of the recurring decision points across the six
flows, framed as *UX choices that imply primitive choices*. Each
bullet names the choice, names the primitive implication, and (where
applicable) names the flow(s) most affected.

**1. Lifecycle binding: where does a thread live in tmux?**

If stewards live as *sibling windows of the canonical's tmux session*
(today's overlay model), every `gc handoff` of the canonical takes
the steward with it (survey §5). This forces either:

- framing A from `gc-h1gxg` (decouple identity from tmux session, make
  restart pane-scoped) — UX cost: minimal, operator notices nothing
  except "scratches survive handoff now";
- or framing C (move stewards out of the canonical's tmux session
  entirely) — UX cost: lose one-keystroke tmux-window switching to the
  steward, gain independence.

Flows most affected: 1 (spawn), 2 (resume), 6 (navigation).

**2. Bead-backing: do threads write to Dolt?**

The cheapest steward is pure tmux (no bead, no Dolt cost, no audit).
The richest steward is a full bead with transcript artifact, comment
edges, mail authorship, activity timeline. The decision is on a
spectrum:

- *No bead* → flow 2 (resume) and flow 6 (navigation) operate on
  tmux state only; flow 3 (dismiss) and flow 5 (outbound notification)
  have no audit trail.
- *Lazy bead* → bead created at dismiss-with-summary only; navigation
  during life is tmux-driven, navigation after life is bead-driven.
- *Eager bead* → bead at spawn; flows 2, 3, 5, 6 all index on bead
  store. Cost: Dolt write per spawn (CLAUDE.md "Nudge First, Mail
  Rarely" tension — but the operator's spawn rate is bounded).

Flows most affected: 2 (resume), 3 (dismiss), 5 (notifications),
6 (navigation).

**3. Mail authorship: who is the steward, structurally?**

If the steward is allowed to author mail in its own name, then peer
instances need real bead-store identities and the canonical mail
resolver (survey §3) must distinguish "addressed to canonical mayor"
from "addressed to mayor/steward-abc." The alternative is
operator-as-author with steward-as-prose-mention; cleaner audit but
heavier ceremony in flow 5.

Implications for `gc-h1gxg` framing:
- Framing A (decouple identity from tmux): does not require the
  steward to have its own bead identity (overlay can keep stewards
  identity-less). Compatible with operator-as-author.
- Framing B (N peer instances, one canonical): natural fit for
  steward-as-author; the steward *is* a peer instance, and peer
  instances by definition have distinct identities. Forces the
  resolver question.
- Framing C: depends on the hybrid shape.

Flows most affected: 5 (outbound notifications), 3 (dismiss handoff
mail).

**4. Consultant visibility: is the workflow-spawned peer
operator-visible?**

The operator's mental model of "agents that exist in my rig" today
includes canonical agents and polecats. A consultant is a third
flavor: ephemeral, model-backed, not workflow-driven by the operator.

If the consultant is hidden, the dashboard does not change but the
operator cannot intervene (flow 4). If the consultant is visible in
its own dashboard bucket, the dashboard grows a "Consultants" section
and the session bead grows a `flavor` discriminator. If the consultant
looks identical to a polecat, the operator cannot tell which sessions
they themselves "own."

Flows most affected: 4 (workflow consultant), 6 (multi-thread
management).

**5. Transcript persistence: how much of the conversation survives?**

A theme across flows 2, 3, 4, 5: how much of the steward's reasoning
is *retained* and *retrievable*?

- *Scrollback only* (today) — bounded, lost on tmux session death,
  invisible to any non-attached observer.
- *Provider-side state* — opaque to gascity; depends on the provider.
- *Bead artifact* — gascity persists either a full transcript or a
  summary on the thread's bead; retrievable via `gc bd show` or the
  dashboard.
- *On-disk file referenced by bead* — gascity owns a transcript
  directory; the bead carries the path.

This decision affects every flow where the operator wants to "come
back later" (flows 2, 3, 5, 6). It is the most expensive UX choice in
terms of storage and the most valuable in terms of operator continuity.

**6. Notification routing: how does the operator learn about events
their threads are not actively involved in?**

When the consultant finishes (flow 4), when the canonical replies to
a steward's outbound notification (flow 5), when a sleeping steward
"would like attention," the operator needs to find out. Today there
is no notification surface; the dashboard requires polling.

Options:
- *Pull-only* (today) — operator checks periodically.
- *Dashboard toast/badge* — non-intrusive, browser-only.
- *Tmux status-bar integration* — operator's tmux renders unread
  counts.
- *Email/system notification* — heaviest, most disruptive.

This decision interacts with flow 5's `reply_to` mechanics: if the
canonical's reply goes to the operator, where does the operator
*see* it?

**7. Thread-to-bead binding: required, optional, or forbidden?**

The strongest UX cohesion comes from every thread being attached to
at least one bead (flow 1 default `--bead=convoy-i'm-in`; flow 6
navigation rooted in bead detail panels; flow 3 dismiss with
summary-to-bead). The cost is the operator cannot spawn a "freeform"
steward without first naming a bead — and the most ad-hoc thinking
("I'm not sure what I'm working on yet") wants to be freeform.

The decision splits:
- *Required* — every steward has a `parent_bead`; clean navigation,
  inflexible spawn.
- *Optional with default* — operator's session has a "current bead"
  (the convoy they're working in); spawn defaults to that. Implies an
  operator-session notion that does not exist today.
- *Optional with no default* — spawn freely; navigation suffers
  proportionally for unbound threads.

**8. Operator agency over consultants.**

Flow 4's central UX question — "can the operator intervene in a
workflow-spawned consultant?" — has implications for ZFC. If the
consultant is a *pure* formula-driven agent (no operator input
channel), the formula owns all decisions; the operator is observer.
If the operator can side-channel an instruction in, the consultant's
behavior diverges from the formula's intent in a way that's hard to
encode in the bead/mol contract. This is a primitive choice as much
as a UX choice: it determines whether consultants have an
operator-input port at all.

---

## Out of scope (intentionally)

- Implementation of any of the above. The doc is sketches, not specs.
- Sequence diagrams or state machines. The flows above are 1-2 step
  user actions; the underlying state changes belong in a downstream
  design doc once a framing is chosen.
- Naming conventions for the new concepts. "Steward," "consultant,"
  "thread" are used here for clarity; the project may choose other
  names.
- The relationship of these flows to the *upstream* gastown project.
  This doc speaks to the gascity SDK; consumer overlays adopt as they
  please.
