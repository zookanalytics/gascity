---
title: "Queued-nudge session fence"
---

| Field | Value |
|---|---|
| Status | Implemented |
| Scope | What the queued-nudge fence guarantees, and why an epoch drift retargets instead of dropping |
| Consumers | `cmd/gc/cmd_nudge.go` delivery paths, `cmd/gc/cmd_wait.go` durable waits, anything reasoning from a nudged agent's silence |
| Related | `engdocs/design/idle-claim-nudge-followups.md`; the durable-waits feature that introduced the fence (`204c85b13`) |

A queued nudge carries a **fence** — the target's session id and
`continuation_epoch` at enqueue time — and delivery re-checks it against the
live session. This document records what each half of that fence is for,
because the two halves protect different things and only one of them is an
identity guarantee.

## What the fence is

`queuedNudgeOptionsFromTarget` stamps `{SessionID, ContinuationEpoch}` onto
every queued item. Three delivery paths re-check it through the same helper
(`queuedNudgeMatchesTargetFence`), so the rule below holds everywhere:

- the hook drain (`gc nudge drain`),
- the per-session poller (legacy mode),
- the supervisor nudge dispatcher.

Each resolves its target from the **live** session bead on every pass, so the
compared epoch is always the current one.

## The two halves are not equivalent

**Session id — an identity guarantee.** A nudge stamped for session A is not
session B's message and never becomes one. This half stays strict. In practice
the claim gate (`queuedNudgeClaimableForTarget`) already refuses to claim
another session's item, so it stays *pending* rather than reaching delivery at
all; the delivery-side check is defense in depth.

**Continuation epoch — not an identity guarantee, except for waits.** The epoch
identifies a *conversation*, not an agent. A session configured
`mode=always` / `wake_mode=fresh` bumps `continuation_epoch` on **every** wake
(`shouldBumpContinuationEpoch`) while keeping the same session bead. So for
those agents — deacon, mayor, mechanik, boot, the witnesses — any nudge that
does not get delivered before the next recycle meets a moved epoch by
construction.

That mattered because an epoch mismatch is not a retry. `failedQueuedNudge`
special-cases `errNudgeSessionFenceMismatch` and dead-letters the item on its
**first** attempt, bypassing attempts and TTL: the shadow bead closes
`state=failed`, `last_error="queued nudge session fence mismatch"`. Fencing on
the epoch there did not protect the message, it destroyed it — and the sender
had addressed the *agent*, not one of its conversations.

Measured consequence before the fix: of boot's five nudges to a
`mode=always`/`wake_mode=fresh` agent over 14 days, four never landed.

## The rule

An epoch drift is **retargeted** onto the live conversation when the item
provably names the same session. Two cases keep the strict fence:

1. **`source=wait`.** A wait-sourced nudge stamps the wait's own
   `registered_epoch`, which *is* a conversation-scoped guarantee: the
   conversation that registered the wait is the one owed the reminder. The wait
   state machine independently cancels a stale-epoch wait as
   `continuation-stale` (`cmd_wait.go`), after which the queued nudge is
   withdrawn as `wait-canceled`. Retargeting it would contradict that.
2. **An item that names no session.** Identity is unprovable, so the epoch is
   the only evidence there is.

The rule is expressed once, in `queuedNudgeMatchesTargetFence` /
`queuedNudgeEpochRetargetable`, so all three delivery paths inherit it.

## Telling delivery from silence

A caller that nudges an agent and hears nothing cannot act until it knows
whether the message arrived — "healthy but did not answer" and "the nudge never
arrived" call for opposite responses. An interrogation ladder that kills a
target after N unanswered nudges (the shutdown dance) is only a valid
false-positive guard if the nudges themselves are known to have landed.

So the queue-time result carries a handle and the fate is queryable:

- `gc session nudge --delivery=queue --json` returns `nudge_id`.
- `gc nudge show <nudge-id> [--json]` resolves one of `pending`, `in_flight`,
  `delivered`, `dropped` (with the reason), or `unknown`.

Resolution reads the flock'd queue **first** and the durable shadow bead
second. That order is the authority order: while an item is live the queue owns
its state, and once it is delivered or terminalized it leaves the queue
entirely and only the shadow bead remembers what happened.

`gc nudge show` reports the outcome rather than judging it — a `dropped`
outcome still exits 0, and exit 1 means the id is unknown to this city. The
delivered/dropped classification lives in `nudgequeue.OutcomeForState`, beside
the state codes it reads, so a new state cannot silently read as delivered at a
call site that never learned about it.
