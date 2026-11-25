---
title: Hold and Blocked Label Conventions
description: The canonical hold/blocked label taxonomy for this project's own bd tracker — which label to use, when to use status or a dependency edge instead, and what happened to the old ad hoc labels — and what the runtime does with any hold:<value> label.
---

## Why this exists

Before 2026-07-14 this repo's bd tracker had accumulated at least 8 overlapping
ad hoc "hold"-family labels with unclear, possibly inconsistent semantics
(`arch-hold`, `blocked`, `blocked-by-operator`, `blocked-on-external`,
`blocked-on-upstream`, `blocked-prereq`, `human-hold`/`human`, `on-hold`),
alongside the one label that already followed the sanctioned convention,
`hold:mayor`. `ga-tug8ry` audited and consolidated them down to two canonical
values; `ga-tug8ry.2` migrated every live bead onto the result. This page is
the durable reference so nobody reinvents another ad hoc hold label — if
you're about to pause a bead and reach for a new label name, stop and use one
of the two values below instead.

Full rationale and the live census this decision was based on:
`bd show ga-tug8ry.1` (the decision) and `bd show ga-tug8ry.2` (the
migration record, including before/after counts).

## Three orthogonal "not ready" mechanisms

A bead can be "not simply ready to work" for three structurally different
reasons. Pick the mechanism that matches *why* you're pausing it, not just
"it's blocked":

| Mechanism | How to set it | Meaning |
|---|---|---|
| Dependency edge | `bd dep add <a> <b>` | Bead A cannot start until bd-tracked bead B closes. Gates `bd ready`. Computed from real edges, not a manual claim. |
| Bead status | `bd update <id> --status blocked` | "I cannot currently proceed," with no further structure about why or who must act. |
| `hold:<value>` label | `bd set-state <id> hold=<value> --reason "..."` | "I am paused pending a specific actor or condition." Structured, audited (files an event bead), and names the *who*. |

These are orthogonal and combine freely — a bead can be `status=blocked`
**and** `hold:external` at the same time. Use a dependency edge when the
blocker is itself a bd bead; use `status=blocked` when nothing more specific
applies; use `hold:<value>` only when a specific actor or external condition
is the actual reason you're paused.

## Canonical `hold:<value>` values

In this repository's own tracker only two values are canonical. Don't
introduce a third there without a new architecture decision — see
`ga-tug8ry.1` for the reasoning that narrowed the taxonomy to these two. A
city or pack that runs Gas City chooses its own values; the runtime honors
any of them the same way (see "What the runtime does with a hold").

- **`hold:mayor`** — the required next actor is the mayor. Covers both
  mayor-initiated pauses and automation-escalated-to-mayor cases; both are
  the same operational state ("nothing proceeds until the mayor acts") and
  share one value rather than being split in two.
- **`hold:external`** — the required next actor or condition is outside this
  bd instance's control (an external repo's maintainers, an upstream PR
  merge, etc.). Established by `ga-h7hnpt`.

Set either with the sanctioned command — never with a plain `bd label add`:

```bash
bd set-state <id> hold=mayor --reason "why, and who/what unblocks it"
bd set-state <id> hold=external --reason "why, and who/what unblocks it"
```

`bd set-state` removes any existing label in the `hold:` dimension, adds the
new one, and files an audit event bead. It does **not** touch `status`,
`owner`, or `metadata` — update those separately (or add a dependency edge)
if they also need to change.

## Retired labels

These labels are legacy. If you see one on a live bead, treat it as drift
worth a bug report, not a pattern to follow.

| Legacy label | Replace with | Notes |
|---|---|---|
| `blocked-by-operator` | `hold:mayor` | "Operator" meant the human operator/mayor seat. |
| `blocked-on-upstream` | `hold:mayor` | Means "next step in our own merge pipeline," not an external repo — despite the name, this is not a `hold:external` synonym. |
| `human-hold` | `hold:mayor` | Named the "next actor is mayor" state as a bare label. Bare `human` is not a hold label — see the out-of-scope list. |
| `blocked-on-external` | `hold:external` | Direct predecessor of `hold:external`; carry forward any `blocker_scope`/`external_blocker`/`external_pr`/`pr`/`repo` metadata unchanged. |
| `blocked` | none — use native `status=blocked` | Redundant with the bead's own `Status` field; keeping both invites drift between them. |
| `arch-hold` | none — owned by the `maintainer-pr-review` pack | Not a generic bd hold; it's that pack's own gate, cleared via `gc maintainer-pr-review clear-hold`. It only looked like one of ours because it lacks the `mpr-` prefix its sibling `mpr-human-hold` carries. |
| `blocked-prereq` | none today; if it recurs, use a dependency edge (prerequisite is a bd bead) or `hold:external` with PR numbers recorded in metadata (prerequisite is bare GitHub PR numbers) | Historical: blocked on specific GitHub PRs merging first, with no corresponding bd bead. |
| `on-hold` | none — already superseded | Any bead needing this should already carry the canonical `hold:mayor`/`hold:external` in its place. |

**Explicitly out of scope — do not migrate these, they mean something
different:**

- `human` — answers "who executes this" (a human merge/PR action needed),
  not "what is this bead waiting on." It is not a hold state. Keep
  `human-hold` retired; that one named the hold.
- `mpr-human-hold` and other `mpr-*` labels — owned end-to-end by the
  `maintainer-pr-review` pack, with its own metadata namespace and its own
  clearing tool. Not a generic bd hold label.
- `build-blocker`, `ci-blocker`, `pre-push-blocker`, `push-blocking`,
  `test-blocker` — a different semantic axis ("pipeline stage X is red
  because of me"), not "I am waiting on decision-maker Y."
- `needs-mayor` / `needs-mayor-decision` — a routing/queue-placement label
  (parallel to `needs-architecture`, `needs-design`, `needs-pm`,
  `ready-to-build`), not a pause-state label. It may legitimately co-occur
  with `hold:mayor`.

## What the runtime does with a hold

The two values above are this tracker's vocabulary, not the runtime's. Gas City
reads the whole `hold` state dimension and honors any value in it: a city or a
pack chooses its own values, and no Go code reads a value as an agent, a role
or a route. `internal/beadmeta` owns the reading (`ActiveHoldValue`,
`HasActiveHold`), and every runtime consumer answers with it.

A label is an **active hold** when, trimmed, it begins with `hold:` in any case
and its value, the rest of the label trimmed, is not empty and is not `cleared`
in any case:

- `hold:mayor`, `hold:external`, `hold:po` and `hold:release-train` are active
  holds alike, and none of them needs an agent of that name to exist.
- `hold:cleared` is inactive. `bd set-state <id> hold=cleared` leaves it behind
  when it releases a hold. `bd set-state` replaces one `hold:` label, the first
  it finds, and `bd label add` adds one beside any other, so a bead can carry
  `hold:cleared` beside an active hold, and the active hold still holds.
- A `hold:` label whose value is empty or only whitespace, `hold` with no
  colon, and labels that only contain the word (`on-hold`, `mpr-human-hold`)
  are not holds.

An active hold answers "is this bead work for whoever is asking?" with no, so a
held bead is:

- not served by any tier of the default work query, and not returned by
  `gc hook` or claimed by `gc hook --claim`;
- not pre-assigned to a claimant as a continuation-group sibling;
- not counted as pool demand, and not dispatched by the control dispatcher's
  routed tiers;
- not wake demand for its assignee when it is OPEN, does not keep that
  session awake, and is not a drain-ack strand;
- not given a route by the route-restore lanes (the detached-orphan sweep and
  `gc.run_target` route recovery). A pack that parks work on a hold may clear
  its `gc.routed_to` because the next actor is not the pool, and that empty
  route is a parking decision. A hold never implies a route either: nothing
  writes a route from a hold's value.

A hold does not answer "does a session still need to exist for this bead?".
Pool accounting still counts a held OPEN assigned bead toward its template's
seats, held in_progress work keeps its owner's session in demand and awake,
and orphan release still sees the assignment, so a parked bead's owner stays
visible to the pool and to crash recovery.

bd's `--exclude-label` matches one label exactly, so no reader flag can
exclude every hold. The generated work query drops held rows with jq before
each read cuts its answer to a window, and reads again unbounded when a full
window held a held row; `gc ready --until-unblocked` leaves held rows out
itself. A held bead never takes an unheld bead's place in a window.

Removing every active hold label lifts the hold, and the bead is ordinary work
at the next query. `bd set-state <id> hold=cleared` does that for a bead that
carries one hold label. It replaces only the first `hold:` label it finds, so on
a bead that carries more than one, remove each active one with
`bd label remove <id> hold:<value>`. When the bead still declares a route
(`gc.run_target`, or the session back-reference a handed-off bead carries), the
route-restore lanes recover it once the hold lifts.

## See also

- `bd show ga-tug8ry.1` — the architecture decision: full live census,
  per-label disposition rationale, and a label-flow diagram.
- `bd show ga-tug8ry.2` — the migration record: before/after counts and the
  beads intentionally skipped (bare `human` used for an unrelated reason).
- [Beads architecture](../architecture/beads.md) — the generic `Label` and
  `Store` mechanism this convention is built on.
