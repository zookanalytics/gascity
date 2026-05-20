# Architectural validation: `b4714629c` (sling assignee stamping for singletons)

**Bead:** gc-0snme2 (validates gc-yb5uhi / commit b4714629c)
**Date:** 2026-05-19
**Author:** gascity/gc-toolkit.nux

## TL;DR

The fix is **architecturally sound** and is best understood as restoring a
previously-removed invariant (singletons get a direct assignee at routing
time) that was lost when sling unified on the `gc.routed_to` metadata
path. It does **not** paper over the named-origin Tier 3 short-circuit;
it correctly recognizes that the short-circuit is a load-bearing
protection against debug instances racing for pool work, and that for
true singletons there is no race to participate in.

There are two real follow-ups worth filing — neither blocks the commit:

1. **Test gap.** The unit tests assert `assignee == target` (a precondition
   for Tier 2) but never run the actual hook query end-to-end. Add an
   integration-level assertion that `EffectiveWorkQuery()` evaluated with
   `GC_SESSION_ORIGIN=named` and the post-fix bead state surfaces the
   bead.
2. **On-death reclaim for singletons.** A singleton that dies *after*
   claiming routed work loses the work to a stranding hole that the
   sling-time fix does not address. `EffectiveOnDeath` clears the
   assignee on death; for singletons, Tier 2 then misses the bead and
   Tier 3 remains short-circuited. This was true before the fix as
   well (the singleton never saw the work the first time either), so it
   is not a regression — but the fix surfaces the gap by making the
   first-pass case work.

A more invasive Option-B style change (Tier 3 fires for named singletons)
would also resolve (2), but is **not** recommended in the same change
set: it requires the work_query template generator to branch on agent
shape, and the dispatch / molecule / convergence stack already relies on
the "singleton has assignee, pool has only metadata" split that the
current fix codifies.

## 1. Routing architecture as it stands today

### The two-field model

A bead's routing state is expressed by two fields:

- **`assignee`** — a string identifier of *who owns this bead*.
  Resolvable as one of `$GC_SESSION_ID` (bead ID), `$GC_SESSION_NAME`
  (runtime tmux session name), or `$GC_ALIAS` (qualified agent name).
- **`metadata["gc.routed_to"]`** — a string template name of *where this
  bead is supposed to land*. Always the qualified name (or pool name).
  Stable across crashes and reschedules; reclaimed-after-death work
  recovers via this field.

These are **not redundant**. The fields express different lifecycle
stages: `gc.routed_to` is the persistent target (set at routing time and
preserved through recovery), `assignee` is the current claim (set when
a session takes responsibility).

For pool agents the two diverge: many polecats *could* race for one
`gc.routed_to=mypool` bead but exactly one of them ends up as `assignee`.
For singletons the two collapse: the singleton is the only possible
target, so "where it should land" and "who owns it" name the same
session.

### Tier model in `EffectiveWorkQuery`

Defined at `internal/config/config.go:2523-2592`. Each agent's discovery
query has three tiers, evaluated in order:

| Tier | Query | Purpose |
|------|-------|---------|
| 1 | `bd list --status=in_progress --assignee=$id` | Crash recovery: pick up work I already owned |
| 2 | `bd ready --assignee=$id` | Pre-assigned: pick up work routed *to me by name* |
| 3 | `bd ready --metadata-field gc.routed_to=<target> --unassigned` | Pool race: pick up generic config-level demand |

`$id` iterates `$GC_SESSION_ID`, `$GC_SESSION_NAME`, `$GC_ALIAS`. Any
match wins, so the *form* of the stored assignee (tmux session name vs.
qualified name vs. bead ID) doesn't matter as long as one of those
identifiers carries it.

### The named-origin short-circuit

`internal/config/config.go:2548-2551, 2583-2586`:

```sh
case "$GC_SESSION_ORIGIN" in
  ephemeral|"") ;;     # ephemeral sessions + controller probes proceed to Tier 3
  *) exit 0 ;;          # named sessions stop at Tier 2
esac
```

The intent (per the code comment): "Only ephemeral sessions and
controller probes consume generic config demand."

**This is correct, with a subtle reading.** "Generic config demand"
means "demand routed to the agent *template*, to be raced by any
ephemeral instance." For a multi-session pool, ephemeral instances race
for `gc.routed_to=<pool-name>`. A *named* invocation of a pool agent
(operator debug session, hand-launched polecat) must not steal pool
work — the short-circuit is what protects that invariant.

For a true singleton, there is no race: only one session ever exists
with that identity. But the short-circuit still fires, because the
template generator does not know its target's shape.

The current fix recognizes that the cleaner response is to make
singletons *not need* Tier 3 by stamping their assignee at routing
time, so Tier 2 always surfaces the work. The short-circuit stays as-is
because it is correct for its actual purpose (named pool-agent
invocations).

### Singleton vs pool predicates

`internal/config/config.go:2751-2772`:

```go
func (a *Agent) SupportsInstanceExpansion() bool {
    ...
    m := a.EffectiveMaxActiveSessions()
    if m == nil { return true }              // unbounded → multi
    if *m < 0 || *m > 1 { return true }      // capped >1 → multi
    // *m == 1: pool-flavor or named-flavor?
    if a.MinActiveSessions != nil || a.ScaleCheck != "" {
        return true                           // pool with max=1 (still pool semantics)
    }
    return false                              // true named singleton
}
```

`!SupportsInstanceExpansion()` is the precise predicate the fix uses
for "singleton." It excludes:
- Namepools (a.Namepool, a.NamepoolNames)
- Unbounded agents (max=nil)
- Multi-session agents (max>1)
- Singleton-pools (max=1 with Min or ScaleCheck — these run pool
  reconciliation and are addressed by `gc.routed_to` matching the pool
  template)

…and includes only the named-session flavor (max=1, no pool markers, no
ScaleCheck), which is exactly the set that has `GC_SESSION_ORIGIN=named`
and stable qualified-name identity. The predicate is correct.

### Where the fix lands

The diff (7 files, 187/10) touches three layers:

- **`internal/sling/sling.go:106-111`** — adds `Singleton bool` to
  `RouteRequest`. Doc explains the routing-stamp asymmetry.
- **`internal/sling/sling_core.go:354-362, 1141-1149`** — both
  `finalize()` and `DoSlingBatch()` populate `Singleton:
  !a.SupportsInstanceExpansion()`.
- **`cmd/gc/cmd_sling.go:629-639`** and
  **`internal/api/handler_sling.go:441-453`** — when `Singleton=true`,
  both routers also `Store.Update(beadID, beads.UpdateOpts{Assignee:
  &target})` after the `gc.routed_to` metadata write.

The split (typed field at the sling boundary, routers do the actual
write) is consistent with the rest of the typed-router refactor — the
core sling logic doesn't grow a direct dependency on the store update
API.

## 2. Does the fix conflate `assignee` and `gc.routed_to`?

**No.** Two pieces of evidence:

### 2a. Dispatch/control already encoded this exact pattern

`internal/dispatch/control.go:840-874` (`applyAttemptStepRoute`) — the
ralph/retry/fanout path that re-routes work on attempt cycles —
already branches on singleton vs multi-session:

```go
if binding.metadataOnly {     // pool / multi-session target
    step.Assignee = ""
    return
}
step.Assignee = binding.sessionName  // singleton: direct assignment
```

`binding.metadataOnly` is set when
`isAttemptMultiSessionTarget(...) → SupportsInstanceExpansion()`. So
dispatch has long stamped assignee on singletons and kept it empty for
pools. The sling fix is bringing sling onto the same model.

### 2b. `CheckBeadState` already expects this shape

`internal/sling/sling_attachment.go:349-376` — the pre-flight check that
warns "already routed" vs "already assigned":

```go
if !isMulti {
    if b.Assignee == target {
        return BeadCheckResult{Idempotent: true}
    }
    // ...warns about mismatched assignee...
}
```

The "idempotent re-sling to singleton" branch checks `b.Assignee ==
target` — which is exactly the post-fix shape. Pre-fix this branch was
unreachable on a first re-sling (because `b.Assignee` was always empty
for routed beads); post-fix it correctly short-circuits the second
sling.

### 2c. Convention drift: assignee value differs by path

One real smell. Dispatch sets `assignee = binding.sessionName`
(`config.NamedSessionRuntimeName(...)`, the tmux runtime name like
`loomington-gascity-gc-toolkit-mechanik`). Sling sets `assignee =
a.QualifiedName()` (e.g., `gc-toolkit.mechanik`).

Both work — the Tier 1/2 loop iterates `$GC_SESSION_NAME` and
`$GC_ALIAS` separately — but downstream code that does string equality
on `bead.Assignee` will see different values depending on origin.
`ralph.go:549-561 retryPreservedAssignee` is the most prominent reader;
it just passes the value through, so the divergence does not bite
today.

**Worth a follow-up to pick one convention,** but it does not block the
fix. The sling convention (qualified name) is arguably the better
choice: it's stable across rename of the runtime session-name template,
and it's what the bead store already uses for `assignee` queries in
`EffectiveOnDeath`.

## 3. Behavior changes worth calling out

### 3a. Assignee overwrite on re-sling (codified by test)

`TestDoSlingBeadToSingletonOverwritesExistingAssignee` codifies that
slinging to a singleton replaces any existing assignee, even one set by
a human. This is a behavior addition not explicitly discussed in the
original bead's "Tasks" section but follows from the chosen approach:

- If the fix preserved an existing assignee, the singleton would still
  miss the bead via its own hook (because the assignee would not match
  the singleton's identity).
- The pre-flight `CheckBeadState` already emits a warning
  (`internal/sling/sling_attachment.go:365-366`) before the overwrite
  occurs, so the operator is informed.

This is the right call. `gc sling <singleton> <bead>` is an
operator-initiated "this is now mechanik's work" command. Preserving a
stale claim would defeat the whole point.

### 3b. Footer / external observer queries see different counts

External queries that ran `bd ready --metadata-field
gc.routed_to=<singleton> --unassigned` (Tier-3 shape, no identity vars
set) used to see routed-to-singleton beads. Post-fix they will not —
the bead is now `--assigned`, not `--unassigned`. This matches the
status-line footer symptom called out in gc-yb5uhi.

Anyone running such a query for *external observer* purposes (e.g.,
"how many beads are queued for mechanik?") needs to drop the
`--unassigned` filter for singletons. Worth checking the tk-foudmx
footer change for compatibility, but that lives in the gc-toolkit pack
and is out of scope for this memo.

### 3c. Manual routing via `bd update --set-metadata gc.routed_to=…`

Routing a bead to a singleton *without* using `gc sling` (e.g., a human
running `bd update <id> --set-metadata gc.routed_to=gc-toolkit.mechanik`
directly) is still affected by the original asymmetry — `bd update`
does not know about agent shape. The bead would be routed but invisible
to mechanik's hook.

In practice this should rarely happen (sling is the canonical path) but
it's a known sharp edge. If it becomes a real issue, the right answer
is to teach `bd update` to consult agent shape, not to widen Tier 3.

## 4. Test coverage gap

Current tests assert:

- The `assignee` field gets stamped (`cmd/gc/cmd_sling_test.go:451-457,
  486-491, 503-559`)
- Pool agents keep empty assignee
  (`cmd/gc/cmd_sling_test.go:705-708`)
- The `Singleton` flag is set correctly for the four shape cases
  (`internal/sling/sling_test.go:1493-1554`)
- Existing assignee is overwritten
  (`cmd/gc/cmd_sling_test.go:530-559`)

Gaps:

1. **No end-to-end work_query evaluation.** No test runs
   `EffectiveWorkQuery()` (as a shell command) against the post-fix
   bead state with `GC_SESSION_ORIGIN=named` and asserts the bead is
   returned. The Tier-2 bypass is *inferred* from the assignee
   assertion + a shell-script reading. This is a small inferential
   leap (Tier 2 is a single trivial `bd ready --assignee=<id>` query),
   but the original bug was an asymmetry in shell-script generation,
   so testing the shell script directly is the most direct guard
   against regression.

2. **No coverage for the named-pool case end-to-end.** A test asserts
   `Singleton=false` for a named pool (max=1 + Min=1), but no test
   asserts that the named pool's actual hook still works the same way
   pre- and post-fix. Existing tests probably cover this implicitly,
   but it's worth a single explicit assertion.

3. **No coverage for the post-on_death scenario for singletons** (see
   §5).

## 5. The on-death reclaim hole (pre-existing, not a regression)

`EffectiveOnDeath` (`internal/config/config.go:2807-2831`) reopens
in-progress work on session death:

```sh
bd list --assignee=<qualified-name> --status=in_progress --json | ...
  bd update "$id" --assignee "" --status open
```

For pool agents this is correct: the next ephemeral pool instance
boots, runs Tier 3 (gc.routed_to + --unassigned), reclaims the bead.

For singletons (post-fix) this leaves a hole:

- Pre-death state: `assignee=mechanik, status=in_progress, gc.routed_to=mechanik`
- on_death clears assignee, sets status=open
- Post-death state: `assignee="", status=open, gc.routed_to=mechanik`
- New mechanik instance boots, runs hook:
  - Tier 1: status=in_progress required → miss
  - Tier 2: assignee=mechanik required → miss (cleared)
  - Tier 3: short-circuited for named origin → miss
- Bead is stranded until manual re-sling

This is **not a regression** — pre-fix the singleton couldn't see the
bead even on first sling. But the fix surfaces the gap: the first-pass
case now works, so the second-pass-after-death case becomes a visible
deviation.

The cleanest patch is a singleton-aware on_death: if the agent is a
singleton, *preserve* the assignee on death, just flip status back to
open. Tier 2 then re-surfaces it on the next instance. This is a
two-line change in `EffectiveOnDeath` and a unit test.

(Alternative: teach Tier 3 to fire for named singletons. Same
architectural objection as the original bead's Option B — the
work_query generator would need to branch on agent shape, and the
existing dispatch/molecule code base already commits to the
"singleton-has-assignee" model. The on_death patch is the smaller,
local fix.)

## 6. Other options the original bead missed

The original analysis considered Option A (sling-side stamping, chosen)
and Option B (work_query branches on shape). Two additional options
worth listing for completeness:

- **Option C — generic-target Tier 3 variant.** Generate two work_query
  forms: one with the Tier 3 short-circuit (for pool agents), one
  without (for singletons). Effectively a code-genned Option B.
  Cleaner conceptually but spreads the work_query template into a
  switch. Rejected for the same reason as B.

- **Option D — `assignee_intent` metadata layer.** Introduce a third
  metadata key like `gc.assignee_intent=<target>` that singleton hooks
  treat as equivalent to an assignee match. Strictly worse than the
  chosen fix (adds a key for every reader to learn) and offers no
  advantage. Mentioned only to rule out.

The chosen Option A is the best fit *given the constraint that
work_query template generation should not depend on agent shape*. That
constraint may be wrong long-term — the template is generated from
agent config, the agent struct is in scope, threading the shape through
is feasible — but reconsidering it is a separate architectural
conversation, not a blocker for this fix.

## 7. Recommended follow-ups

Filed as separate beads:

1. **End-to-end work_query test** → `gc-pyc6tt` (P2). Add a test in
   `internal/config/config_test.go` (or `cmd/gc/cmd_sling_test.go`)
   that runs `a.EffectiveWorkQuery()` as a shell command with
   `GC_SESSION_ORIGIN=named`, `GC_ALIAS=gc-toolkit.mechanik`, against a
   real-ish bead store seeded by a sling call, and asserts the bead is
   returned. Closes the inferential gap.

2. **Singleton-aware `EffectiveOnDeath`** → `gc-p2lu8o` (P1, bug). Branch
   on the agent's singleton flavor in `EffectiveOnDeath`: for
   singletons, leave assignee alone on death, just reopen status. Add a
   test for the reclaim cycle. Closes §5.

3. **Assignee-value convention drift** → `gc-9caakt` (P3). Pick one
   convention — qualified name or runtime session name — for `assignee`
   and align dispatch+sling to it. Sling's convention (qualified name)
   is the recommended choice; dispatch's `applyAttemptStepRoute` is
   where the change would go. Mostly cleanup; the existing
   inconsistency does not cause runtime bugs.

## 8. Verdict

**`b4714629c` is architecturally sound.** Land as-is. Two priority
follow-ups filed (`gc-pyc6tt` test, `gc-p2lu8o` on_death); the third
(`gc-9caakt` convention drift) can wait for a refactor that's already
touching that code.
