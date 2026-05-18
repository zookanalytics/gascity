---
title: "Downstream override of upstream [global].session_live"
status: Proposed
---

## Context

A downstream pack that imports an upstream pack inherits the upstream's
`[global].session_live` commands. `applyPackGlobals`
(`internal/config/pack.go:2099`) appends every loaded pack's
`SessionLive` to every in-scope agent, in load order: included globals
first, then this pack's own (`pack.go:1450`). The downstream pack can
therefore add more commands but it cannot:

- **Suppress** an upstream entry (the upstream theme script still
  runs).
- **Wrap** an upstream entry (run upstream's `tmux-theme.sh` with one
  argument overridden).
- Express **"default that downstream replaces"** at the upstream side.

In practice the downstream pack works around this by adding an overlay
script that runs after the upstream's and overwrites the side-effect
(in the gc-toolkit case, tmux options — mutable and last-write-wins).
The overlay pattern works but it:

- Costs an extra subprocess per `session_live` cycle.
- Hides the actual final state from the upstream's tests (the upstream
  sees its setting "win" but never observes the override).
- Does not solve per-agent customization at the declarative `agent.toml`
  `session_live` level. The agent's own entries are appended before
  globals (`pack.go:2103`) and the global clobbers — validated during
  gc-toolkit thread-title work.

This design surveys five candidate mechanisms and recommends one
(plus a clear "later, if needed" path).

## What this document does not address

- The **agent-vs-global ordering** problem (per-agent declarations run
  before globals and lose to them). That is a related but separable
  issue — it is about ordering, not about pack-level overrides. It is
  noted in [§ Out of scope](#out-of-scope) and should be tracked
  separately.
- Implementation of any option here. This document is research/design
  only; gastown maintainers (gascity-keeper / mayor) decide whether
  to advance.

## Existing precedents in the import boundary

The `Import` struct (`internal/config/config.go:656`) already has
per-import controls:

- `Transitive *bool` — when `false`, the import contributes only its
  *own* globals (not its transitively imported packs' globals). This
  is already enforced via `cachedPackLocalGlobals`
  (`pack.go:600`).
- `Shadow string` (enum `"warn" | "silent"`) — controls warning
  behavior when a downstream agent name collides with the import's.

So filtering pack outputs at the import boundary is an established
pattern, and a globals-suppress field would fit naturally alongside.

## Concrete use cases

1. **gc-toolkit replaces gastown's tmux theme.** gc-toolkit imports
   gastown wholesale. gastown's `[global].session_live` runs
   `tmux-theme.sh` and `tmux-keybindings.sh`. gc-toolkit wants to
   ship a different theme and a partly overlapping keybinding set.
   Today: gc-toolkit ships `tmux-bindings.sh` that runs *after*
   gastown's two scripts and clobbers what it disagrees with.
2. **A pack imports gastown but uses a different terminal multiplexer.**
   gastown's `tmux-*` scripts are a no-op on Zellij but still cost a
   subprocess each.
3. **CI / headless deployment.** An automated runner imports a
   pack-with-theme but never has a tmux server attached. Theme writes
   are wasted (and harmless) but visible in logs.
4. **Vendored pack used in two environments.** Same upstream pack,
   two downstream products — one wants the upstream defaults, the
   other wants its own. The pack author cannot offer a clean
   "default that gets replaced" without coordination.

The gc-toolkit case is #1 and is the immediate driver. Cases #2-#4
generalize the same shape.

## Design options

### Option 1 — Per-import suppress flag

Add a field on `Import` that drops the import's contribution to
`PackGlobals` at apply time.

```toml
[imports.gastown]
source = "../gastown"
globals_mode = "skip"   # default: "include"
```

**Mechanism.** Add `GlobalsMode string` (enum `"include" | "skip"`,
default `"include"`) to `config.Import`. When `"skip"`, filter the
import's resolved globals out of the slice returned for that import
in `expandCityPacks` / `expandPacks` (around `pack.go:565` and
`pack.go:102`). `applyPackGlobals` is unchanged; it just sees fewer
inputs.

**Transitivity.** Two reasonable interpretations:

- **a)** `"skip"` drops only the direct import's own globals; its
  transitive imports' globals (if `transitive = true`) still apply.
- **b)** `"skip"` drops everything that flowed in through this
  import, including transitive globals.

(b) is the more useful default for the gc-toolkit case (gastown's own
globals are exactly what we want gone). (a) would be more conservative
and composes more predictably with `transitive = true`. Recommend (b),
because users who want (a) can already get it by setting
`transitive = false` on a *separate* import of just gastown's globals
provider — and because the common mental model of "skip the import's
globals" reads as "remove what this import brought in."

**Surface area.** One field on `Import`; one filter site; one
documented enum.

**Complexity.** Low. ~10 lines of Go + tests + schema.

**Migration cost.** Zero. Default `"include"` matches today's
behavior. Existing packs unaffected.

**Power.** Coarse but composable. A downstream that skips upstream's
globals can re-declare what it wants in its own `[global]` and own the
result end-to-end. The skipped script never runs, so there is no
hidden cost.

**Limitations.** All-or-nothing per import. To keep one upstream
entry and replace another, you must skip + re-implement both.

### Option 2 — Per-entry named globals (override by name)

Restructure `[global].session_live` entries so each can carry a name;
downstream entries with a matching name *replace* upstream entries
during apply.

```toml
# Upstream
[[global.session_live]]
name = "tmux-theme"
command = "{{.ConfigDir}}/scripts/tmux-theme.sh {{.Session}} {{.Agent}}"

[[global.session_live]]
name = "keybindings"
command = "{{.ConfigDir}}/scripts/keybindings.sh"
```

```toml
# Downstream
[[global.session_live]]
name = "tmux-theme"     # replaces upstream's tmux-theme entry
command = "{{.ConfigDir}}/scripts/my-theme.sh"
```

**Mechanism.** Change `PackGlobal.SessionLive` from `[]string` to a
sum type (string OR `{name, command}` table). Accept both for
back-compat. `applyPackGlobals` walks entries in load order, keying
named entries by `name`: later wins. Anonymous entries always append
(today's semantics).

**Surface area.** Substantial:

- TOML schema and Go type change for `PackGlobal`.
- `ResolvedPackGlobal` carries name → command map *and* an ordered
  list for the anonymous tail.
- Apply-time merge logic instead of pure append.
- Edge cases:
  - Two siblings of the same import both name `"tmux-theme"`: first
    wins or last wins?
  - A pack imports gastown and maintenance and both define a
    `"tmux-theme"`: deterministic order required (today: included
    first by `pack.go:1450`).
  - "Suppress only" form (`name = "tmux-theme"` with no command)
    needs to be a separate construct or else collide with "replace
    with empty string."

**Complexity.** High. Touches type, parser, merge logic, several
tests. Schema migration story for existing string-form entries.

**Migration cost.** Zero on the consumer side: existing string
entries continue to append. **Non-zero on the producer side**:
upstream packs that want overridable entries must adopt the named
form, then existing downstream `string` entries continue to land in
the anonymous tail.

**Power.** Highest. Per-entry replace, suppress (with explicit
"unset" semantics), and the "default that downstream replaces"
pattern all fall out naturally.

**Verdict.** Powerful but expensive. The cost is justified only when
*multiple* concrete cases need per-entry control. The gc-toolkit
case does not — it wants neither of upstream's two scripts.

### Option 3 — Pre/post hook slots

Add `pre` / `post` / `replace` slots on the global structure (or
under each import).

```toml
[imports.gastown.globals]
pre = ["{{.ConfigDir}}/before-upstream.sh"]
post = ["{{.ConfigDir}}/after-upstream.sh"]
# or: replace = ["{{.ConfigDir}}/own.sh"]
```

**Mechanism.** Extend the global apply loop to splice extra
commands before and/or after each import's `SessionLive`. `replace`
is equivalent to `globals_mode = "skip"` + a downstream `[global]`
entry, with the bonus that the replacement lives at the import site
rather than in the consumer's own `[global]`.

**Surface area.** 2–3 new fields under `Import` (or under
`[global]`). New interleaving logic in `applyPackGlobals`. Slot
ordering documented.

**Complexity.** Medium. Cleaner conceptual model than Option 2
("around" pointcut) but solves the same use cases with more API
surface and without per-entry granularity.

**Migration cost.** Zero (default empty slots).

**Power.** Moderate. Solves "wrap all of upstream's globals" (which
no concrete use case demands today). Does *not* solve "replace one
of two upstream entries." `replace` is a strictly noisier spelling
of Option 1.

**Verdict.** Adds API surface without uniquely solving a real use
case. The aliasing-to-shell-script wrap pattern is already trivial
(downstream calls upstream's script from its own).

### Option 4 — Default semantics (stringly-typed key prefix)

Convention-driven naming embedded in the string:

```toml
# Upstream
[global]
session_live = [
    "default:tmux-theme={{.ConfigDir}}/scripts/tmux-theme.sh",
]

# Downstream
[global]
session_live = [
    "tmux-theme={{.ConfigDir}}/scripts/my-theme.sh",
]
```

**Mechanism.** Parser splits each entry on `:` and `=`; the
key-prefixed entries participate in a name-keyed merge.

**Verdict.** Same semantics as Option 2 with worse syntax —
escape rules, parser surprises, fragile to typos. Rejected on
form alone. (If we want name-keyed override, Option 2 is the
correct shape.)

### Option 5 — Composability primitives (env vars to chained scripts)

Inject `GC_GLOBAL_INDEX`, `GC_GLOBAL_SOURCE_PACK`, `GC_GLOBAL_TOTAL`
into each `session_live` invocation. Scripts can introspect and
decide to skip, wrap, or chain.

**Mechanism.** Set env vars in the `session_live` runner before each
call.

**Surface area.** Minimal Go (env-var injection); large *script-side*
contract (downstream scripts now contain "skip if index == 0 and pack
== gastown" logic).

**Verdict.** **Rejected.** Pushes a judgment call into scripts —
fails ZFC. Also non-declarative; the override behavior is not visible
in any pack.toml. Hard to test (script ordering becomes part of the
configuration's truth). The same composability is available in a
straightforward way: the downstream script calls the upstream script
explicitly, and the framework only needs to know how to *not* run the
upstream entry — which is Option 1.

## Recommendation

**Adopt Option 1 (`globals_mode = "skip"` on `Import`).** Defer
Option 2 unless and until at least one concrete use case demands
per-entry granularity that skip-and-reimplement cannot satisfy.

### Why Option 1

1. **Smallest viable surface area** that unblocks the driving case
   and the foreseeable cases listed in
   [§ Concrete use cases](#concrete-use-cases). The Bitter Lesson
   favors the smaller knob: model and pack authors get smarter, the
   knob does not.
2. **Fits an existing precedent** (`transitive`, `shadow`). The
   conceptual category — per-import filter on what the import
   contributes to the consumer — already exists.
3. **No new ordering semantics.** `applyPackGlobals` keeps its
   current append model. Only the input set changes.
4. **Zero migration.** Default `"include"` is today's behavior. No
   existing pack needs to change.
5. **Composes cleanly.** A downstream that needs per-entry control
   can already get it by:
   - `globals_mode = "skip"` on the import, *and*
   - re-declaring whichever upstream entries it wants, with whatever
     args it wants, in its own `[global].session_live`.

   This is more verbose than Option 2's "override by name" but it
   makes the final state explicit and discoverable from the
   downstream pack alone — no need to read the upstream pack to
   know what is and isn't running.
6. **The Primitive Test passes:**
   - **Atomicity.** Cannot be built out of the existing primitives
     today (no way to suppress an import's globals contribution).
   - **Bitter Lesson.** A boolean per-import filter does not encode
     domain judgment; it stays useful regardless of how clever
     pack authors get.
   - **ZFC.** No Go conditional decides which entries are filtered
     based on runtime state — the user declares it in TOML.

### When to revisit (Option 2)

Add named-entry override **only** when both are true:

1. ≥2 concrete pack-pair use cases need to keep some upstream
   entries and replace others.
2. The skip-and-reimplement workaround is meaningfully more
   expensive than per-entry override (large upstream entry counts,
   non-trivial parameterization, or the upstream entry references
   private upstream paths that the downstream cannot reproduce).

Until then, Option 2 is speculative complexity (YAGNI).

### Rejected directly

- **Option 3** — duplicates Option 1's value (`replace` slot) while
  costing more surface area. The "wrap" use case is solvable by
  shell.
- **Option 4** — same semantics as Option 2 in a worse spelling.
- **Option 5** — pushes ordering judgment into scripts; fails ZFC.

## Implementation sketch (for Option 1)

For reference only; not part of this research. A future
implementation bead would:

1. Add `GlobalsMode string \`toml:"globals_mode,omitempty"
   jsonschema:"enum=include,enum=skip"\`` to `config.Import`
   (`config.go:656`). Empty string treated as `"include"`.
2. In `expandCityPacks` (`pack.go:565`) and the rig-import path
   (`pack.go:102` / `pack.go:189`), branch on `imp.GlobalsMode`:
   when `"skip"`, do not append the import's globals into
   `allGlobals` / `rigGlobals`.
3. Decide explicitly between interpretation (a) and (b) above
   for transitive globals. Recommend (b): a skipped import
   contributes zero globals (its own and its transitive). Document
   the choice.
4. Tests in `internal/config/pack_test.go`:
   - `globals_mode = "skip"` removes the import's own globals.
   - `globals_mode = "skip"` removes the import's transitive
     globals (or doesn't, per the chosen interpretation).
   - Default behavior unchanged when `globals_mode` is unset.
   - Coexistence with `transitive = false`.
   - Downstream's own `[global]` still runs alongside.
5. JSON schema regen for `pack.toml`.

## Out of scope

The **agent-vs-global ordering** issue surfaces in the same area but
is a separate concern. The agent's own `session_live` is appended
*before* pack globals at apply time, so an agent declaration cannot
win over a global at the `agent.toml` layer. Two design directions
exist:

- Document the limitation, and steer per-agent overrides through
  patch-level `session_live_append` (which sits in the pack apply
  pipeline and currently lands before globals, so this also does
  not fully resolve the issue).
- Add an explicit `session_live_post_globals` slot, or reverse the
  apply order so the agent's terminal declaration is authoritative.

Either choice changes existing semantics for every pack and warrants
its own design bead. The override mechanism proposed here neither
fixes nor worsens that ordering question.

## Open questions

- **Naming.** `globals_mode` mirrors `shadow`'s string-enum form.
  Alternatives:
  - `globals = false` (boolean form) — minimal but inflexible.
  - `[imports.gastown.globals] session_live = false` (nested form)
    — future-proof if globals grows beyond `session_live`.

  Recommend `globals_mode` (matches existing pattern, leaves room
  for future enum values like `"local-only"` without restructuring).
- **Transitive interpretation (a vs b).** Recommended (b); needs
  explicit decision before implementation. (b) is simpler to reason
  about ("skip drops this import's globals contribution to me").
- **Rig-level imports.** The same field should be honored on
  `[rigs.imports.X]` for symmetry. The implementation site
  (`pack.go:102`) covers this.
- **Diagnostic visibility.** Should a skipped import emit a debug
  log line or a `gc doctor` informational entry, so a user can see
  *why* the upstream's globals aren't taking effect? Recommend yes,
  at debug level.

## Related work

- gc-toolkit composable status-line bead (filed in the tk rig) —
  the immediate consumer use case forcing the overlay-and-overwrite
  pattern today.
- `Import.Transitive` and `Import.Shadow` (`config.go:656`) — the
  existing per-import filter precedents.
- `applyPackGlobals` (`pack.go:2099`) — the apply site where the
  filtered input set is consumed.
