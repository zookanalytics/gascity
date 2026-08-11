---
name: Can the pack ship Codex hooks already city-bound? (gc-bvlmh)
description: Traces the per-provider overlay staging path end to end to answer whether pack file templating can render a city-root token into .codex/hooks.json, and lays out the decision material the gc-bvlmh visit needs to rule on gc-tn0i.
---

# gc-bvlmh: pack-side city-binding is not reachable today

## Verdict

**Outcome (b). Pack-side binding is not available.**

No template expansion of any kind is applied to per-provider overlay files.
A pack cannot ship `.codex/hooks.json` already bound to a city root, because
the path that puts that file into an agent's working directory is a byte copy
(or a JSON merge of two byte-copied documents), and the city root is a
per-install absolute path that only the running process knows.

So gc-beez's fork-local `NormalizeManagedCodexHooks` is **not sheddable by a
pack change**, and gc-tn0i cannot be solved pack-side.

The visit's lead reached this same conclusion, but from a premise that is
false, and the correction matters: **non-prompt pack-file templating does
exist, it carries a `CityRoot` token, and it is upstream code.** It is simply
wired to a different loader for a different file class. That turns outcome
(b)'s "deepen the fork vs. contribute the normalizer upstream" into a
three-way choice with a third option that is smaller than either — see
[Decision material](#decision-material).

## The question this bead was filed to answer

> Can pack file templating render a city-root token into *arbitrary*
> (non-prompt) pack files, specifically `per-provider/codex/.codex/hooks.json`?

Answered from the code, end to end, plus two executable probes. The
grep-absence reasoning the visit flagged as insufficient is not relied on
anywhere below.

## Answer, from the code

### The staging path, end to end

Every route that stages a per-provider overlay into a working directory
converges on the same three functions, and none of them expands a template:

| Step | Location |
|---|---|
| Session-start staging (generic) | `internal/runtime/staging.go:38,43` |
| Session-start staging (tmux) | `internal/runtime/tmux/adapter.go:110,117` |
| Session-start staging (k8s) | `internal/runtime/k8s/staging.go:121` |
| Reconciler staging | `cmd/gc/build_desired_state.go:4679,4684` |
| ↓ all five call | `runtime.StageProviderOverlayDir` — `internal/runtime/staging.go:106` |
| ↓ which calls | `overlay.CopyDirForProviders` — `internal/overlay/overlay.go:272` |
| ↓ which walks and calls | `copyOrMergeFile` — `internal/overlay/overlay.go:328` |
| ↓ which ends in | `copyFile` (`overlay.go:412`) **or** `MergeSettingsJSON` (`internal/overlay/merge.go:99`) |

`copyOrMergeFile` branches on one question — is this a mergeable settings
path (`IsMergeablePath`, `internal/overlay/merge.go:18`, where
`.codex/hooks.json` is listed)? Either branch reads bytes and writes bytes.
Neither branch, and nothing above them in the chain, constructs a
`text/template` or calls `Execute`. There is no hook, option, or filename
convention in this path that would let a pack opt a file into expansion.

### Probe 1 — a city-root token is copied through literally

Staging a pack overlay whose `.codex/hooks.json` contains
`gc --city '{{ .CityRoot }}' prime --hook`, through the exact function
session create uses (`overlay.CopyDirForProviders`), produces:

```json
"command": "gc --city '{{ .CityRoot }}' prime --hook"
```

The token is not rendered. The same probe also shipped the file under the
upstream `.template.<ext>` naming convention (`hooks.template.json`): it was
copied through byte-for-byte as well, and landed in the working directory as
a stray unrecognized file — the convention has no meaning in this path.

### Probe 2 — staging drops a binding a prior normalization wrote

Same function, with a working directory already holding a normalized,
city-bound `.codex/hooks.json`, then staging the raw pack file over it:

```
before (staged, normalized):  gc --city '/home/zook/loomington' prime --hook
after  (overlay staged):      gc prime --hook
```

`.codex/hooks.json` is a mergeable path, so this is not a blind overwrite —
it is the identity-keyed merge in `MergeSettingsJSON`. Hook entries are
merged by identity, and for these entries the identity is the `matcher`
value (`internal/overlay/merge.go:76-88`). The pack's `SessionStart` entry
and the normalized one on disk share the matcher `"startup"`, so *same
identity → overlay replaces base*, and the `--city` binding is discarded.

That confirms the mechanism behind the bead's "what is actually at stake"
paragraph, and it is worth stating precisely because the merge could
plausibly have appended instead: it does not. The binding is lost, not
duplicated.

### What the lead missed: a non-prompt pack-file templater already exists

The lead concluded that "every `text/template` consumer under `cmd/gc` and
`internal/runtime` is a prompt / probe / lint path." That is true of those
two directories, and it is the wrong search space. `internal/materialize`
holds a third kind:

- `internal/materialize/mcp.go:105-115` — a pack file named
  `<name>.template.toml` is recognized as templated.
- `internal/materialize/mcp.go:314-327` — `expandMCPTemplate` runs it through
  `text/template` with `missingkey=error`.
- `internal/materialize/mcp_runtime.go:46,71` — the data map it renders with
  carries **`CityRoot` = the city path**, alongside `RigRoot`, `WorkDir`,
  `AgentName`, and the agent's env.

So the exact capability the bead hypothesized — a city-root token rendered
into a non-prompt pack file — is already built, already proven, and (checked
against `upstream/main`) **is upstream code, not a fork invention**:

```
$ git show upstream/main:internal/materialize/mcp_runtime.go | grep -n CityRoot
58:  "CityRoot":      cityPath,
82:  data["CityRoot"] = cityPath
```

It does not help `.codex/hooks.json` today, for a structural reason rather
than a philosophical one: MCP templating belongs to the MCP *catalog loader*,
which parses a TOML file into a `MCPServer` struct in memory. It never writes
a staged file, and overlay staging never consults it. Two different loaders,
two different file classes, no shared seam.

The conclusion is unchanged — **the answer to the question is still no**.
What changes is the option set, because "render a `CityRoot` token into a
pack file" is demonstrably not foreign to this codebase's design; it is
missing from exactly one path.

## A finding that raises gc-tn0i's price

gc-tn0i, as filed, asks for `NormalizeManagedCodexHooks` to be called from
`internal/runtime/staging.go`, `internal/runtime/tmux/adapter.go`, and
`internal/runtime/k8s/staging.go`.

**Those three call sites cannot do it without a core type change.** All three
stage from a `runtime.Config`, and `runtime.Config` has no city-root field:

```
$ awk '/^type Config struct/,/^}/' internal/runtime/runtime.go | grep -i city
(no matches)
```

The only city field in that file is `LiveRuntime.City`
(`internal/runtime/runtime.go:361`), on the process-scan result type — a
different struct, populated from a scanned process's environment, not
available to a staging call.

The reconciler site works only because it is not on that type:
`normalizeStagedCodexHooks` (`cmd/gc/build_desired_state.go:4703`) reads
`bp.cityPath` from the builder's own params.

So implementing gc-tn0i as written means: add a city-root field to
`runtime.Config`, populate it on every construction path, and call a
fork-local normalizer from three core runtime files. That is materially more
divergence than the bead assumed when it estimated the cost, in precisely
the packages that make absorbing upstream painful.

## What is actually at stake, restated with evidence

Verified, not inferred:

- The gc-toolkit pack ships its own `overlay/per-provider/codex/.codex/hooks.json`,
  and it is unbound (no `--city`). Checked in the pack cache.
- The core embedded asset
  (`internal/bootstrap/packs/core/overlay/per-provider/codex/.codex/hooks.json`)
  is *also* unbound. No pack can fix this by dropping its own copy and
  deferring to core — the core file has the same problem, for the same
  reason: the value is per-install.
- The normalizer upgrades in place, so the live file on disk keeps the pack's
  `matcher: "startup"` and gains the binding. Confirmed on a live agent:
  `/home/zook/loomington/rigs/gc-toolkit/.codex/hooks.json` currently reads
  `gc --city '/home/zook/loomington' prime --hook …` on every entry.
- The binding form is a global flag, `gc --city <root> <subcommand>`
  (`internal/hooks/hooks.go:974`), not a `prime`-specific one.
- Only the reconciler normalizes. Session create stages and stops
  (`stageStartFiles`, `internal/runtime/tmux/adapter.go:101-119`).

Net: at every session create the binding is dropped, and it stays dropped
until the next reconciler tick re-imposes it. That is the narrower re-opening
of what `9580649b1` ("fix(hooks): bind managed Codex hooks to city root",
#3866) closed, exactly as the bead described.

## Decision material

The decision belongs to the gc-bvlmh visit. The three options, with what each
actually costs given the findings above:

**Option 1 — implement gc-tn0i as filed.** Add a city field to
`runtime.Config`, populate it everywhere, call the fork-local normalizer from
three core runtime files. Closes the window. Deepens the fork in
`internal/runtime/*` *and* changes a core type, which is worse than the
bead's estimate. Nothing about it is upstreamable later without rework.

**Option 2 — contribute the normalizer upstream.** Offer
`NormalizeManagedCodexHooks` to `gastownhall/gascity`. Sheds the fork patch
if accepted. But it asks upstream to adopt a post-hoc "re-write the file
staging just wrote" pass — repairing a staged artifact rather than staging
the right artifact — and it carries a Codex-specific hook vocabulary into
core. It is the harder sell of the two upstream options, and it leaves the
underlying gap (overlay files cannot express install-specific values) in
place for the next provider that needs one.

**Option 3 — contribute overlay-file templating upstream.** Teach the
overlay staging path to render a file through `text/template` with a data map
that already exists in this codebase, keyed off the `.template.<ext>`
convention that already exists in this codebase, both of them upstream
(`internal/materialize/mcp.go`, `mcp_runtime.go`). The pack then ships
`.codex/hooks.template.json` containing `{{ .CityRoot }}`, staging renders it
bound, and nothing needs normalizing at session start *or* at reconcile
time — the normalizer becomes sheddable, which is the outcome (a) this bead
was hoping for, reached by an upstream change instead of a pack change.

Option 3's real cost is not in the renderer; it is that
`StageProviderOverlayDir` currently takes no city/agent context, so the data
map has to be threaded to it — the same plumbing Option 1 needs, done once,
generically, in a form upstream can plausibly want. It is also the only
option that leaves the fork smaller than it found it.

**Recommendation (for the visit to accept or reject): Option 3**, with
Option 1 as the fallback only if upstream declines and the window is judged
urgent. Option 3 is the smallest change that removes the fork patch instead
of extending it, and it is the only one that generalizes past Codex.

Not assessed here, and worth the visit's attention: how long the window
actually stays open in practice (reconciler tick interval), which determines
whether this is urgent or merely untidy.

## What this bead did not do

Per the routing note's scope:

- No code changed. `internal/runtime/staging.go`,
  `internal/runtime/tmux/adapter.go`, `internal/runtime/k8s/staging.go`,
  `cmd/gc/build_desired_state.go` and `internal/hooks/` are untouched.
- gc-tn0i stays blocked, awaiting the visit's ruling.
- No gc-toolkit pack bead was filed. The cross-repo caution does not fire:
  the answer is (b), so there is no pack-side change to route to
  `zookanalytics/gc-toolkit`.

The two probes were throwaway programs run against the real
`internal/overlay` package and deleted; they are described above in enough
detail to reconstruct in a few minutes.
