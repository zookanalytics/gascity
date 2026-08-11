---
title: "Templated Overlay Staging"
---

| Field | Value |
|---|---|
| Status | Implemented |
| Date | 2026-08-11 |
| Author(s) | Claude |
| Issue | gc-rpktx (folded from gc-tn0i) |
| Supersedes | N/A |

## Summary

Pack overlay files are staged into an agent's working directory by a
byte-copy / JSON-merge walk that has no knowledge of the installation it is
staging into. That is fine for content a pack author can write down in full,
and wrong for content that only becomes correct once the install is known —
above all the city root.

This design adds one seam to the overlay staging walk: a pack file named
`<name>.template.<ext>` is rendered through `text/template` against a data map
and lands at `<name>.<ext>`. The convention, the data map, and the
`missingkey=error` policy are the ones the MCP catalog loader already uses for
`<name>.template.toml`, so packs gain a second templated file class rather than
a second templating system.

## Problem

Per-provider overlay staging copies a pack's file verbatim. A pack that needs
to ship a file carrying an install-specific value has no way to express it, so
the value is either absent or wrong at the moment the file lands. The concrete
case is a Codex hooks document whose managed commands must be bound to a
specific city (`gc --city <root> prime --hook`): staging writes it unbound, and
something downstream has to notice and rewrite the file.

Rewriting after the fact is a poor shape for three reasons:

- It is a second writer for a file the staging walk already owns, so the
  correct content exists only after two passes agree.
- The window between the two passes is real. The rewrite runs inside the
  reconciler's desired-state build, so a freshly staged workdir holds an
  unbound file until the next patrol tick.
- It is per-file-format work. Each provider that needs an install-specific
  value needs its own post-hoc normalizer carrying its own vocabulary.

The underlying gap is not Codex-specific and not hooks-specific: staging has
no way to bind anything.

## Design

### The convention

A pack overlay file whose name is `<name>.template.<ext>` is templated. It is
rendered during staging and written to `<name>.<ext>`; the `.template` segment
never reaches the destination.

This is deliberately the rule `materialize.MCPIdentityForFilename` already
applies to `<name>.template.toml`. A pack author learns one naming rule for
every templated pack file class.

`overlay.TemplateTargetName` is the single decision point. Everything that
keys on the *destination* — JSON merge eligibility, Claude hook wrapping,
provider preserve-existing — consults the resolved target name, so
`.codex/hooks.template.json` merges under exactly the rules
`.codex/hooks.json` merges under. Without that, a templated settings file
would silently lose the identity-keyed merge its non-templated twin gets and
append duplicate hook entries instead of replacing them.

### The data map

`materialize.PackTemplateData` builds the expansion surface, and both
templated file classes share it: MCP catalog entries and overlay files. It
already carried `CityRoot` alongside `RigRoot`, `WorkDir`, `AgentName`, the
effective bead queries, and the agent env — it was named `MCPTemplateData`
only because MCP was its sole consumer.

Rendering uses `missingkey=error`, matching the MCP path. An unresolvable
token fails staging with an error naming the template rather than installing a
half-bound file. This is the property that makes the seam safe to add to a
walk whose per-provider half is historically best-effort: a render failure
writes nothing, reports on stderr, and `runtime.StageProviderOverlayDir`
promotes any non-preservation stderr line to a hard error.

### Threading

The renderer is small; reaching it is the work. `StageProviderOverlayDir` took
no installation context at all, so the data map is threaded to it from every
staging path:

| Path | Carrier |
|---|---|
| `runtime.StageSessionWorkDirWithWarnings` (subprocess, ACP) | `runtime.Config.OverlayTemplateData` |
| `runtime/tmux.stageStartFiles` | `runtime.Config.OverlayTemplateData` |
| `runtime/k8s.stageProviderOverlays` | `runtime.Config.OverlayTemplateData` |
| `cmd/gc` reconciler pre-fingerprint materialization | built at the call site from `agentBuildParams` |
| `cmd/gc` worker create / resume | `applyWorkerOverlayHints` |
| `cmd/gc init` city-root universal staging | `CityRoot` only — no agent exists yet |

`runtime.Config` gains `OverlayTemplateData`, populated through the single
`agent.StartupHints.ToRuntimeConfig` mapping, so every consumer that builds a
Config from hints gets it without per-call-site threading.

The map is an **explicit parameter** of `StageProviderOverlayDir` rather than a
functional option. An option would let a new staging path reach the seam
without deciding what an installed pack file binds to; a parameter makes the
compiler ask.

### Fingerprint classification

`OverlayTemplateData` is excluded from every fingerprint half. The map mirrors
the agent env, and the env contributes to the fingerprint through an
allow-list precisely so that service-discovery churn does not read as config
drift; hashing the whole map would route ~50 `GC_*` vars straight back in. The
identity the map derives from — city, rig, workdir, agent — is already covered
by fields that do hash.

## Alternatives considered

**Add a city field to `runtime.Config` and call the existing normalizer from
the remaining staging paths.** Needs the same threading this design needs, and
spends it on a Codex-specific post-hoc rewrite instead of a generic seam. It
also leaves the next provider that needs an install-specific value with
nothing.

**Contribute the normalizer upstream.** Asks upstream to adopt a
"re-write the file staging just wrote" pass, carries Codex hook vocabulary
into core, and leaves the underlying gap open.

**Accept raw staged files.** The existing normalizer does three jobs —
city-binding, wrapping prompt hooks in `gc hook run`, and deduping managed
`SessionStart` entries. Dropping it wholesale loses the other two.

## Boundaries

- **`CopyFiles` staging is deliberately untouched.** The seam lives in the
  overlay directory walk. `CopyFiles` entries name an exact source and
  destination, so silently renaming a file the operator spelled out would be
  the wrong behavior there; a templated file listed in `copy_files` is copied
  verbatim, as before.

- **The exec provider's script-side staging is out of scope.** `startConfig`
  hands `pack_overlay_dirs` to a user-supplied script, which does its own
  copying and does not consult this seam. A pack that ships templated files and
  targets a script-staged exec provider would stage them unrendered. Closing
  that means either rendering host-side before handoff or publishing the
  convention as part of the exec script contract.
- **The data map is built eagerly, and it is not free.**
  `PackTemplateData` resolves `Branch`/`DefaultBranch` by shelling out to git
  (1-3 subprocesses per call). The reconciler already paid this once per agent
  per desired-state build via the MCP path; building the map for overlay
  staging takes it to three — one more in `resolveTemplate`, one in
  `prepareTemplateResolution`. On a 30s patrol that is a few milliseconds per
  agent per tick, against a path that already walks and rewrites whole overlay
  trees per agent per tick, so it was not worth threading a precomputed map
  through three signatures to avoid. If it ever shows up in a profile, the fix
  is local: hoist one map per `resolveTemplate` call, or memoize the branch
  lookup per workdir.

- **Assets are unchanged.** No pack file in this repo is converted to
  `.template.<ext>` here. `internal/hooks.NormalizeManagedCodexHooks` reads the
  embedded Codex hooks asset by its current path and compares against it, so
  converting the asset is coupled to shedding the normalizer. Both belong in
  the follow-up, which keeps this change a clean cherry-pick.

## Follow-ups

1. Ship `internal/bootstrap/packs/core/overlay/per-provider/codex/.codex/hooks.template.json`
   with a `{{.CityRoot}}`-bound managed command, and shed
   `NormalizeManagedCodexHooks` along with its reconciler call site — its
   city-binding job moves into staging, and its wrapping/dedup jobs move into
   the asset itself.
2. Decide the exec-provider boundary above.
