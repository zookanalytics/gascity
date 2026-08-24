# Beads Version Bump Anchors

Bumping the `github.com/steveyegge/beads` pin is a coordinated multi-file
edit, not a `go get`. The version appears in go.mod, in `deps.env`, in
Dockerfiles, in a dozen workflow assignments, in checksum pins, and in two
guards that fail only *after* the obvious edits look complete. Miss one and
the board goes red in a place that does not name beads.

This page is the anchor list. It exists because the list has twice been lost:
first to a session scratchpad under `/tmp` (reaped), then to the notes of a
closed bead (`gc-lbfmj`). Add anchors here as they are discovered.

## Anchors

### A — library / current cell

| File | Field |
|---|---|
| `deps.env` | `BD_CURRENT_VERSION` (pseudo-version) |
| `deps.env` | `BD_CURRENT_REF` (full 40-char commit SHA) |
| `go.mod` + `go.sum` | the `require` entry |
| `contrib/k8s/Dockerfile.agent` | `ARG BD_SOURCE_REF` (full SHA) |
| `contrib/k8s/Dockerfile.agent` | `ARG BD_BUILD` (`BD_SOURCE_REF[:10]`) |

### B — installable default

| File | Field |
|---|---|
| `deps.env` | `BD_VERSION` |
| `contrib/k8s/Dockerfile.agent` | `ARG BD_VERSION` |
| `.github/scripts/install-bd-archive.sh` | per-platform archive SHA pins |
| `.github/workflows/*` | every `BD_VERSION:` assignment |
| `.devcontainer/README.md` | the quoted current version |

`BD_PREV_VERSION` is the compatibility-matrix floor and does **not** move
with a bump. Neither do `bdMinVersion`, `bdReadyProjectionMinVersion`, or
the `bd_compatibility` enum — floors are separate decisions.

## The traps

These are the anchors that are not covered by `TestBDVersionPins`, which
compares version *strings* only. Each fails somewhere that does not mention
beads.

### 1. Archive checksum — `ARG BD_SOURCE_SHA256`

`contrib/k8s/Dockerfile.agent` verifies the source archive with
`sha256sum --check --strict`. The pin test never looks at it, so a stale
checksum passes every Go test and fails the image build. Recompute it
against the archive at the new ref.

### 2. gRPC version — `ARG GRPC_VERSION`

The Dockerfile pins a gRPC version with a `go get` and then asserts the
result equals that pin. When beads at the new ref already ships a *newer*
gRPC, leaving the old pin makes the `go get` a **downgrade** — inverting the
block's purpose — and the assertion still passes, because it checks for
exactly the pinned version. The build goes green while shipping the older,
possibly vulnerable, library.

Set `GRPC_VERSION` to the version beads already ships. Do not delete the
block: the `go get` becomes a no-op and the assertion keeps guarding future
regressions.

### 3. Native dependency module ceiling — `max_modules`

`scripts/check-native-dependency-surface.sh` caps the total size of the
module graph at an exact count. A beads bump routinely grows that graph, and
the guard is not part of the unit-test suite — it fails in CI under
**Preflight / static checks**, with a message that names no dependency:

    native dependency guard: module graph has 740 modules; max is 727

Raise `max_modules` to the new exact count **in the bump commit**. The
precedent is upstream's own bumps: v1.0.4 → v1.1.0 raised the ceiling
725 → 727 in the same commit.

Before raising it, confirm the growth is what you think it is. The ceiling
guards *native dependency surface*, so the question is whether the new
modules are real surface or graph noise:

```bash
# Which module paths are new, against the pre-bump commit
git worktree add /var/tmp/pre --detach <bump-commit>^
(cd /var/tmp/pre && go list -m all) | awk '{print $1}' | LC_ALL=C sort -u > /var/tmp/pre.paths
go list -m all | awk '{print $1}' | LC_ALL=C sort -u > /var/tmp/post.paths
LC_ALL=C comm -13 /var/tmp/pre.paths /var/tmp/post.paths

# For each new path: does anything we build actually import it?
go mod why -m <path>                      # "main module does not need" => graph-only
go list -deps ./cmd/gc | grep -c '^<path>' # 0 => links nothing into gc
```

A module that is absent from our `go.mod`, reports *"main module does not
need module X"*, and has zero reach in `./cmd/gc` adds no native dependency
surface. It is in the graph only because Go's module-graph pruning admits
every requirement declared in a directly-required module's own `go.mod` —
including that dependency's build tooling. We have no lever to remove those
short of `exclude` directives that fight upstream; raising the ceiling is
correct, and the binary-size cap remains the check that measures real
growth.

If instead a new module *is* reachable, treat it as genuine growth and
decide whether to accept or avoid it before touching the number.

### 4. Native dependency binary ceiling — `max_binary_bytes`

The same script also caps the size of the built `gc` binary, and a beads
bump grows that too — the v1.2.1 bump cost about 2.5 MB.

**This one hides behind the module check.** The script is a straight-line
`set -e` sequence: the module comparison runs first and the binary is not
even built until it passes. So a bump that breaches both ceilings reports
only the module one, and raising `max_modules` alone turns a red board into
a *differently* red board. After changing `max_modules`, always run the
guard to completion before believing it is fixed.

Unlike the module count, binary size is **not** reproducible across
environments — the same commit measured 267,809,008 bytes on CI (go 1.26.5)
and 268,260,480 locally (go 1.26.6), a 0.17% spread. Pinning it to the exact
current size would go red on a Go patch bump alone, so this ceiling is
deliberately a round number with a few percent of headroom, and it is the
one number here you should *not* ratchet tight.

Watch the headroom rather than just the pass/fail: the cap sat at
270,000,000 from 2026-05-31, when the binary was 236,197,920 (14% headroom),
and by 2026-08-14 the binary had reached 267,809,008 (0.8%) without anyone
noticing. A guard that is one dependency bump from tripping is not
protecting anything. If a bump leaves under a couple of percent of room,
raise the ceiling *and* say so in the PR, so the growth trend gets looked at
rather than absorbed a second time.

## After the bump

Land the bump as its own commit with its own full gate run, including
`make check-native-dependency-surface`.
