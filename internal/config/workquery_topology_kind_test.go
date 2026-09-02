package config

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
)

// A workflow-topology bead — a graph.v2 root, a scope latch, a step-spec
// sidecar — carries a route but no executable body, and the hook declines to
// serve one (cmd/gc's isWorkflowTopologyHookCandidate). bd has no flag that
// excludes it, and nothing runs after the reconciler's count-form to apply the
// rule in Go, so that form carries the exclusion in its own jq. This file pins
// it at the generator level and over a simulated bd state.

func TestPoolDemandCountShellExcludesWorkflowTopologyKinds(t *testing.T) {
	got := poolDemandCountShell("worker", QueryTopology{})
	if !strings.Contains(got, excludeWorkflowTopologyKindsJQClause()) {
		t.Fatalf("poolDemandCountShell() = %q, missing the topology exclusion clause", got)
	}
	if !strings.Contains(got, `.metadata["`+beadmeta.KindMetadataKey+`"]`) {
		t.Errorf("poolDemandCountShell() = %q, missing a %s reference", got, beadmeta.KindMetadataKey)
	}
	// Generated from the kind set itself, so a kind added to
	// beadmeta.WorkflowTopologyKinds cannot ship without the count-form
	// declining it — the same derivation the Go readers get from
	// beadmeta.IsWorkflowTopologyKind.
	for _, kind := range beadmeta.WorkflowTopologyKinds {
		if !strings.Contains(got, `. == "`+kind+`"`) {
			t.Errorf("poolDemandCountShell() = %q, missing topology kind %q", got, kind)
		}
	}
}

func TestEffectivePoolDemandQueryDoesNotCountRoutedTopologyBeads(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; count-form exercises a jq pipeline")
	}
	for _, kind := range beadmeta.WorkflowTopologyKinds {
		t.Run(kind, func(t *testing.T) {
			a := Agent{Name: "worker", Dir: "hello-world"}
			out := runShellWithFakeBd(t, a.EffectivePoolDemandQuery(), nil, `#!/bin/sh
set -eu
case "$*" in
  *"--metadata-field gc.routed_to=hello-world/worker"*)
    printf '[{"id":"topology","metadata":{"gc.routed_to":"hello-world/worker","gc.kind":"`+kind+`"}}]'
    ;;
  *)
    printf '[]'
    ;;
esac
`)
			if strings.TrimSpace(out) != "0" {
				t.Fatalf("EffectivePoolDemandQuery() count = %q for a routed %s bead, want 0", strings.TrimSpace(out), kind)
			}
		})
	}
}

// The exclusion is keyed on kind, not on root-ness: a vapor/root-only wisp root
// IS the work, and it is what wakes a scaled-to-zero pool. A bead carrying no
// gc.kind at all — legacy and hand-created work — counts for the same reason,
// matching the Go filter's fail-open.
func TestEffectivePoolDemandQueryStillCountsNonTopologyRows(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; count-form exercises a jq pipeline")
	}
	a := Agent{Name: "worker", Dir: "hello-world"}
	out := runShellWithFakeBd(t, a.EffectivePoolDemandQuery(), nil, `#!/bin/sh
set -eu
case "$*" in
  *"--metadata-field gc.routed_to=hello-world/worker"*)
    printf '[{"id":"wisp-root","metadata":{"gc.routed_to":"hello-world/worker","gc.kind":"wisp"}},{"id":"plain","metadata":{"gc.routed_to":"hello-world/worker"}},{"id":"root","metadata":{"gc.routed_to":"hello-world/worker","gc.kind":"workflow"}}]'
    ;;
  *)
    printf '[]'
    ;;
esac
`)
	if strings.TrimSpace(out) != "2" {
		t.Fatalf("EffectivePoolDemandQuery() count = %q, want 2 (wisp root and kindless bead count; the workflow root does not)", strings.TrimSpace(out))
	}
}

// routedTopologyPageState renders a routed ready tier holding count workflow
// roots ahead of one claimable row, oldest first — the ordering `--sort oldest`
// asks for, so the roots occupy the head of every page the reader returns.
func routedTopologyPageState(count int, claimable string) string {
	rows := make([]string, 0, count+1)
	for i := 0; i < count; i++ {
		rows = append(rows, fmt.Sprintf(`{"id":"root-%02d","metadata":{"gc.routed_to":"hello-world/worker","gc.kind":%q}}`, i, beadmeta.KindWorkflow))
	}
	return "[" + strings.Join(append(rows, claimable), ",") + "]"
}

// fakeBdHonoringRoutedLimit is a fake bd that PAGES the routed tier the way the
// real one does: it returns the first --limit rows of state and everything only
// when the caller asks for --limit 0. A fake that ignores the flag cannot see a
// windowing bug, because every window it is given holds every row.
func fakeBdHonoringRoutedLimit(state string) string {
	return `#!/bin/sh
set -eu
routed='` + state + `'
case "$*" in
  *"--metadata-field gc.routed_to=hello-world/worker"*)
    limit=$(printf '%s\n' "$*" | sed -n 's/.*--limit[= ]\([0-9][0-9]*\).*/\1/p')
    [ -n "$limit" ] || limit=0
    if [ "$limit" -gt 0 ]; then
      printf '%s' "$routed" | jq -c ".[:$limit]"
    else
      printf '%s' "$routed"
    fi
    ;;
  *)
    printf '[]'
    ;;
esac
`
}

// A routed queue whose oldest rows are all workflow topology must still serve
// the claimable row behind them. The worker tier drops topology AFTER its
// reader has chosen a page, so a page filled with roots left the hook with
// nothing to claim while the count form — which reads every row — kept
// reporting demand. That is the claim/count split, one page of roots wide
// instead of one root wide: every seat the reconciler spawns for that demand
// drains on the same page, for the life of the run.
func TestWorkQueryServesClaimableWorkBehindAPageOfWorkflowTopologyRows(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; the pool-demand tiers exercise a jq pipeline")
	}
	a := Agent{Name: "worker", Dir: "hello-world"}
	// One more root than the worker tier's window, so the claimable row cannot
	// sit inside the first page under any windowing order. The step carries no
	// gc.kind: the compiler stamps one only on roots and control beads, so an
	// ordinary frontier step is exactly the kindless row the exclusion passes.
	state := routedTopologyPageState(routedReadyTierWindow+1, `{"id":"gc-step","metadata":{"gc.routed_to":"hello-world/worker"}}`)
	bd := fakeBdHonoringRoutedLimit(state)

	out := runShellWithFakeBd(t, a.EffectiveWorkQuery(), map[string]string{"GC_SESSION_ORIGIN": "ephemeral"}, bd)
	if !strings.Contains(out, `"gc-step"`) {
		t.Fatalf("EffectiveWorkQuery() = %q, want the claimable step behind the page of workflow roots", out)
	}
	if strings.Contains(out, `"root-`) {
		t.Fatalf("EffectiveWorkQuery() = %q, want no workflow-topology row served", out)
	}

	// The two sides read the same demand shape: what the worker is served and
	// what the reconciler counts describe the same single row.
	count := runShellWithFakeBd(t, a.EffectivePoolDemandQuery(), nil, bd)
	if strings.TrimSpace(count) != "1" {
		t.Fatalf("EffectivePoolDemandQuery() count = %q, want 1 to match the one row the work query serves", strings.TrimSpace(count))
	}
}

// The migration tier reads workflow roots by construction, so every row it can
// return is one the hook refuses. It must therefore fall through to the tier
// behind it rather than short-circuiting on a page the worker cannot claim.
func TestWorkQueryFallsThroughAMigrationTierCarryingOnlyWorkflowRoots(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not available; the pool-demand tiers exercise a jq pipeline")
	}
	a := Agent{Name: "worker", Dir: "hello-world"}
	out := runShellWithFakeBd(t, a.EffectiveWorkQuery(), map[string]string{"GC_SESSION_ORIGIN": "ephemeral"}, `#!/bin/sh
set -eu
case "$*" in
  *"--metadata-field gc.run_target=hello-world/worker"*)
    printf '[{"id":"legacy-root","metadata":{"gc.run_target":"hello-world/worker","gc.kind":"workflow"}}]'
    ;;
  *"ephemeral=true AND status=open"*)
    printf '[{"id":"gc-wisp","metadata":{"gc.routed_to":"hello-world/worker","gc.kind":"wisp"}}]'
    ;;
  *)
    printf '[]'
    ;;
esac
`)
	if !strings.Contains(out, `"gc-wisp"`) {
		t.Fatalf("EffectiveWorkQuery() = %q, want the ephemeral wisp root behind the migration tier", out)
	}
	if strings.Contains(out, "legacy-root") {
		t.Fatalf("EffectiveWorkQuery() = %q, want no workflow-topology row served", out)
	}
}
