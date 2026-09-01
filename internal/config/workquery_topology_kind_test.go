package config

import (
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
