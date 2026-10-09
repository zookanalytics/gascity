package sling

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	convoycore "github.com/gastownhall/gascity/internal/convoy"
	"github.com/gastownhall/gascity/internal/runtime"
)

func poolAgent() config.Agent {
	return config.Agent{Name: "worker", MaxActiveSessions: intPtr(3)}
}

// writeRetainingGraphV2Formula writes graph-reaction, a graph.v2 formula that
// reacts to its input convoy without driving it and so declares
// retain_input_routes.
func writeRetainingGraphV2Formula(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "graph-reaction.toml"), []byte(`
formula = "graph-reaction"
contract = "graph.v2"
retain_input_routes = true

[[steps]]
id = "react"
title = "React to {{convoy_id}}"
`), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A bead an earlier plain sling routed to a pool keeps gc.routed_to when a
// graph.v2 workflow is later poured over the convoy tracking it. Both surfaces
// then dispatch the same work: the pool claims the bead directly while the
// workflow's own steps run, so two workers land on one branch. Pouring must
// retire the direct route so the workflow is the only live dispatch surface.
func TestGraphWorkflowPourRetiresConvoyMemberPoolRoute(t *testing.T) {
	formulaDir := t.TempDir()
	writeGraphV2ConvoyFormula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	convoy, err := deps.Store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	if err != nil {
		t.Fatal(err)
	}
	work, err := deps.Store.Create(beads.Bead{
		Title:    "work",
		Type:     "task",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := convoycore.TrackItem(deps.Store, convoy.ID, work.ID); err != nil {
		t.Fatal(err)
	}

	result, err := DoSlingBatch(SlingOpts{
		Target:        poolAgent(),
		BeadOrFormula: convoy.ID,
		OnFormula:     "graph-work",
	}, deps, deps.Store)
	if err != nil {
		t.Fatalf("DoSlingBatch: %v", err)
	}
	if result.WorkflowID == "" {
		t.Fatalf("result = %+v, want a graph workflow launch", result)
	}

	after, err := deps.Store.Get(work.ID)
	if err != nil {
		t.Fatalf("Get(work): %v", err)
	}
	if got := after.Metadata[beadmeta.RoutedToMetadataKey]; got != "" {
		t.Fatalf("member %s still routed to %q after pour; the pool and the workflow would both dispatch it", work.ID, got)
	}
}

// The bare-bead attach path reaches the same invariant through the synthetic
// input convoy the pour mints: the work bead loses its claim route and keeps
// only the execution-semantics record.
func TestGraphWorkflowPourRetiresAttachedBeadPoolRoute(t *testing.T) {
	formulaDir := t.TempDir()
	writeGraphV2ConvoyFormula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	work, err := deps.Store.Create(beads.Bead{
		Title:    "work",
		Type:     "task",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AttachFormula(context.Background(), "graph-work", work.ID, poolAgent(), FormulaOpts{}); err != nil {
		t.Fatalf("AttachFormula: %v", err)
	}

	after, err := deps.Store.Get(work.ID)
	if err != nil {
		t.Fatalf("Get(work): %v", err)
	}
	if got := after.Metadata[beadmeta.RoutedToMetadataKey]; got != "" {
		t.Fatalf("attached bead still routed to %q after pour", got)
	}
	if got := after.Metadata[beadmeta.ExecutionRoutedToMetadataKey]; got != "worker" {
		t.Fatalf("gc.execution_routed_to = %q, want %q", got, "worker")
	}
}

// A workflow whose formula declares retain_input_routes reacts to its input
// without driving it, so it is not a second dispatch surface for that work.
// Starting it must leave every convoy member's pool route as it was: nothing
// re-routes a bead whose route the reaction retired, so the retire would strand
// it.
func TestGraphWorkflowPourKeepsConvoyMemberPoolRoutesForRetainingFormula(t *testing.T) {
	formulaDir := t.TempDir()
	writeRetainingGraphV2Formula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	convoy, err := deps.Store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]string{}
	for _, route := range []string{"worker", "other-pool"} {
		member, err := deps.Store.Create(beads.Bead{
			Title:    "work for " + route,
			Type:     "task",
			Metadata: map[string]string{beadmeta.RoutedToMetadataKey: route},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := convoycore.TrackItem(deps.Store, convoy.ID, member.ID); err != nil {
			t.Fatal(err)
		}
		routes[member.ID] = route
	}

	result, err := DoSlingBatch(SlingOpts{
		Target:        poolAgent(),
		BeadOrFormula: convoy.ID,
		OnFormula:     "graph-reaction",
	}, deps, deps.Store)
	if err != nil {
		t.Fatalf("DoSlingBatch: %v", err)
	}
	if result.WorkflowID == "" {
		t.Fatalf("result = %+v, want a graph workflow launch", result)
	}
	if len(result.MetadataErrors) != 0 {
		t.Fatalf("MetadataErrors = %v, want none", result.MetadataErrors)
	}

	for id, want := range routes {
		after, err := deps.Store.Get(id)
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if got := after.Metadata[beadmeta.RoutedToMetadataKey]; got != want {
			t.Errorf("member %s routed to %q after a retaining pour, want %q kept", id, got, want)
		}
	}
}

// The bare-bead attach path keeps the route as well. It reaches the bead twice,
// once as the single member of the synthetic input convoy and once through the
// attach restamp, and neither may retire it. The execution route is still
// recorded.
func TestGraphWorkflowPourKeepsAttachedBeadPoolRouteForRetainingFormula(t *testing.T) {
	formulaDir := t.TempDir()
	writeRetainingGraphV2Formula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	work, err := deps.Store.Create(beads.Bead{
		Title:    "work",
		Type:     "task",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.AttachFormula(context.Background(), "graph-reaction", work.ID, poolAgent(), FormulaOpts{})
	if err != nil {
		t.Fatalf("AttachFormula: %v", err)
	}
	if result.WorkflowID == "" {
		t.Fatalf("result = %+v, want a graph workflow launch", result)
	}

	after, err := deps.Store.Get(work.ID)
	if err != nil {
		t.Fatalf("Get(work): %v", err)
	}
	if got := after.Metadata[beadmeta.RoutedToMetadataKey]; got != "worker" {
		t.Fatalf("attached bead routed to %q after a retaining pour, want %q kept", got, "worker")
	}
	if got := after.Metadata[beadmeta.ExecutionRoutedToMetadataKey]; got != "worker" {
		t.Fatalf("gc.execution_routed_to = %q, want %q", got, "worker")
	}
}

// A workflow started from a source bead retires that bead's claim route unless
// its root was compiled from a formula declaring retain_input_routes. The retire
// does not depend on the target: a target that resolves to no routing identity
// still retires the route, since the hazard is the stale route rather than the
// execution record.
func TestDoStartGraphWorkflowSourceBeadRouteFollowsRetainInputRoutes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		agent     config.Agent
		retain    bool
		wantRoute string
		wantExec  string
	}{
		{name: "driving workflow retires the route", agent: poolAgent(), wantRoute: "", wantExec: "worker"},
		{name: "driving workflow retires the route without a routing identity", agent: config.Agent{}, wantRoute: "", wantExec: ""},
		{name: "retaining workflow keeps the route", agent: poolAgent(), retain: true, wantRoute: "worker", wantExec: "worker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps(&config.City{Workspace: config.Workspace{Name: "test"}}, runtime.NewFake(), newFakeRunner().run)
			rootMeta := map[string]string{
				beadmeta.KindMetadataKey:            beadmeta.KindWorkflow,
				beadmeta.FormulaContractMetadataKey: beadmeta.FormulaContractGraphV2,
			}
			if tc.retain {
				rootMeta[beadmeta.RetainInputRoutesMetadataKey] = "true"
			}
			root, err := deps.Store.Create(beads.Bead{Title: "workflow root", Type: "task", Metadata: rootMeta})
			if err != nil {
				t.Fatal(err)
			}
			source, err := deps.Store.Create(beads.Bead{
				Title:    "source",
				Type:     "task",
				Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
			})
			if err != nil {
				t.Fatal(err)
			}

			result, err := doStartGraphWorkflow(root.ID, source.ID, source.ID, "", tc.agent, "formula", deps)
			if err != nil {
				t.Fatalf("doStartGraphWorkflow: %v", err)
			}
			if len(result.MetadataErrors) != 0 {
				t.Fatalf("MetadataErrors = %v, want none", result.MetadataErrors)
			}

			after, err := deps.Store.Get(source.ID)
			if err != nil {
				t.Fatalf("Get(source): %v", err)
			}
			if got := after.Metadata[beadmeta.RoutedToMetadataKey]; got != tc.wantRoute {
				t.Fatalf("source gc.routed_to = %q, want %q", got, tc.wantRoute)
			}
			if got := after.Metadata[beadmeta.ExecutionRoutedToMetadataKey]; got != tc.wantExec {
				t.Fatalf("source gc.execution_routed_to = %q, want %q", got, tc.wantExec)
			}
		})
	}
}

// A root that cannot be read leaves the retire decision unknown. The attached
// beads still take the default retire, which keeps a driving workflow the only
// dispatch surface for them, and the read failure is reported, not swallowed.
func TestRetireInputClaimRoutesRetiresAttachedBeadsWhenRootUnreadable(t *testing.T) {
	work := beads.NewMemStore()
	attached, err := work.Create(beads.Bead{
		Title:    "work",
		Type:     "task",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	deps := SlingDeps{
		Store:      work,
		GraphStore: &getErrStore{Store: beads.NewMemStore(), err: errors.New("graph store unavailable")},
	}

	var result SlingResult
	retireInputClaimRoutes(deps, "gc-root", []string{attached.ID}, &result)

	after, err := work.Get(attached.ID)
	if err != nil {
		t.Fatalf("Get(attached): %v", err)
	}
	if got := after.Metadata[beadmeta.RoutedToMetadataKey]; got != "" {
		t.Fatalf("attached bead routed to %q with an unreadable root, want the default retire", got)
	}
	if len(result.MetadataErrors) != 1 || !strings.Contains(result.MetadataErrors[0], "gc-root") {
		t.Fatalf("MetadataErrors = %v, want one error naming the unreadable root", result.MetadataErrors)
	}
}

// Re-pouring the SAME formula with the same vars must stay idempotent rather
// than conflicting with the root it would reuse: the pour's own root key is
// excluded from the singleton scan.
func TestAttachGraphFormulaRepourIsNotSelfConflict(t *testing.T) {
	formulaDir := t.TempDir()
	writeGraphV2ConvoyFormula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	convoy, err := deps.Store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	if err != nil {
		t.Fatal(err)
	}
	work, err := deps.Store.Create(beads.Bead{Title: "work", Type: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if err := convoycore.TrackItem(deps.Store, convoy.ID, work.ID); err != nil {
		t.Fatal(err)
	}
	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.AttachFormula(context.Background(), "graph-work", convoy.ID, poolAgent(), FormulaOpts{})
	if err != nil {
		t.Fatalf("first AttachFormula: %v", err)
	}
	second, err := s.AttachFormula(context.Background(), "graph-work", convoy.ID, poolAgent(), FormulaOpts{})
	if err != nil {
		t.Fatalf("re-pour AttachFormula: %v", err)
	}
	if second.WorkflowID != first.WorkflowID {
		t.Fatalf("re-pour WorkflowID = %q, want the existing root %s", second.WorkflowID, first.WorkflowID)
	}
}
