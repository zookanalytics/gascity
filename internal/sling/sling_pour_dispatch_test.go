package sling

import (
	"context"
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	convoycore "github.com/gastownhall/gascity/internal/convoy"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/sourceworkflow"
)

func poolAgent() config.Agent {
	return config.Agent{Name: "worker", MaxActiveSessions: intPtr(3)}
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

// restampWorkBeadRouting owns the claim-route -> execution-route conversion for
// a single bead, so a workflow started from a source bead loses the claim route
// even when the target resolves to no routing identity.
func TestRestampWorkBeadRoutingRetiresClaimRoute(t *testing.T) {
	store := beads.NewMemStore()
	work, err := store.Create(beads.Bead{
		Title:    "work",
		Type:     "task",
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var result SlingResult
	restampWorkBeadRouting(SlingDeps{Store: store}, work.ID, config.Agent{}, &result)

	after, err := store.Get(work.ID)
	if err != nil {
		t.Fatalf("Get(work): %v", err)
	}
	if got := after.Metadata[beadmeta.RoutedToMetadataKey]; got != "" {
		t.Fatalf("gc.routed_to = %q, want cleared even with no routing identity", got)
	}
	if len(result.MetadataErrors) != 0 {
		t.Fatalf("MetadataErrors = %v, want none", result.MetadataErrors)
	}
}

// A second graph.v2 formula poured over a bead that already has a live
// convoy-first workflow used to mint a brand-new molecule and report a
// successful attach. The binding is reachable only through the input convoy —
// the bead carries no gc.molecule_id and the root no gc.source_bead_id — so the
// singleton check has to walk convoy membership to see it.
func TestAttachGraphFormulaRefusesSecondLiveInputConvoyWorkflow(t *testing.T) {
	formulaDir := t.TempDir()
	writeNamedGraphV2ConvoyFormula(t, formulaDir, "graph-a")
	writeNamedGraphV2ConvoyFormula(t, formulaDir, "graph-b")
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	work, err := deps.Store.Create(beads.Bead{Title: "work", Type: "task"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.AttachFormula(context.Background(), "graph-a", work.ID, poolAgent(), FormulaOpts{})
	if err != nil {
		t.Fatalf("first AttachFormula: %v", err)
	}

	_, err = s.AttachFormula(context.Background(), "graph-b", work.ID, poolAgent(), FormulaOpts{})
	if err == nil {
		t.Fatal("second AttachFormula error = nil, want conflict with the live workflow")
	}
	var conflictErr *sourceworkflow.ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("second AttachFormula error = %v, want *sourceworkflow.ConflictError", err)
	}
	if len(conflictErr.WorkflowIDs) != 1 || conflictErr.WorkflowIDs[0] != first.WorkflowID {
		t.Fatalf("conflict workflow ids = %v, want [%s]", conflictErr.WorkflowIDs, first.WorkflowID)
	}
}

// --force stays the operator's escape hatch: it launches the second workflow
// rather than refusing, matching every other bead-state guard on this path.
func TestAttachGraphFormulaForceOverridesLiveInputConvoyConflict(t *testing.T) {
	formulaDir := t.TempDir()
	writeNamedGraphV2ConvoyFormula(t, formulaDir, "graph-a")
	writeNamedGraphV2ConvoyFormula(t, formulaDir, "graph-b")
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	work, err := deps.Store.Create(beads.Bead{Title: "work", Type: "task"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.AttachFormula(context.Background(), "graph-a", work.ID, poolAgent(), FormulaOpts{})
	if err != nil {
		t.Fatalf("first AttachFormula: %v", err)
	}
	second, err := s.AttachFormula(context.Background(), "graph-b", work.ID, poolAgent(), FormulaOpts{Force: true})
	if err != nil {
		t.Fatalf("forced AttachFormula: %v", err)
	}
	if second.WorkflowID == "" || second.WorkflowID == first.WorkflowID {
		t.Fatalf("forced WorkflowID = %q, want a fresh root different from %s", second.WorkflowID, first.WorkflowID)
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
