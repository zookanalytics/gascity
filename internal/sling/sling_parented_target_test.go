package sling

import (
	"context"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/graphroute"
	"github.com/gastownhall/gascity/internal/runtime"
)

// TestAttachGraphFormulaOnParentedBeadRoutesEveryStep is the regression the
// gc-rfxju report asked for: slinging a bead that HAS a parent must produce
// demand the target pool can see.
//
// The report identified the parent as the discriminator, because the pours that
// deadlocked all had one and the pours that worked did not. The parent turned
// out to be a coincidence — the real cause was a nameless pool binding stamping
// steps with the pool markers and an empty gc.routed_to (see
// TestDecorateGraphWorkflowRecipe_NamelessPoolRouteIsRejected). This test pins
// the coincidence down so it stays one: a parented target routes exactly like a
// parentless one, and every runnable step lands claimable.
func TestAttachGraphFormulaOnParentedBeadRoutesEveryStep(t *testing.T) {
	formulaDir := t.TempDir()
	writeGraphV2ConvoyFormula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	pool := config.Agent{
		Name:              "polecat",
		MinActiveSessions: intPtr(0),
		MaxActiveSessions: intPtr(4),
	}
	cfg.Agents = append(cfg.Agents, pool)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	parent, err := deps.Store.Create(beads.Bead{Title: "epic", Type: "epic", Status: "open"})
	if err != nil {
		t.Fatal(err)
	}
	source, err := deps.Store.Create(beads.Bead{Title: "work", Type: "task", Status: "open", ParentID: parent.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := deps.Store.Get(source.ID); err != nil {
		t.Fatal(err)
	} else if got.ParentID != parent.ID {
		t.Fatalf("source ParentID = %q, want %q — the parented-target precondition did not hold", got.ParentID, parent.ID)
	}

	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.AttachFormula(context.Background(), "graph-work", source.ID, pool, FormulaOpts{})
	if err != nil {
		t.Fatalf("AttachFormula on parented bead: %v", err)
	}

	root, err := deps.Store.Get(result.WorkflowID)
	if err != nil {
		t.Fatalf("Get(root): %v", err)
	}
	if got := root.Metadata["gc.routed_to"]; got == "" {
		t.Errorf("root gc.routed_to is empty; root metadata = %#v", root.Metadata)
	}

	steps, err := deps.Store.ListByMetadata(map[string]string{"gc.root_bead_id": root.ID}, 0)
	if err != nil {
		t.Fatalf("ListByMetadata(gc.root_bead_id=%s): %v", root.ID, err)
	}
	if len(steps) == 0 {
		t.Fatalf("workflow root %s materialized no step beads", root.ID)
	}

	runnable := 0
	for _, step := range steps {
		// Topology beads (workflow root, scope latch, formula spec) are
		// deliberately unrouted — they are graph bookkeeping nobody claims.
		if graphroute.IsWorkflowTopologyKind(step.Metadata["gc.kind"]) {
			continue
		}
		runnable++
		if got := step.Metadata["gc.routed_to"]; got == "" {
			t.Errorf("step %s (%s) has no gc.routed_to; metadata = %#v", step.ID, step.Metadata["gc.step_ref"], step.Metadata)
		}
	}
	if runnable == 0 {
		t.Fatalf("no runnable steps inspected among %d beads under root %s — the assertion above proved nothing", len(steps), root.ID)
	}
}
