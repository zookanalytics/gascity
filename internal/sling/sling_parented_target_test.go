package sling

import (
	"context"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/formula"
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
// TestDecorateGraphWorkflowRecipe_NamelessPoolRouteLeavesStepUnrouted and
// TestEnsureGraphWorkflowHasClaimableStep). This test pins
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

// TestEnsureGraphWorkflowHasClaimableStep is the direct unit coverage for the
// gc-rfxju guard. The incident shape is the third case: the control step routed
// fine while every worker step came out with no route, so a naive "some step is
// routed" check would have passed while the target pool saw nothing.
func TestEnsureGraphWorkflowHasClaimableStep(t *testing.T) {
	step := func(id string, meta map[string]string) formula.RecipeStep {
		if meta == nil {
			meta = map[string]string{}
		}
		return formula.RecipeStep{ID: id, Metadata: meta}
	}
	root := formula.RecipeStep{ID: "wf.root", IsRoot: true, Metadata: map[string]string{"gc.kind": "workflow"}}

	tests := []struct {
		name    string
		steps   []formula.RecipeStep
		wantErr bool
	}{
		{
			name:  "routed worker step is claimable",
			steps: []formula.RecipeStep{root, step("wf.work", map[string]string{"gc.routed_to": "rig/pool"})},
		},
		{
			name:  "worker step bound to a concrete session is claimable",
			steps: []formula.RecipeStep{root, {ID: "wf.work", Assignee: "s-worker-1", Metadata: map[string]string{}}},
		},
		{
			name: "incident shape: control routed, every worker step unrouted",
			steps: []formula.RecipeStep{
				root,
				step("wf.load-context", nil),
				step("wf.implement", nil),
				step("wf.finalize", map[string]string{"gc.kind": "workflow-finalize", "gc.routed_to": "rig/core.control-dispatcher"}),
			},
			wantErr: true,
		},
		{
			name:  "control-only graph needs no pool demand",
			steps: []formula.RecipeStep{root, step("wf.finalize", map[string]string{"gc.kind": "workflow-finalize", "gc.routed_to": "rig/core.control-dispatcher"})},
		},
		{
			name:  "topology beads are never claimable and do not count",
			steps: []formula.RecipeStep{root, step("wf.scope", map[string]string{"gc.kind": "scope"})},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ensureGraphWorkflowHasClaimableStep(&formula.Recipe{Name: "wf", Steps: tt.steps}, "wf")
			if tt.wantErr && err == nil {
				t.Fatal("ensureGraphWorkflowHasClaimableStep = nil, want an error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ensureGraphWorkflowHasClaimableStep = %v, want nil", err)
			}
		})
	}
}
