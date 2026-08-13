package graphroute

import (
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/formula"
)

// TestDecorateGraphWorkflowRecipe_NamelessPoolRouteLeavesStepUnrouted pins what
// decoration does with the binding at the heart of gc-rfxju: pool-shaped but
// nameless (MetadataOnly with no QualifiedName).
//
// Decoration must NOT reject it. `gc formula cook` passes exactly this binding
// on purpose so a cooked DAG stays unrouted until something dispatches it later
// (decorateFormulaCookGraphV2Recipe). What decoration must do is leave the step
// honestly unrouted rather than dressed up as claimable pool work: before the
// fix it stamped gc.continuation_group and gc.session_affinity alongside an
// EMPTY gc.routed_to, and the empty value is dropped at persist time, so the
// bead landed carrying every marker of pool routing and no route at all.
//
// The judgement that an unrouted worker step is fatal belongs to a pour that is
// itself the dispatch; sling.ensureGraphWorkflowHasClaimableStep makes it.
func TestDecorateGraphWorkflowRecipe_NamelessPoolRouteLeavesStepUnrouted(t *testing.T) {
	cfg := &config.City{Agents: []config.Agent{
		{Name: "control-dispatcher", MaxActiveSessions: intPtr(1)},
	}}
	r := &formula.Recipe{
		Name: "wf-test",
		Steps: []formula.RecipeStep{
			{ID: "wf-test.root", IsRoot: true, Metadata: map[string]string{
				"gc.kind": "workflow", "gc.formula_contract": "graph.v2",
			}},
			{ID: "wf-test.work", Metadata: map[string]string{}},
		},
	}
	deps := Deps{Resolver: testAgentResolver{}}

	// routedTo and sessionName both empty -> defaultRoute is
	// {QualifiedName: "", MetadataOnly: true}: the cook-shaped binding.
	if err := DecorateGraphWorkflowRecipe(r, nil, "src-1", "city", "test-city", "city:test", "", "", nil, "test-city", cfg, deps); err != nil {
		t.Fatalf("DecorateGraphWorkflowRecipe: %v", err)
	}
	work := r.Steps[1]
	if got, ok := work.Metadata["gc.routed_to"]; ok {
		t.Errorf("work gc.routed_to = %q, want the key absent rather than an empty route", got)
	}
	if got := work.Metadata["gc.continuation_group"]; got != "" {
		t.Errorf("work gc.continuation_group = %q, want empty: an unrouted step is not claimable pool work", got)
	}
	if got := work.Metadata["gc.session_affinity"]; got != "" {
		t.Errorf("work gc.session_affinity = %q, want empty: an unrouted step is not claimable pool work", got)
	}
}

// TestDecorateGraphWorkflowRecipe_EmptyDefaultBindingStillAllowed guards the
// other legitimate no-default-binding caller: a drain-item recipe (see
// decorateDrainItemRecipe) passes a zero GraphRouteBinding on purpose, and its
// runnable steps carry their own gc.run_target. The nameless default must not
// clobber those per-step targets.
func TestDecorateGraphWorkflowRecipe_EmptyDefaultBindingStillAllowed(t *testing.T) {
	cfg := &config.City{Agents: []config.Agent{
		{Name: "worker", Dir: "frontend", MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(1)},
		{Name: "control-dispatcher", MaxActiveSessions: intPtr(1)},
	}}
	r := &formula.Recipe{
		Name: "wf-drain",
		Steps: []formula.RecipeStep{
			{ID: "wf-drain.root", IsRoot: true, Metadata: map[string]string{
				"gc.kind": "workflow", "gc.formula_contract": "graph.v2",
			}},
			{ID: "wf-drain.work", Metadata: map[string]string{"gc.run_target": "worker"}},
		},
	}
	deps := Deps{Resolver: testAgentResolver{}}

	if err := DecorateGraphWorkflowRecipeWithDefaultBinding(r, nil, "", "", "", "", GraphRouteBinding{}, nil, "test-city", cfg, deps); err != nil {
		t.Fatalf("DecorateGraphWorkflowRecipeWithDefaultBinding: %v", err)
	}
	if got := r.Steps[1].Metadata["gc.routed_to"]; got != "frontend/worker" {
		t.Errorf("work gc.routed_to = %q, want frontend/worker", got)
	}
}

// TestApplyGraphRouteBinding_NamelessPoolRouteStampsNothing is the defensive
// half of the fix. ApplyGraphRouteBinding is also reached directly from the
// CLI's own decorator (cmd_convoy_dispatch.go), which builds bindings locally,
// so the stamp site must not persist a route-shaped lie on its own.
//
// ApplyGraphControlRouteBinding already deletes gc.routed_to rather than
// writing an empty string; the execution path did not, and additionally stamped
// the pool markers. A nameless binding must leave the step visibly unrouted
// instead of looking like claimable pool work.
func TestApplyGraphRouteBinding_NamelessPoolRouteStampsNothing(t *testing.T) {
	step := &formula.RecipeStep{
		Metadata: map[string]string{"gc.routed_to": "stale/route"},
	}

	ApplyGraphRouteBinding(step, GraphRouteBinding{MetadataOnly: true})

	if got, ok := step.Metadata["gc.routed_to"]; ok {
		t.Errorf("gc.routed_to = %q, want the key absent for a nameless binding", got)
	}
	if got := step.Metadata["gc.continuation_group"]; got != "" {
		t.Errorf("gc.continuation_group = %q, want empty: an unrouted step is not pool work", got)
	}
	if got := step.Metadata["gc.session_affinity"]; got != "" {
		t.Errorf("gc.session_affinity = %q, want empty: an unrouted step is not pool work", got)
	}
	if step.Assignee != "" {
		t.Errorf("Assignee = %q, want empty", step.Assignee)
	}
}
