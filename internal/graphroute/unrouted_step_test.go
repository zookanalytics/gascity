package graphroute

import (
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/formula"
)

// TestDecorateGraphWorkflowRecipe_NamelessPoolRouteIsRejected pins the fix for
// the silent-deadlock class reported in gc-rfxju.
//
// A graph.v2 pour whose default binding resolves pool-shaped but nameless
// (MetadataOnly with no QualifiedName) used to stamp every runnable step with
// the pool markers gc.continuation_group and gc.session_affinity while writing
// an EMPTY gc.routed_to. The empty value is dropped at persist time, so the
// step beads landed carrying every marker of pool routing and no route at all.
// Nothing errored: `gc sling` reported ok/routed=true, the workflow root was
// created and routed, and the steps were invisible to every demand and claim
// reader — so the pool never spawned and the workflow sat in_progress forever
// with no failure signal anywhere.
//
// A nameless pool route is undeliverable under every configuration, so the pour
// must fail loudly at sling time instead of materializing dead beads.
func TestDecorateGraphWorkflowRecipe_NamelessPoolRouteIsRejected(t *testing.T) {
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
	// {QualifiedName: "", MetadataOnly: true}: the exact broken binding.
	err := DecorateGraphWorkflowRecipe(r, nil, "src-1", "city", "test-city", "city:test", "", "", nil, "test-city", cfg, deps)
	if err == nil {
		t.Fatalf("DecorateGraphWorkflowRecipe returned nil error; want rejection of nameless pool route. work step metadata = %#v", r.Steps[1].Metadata)
	}
	if !strings.Contains(err.Error(), "wf-test.work") {
		t.Errorf("error = %q, want it to name the undeliverable step wf-test.work", err)
	}
}

// TestDecorateGraphWorkflowRecipe_EmptyDefaultBindingStillAllowed guards the
// legitimate no-default-binding pour: a drain-item recipe (see
// decorateDrainItemRecipe) passes a zero GraphRouteBinding on purpose, and its
// runnable steps carry their own gc.run_target. That binding is NOT pool-shaped
// (MetadataOnly is false), so the nameless-pool-route rejection must not fire.
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
