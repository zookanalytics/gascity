package config

import (
	"slices"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
)

// This file expresses the route-scoped serve rules at the shell-generator
// level: route-scoped, unassigned pool-demand queries (Tier 3, its migration
// fallback, the graph-anchor probe and the reconciler's count-form) drop beads
// carrying an active hold, and bd's native human label by its exact flag. The
// generated assignee-scoped tiers serve work as well, so they drop held rows
// too but never the human label; the assigned-ready tier's behavior is pinned
// in workquery_assigned_ready_hold_test.go and the hold predicate's in
// workquery_active_hold_test.go.

// routedReadAndWindowScripts are the route-scoped reads that serve a worker,
// each with the window its tier cuts the answer to.
func routedReadAndWindowScripts(topo QueryTopology) map[string]string {
	return map[string]string{
		"routedReadyTierScript":                          routedReadyTierScript(topo),
		"poolDemandFirstRowFunctionScript":               poolDemandFirstRowFunctionScript(topo),
		"assignedGraphWorkflowAnchorReadyFunctionScript": assignedGraphWorkflowAnchorReadyFunctionScript(topo),
	}
}

func TestRouteScopedReadsDropActiveHoldsBeforeTheirWindow(t *testing.T) {
	for topoName, topo := range assignedHoldTopologies() {
		for name, got := range routedReadAndWindowScripts(topo) {
			if !strings.Contains(got, `[.[]`+excludeActiveHoldsJQClause()+excludeClaimantsJQClause()+`]`) {
				t.Errorf("%s/%s() does not drop held rows, and rows that exclude this claimant, before its window: %q", topoName, name, got)
			}
			if !strings.Contains(got, "--limit=0") {
				t.Errorf("%s/%s() cannot read past a held window: %q", topoName, name, got)
			}
		}
	}
}

func TestLegacyEphemeralPoolDemandShellRouteScopedExcludesActiveHolds(t *testing.T) {
	got := legacyEphemeralPoolDemandShell(20, QueryTopology{})
	if !strings.Contains(got, excludeActiveHoldsJQClause()) {
		t.Errorf("legacyEphemeralPoolDemandShell() = %q, missing the active hold filter", got)
	}
}

func TestPoolDemandCountShellExcludesActiveHolds(t *testing.T) {
	got := poolDemandCountShell("hello-world/worker", QueryTopology{})
	if !strings.Contains(got, excludeActiveHoldsJQClause()) {
		t.Errorf("poolDemandCountShell() = %q, missing the active hold filter (the reconciler count-form must not count held rows)", got)
	}
}

func TestEffectiveRoutedPoolQueryCarriesHoldExclusionForLegacyAlias(t *testing.T) {
	a := &Agent{Name: ControlDispatcherAgentName, Dir: "rig"}
	got := a.EffectiveRoutedPoolQuery()
	if !strings.Contains(got, excludeActiveHoldsJQClause()) {
		t.Errorf("EffectiveRoutedPoolQuery() (legacy-alias agent) = %q, missing the active hold filter", got)
	}
}

// bd's native human label parks a bead for a person. It is not a hold, and the
// assignee-scoped tiers serve it, so the exclusion rides the pool-demand serve
// rules alone, as an exact --exclude-label flag. Every route-scoped query
// renders from those rules and must carry it; the assignee-scoped probes must
// stay transparent to it.

func TestPoolDemandServeRulesExcludeHumanLabelAndActiveHolds(t *testing.T) {
	rules := PoolDemandServeRulesForQuery()
	if !slices.Contains(rules.ExcludeLabels, beadmeta.HumanLabel) {
		t.Errorf("PoolDemandServeRulesForQuery().ExcludeLabels = %v, missing the human label", rules.ExcludeLabels)
	}
	if !rules.ExcludeActiveHolds {
		t.Errorf("PoolDemandServeRulesForQuery().ExcludeActiveHolds = false; the routed tier must not serve held work")
	}
	for _, label := range rules.ExcludeLabels {
		if _, held := beadmeta.ActiveHoldValue(label); held || strings.HasPrefix(strings.ToLower(strings.TrimSpace(label)), beadmeta.HoldLabelPrefix) {
			t.Errorf("PoolDemandServeRulesForQuery().ExcludeLabels names hold label %q; holds are excluded by the active hold predicate, not by an exact flag", label)
		}
	}
	if beadmeta.HasActiveHold([]string{beadmeta.HumanLabel}) {
		t.Errorf("the human label reads as an active hold; it is not one")
	}
}

func TestRouteScopedPoolDemandShellsExcludeHumanLabel(t *testing.T) {
	want := `--exclude-label "human"`
	for name, got := range map[string]string{
		"bdReadyPoolDemandShell":          bdReadyPoolDemandShell("--limit 0", QueryTopology{}),
		"bdReadyPoolDemandMigrationShell": bdReadyPoolDemandMigrationShell("--limit=20", QueryTopology{}),
		"poolDemandCountShell":            poolDemandCountShell("hello-world/worker", QueryTopology{}),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("%s() = %q, missing %q", name, got, want)
		}
	}
	if got := legacyEphemeralPoolDemandShell(20, QueryTopology{}); !strings.Contains(got, `"human"`) {
		t.Errorf("legacyEphemeralPoolDemandShell() = %q, missing the human label in its jq label filter", got)
	}
}

func TestAssigneeScopedProbesStayHumanTransparent(t *testing.T) {
	for name, got := range map[string]string{
		"ephemeralAssignedReadyProbeScript":       ephemeralAssignedReadyProbeScript("cand", QueryTopology{}),
		"ephemeralAssignedInProgressProbeScript":  ephemeralAssignedInProgressProbeScript("cand", QueryTopology{}),
		"assignedReadyTierCommand/bd104":          assignedReadyTierCommand("cand", QueryTopology{}),
		"assignedReadyTierCommand/bd105":          assignedReadyTierCommand("cand", QueryTopology{Beads: BeadsConfig{BDCompatibility: BeadsBDCompatibility105}}),
		"assignedReadyTierCommand/bd105federated": assignedReadyTierCommand("cand", QueryTopology{Beads: BeadsConfig{BDCompatibility: BeadsBDCompatibility105}, FederatedReady: true}),
	} {
		if strings.Contains(got, `"human"`) {
			t.Errorf("%s() = %q, assignee-scoped tier must stay human-transparent", name, got)
		}
	}
}
