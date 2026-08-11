package agentutil

import (
	"errors"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// routeTargetCity mirrors the live shape that produced the defect: a
// binding-qualified pool agent bound into two rigs, plus a city-scoped
// singleton whose route identity is legitimately bare.
func routeTargetCity() *config.City {
	return &config.City{
		Rigs: []config.Rig{
			{Name: "gascity", Path: "/rigs/gascity"},
			{Name: "signal-loom", Path: "/rigs/signal-loom"},
		},
		Agents: []config.Agent{
			{Name: "polecat", BindingName: "gc-toolkit", Dir: "gascity", Scope: "rig", MaxActiveSessions: intPtr(2)},
			{Name: "polecat", BindingName: "gc-toolkit", Dir: "signal-loom", Scope: "rig", MaxActiveSessions: intPtr(2)},
			{Name: "mayor", BindingName: "gc-toolkit", Scope: "city"},
			{Name: "refinery", BindingName: "gc-toolkit", Dir: "gascity", Scope: "rig"},
		},
	}
}

// A bare rig-pool name resolves to no live identity. Exactly one
// rig-qualification of it is live in the rig store being written to, so the
// write is qualified rather than refused.
func TestResolveRouteTargetQualifiesBareRigPoolName(t *testing.T) {
	got, err := ResolveRouteTarget(routeTargetCity(), "rig:gascity", "gc-toolkit.polecat")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "gascity/gc-toolkit.polecat" {
		t.Errorf("got %q, want %q", got, "gascity/gc-toolkit.polecat")
	}
}

// The same bare name written to the OTHER rig's store qualifies to that rig.
// The store being written to is what picks the candidate.
func TestResolveRouteTargetQualifiesPerStore(t *testing.T) {
	got, err := ResolveRouteTarget(routeTargetCity(), "rig:signal-loom", "gc-toolkit.polecat")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "signal-loom/gc-toolkit.polecat" {
		t.Errorf("got %q, want %q", got, "signal-loom/gc-toolkit.polecat")
	}
}

// From the city store no rig-qualification is reachable, so the write is
// refused and every candidate is named.
func TestResolveRouteTargetRefusesFromCityStoreAndNamesCandidates(t *testing.T) {
	_, err := ResolveRouteTarget(routeTargetCity(), "city:loomington", "gc-toolkit.polecat")
	if err == nil {
		t.Fatal("expected a refusal for an unresolvable route target")
	}
	var unresolvable *UnresolvableRouteTargetError
	if !errors.As(err, &unresolvable) {
		t.Fatalf("expected *UnresolvableRouteTargetError, got %T", err)
	}
	for _, want := range []string{"gascity/gc-toolkit.polecat", "signal-loom/gc-toolkit.polecat"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name candidate %q", err.Error(), want)
		}
	}
}

// THE constraint that decides whether this guard is correct or catastrophic:
// the test is "does this resolve to a live identity", never "does this look
// qualified". A city-scope agent's route identity is bare by design and must
// pass through untouched.
func TestResolveRouteTargetLeavesLiveCityScopeBareIdentityUntouched(t *testing.T) {
	got, err := ResolveRouteTarget(routeTargetCity(), "rig:gascity", "gc-toolkit.mayor")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "gc-toolkit.mayor" {
		t.Errorf("got %q, want %q untouched", got, "gc-toolkit.mayor")
	}
}

// A city-scoped named session is a live route identity even though no
// config.Agent carries its name.
func TestResolveRouteTargetLeavesLiveNamedSessionUntouched(t *testing.T) {
	cfg := routeTargetCity()
	cfg.NamedSessions = []config.NamedSession{
		{Name: "mechanik", BindingName: "gc-toolkit", Scope: "city"},
	}
	got, err := ResolveRouteTarget(cfg, "rig:gascity", "gc-toolkit.mechanik")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "gc-toolkit.mechanik" {
		t.Errorf("got %q, want %q untouched", got, "gc-toolkit.mechanik")
	}
}

// Sentinels name no agent on purpose and must survive the guard.
func TestResolveRouteTargetLeavesSentinelUntouched(t *testing.T) {
	got, err := ResolveRouteTarget(routeTargetCity(), "rig:gascity", "human")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "human" {
		t.Errorf("got %q, want %q untouched", got, "human")
	}
}

// An agent with scope="rig" and no dir is a per-rig TEMPLATE, not a live
// identity: sessions exist per rig, so routing to its bare qualified name
// leaves the bead invisible. It must be qualified even though it is a
// configured agent that resolves by name.
func TestResolveRouteTargetRefusesUnboundRigTemplate(t *testing.T) {
	cfg := &config.City{
		Rigs:   []config.Rig{{Name: "gascity", Path: "/rigs/gascity"}},
		Agents: []config.Agent{{Name: "polecat", BindingName: "gc-toolkit", Scope: "rig", MaxActiveSessions: intPtr(2)}},
	}
	got, err := ResolveRouteTarget(cfg, "rig:gascity", "gc-toolkit.polecat")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "gascity/gc-toolkit.polecat" {
		t.Errorf("got %q, want the rig-qualified form %q", got, "gascity/gc-toolkit.polecat")
	}
}

// A slot-suffixed target still collapses to its base pool identity — the
// guard must not regress NormalizePoolRouteTarget's behavior.
func TestResolveRouteTargetCollapsesPoolSlot(t *testing.T) {
	got, err := ResolveRouteTarget(routeTargetCity(), "rig:gascity", "gascity/gc-toolkit.polecat-2")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "gascity/gc-toolkit.polecat" {
		t.Errorf("got %q, want %q", got, "gascity/gc-toolkit.polecat")
	}
}

// A target that no rig qualifies carries no evidence that it is an
// unqualified rig-pool name, so it passes through unchanged. Refusing on
// absence alone would turn every route whose target is not discoverable by
// name in cfg into a hard dispatch failure; the pack-side doctor backstop
// reports these after the fact.
func TestResolveRouteTargetPassesThroughTargetWithNoQualification(t *testing.T) {
	got, err := ResolveRouteTarget(routeTargetCity(), "rig:gascity", "gc-toolkit.ghost")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "gc-toolkit.ghost" {
		t.Errorf("got %q, want unchanged", got)
	}
}

// An already-live rig-qualified identity is returned unchanged.
func TestResolveRouteTargetLeavesLiveQualifiedIdentityUntouched(t *testing.T) {
	got, err := ResolveRouteTarget(routeTargetCity(), "rig:gascity", "gascity/gc-toolkit.refinery")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "gascity/gc-toolkit.refinery" {
		t.Errorf("got %q, want %q untouched", got, "gascity/gc-toolkit.refinery")
	}
}

// With no config to validate against there is no live identity set, so the
// guard cannot judge and must pass the value through unchanged.
func TestResolveRouteTargetNilConfigPassesThrough(t *testing.T) {
	got, err := ResolveRouteTarget(nil, "rig:gascity", "gc-toolkit.polecat")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "gc-toolkit.polecat" {
		t.Errorf("got %q, want unchanged", got)
	}
}

// An empty target clears the route; there is nothing to resolve.
func TestResolveRouteTargetEmptyTarget(t *testing.T) {
	got, err := ResolveRouteTarget(routeTargetCity(), "rig:gascity", "  ")
	if err != nil {
		t.Fatalf("ResolveRouteTarget: unexpected error: %v", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

// An unknown store ref must not silently pick a candidate: with no store to
// scope the qualification, an ambiguous bare name stays refused.
func TestResolveRouteTargetRefusesWithUnknownStoreRef(t *testing.T) {
	_, err := ResolveRouteTarget(routeTargetCity(), "", "gc-toolkit.polecat")
	if err == nil {
		t.Fatal("expected a refusal when no store scopes the qualification")
	}
}

func TestIsRouteTargetSentinel(t *testing.T) {
	for _, target := range []string{"human", "HUMAN", " human "} {
		if !IsRouteTargetSentinel(target) {
			t.Errorf("IsRouteTargetSentinel(%q) = false, want true", target)
		}
	}
	for _, target := range []string{"", "humanoid", "gc-toolkit.polecat"} {
		if IsRouteTargetSentinel(target) {
			t.Errorf("IsRouteTargetSentinel(%q) = true, want false", target)
		}
	}
}
