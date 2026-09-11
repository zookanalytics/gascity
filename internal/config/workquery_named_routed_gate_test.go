package config

import (
	"strings"
	"testing"
)

// These tests pin the named-session origin gate on the hook work query. The query
// has an ungated assigned tier (--assignee=<self>) and a routed tier
// (routed_to=<self>, assignee="") behind poolDemandOriginGateScript(). For a
// non-ephemeral origin the gate admits the routed tier only when the probe target
// ($1) equals the session's own claim alias (GC_ALIAS), so a named session surfaces
// its own routed work and nothing routed elsewhere. Both the combined query
// (buildWorkQuery) and the split-tier query (routedPoolWorkQueryProbeScript) share
// that gate; namedSelfTargetAdmit in workquery.go carries why the identities line up.

// fakeBDSelfRoutedFrontier returns a fake bd whose only ready work is a single
// unassigned bead routed to route. list/query/show are empty, so the assigned
// and migration/ephemeral tiers all miss — the bead is discoverable ONLY through
// the routed (pool-demand) tier keyed on gc.routed_to=route. That isolates the
// origin gate: if the routed tier never runs, the bead never surfaces.
func fakeBDSelfRoutedFrontier(route, beadID string) string {
	return `#!/bin/sh
set -eu
case "$1" in
  list|query|show)
    printf '[]'
    ;;
  ready)
    case "$*" in
      *"--metadata-field gc.routed_to=` + route + `"*)
        printf '[{"id":"` + beadID + `","status":"open","assignee":"","metadata":{"gc.routed_to":"` + route + `"}}]'
        ;;
      *)
        printf '[]'
        ;;
    esac
    ;;
  *)
    printf '[]'
    ;;
esac
`
}

// TestEffectiveWorkQueryNamedSessionSurfacesSelfRoutedWork: the combined work query
// must surface a named session's own routed, unassigned work. With origin=named and
// GC_ALIAS equal to the routed target, the gate admits the routed tier.
func TestEffectiveWorkQueryNamedSessionSurfacesSelfRoutedWork(t *testing.T) {
	a := Agent{Name: "olivia"}
	out := runEffectiveWorkQuery(t, a, map[string]string{
		"GC_SESSION_ORIGIN": "named",
		"GC_ALIAS":          "olivia",
	}, fakeBDSelfRoutedFrontier("olivia", "gcg-frontier"))
	if !strings.Contains(out, "gcg-frontier") {
		t.Fatalf("named session (origin=named, alias=olivia) did not surface its own routed unassigned work.\nEffectiveWorkQuery output = %q\nwant candidate gcg-frontier", out)
	}
}

// TestEffectiveRoutedPoolQueryNamedSessionSurfacesSelfRoutedWork: the split-tier
// routed-pool path (EffectiveRoutedPoolQuery, used when a pack spells the routed-pool
// tier out as its own prompt slot) carries the same gate and must admit a named
// session's self-target work too.
func TestEffectiveRoutedPoolQueryNamedSessionSurfacesSelfRoutedWork(t *testing.T) {
	a := Agent{Name: "olivia"}
	out := runShellWithFakeBd(t, a.EffectiveRoutedPoolQuery(), map[string]string{
		"GC_SESSION_ORIGIN": "named",
		"GC_ALIAS":          "olivia",
	}, fakeBDSelfRoutedFrontier("olivia", "gcg-frontier"))
	if !strings.Contains(out, "gcg-frontier") {
		t.Fatalf("named session split-tier routed-pool query did not surface self-routed work.\nEffectiveRoutedPoolQuery output = %q\nwant gcg-frontier", out)
	}
}

// TestEffectiveWorkQueryEphemeralSessionStillSurfacesRoutedWork: a pool seat
// (GC_SESSION_ORIGIN=ephemeral) falls through the gate's ephemeral arm and discovers
// routed work. This control guards that the ephemeral arm stays open.
func TestEffectiveWorkQueryEphemeralSessionStillSurfacesRoutedWork(t *testing.T) {
	a := Agent{Name: "olivia"}
	out := runEffectiveWorkQuery(t, a, map[string]string{
		"GC_SESSION_ORIGIN": "ephemeral",
	}, fakeBDSelfRoutedFrontier("olivia", "gcg-frontier"))
	if !strings.Contains(out, "gcg-frontier") {
		t.Fatalf("ephemeral session lost routed discovery (untouched arm regressed): %q", out)
	}
}

// TestEffectiveWorkQueryNamedSessionDoesNotSurfaceOtherPoolRoutedWork guards the
// over-claim boundary: a named identity that ALSO backs a pool (PoolName set) has
// poolDemandTarget()=PoolName, so its probe target is the pool, not its own claim
// identity (GC_ALIAS). The self-target admit must NOT fire, or the session would
// claim work routed to a different (pool) queue.
func TestEffectiveWorkQueryNamedSessionDoesNotSurfaceOtherPoolRoutedWork(t *testing.T) {
	a := Agent{Name: "olivia", PoolName: "crew"}
	out := runEffectiveWorkQuery(t, a, map[string]string{
		"GC_SESSION_ORIGIN": "named",
		"GC_ALIAS":          "olivia",
	}, fakeBDSelfRoutedFrontier("crew", "crew-bead"))
	if strings.Contains(out, "crew-bead") {
		t.Fatalf("named+pool hybrid over-claimed pool-routed work through the self-target admit (target=crew != alias=olivia must stay gated): %q", out)
	}
}

// TestEffectiveWorkQueryNamedSessionWithoutAliasStaysGated is the fail-closed case:
// origin=named with no GC_ALIAS set. The [ -n "$GC_ALIAS" ] guard keeps the routed
// tier gated rather than admitting generic pool-demand-shaped discovery under no
// identity.
func TestEffectiveWorkQueryNamedSessionWithoutAliasStaysGated(t *testing.T) {
	a := Agent{Name: "olivia"}
	out := runEffectiveWorkQuery(t, a, map[string]string{
		"GC_SESSION_ORIGIN": "named",
		// GC_ALIAS deliberately unset.
	}, fakeBDSelfRoutedFrontier("olivia", "gcg-frontier"))
	if strings.Contains(out, "gcg-frontier") {
		t.Fatalf("named session with empty GC_ALIAS admitted routed discovery; must fail closed: %q", out)
	}
}

// TestEffectiveWorkQueryNamedSessionStillFindsAssignedWork covers the ungated
// assigned (crash-recovery) tier: assignee=<self> work is served regardless of
// origin. The gate governs only the routed, unassigned tier, so an assigned bead
// surfaces even for a named session.
func TestEffectiveWorkQueryNamedSessionStillFindsAssignedWork(t *testing.T) {
	a := Agent{Name: "olivia"}
	out := runEffectiveWorkQuery(t, a, map[string]string{
		"GC_SESSION_ORIGIN": "named",
		"GC_ALIAS":          "olivia",
	}, `#!/bin/sh
set -eu
case "$1" in
  list)
    case "$*" in
      *"--assignee=olivia"*)
        printf '[{"id":"assigned-bead","status":"in_progress","assignee":"olivia","metadata":{"gc.routed_to":"olivia"}}]'
        ;;
      *) printf '[]' ;;
    esac
    ;;
  show)
    printf '[{"id":"assigned-bead","dependencies":[]}]'
    ;;
  ready|query)
    printf '[]'
    ;;
  *)
    printf '[]'
    ;;
esac
`)
	if !strings.Contains(out, "assigned-bead") {
		t.Fatalf("named session lost its ungated assigned (crash-recovery) tier: %q", out)
	}
}

// TestNamedSessionOriginGatesAdmitOnlySelfTarget asserts the generated gate string
// directly. The origin gate must:
//   - keep the ephemeral / no-origin arm a plain fall-through, so pool seats and the
//     reconciler's session-less demand detection are never gated, and
//   - admit a non-ephemeral session's routed tier only when both the alias is set
//     and the probe target equals it ([ -n "$GC_ALIAS" ] && [ "$1" = "$GC_ALIAS" ]).
func TestNamedSessionOriginGatesAdmitOnlySelfTarget(t *testing.T) {
	gate := poolDemandOriginGateScript()
	const admit = `[ -n "$GC_ALIAS" ] && [ "$1" = "$GC_ALIAS" ]`
	const unchangedArm = `case "$GC_SESSION_ORIGIN" in ephemeral|"") ;; `

	if !strings.Contains(gate, admit) {
		t.Fatalf("origin gate lost the self-target admit clause %q: %q", admit, gate)
	}
	if !strings.HasPrefix(gate, unchangedArm) {
		t.Fatalf("origin gate changed the ephemeral/no-origin fall-through arm: %q", gate)
	}
}

// TestPoolDemandQueryHasNoOriginGate pins that the origin gate lives ONLY on the
// claim-discovery paths (work + routed pool). The reconciler scale-check
// (buildPoolDemandQuery -> poolDemandCountShell) must never carry it, so the gate
// cannot move a pool scale decision. The golden fixtures pin the exact PoolDemand
// bytes; this states the invariant directly.
func TestPoolDemandQueryHasNoOriginGate(t *testing.T) {
	topos := []QueryTopology{
		{},
		{Beads: BeadsConfig{BDCompatibility: BeadsBDCompatibility105}},
		{FederatedReady: true},
	}
	for _, a := range []Agent{{Name: "olivia"}, {Name: "worker", PoolName: "crew"}} {
		for _, topo := range topos {
			q := a.EffectivePoolDemandQueryFor(topo)
			if strings.Contains(q, "GC_SESSION_ORIGIN") {
				t.Fatalf("pool-demand query carries the origin gate: %q", q)
			}
			if strings.Contains(q, "GC_ALIAS") {
				t.Fatalf("pool-demand query references GC_ALIAS: %q", q)
			}
		}
	}
}
