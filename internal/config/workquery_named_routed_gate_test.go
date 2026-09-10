package config

import (
	"strings"
	"testing"
)

// A named session's hook work query has two tiers: an ungated assigned tier
// (--assignee=<self>) and a routed tier (routed_to=<self>, assignee=EMPTY) that
// sits behind an origin gate. A live named session carries
// GC_SESSION_ORIGIN=named, so before the self-target admit the gate exit-0s the
// routed tier and the session never claims work routed to its own identity;
// assigned work still flows because its tier is ungated. The gate admits the
// routed tier for a non-ephemeral session only when the probe target ($1) equals
// the session's own claim alias (GC_ALIAS), so it surfaces exactly the work the
// session itself can claim and nothing routed elsewhere. The identities line up
// by construction for a plain named session: poolDemandTarget() (baked in as $1),
// RoutedToIdentity() (the claim-match target), and GC_ALIAS all resolve to the
// same QualifiedName(). poolDemandOriginGateScript() is the single origin gate;
// buildWorkQuery (the combined work query) and routedPoolWorkQueryProbeScript
// (the split-tier EffectiveRoutedPoolQuery) both use it.

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

// TestEffectiveWorkQueryNamedSessionSurfacesSelfRoutedWork is the primary RED pin:
// the combined work query must surface a named session's own routed, unassigned
// work. Fails on the pre-fix gate, which exit-0s for GC_SESSION_ORIGIN=named
// before probe_pool_demand.
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

// TestEffectiveRoutedPoolQueryNamedSessionSurfacesSelfRoutedWork is the RED pin
// for the split-tier prompt path (the plain gate via routedPoolWorkQueryProbeScript).
// A pack that spells out the routed-pool tier as its own prompt slot hit the same
// deadlock; the gate must admit self-target work there too.
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

// TestEffectiveWorkQueryEphemeralSessionStillSurfacesRoutedWork is the control for
// the positive pins: a pool seat (GC_SESSION_ORIGIN=ephemeral) already discovered
// routed work and must keep doing so. Green before and after; if it ever reddens,
// the change broke the untouched ephemeral arm rather than the named arm.
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
// poolDemandTarget()=PoolName, so its probe target is the POOL, not its own claim
// identity (GC_ALIAS). The self-target admit must NOT fire — else the session
// would claim work routed to a different (pool) queue. This is the sentinel for
// the "delete the whole gate" mutation.
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

// TestEffectiveWorkQueryNamedSessionWithoutAliasStaysGated pins fail-closed
// behavior: origin=named but no GC_ALIAS in the environment. The [ -n "$GC_ALIAS" ]
// guard keeps the routed tier gated rather than admitting generic
// pool-demand-shaped discovery under no identity.
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

// TestEffectiveWorkQueryNamedSessionStillFindsAssignedWork is the control proving
// the ungated assigned (crash-recovery) tier is untouched: assignee=<self> work is
// served regardless of origin, which is why the defect only ever hid routed,
// unassigned work.
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

// TestNamedSessionOriginGatesAdmitOnlySelfTarget is the per-clause mutation pin on
// the generated gate string itself. The origin gate must:
//   - keep the ephemeral / no-origin arm a plain fall-through (pool seats and the
//     reconciler's session-less demand detection are unchanged), and
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

// TestPoolDemandQueryHasNoOriginGate pins the constraint that the origin gate lives
// ONLY on the claim-discovery paths (work + routed pool). The reconciler
// scale-check (buildPoolDemandQuery -> poolDemandCountShell) must never carry it,
// so widening the gate cannot move a pool scale decision. The golden fixtures pin
// the exact PoolDemand bytes; this states the invariant directly.
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
