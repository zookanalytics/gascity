package agentutil

import (
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
)

// routeTargetSentinels are routing values that deliberately name no agent.
// They are written and read by higher layers to park work outside the pool
// model, so the live-identity guard must let them through untouched.
var routeTargetSentinels = map[string]bool{
	"human": true,
}

// IsRouteTargetSentinel reports whether target is a routing sentinel that
// names no agent on purpose and so must never be qualified or refused.
func IsRouteTargetSentinel(target string) bool {
	return routeTargetSentinels[strings.ToLower(strings.TrimSpace(target))]
}

// UnresolvableRouteTargetError reports a routing address that names no live
// agent identity reachable from the store being written to. Candidates holds
// every rig-qualified identity the address could have meant, so the caller can
// tell an operator which one to write.
type UnresolvableRouteTargetError struct {
	// Target is the address as supplied.
	Target string
	// StoreRef is the workflow store being written to ("rig:<name>" or
	// "city:<name>"), which scopes which candidates are reachable.
	StoreRef string
	// Candidates are the live rig-qualified identities Target could name,
	// including ones unreachable from StoreRef.
	Candidates []string
}

// Error returns the unresolvable-route diagnostic, naming the candidates the
// address could have meant when any are known.
func (e *UnresolvableRouteTargetError) Error() string {
	store := e.StoreRef
	if store == "" {
		store = "<unknown store>"
	}
	if len(e.Candidates) == 0 {
		return fmt.Sprintf("route target %q names no live agent identity (store %s); run 'gc agent list' for the addressable identities", e.Target, store)
	}
	return fmt.Sprintf("route target %q names no live agent identity reachable from store %s; did you mean one of: %s", e.Target, store, strings.Join(e.Candidates, ", "))
}

// ResolveRouteTarget validates a routing address against the live agent
// identities configured for cfg — the expanded per-rig/per-city set that
// `gc agent list` prints — and returns the identity to record.
//
// An address that already resolves is returned in its canonical routed-to form
// (pool slots collapsed to their base). An address that does not resolve is
// qualified when exactly one rig-qualification of it is live in storeRef's rig,
// and otherwise refused with an *UnresolvableRouteTargetError naming the
// candidates.
//
// Refusal requires positive evidence that a better identity exists — at least
// one live rig-qualification of the address. An address that resolves to
// nothing and that no rig qualifies is passed through unchanged rather than
// refused: with no live qualification, cfg carries no evidence that the value
// is an unqualified rig-pool name, and refusing on absence alone would convert
// every route whose target is not discoverable by name in cfg (a synthesized
// target, a config that lists no agents) from a working dispatch into a hard
// failure. Detecting those is the pack-side doctor backstop's job, which
// reports after the fact; this guard only refuses what it can prove.
//
// The guard exists because a routing address that matches no live identity is
// structurally invisible: the pool offer predicate is open + unassigned +
// gc.routed_to matching a live identity, so a bead carrying an unresolvable
// address sits open forever and nothing reports it (see the sibling case in
// NormalizePoolRouteTarget, which collapses slot-suffixed targets for the same
// reason).
//
// The test is always "does this address resolve to a live identity", never
// "does this address look qualified". Bare identities are common and correct:
// a city-scoped agent's route identity has no rig prefix by design, so a
// syntactic rule would refuse every route to every city-scoped agent in the
// town. Conversely a scope="rig" agent with no dir is a per-rig template whose
// own qualified name is not claimable, so being configured is not sufficient
// either — only the expanded identity counts.
//
// storeRef is the workflow store being written to, in the format
// AgentReachesWorkflowStore uses ("rig:<name>" or "city:<name>"). A nil cfg
// leaves the address unchanged: with no config there is no identity set to
// judge against.
func ResolveRouteTarget(cfg *config.City, storeRef, target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" || cfg == nil || IsRouteTargetSentinel(target) {
		return target, nil
	}
	if identity, ok := liveRouteIdentity(cfg, target); ok {
		return identity, nil
	}
	candidates := rigQualifiedRouteCandidates(cfg, target)
	if len(candidates) == 0 {
		return target, nil
	}
	// Candidates are always rig-bound, so a candidate is reachable exactly when
	// it belongs to the rig store being written to. An empty storeRef matches
	// nothing and so refuses rather than guessing.
	var reachable []string
	for _, c := range candidates {
		if storeRef == "rig:"+c.rig {
			reachable = append(reachable, c.identity)
		}
	}
	if len(reachable) == 1 {
		return reachable[0], nil
	}
	identities := make([]string, 0, len(candidates))
	for _, c := range candidates {
		identities = append(identities, c.identity)
	}
	return "", &UnresolvableRouteTargetError{Target: target, StoreRef: storeRef, Candidates: identities}
}

// liveRouteIdentity reports the canonical routed-to identity for an address
// that names a live agent or named session, collapsing pool slots to their
// base. It returns false for an address that names nothing addressable —
// including a per-rig template, which is configured but not itself claimable.
func liveRouteIdentity(cfg *config.City, target string) (string, bool) {
	if a, ok := ResolveAgent(cfg, target, ResolveOpts{AllowPoolMembers: true}); ok && !isUnboundRigTemplate(a.Scope, a.Dir) {
		if identity := NormalizePoolRouteTarget(cfg, RoutedToIdentity(&a)); identity != "" {
			return identity, true
		}
	}
	for i := range cfg.NamedSessions {
		s := cfg.NamedSessions[i]
		if isUnboundRigTemplate(s.Scope, s.Dir) {
			continue
		}
		if s.QualifiedName() == target {
			return target, true
		}
	}
	return "", false
}

// routeCandidate is one rig-qualification of an unresolvable address, paired
// with the rig that makes it live so the caller can scope it to a store.
type routeCandidate struct {
	rig      string
	identity string
}

// rigQualifiedRouteCandidates returns every rig-qualification of target that
// names a live identity. Two shapes qualify: a rig-bound agent or named
// session addressed as "<rig>/<target>", and a per-rig template whose own
// qualified name is target and which is therefore live once bound to a rig.
func rigQualifiedRouteCandidates(cfg *config.City, target string) []routeCandidate {
	var out []routeCandidate
	seen := make(map[string]bool)
	for _, rig := range cfg.Rigs {
		rigName := strings.TrimSpace(rig.Name)
		if rigName == "" {
			continue
		}
		identity, ok := liveRouteIdentity(cfg, rigName+"/"+target)
		if !ok && templateBindsToRig(cfg, target) {
			identity, ok = rigName+"/"+target, true
		}
		if !ok || seen[identity] {
			continue
		}
		seen[identity] = true
		out = append(out, routeCandidate{rig: rigName, identity: identity})
	}
	return out
}

// templateBindsToRig reports whether target names a per-rig template — an
// agent or named session declared scope="rig" with no dir. Such a template
// materializes one live identity per rig, so "<rig>/<target>" is addressable
// even when the generic resolution path cannot synthesize it.
func templateBindsToRig(cfg *config.City, target string) bool {
	for i := range cfg.Agents {
		a := cfg.Agents[i]
		if isUnboundRigTemplate(a.Scope, a.Dir) && RoutedToIdentity(&a) == target {
			return true
		}
	}
	for i := range cfg.NamedSessions {
		s := cfg.NamedSessions[i]
		if isUnboundRigTemplate(s.Scope, s.Dir) && s.QualifiedName() == target {
			return true
		}
	}
	return false
}

// isUnboundRigTemplate reports whether a scope/dir pair describes a per-rig
// template rather than a concrete identity. Sessions for such a template exist
// per rig, so its own qualified name is never claimable.
func isUnboundRigTemplate(scope, dir string) bool {
	return strings.EqualFold(strings.TrimSpace(scope), "rig") && strings.TrimSpace(dir) == ""
}
