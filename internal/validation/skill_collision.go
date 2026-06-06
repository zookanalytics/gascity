// Package validation hosts startup-time validators that guard against
// configurations the Gas City runtime cannot safely materialize. The
// validators are pure: they take a parsed *config.City and return
// diagnostic structs, never touching I/O.
package validation

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
)

// supportedSkillVendors lists the providers whose skill sinks the
// materializer writes under a scope root. Providers outside this set
// have no sink (see "Vendor mapping" in
// engdocs/proposals/skill-materialization.md), so their agent-local
// skills cannot collide.
var supportedSkillVendors = map[string]struct{}{
	"claude":   {},
	"codex":    {},
	"gemini":   {},
	"opencode": {},
}

// citySentinel is the ScopeRoot marker used for city-scoped groupings.
// The validator operates on an in-memory *config.City and does not know
// the filesystem path of the city root; callers that want to substitute
// a real path (e.g. the doctor check) can do so when formatting errors.
const citySentinel = "<city>"

// SkillCollision describes an agent-local skill name that cannot be
// materialized unambiguously. Two shapes exist:
//
//   - Inter-agent: two or more agents sharing the same (ScopeRoot, Vendor)
//     sink each supply the same skill name. AgentNames lists the colliding
//     agents; Sources is empty.
//   - Intra-agent: a single agent supplies the same skill name from two or
//     more of its own local roots (the convention agents/<name>/skills/
//     plus patch-supplied skills_dirs). AgentNames holds that one agent;
//     Sources lists the colliding roots.
//
// Both shapes are hard errors: the per-name sink slot holds exactly one
// symlink target, so an ambiguous name must be renamed rather than
// silently shadowed.
type SkillCollision struct {
	// ScopeRoot is the scope root the colliding agents materialize
	// into. For rig-scoped agents this is the rig's configured path
	// (which may be relative to the city root). For city-scoped
	// agents this is the sentinel "<city>".
	ScopeRoot string
	// Vendor is the provider whose sink the collision lands in
	// (one of "claude", "codex", "gemini", "opencode").
	Vendor string
	// SkillName is the colliding agent-local skill name.
	SkillName string
	// AgentNames lists, in sorted order, every agent providing the
	// same agent-local skill name into this (ScopeRoot, Vendor) sink.
	// An intra-agent collision lists exactly the one offending agent.
	AgentNames []string
	// Sources lists, in sorted order, the agent-local skill roots that a
	// single agent uses to supply SkillName more than once. Non-empty only
	// for an intra-agent collision; nil for the inter-agent case.
	Sources []string
}

// IsIntraAgent reports whether this collision is a single agent supplying
// the same skill name from more than one of its own roots (Sources set),
// as opposed to two distinct agents colliding at a shared sink.
func (c SkillCollision) IsIntraAgent() bool { return len(c.Sources) > 0 }

// ValidateSkillCollisions groups agents by (scope-root, vendor) and
// returns every agent-local skill name that cannot be materialized
// unambiguously. Two collision shapes are detected (see SkillCollision):
//
//   - Inter-agent: two or more agents at the same (scope-root, vendor)
//     sink each supply the same skill name. They would fight over one
//     symlink slot in the shared sink.
//   - Intra-agent: a single agent supplies the same name from two or more
//     of its own local roots (the convention agents/<name>/skills/ plus
//     patch-supplied skills_dirs). Its own sources would shadow each other.
//
// Each agent contributes the union of skill names across ALL its local
// roots (config.Agent.AgentLocalSkillRoots), so multi-source agents are
// fully accounted for. Returns nil when there are no collisions.
//
// Scope-root derivation mirrors the spec:
//   - agent.Scope == "city" → scope root = city sentinel
//   - agent.Scope == "rig"  → scope root = rig path (from agent.Dir
//     looked up in cfg.Rigs); if no matching rig is found the agent's
//     Dir is used as-is (supports inline agents with a custom Dir)
//   - empty scope is treated as "rig" (the default)
//
// Agents whose provider is not in the skill-sink vendor set contribute
// nothing — they have no sink, so they cannot collide. Agents with no
// local skill roots, or whose roots hold no skills, also contribute
// nothing.
//
// Collisions are returned sorted by (ScopeRoot, Vendor, SkillName, shape)
// so tests and user-facing output are stable.
func ValidateSkillCollisions(cfg *config.City) []SkillCollision {
	if cfg == nil || len(cfg.Agents) == 0 {
		return nil
	}

	rigPath := make(map[string]string, len(cfg.Rigs))
	for _, rig := range cfg.Rigs {
		rigPath[rig.Name] = rig.Path
	}

	type nameKey struct{ scope, vendor, name string }
	// providers: (scope, vendor, name) → agentQualifiedName → set of the
	// agent's own roots that supply that name. The agent dimension lets us
	// split inter-agent (≥2 agents) from intra-agent (1 agent, ≥2 roots).
	providers := make(map[nameKey]map[string]map[string]struct{})

	for i := range cfg.Agents {
		a := &cfg.Agents[i]

		// Agent provider falls back to workspace provider when not
		// set per-agent — matches the effective-provider resolution
		// used throughout the binary. Without this fallback,
		// workspace-level "provider = claude" configs with
		// non-overriding agents would bypass collision detection.
		// TrimSpace mirrors cmd/gc/skill_integration.go's
		// effectiveAgentProvider so whitespace-only overrides don't
		// bypass either the materializer or this gate.
		vendor := strings.TrimSpace(a.Provider)
		if vendor == "" {
			vendor = cfg.Workspace.Provider
		}
		if _, ok := supportedSkillVendors[vendor]; !ok {
			continue
		}

		scope := scopeRootFor(a, rigPath)
		if scope == "" {
			// Rig-scoped agent without a resolvable rig — skip. It
			// can't contribute a concrete sink anyway.
			continue
		}

		qn := a.QualifiedName()
		for name, roots := range agentLocalSkillSources(a) {
			key := nameKey{scope: scope, vendor: vendor, name: name}
			byAgent := providers[key]
			if byAgent == nil {
				byAgent = make(map[string]map[string]struct{})
				providers[key] = byAgent
			}
			rootSet := byAgent[qn]
			if rootSet == nil {
				rootSet = make(map[string]struct{})
				byAgent[qn] = rootSet
			}
			for _, root := range roots {
				rootSet[root] = struct{}{}
			}
		}
	}

	var collisions []SkillCollision
	for key, byAgent := range providers {
		// Inter-agent: more than one distinct agent supplies this name.
		if len(byAgent) >= 2 {
			names := make([]string, 0, len(byAgent))
			for n := range byAgent {
				names = append(names, n)
			}
			sort.Strings(names)
			collisions = append(collisions, SkillCollision{
				ScopeRoot:  key.scope,
				Vendor:     key.vendor,
				SkillName:  key.name,
				AgentNames: names,
			})
		}
		// Intra-agent: a single agent supplies this name from ≥2 roots.
		for qn, rootSet := range byAgent {
			if len(rootSet) < 2 {
				continue
			}
			roots := make([]string, 0, len(rootSet))
			for r := range rootSet {
				roots = append(roots, r)
			}
			sort.Strings(roots)
			collisions = append(collisions, SkillCollision{
				ScopeRoot:  key.scope,
				Vendor:     key.vendor,
				SkillName:  key.name,
				AgentNames: []string{qn},
				Sources:    roots,
			})
		}
	}

	sort.Slice(collisions, func(i, j int) bool {
		if collisions[i].ScopeRoot != collisions[j].ScopeRoot {
			return collisions[i].ScopeRoot < collisions[j].ScopeRoot
		}
		if collisions[i].Vendor != collisions[j].Vendor {
			return collisions[i].Vendor < collisions[j].Vendor
		}
		if collisions[i].SkillName != collisions[j].SkillName {
			return collisions[i].SkillName < collisions[j].SkillName
		}
		// Stable tiebreak: inter-agent (no Sources) before intra-agent,
		// then by first agent name.
		if collisions[i].IsIntraAgent() != collisions[j].IsIntraAgent() {
			return !collisions[i].IsIntraAgent()
		}
		return firstOrEmpty(collisions[i].AgentNames) < firstOrEmpty(collisions[j].AgentNames)
	})
	return collisions
}

// firstOrEmpty returns the first element of s, or "" when s is empty.
func firstOrEmpty(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

// scopeRootFor returns the scope-root key for an agent. Empty scope is
// treated as rig-scoped per the spec. Rig-scoped agents resolve their
// rig by Dir against cfg.Rigs; if no rig matches, Dir is used as-is
// (inline-agent case). Rig-scoped agents with empty Dir collapse to
// the city sentinel — which is defensive; a well-formed expanded
// config always stamps Dir = rigName for rig-scoped agents.
func scopeRootFor(a *config.Agent, rigPath map[string]string) string {
	scope := a.Scope
	if scope == "" {
		scope = "rig"
	}
	switch scope {
	case "city":
		return citySentinel
	case "rig":
		if a.Dir == "" {
			// No rig stamped — fall back to city sentinel so we
			// at least bucket the agent somewhere. In practice
			// pack expansion sets Dir for rig-scoped agents.
			return citySentinel
		}
		if path, ok := rigPath[a.Dir]; ok && path != "" {
			return path
		}
		return a.Dir
	default:
		// Unknown scope values are validated elsewhere
		// (config.ValidateAgents). Treat as rig for best-effort
		// bucketing.
		return a.Dir
	}
}

// agentLocalSkillSources returns, for one agent, a map of skill name → the
// agent-local roots that supply it, reading across every root in
// AgentLocalSkillRoots (convention + patch-supplied). A name appearing in
// two roots yields a two-element slice — the signal for an intra-agent
// collision. Returns nil when the agent has no local skills.
func agentLocalSkillSources(a *config.Agent) map[string][]string {
	roots := a.AgentLocalSkillRoots()
	if len(roots) == 0 {
		return nil
	}
	out := make(map[string][]string)
	for _, root := range roots {
		for _, name := range listAgentLocalSkills(root) {
			out[name] = append(out[name], root)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// listAgentLocalSkills returns the sorted list of skill names under the
// agent's local skills directory. A subdirectory counts as a skill if
// it contains a SKILL.md file (case-sensitive — matches the vendor
// convention and every existing caller).
func listAgentLocalSkills(dir string) []string {
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, e.Name(), "SKILL.md")); err != nil {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}
