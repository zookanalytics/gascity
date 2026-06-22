package config

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/formula"
)

// ScopedFormulaPatch pairs a collected [[patches.formula]] overlay with the
// dispatch scope that contributed it. A patch shipped by a city-level pack
// applies to every dispatch scope (the city and every rig); a patch shipped by a
// rig-scoped pack applies only when dispatching in that rig.
//
// Carrying the scope is load-bearing. The overlays are applied at
// formula-resolve time against the TARGET rig's search paths (see
// Parser.WithPatches / Parser.Resolve), so a rig-scoped overlay that is allowed
// to reach an unrelated rig's same-named formula would be applied to a formula
// that lacks the overridden step and fail dispatch with "cannot override unknown
// step id" — even though config load succeeded. Scoping confines a rig-scoped
// overlay (e.g. the gascity-keeper refinery overlay, shipped by a sub-pack a
// single rig imports) to the rig whose packs contributed it.
type ScopedFormulaPatch struct {
	// Patch is the overlay itself.
	Patch formula.Patch
	// Rig is the rig name whose dispatch scope this patch applies to. Empty
	// means the patch was contributed at city scope and applies to every scope
	// (the city and every rig).
	Rig string
}

// appliesToRig reports whether this patch is active when dispatching in the
// given rig scope. City-scoped patches (Rig == "") are active everywhere;
// rig-scoped patches are active only in their own rig. rigName "" is the city
// scope, which sees only city-scoped patches.
func (sp ScopedFormulaPatch) appliesToRig(rigName string) bool {
	return sp.Rig == "" || sp.Rig == rigName
}

// cachedPackFormulaPatches returns the formula overlays a loaded pack and its
// closure contributed, from the load cache. Mirrors cachedPackDoctors. Patches
// are copied so callers cannot mutate the cached canonical result.
func cachedPackFormulaPatches(cache *packLoadCache, topoDir string) []formula.Patch {
	if cache == nil {
		return nil
	}
	absDir, err := filepath.Abs(topoDir)
	if err != nil {
		absDir = topoDir
	}
	result, ok := cache.results[absDir]
	if !ok {
		return nil
	}
	return append([]formula.Patch(nil), result.formulaPatches...)
}

// appendUniqueFormulaPatches appends each patch in src — tagged with the given
// dispatch scope (rig name, or "" for city scope) — to dst, skipping any whose
// (scope, content) pair is already present. The same pack patch reaches
// collection more than once through diamond-shaped import graphs or several rigs
// importing the same pack; without dedup an [[...append_step]] would be applied
// twice and the second application would fail its own duplicate-id guard. Two
// genuinely different overlays of the same formula (different content), and the
// same overlay contributed at two different scopes, are both kept — only a
// same-scope content collision is collapsed. A real conflict between two
// distinct overlays surfaces at apply time, which is correct.
func appendUniqueFormulaPatches(dst []ScopedFormulaPatch, rig string, src ...formula.Patch) []ScopedFormulaPatch {
	seen := make(map[string]bool, len(dst))
	for _, p := range dst {
		seen[scopedFormulaPatchKey(p.Rig, p.Patch)] = true
	}
	for _, p := range src {
		key := scopedFormulaPatchKey(rig, p)
		if seen[key] {
			continue
		}
		seen[key] = true
		dst = append(dst, ScopedFormulaPatch{Patch: p, Rig: rig})
	}
	return dst
}

// scopedFormulaPatchKey returns a dedup key combining the contributing scope
// with the patch's stable content key, so the same overlay can coexist at two
// scopes while diamond/multi-rig duplicates within one scope collapse.
func scopedFormulaPatchKey(rig string, p formula.Patch) string {
	return rig + "\x00" + formulaPatchKey(p)
}

// formulaPatchKey returns a stable content key for dedup. JSON marshaling
// follows the patch's pointers (steps, vars), so identical overlays produce
// identical keys. A marshal error falls back to a pointer-free best effort so
// dedup degrades to "keep both" rather than panicking.
func formulaPatchKey(p formula.Patch) string {
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Sprintf("%s\x00%#v", p.Formula, p)
	}
	return string(b)
}

// FormulaPatchesForRig returns the [[patches.formula]] overlays that apply when
// dispatching in the given rig's scope: every city-scoped patch plus the patches
// contributed by that rig's own packs. Patches scoped to OTHER rigs are excluded
// so a rig-scoped overlay never reaches a same-named formula in an unrelated rig
// (the scenario that produced "cannot override unknown step id" at dispatch).
// Results are de-duplicated by content, so an overlay contributed both at city
// scope and by the rig is returned once. rigName "" selects the city scope,
// which sees only city-scoped patches. Returns nil when nothing applies.
func (c *City) FormulaPatchesForRig(rigName string) []formula.Patch {
	if c == nil || len(c.FormulaPatches) == 0 {
		return nil
	}
	var out []formula.Patch
	seen := make(map[string]bool, len(c.FormulaPatches))
	for _, sp := range c.FormulaPatches {
		if !sp.appliesToRig(rigName) {
			continue
		}
		key := formulaPatchKey(sp.Patch)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, sp.Patch)
	}
	return out
}

// validateFormulaPatches fails config load when a collected [[patches.formula]]
// targets a formula that does not resolve in any scope where the patch is
// active, or overlays a step that does not exist in the formula as a runtime
// consumer would resolve it. A no-op when no patches were collected.
//
// Validation mirrors dispatch exactly: each dispatch scope (the city layers and
// every rig's layers, per FormulaLayers.SearchPaths) is checked against the SAME
// patch set a runtime consumer applies there — FormulaPatchesForRig(scope) — so
// any configuration accepted at load time is one runtime can actually dispatch.
// Because patches are scoped to the pack closure that contributed them, a
// rig-scoped overlay is validated (and later applied) only in its own rig: a
// same-named-but-incompatible formula in an unrelated rig neither rejects a valid
// overlay nor hides a broken one. A city-scoped overlay claims to apply
// everywhere, so it must apply cleanly in every scope its target resolves in;
// otherwise that scope's dispatch would fail and load fails instead.
//
// A patch whose target resolves in NONE of its active scopes is a dead overlay
// (unknown-formula error — typically a typo). A patch whose target resolves but
// fails to apply in an active scope is an apply error.
func validateFormulaPatches(cfg *City) error {
	if len(cfg.FormulaPatches) == 0 {
		return nil
	}
	scopes := effectiveFormulaSearchScopes(cfg.FormulaLayers)
	if len(scopes) == 0 {
		return fmt.Errorf("config declares [[patches.formula]] but no formula search paths are configured")
	}

	// Track, per collected patch, whether its target resolved in at least one
	// scope where the patch is active. A patch that resolves nowhere it could
	// run is a dead overlay and is rejected after every scope has been checked.
	resolvedSomewhere := make([]bool, len(cfg.FormulaPatches))

	for _, scope := range scopes {
		patchSet := cfg.FormulaPatchesForRig(scope.rig)
		if len(patchSet) == 0 {
			continue
		}
		parser := formula.NewParser(scope.paths...).WithPatches(patchSet...)
		for i := range cfg.FormulaPatches {
			sp := cfg.FormulaPatches[i]
			if !sp.appliesToRig(scope.rig) {
				continue
			}
			loaded, err := parser.LoadByName(sp.Patch.Formula)
			if err != nil {
				continue // target formula is not visible in this scope
			}
			resolvedSomewhere[i] = true
			if _, err := parser.Resolve(loaded); err != nil {
				return fmt.Errorf("patches.formula %q (%s): %w", sp.Patch.Formula, scope.label, err)
			}
		}
	}

	for i := range cfg.FormulaPatches {
		if !resolvedSomewhere[i] {
			return fmt.Errorf("patches.formula targets unknown formula %q: not found in any configured formula search path", cfg.FormulaPatches[i].Patch.Formula)
		}
	}
	return nil
}

// formulaSearchScope is one effective formula search-path set a runtime consumer
// resolves against: the city layers, or a single rig's layers. rig is the rig
// name the scope dispatches as ("" for the city scope); it selects the scope's
// active patch set via FormulaPatchesForRig.
type formulaSearchScope struct {
	rig   string
	label string
	paths []string
}

// effectiveFormulaSearchScopes returns the effective search-path sets runtime
// consumers resolve formulas against — the city layers and each rig's layers
// (mirroring FormulaLayers.SearchPaths). Empty path sets are dropped. Each rig
// is its own scope even when its paths match another's, because the active patch
// set is keyed by rig: two rigs with identical layers can still see different
// rig-scoped overlays, so collapsing them would skip validating one rig's
// overlay. Rig scopes are visited in sorted name order for deterministic error
// output.
func effectiveFormulaSearchScopes(fl FormulaLayers) []formulaSearchScope {
	nonEmptyPaths := func(paths []string) []string {
		out := make([]string, 0, len(paths))
		for _, p := range paths {
			if strings.TrimSpace(p) != "" {
				out = append(out, p)
			}
		}
		return out
	}

	var scopes []formulaSearchScope
	if paths := nonEmptyPaths(fl.City); len(paths) > 0 {
		scopes = append(scopes, formulaSearchScope{rig: "", label: "city scope", paths: paths})
	}
	rigNames := make([]string, 0, len(fl.Rigs))
	for name := range fl.Rigs {
		rigNames = append(rigNames, name)
	}
	sort.Strings(rigNames)
	for _, name := range rigNames {
		if paths := nonEmptyPaths(fl.Rigs[name]); len(paths) > 0 {
			scopes = append(scopes, formulaSearchScope{
				rig:   name,
				label: fmt.Sprintf("rig %q scope", name),
				paths: paths,
			})
		}
	}
	return scopes
}
