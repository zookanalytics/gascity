package config

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/formula"
)

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

// appendUniqueFormulaPatches appends src to dst, skipping any patch whose
// content is byte-identical (canonical JSON) to one already present. The same
// pack patch reaches collection more than once through diamond-shaped import
// graphs or several rigs importing the same pack; without dedup an
// [[...append_step]] would be applied twice and the second application would
// fail its own duplicate-id guard. Two genuinely different overlays of the
// same formula (different content) are both kept — a real conflict between
// them surfaces at apply time, which is correct.
func appendUniqueFormulaPatches(dst []formula.Patch, src ...formula.Patch) []formula.Patch {
	seen := make(map[string]bool, len(dst))
	for _, p := range dst {
		seen[formulaPatchKey(p)] = true
	}
	for _, p := range src {
		key := formulaPatchKey(p)
		if seen[key] {
			continue
		}
		seen[key] = true
		dst = append(dst, p)
	}
	return dst
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

// validateFormulaPatches fails config load when a collected [[patches.formula]]
// targets a formula that does not resolve in any scope, or overlays a step that
// does not exist in the formula as a runtime consumer would resolve it. A no-op
// when no patches were collected.
//
// Validation runs against each EFFECTIVE search-path set a runtime consumer
// resolves against (the city layers and every rig's layers, per
// FormulaLayers.SearchPaths), never against a single union of all layers. A
// union collapses same-named formulas from different rigs and resolves them
// with nondeterministic last-layer-wins precedence, so the patch could be
// validated against a different rig's formula than the one that will actually
// run — masking a genuinely broken overlay or rejecting a valid one. Checking
// each scope independently is deterministic and matches dispatch.
//
// A target that resolves in NO scope is an unknown-formula error. A target that
// resolves but the patch fails to apply in EVERY scope it resolves in is an
// apply error (the overlay cannot work anywhere). A target that applies cleanly
// in at least one scope is accepted: the same global patch set is applied in
// every rig at dispatch, and a same-named-but-incompatible formula in an
// unrelated scope must not reject an overlay that is valid where it is meant to
// run (see the gascity-keeper refinery overlay, whose patched mol-refinery
// formula is shipped by the keeper pack a single rig imports).
func validateFormulaPatches(cfg *City) error {
	if len(cfg.FormulaPatches) == 0 {
		return nil
	}
	scopes := effectiveFormulaSearchScopes(cfg.FormulaLayers)
	if len(scopes) == 0 {
		return fmt.Errorf("config declares [[patches.formula]] but no formula search paths are configured")
	}

	// Distinct target names, first-seen order, for deterministic error output.
	seen := make(map[string]bool, len(cfg.FormulaPatches))
	var targets []string
	for _, p := range cfg.FormulaPatches {
		if seen[p.Formula] {
			continue
		}
		seen[p.Formula] = true
		targets = append(targets, p.Formula)
	}

	for _, target := range targets {
		resolvedInScope := false
		appliedCleanly := false
		var applyErr error
		var applyScope string
		for _, scope := range scopes {
			parser := formula.NewParser(scope.paths...).WithPatches(cfg.FormulaPatches...)
			loaded, err := parser.LoadByName(target)
			if err != nil {
				continue // target formula is not visible in this scope
			}
			resolvedInScope = true
			if _, err := parser.Resolve(loaded); err != nil {
				applyErr = err
				applyScope = scope.label
				continue
			}
			appliedCleanly = true
			break
		}
		if !resolvedInScope {
			return fmt.Errorf("patches.formula targets unknown formula %q: not found in any configured formula search path", target)
		}
		if !appliedCleanly {
			return fmt.Errorf("patches.formula %q (%s): %w", target, applyScope, applyErr)
		}
	}
	return nil
}

// formulaSearchScope is one effective formula search-path set a runtime
// consumer resolves against: the city layers, or a single rig's layers.
type formulaSearchScope struct {
	label string
	paths []string
}

// effectiveFormulaSearchScopes returns the distinct effective search-path sets
// runtime consumers resolve formulas against — the city layers and each rig's
// layers (mirroring FormulaLayers.SearchPaths). Empty path sets are dropped;
// byte-identical sets are de-duplicated so a rig that contributes no rig-local
// layers (its SearchPaths collapses to the city layers) is not validated twice.
// Rig scopes are visited in sorted name order for deterministic error output.
func effectiveFormulaSearchScopes(fl FormulaLayers) []formulaSearchScope {
	var scopes []formulaSearchScope
	seenSet := make(map[string]bool)
	add := func(label string, paths []string) {
		nonEmpty := make([]string, 0, len(paths))
		for _, p := range paths {
			if strings.TrimSpace(p) != "" {
				nonEmpty = append(nonEmpty, p)
			}
		}
		if len(nonEmpty) == 0 {
			return
		}
		key := strings.Join(nonEmpty, "\x00")
		if seenSet[key] {
			return
		}
		seenSet[key] = true
		scopes = append(scopes, formulaSearchScope{label: label, paths: nonEmpty})
	}
	add("city scope", fl.City)
	rigNames := make([]string, 0, len(fl.Rigs))
	for name := range fl.Rigs {
		rigNames = append(rigNames, name)
	}
	sort.Strings(rigNames)
	for _, name := range rigNames {
		add(fmt.Sprintf("rig %q scope", name), fl.Rigs[name])
	}
	return scopes
}
