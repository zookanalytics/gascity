package config

import (
	"encoding/json"
	"fmt"
	"path/filepath"

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
// targets a formula that does not resolve, or overlays a step that does not
// exist. It resolves each distinct target once, with the patches applied, over
// the union of all formula search layers. A no-op when no patches were
// collected.
func validateFormulaPatches(cfg *City) error {
	if len(cfg.FormulaPatches) == 0 {
		return nil
	}
	paths := unionFormulaSearchPaths(cfg.FormulaLayers)
	if len(paths) == 0 {
		return fmt.Errorf("config declares [[patches.formula]] but no formula search paths are configured")
	}

	parser := formula.NewParser(paths...).WithPatches(cfg.FormulaPatches...)
	seen := make(map[string]bool, len(cfg.FormulaPatches))
	for _, p := range cfg.FormulaPatches {
		if seen[p.Formula] {
			continue
		}
		seen[p.Formula] = true
		loaded, err := parser.LoadByName(p.Formula)
		if err != nil {
			return fmt.Errorf("patches.formula targets unknown formula %q: %w", p.Formula, err)
		}
		if _, err := parser.Resolve(loaded); err != nil {
			return fmt.Errorf("patches.formula %q: %w", p.Formula, err)
		}
	}
	return nil
}

// unionFormulaSearchPaths returns the deduplicated union of every scope's
// formula layers (city and all rigs), preserving first-seen order. A patch may
// target a city- or a rig-scoped formula, so existence must be checked against
// all layers.
func unionFormulaSearchPaths(fl FormulaLayers) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(paths []string) {
		for _, p := range paths {
			if p == "" || seen[p] {
				continue
			}
			seen[p] = true
			out = append(out, p)
		}
	}
	add(fl.City)
	for _, paths := range fl.Rigs {
		add(paths)
	}
	return out
}
