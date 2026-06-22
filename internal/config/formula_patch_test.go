package config

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/formula"
	"github.com/gastownhall/gascity/internal/fsys"
)

func fpStr(s string) *string { return &s }

// keeperPack writes a pack that ships a formula and overlays it BY NAME via
// [[patches.formula]] — the gascity-keeper shape: a rig-scoped sub-pack
// adjusting a name-pinned formula without renaming or copying it.
func keeperPack(t *testing.T, dir string) {
	t.Helper()
	writeFile(t, dir, "packs/keeper/pack.toml", `
[pack]
name = "keeper"
version = "1.0.0"
schema = 1

[[patches.formula]]
formula = "mol-refinery-patrol"

[[patches.formula.step]]
id = "merge"
title = "Merge with ff-canonical"

[[patches.formula.append_step]]
id = "codex-gate"
title = "Codex review gate"

[patches.formula.vars]
review_required = "true"
`)
	writeFile(t, dir, "packs/keeper/formulas/mol-refinery-patrol.toml", `formula = "mol-refinery-patrol"
description = "base patrol"

[[steps]]
id = "load"
title = "Load context"

[[steps]]
id = "merge"
title = "Merge the branch"
`)
}

func TestPackPatches_FormulaDecodes(t *testing.T) {
	data := []byte(`
[pack]
name = "keeper"
version = "1.0.0"
schema = 1

[[patches.formula]]
formula = "mol-refinery-patrol"

[[patches.formula.step]]
id = "merge"
title = "Merge with ff-canonical"
`)
	cfg, warnings, err := parsePackConfigWithMeta(data, "keeper/pack.toml")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Patches.IsEmpty() {
		t.Fatal("PackPatches.IsEmpty() = true, want false (a formula patch is present)")
	}
	if len(cfg.Patches.Formulas) != 1 {
		t.Fatalf("got %d formula patches, want 1", len(cfg.Patches.Formulas))
	}
	p := cfg.Patches.Formulas[0]
	if p.Formula != "mol-refinery-patrol" {
		t.Errorf("target = %q, want mol-refinery-patrol", p.Formula)
	}
	if len(p.Steps) != 1 || p.Steps[0].ID != "merge" || p.Steps[0].Title != "Merge with ff-canonical" {
		t.Errorf("override step decoded wrong: %+v", p.Steps)
	}
	// [[patches.formula]] must NOT surface as an unknown-field warning.
	for _, w := range warnings {
		if strings.Contains(w, "patches") || strings.Contains(w, "formula") {
			t.Errorf("unexpected undecoded warning for patches.formula: %q", w)
		}
	}
}

func TestExpandPacks_CollectsRigPackFormulaPatches(t *testing.T) {
	dir := t.TempDir()
	keeperPack(t, dir)

	cfg := &City{
		Rigs: []Rig{
			{Name: "gascity", Path: "/work/gascity", Includes: []string{"packs/keeper"}},
		},
	}
	rigFormulaDirs := map[string][]string{}
	if err := ExpandPacks(cfg, fsys.OSFS{}, dir, rigFormulaDirs); err != nil {
		t.Fatalf("ExpandPacks: %v", err)
	}
	// A rig-scoped pack's [[patches.formula]] must reach the city collection.
	if len(cfg.FormulaPatches) != 1 {
		t.Fatalf("got %d collected formula patches, want 1", len(cfg.FormulaPatches))
	}
	if cfg.FormulaPatches[0].Formula != "mol-refinery-patrol" {
		t.Errorf("collected patch target = %q", cfg.FormulaPatches[0].Formula)
	}
}

func TestLoadWithIncludes_FormulaPatchOverlaysImportedFormula(t *testing.T) {
	dir := t.TempDir()
	keeperPack(t, dir)
	writeFile(t, dir, "city.toml", `
[workspace]
name = "test-city"

[[agent]]
name = "mayor"

[[rigs]]
name = "gascity"
path = "/work/gascity"
includes = ["packs/keeper"]
`)

	cfg, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(dir, "city.toml"))
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}
	if len(cfg.FormulaPatches) != 1 {
		t.Fatalf("collected %d patches, want 1", len(cfg.FormulaPatches))
	}

	// Resolve the formula the way a name-pinned consumer would, with the
	// collected patches applied.
	paths := cfg.FormulaLayers.SearchPaths("gascity")
	parser := formula.NewParser(paths...).WithPatches(cfg.FormulaPatches...)
	loaded, err := parser.LoadByName("mol-refinery-patrol")
	if err != nil {
		t.Fatalf("LoadByName: %v", err)
	}
	resolved, err := parser.Resolve(loaded)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.Formula != "mol-refinery-patrol" {
		t.Fatalf("name changed to %q — overlay must keep the name", resolved.Formula)
	}
	if len(resolved.Steps) != 3 {
		t.Fatalf("steps = %d, want 3 (2 base + 1 appended)", len(resolved.Steps))
	}
	var mergeTitle, lastID string
	for _, s := range resolved.Steps {
		if s.ID == "merge" {
			mergeTitle = s.Title
		}
	}
	lastID = resolved.Steps[len(resolved.Steps)-1].ID
	if mergeTitle != "Merge with ff-canonical" {
		t.Errorf("override not applied: merge title = %q", mergeTitle)
	}
	if lastID != "codex-gate" {
		t.Errorf("append not applied: last step = %q", lastID)
	}
	if _, ok := resolved.Vars["review_required"]; !ok {
		t.Errorf("var not added: %v", resolved.Vars)
	}
}

func TestLoadWithIncludes_FormulaPatchUnknownStepFailsLoad(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/keeper/pack.toml", `
[pack]
name = "keeper"
version = "1.0.0"
schema = 1

[[patches.formula]]
formula = "mol-refinery-patrol"

[[patches.formula.step]]
id = "ghost-step"
title = "does not exist in base"
`)
	writeFile(t, dir, "packs/keeper/formulas/mol-refinery-patrol.toml", `formula = "mol-refinery-patrol"
[[steps]]
id = "load"
title = "Load context"
`)
	writeFile(t, dir, "city.toml", `
[workspace]
name = "test-city"

[[rigs]]
name = "gascity"
path = "/work/gascity"
includes = ["packs/keeper"]
`)
	_, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(dir, "city.toml"))
	if err == nil {
		t.Fatal("expected load to fail: patch overrides a step that does not exist")
	}
	if !strings.Contains(err.Error(), "ghost-step") {
		t.Errorf("error %q should name the unknown step", err)
	}
}

func TestLoadWithIncludes_FormulaPatchUnknownFormulaFailsLoad(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/keeper/pack.toml", `
[pack]
name = "keeper"
version = "1.0.0"
schema = 1

[[patches.formula]]
formula = "mol-does-not-exist"
description = "overlay of a missing formula"
`)
	// A formula in the layer so search paths are non-empty, but NOT the target.
	writeFile(t, dir, "packs/keeper/formulas/mol-present.toml", `formula = "mol-present"
[[steps]]
id = "s"
title = "Step"
`)
	writeFile(t, dir, "city.toml", `
[workspace]
name = "test-city"

[[rigs]]
name = "gascity"
path = "/work/gascity"
includes = ["packs/keeper"]
`)
	_, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(dir, "city.toml"))
	if err == nil {
		t.Fatal("expected load to fail: patch targets an unknown formula")
	}
	if !strings.Contains(err.Error(), "mol-does-not-exist") {
		t.Errorf("error %q should name the unknown formula", err)
	}
}

func TestAppendUniqueFormulaPatches_Dedup(t *testing.T) {
	p := formula.Patch{
		Formula:     "mol-x",
		AppendSteps: []*formula.Step{{ID: "added", Title: "Added"}},
	}
	// Same patch arriving twice (diamond / multi-rig) must collapse to one;
	// applying an append twice would otherwise fail the duplicate-id guard.
	out := appendUniqueFormulaPatches(nil, p, p)
	if len(out) != 1 {
		t.Fatalf("dedup: got %d, want 1", len(out))
	}

	// A genuinely different overlay of the same formula is kept.
	other := formula.Patch{Formula: "mol-x", Vars: map[string]*formula.VarDef{"v": {Default: fpStr("1")}}}
	out = appendUniqueFormulaPatches(out, other)
	if len(out) != 2 {
		t.Fatalf("distinct overlays: got %d, want 2", len(out))
	}
}
