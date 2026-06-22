package formula

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fpStr(s string) *string { return &s }

func baseFormulaForPatch() *Formula {
	return &Formula{
		Formula:     "mol-refinery-patrol",
		Description: "base description",
		Type:        TypeWorkflow,
		Vars: map[string]*VarDef{
			"existing_var": {Default: fpStr("base-default")},
		},
		Steps: []*Step{
			{ID: "load", Title: "Load context"},
			{ID: "merge", Title: "Merge the branch", Description: "base merge body"},
			{ID: "report", Title: "Report"},
		},
	}
}

func TestApplyFormulaPatch_OverrideStepPreservesPosition(t *testing.T) {
	base := baseFormulaForPatch()
	patch := &Patch{
		Formula: "mol-refinery-patrol",
		Steps: []*Step{
			{ID: "merge", Title: "Merge with ff-canonical", Description: "overlaid merge body"},
		},
	}

	out, err := ApplyPatch(base, patch)
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if len(out.Steps) != 3 {
		t.Fatalf("step count = %d, want 3 (override must not append)", len(out.Steps))
	}
	if out.Steps[1].ID != "merge" {
		t.Errorf("overridden step moved: position 1 = %q, want \"merge\"", out.Steps[1].ID)
	}
	if out.Steps[1].Title != "Merge with ff-canonical" {
		t.Errorf("override title = %q, want overlaid title", out.Steps[1].Title)
	}
	if out.Steps[1].Description != "overlaid merge body" {
		t.Errorf("override description = %q, want overlaid body", out.Steps[1].Description)
	}
	// Base must be untouched.
	if base.Steps[1].Title != "Merge the branch" {
		t.Errorf("base formula was mutated: %q", base.Steps[1].Title)
	}
}

func TestApplyFormulaPatch_AppendStep(t *testing.T) {
	base := baseFormulaForPatch()
	patch := &Patch{
		Formula: "mol-refinery-patrol",
		AppendSteps: []*Step{
			{ID: "codex-gate", Title: "Codex review gate"},
		},
	}
	out, err := ApplyPatch(base, patch)
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if len(out.Steps) != 4 {
		t.Fatalf("step count = %d, want 4", len(out.Steps))
	}
	if out.Steps[3].ID != "codex-gate" {
		t.Errorf("appended step = %q at end, want \"codex-gate\"", out.Steps[3].ID)
	}
}

func TestApplyFormulaPatch_VarsAddAndOverride(t *testing.T) {
	base := baseFormulaForPatch()
	patch := &Patch{
		Formula: "mol-refinery-patrol",
		Vars: map[string]*VarDef{
			"existing_var": {Default: fpStr("overlaid-default")},
			"new_var":      {Default: fpStr("brand-new")},
		},
	}
	out, err := ApplyPatch(base, patch)
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if got := out.Vars["existing_var"].Default; got == nil || *got != "overlaid-default" {
		t.Errorf("existing_var default = %v, want overlaid-default", got)
	}
	if got := out.Vars["new_var"].Default; got == nil || *got != "brand-new" {
		t.Errorf("new_var default = %v, want brand-new", got)
	}
	// Base var must be untouched.
	if got := base.Vars["existing_var"].Default; got == nil || *got != "base-default" {
		t.Errorf("base var mutated: %v", got)
	}
}

func TestApplyFormulaPatch_ComposeMerge(t *testing.T) {
	base := baseFormulaForPatch()
	base.Compose = &ComposeRules{
		BondPoints: []*BondPoint{{ID: "bp1"}},
	}
	patch := &Patch{
		Formula: "mol-refinery-patrol",
		Compose: &ComposeRules{
			BondPoints: []*BondPoint{{ID: "bp2"}},
		},
	}
	out, err := ApplyPatch(base, patch)
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if out.Compose == nil || len(out.Compose.BondPoints) != 2 {
		t.Fatalf("compose bond points = %v, want 2 merged", out.Compose)
	}
}

func TestApplyFormulaPatch_DescriptionOverride(t *testing.T) {
	base := baseFormulaForPatch()
	patch := &Patch{Formula: "mol-refinery-patrol", Description: "overlaid description"}
	out, err := ApplyPatch(base, patch)
	if err != nil {
		t.Fatalf("ApplyPatch: %v", err)
	}
	if out.Description != "overlaid description" {
		t.Errorf("description = %q, want overlaid", out.Description)
	}
}

func TestApplyFormulaPatch_UnknownStepIDErrors(t *testing.T) {
	base := baseFormulaForPatch()
	patch := &Patch{
		Formula: "mol-refinery-patrol",
		Steps:   []*Step{{ID: "does-not-exist", Title: "x"}},
	}
	_, err := ApplyPatch(base, patch)
	if err == nil {
		t.Fatal("expected error for unknown step id, got nil")
	}
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Errorf("error %q should name the unknown step id", err)
	}
}

func TestApplyFormulaPatch_AppendDuplicateIDErrors(t *testing.T) {
	base := baseFormulaForPatch()
	patch := &Patch{
		Formula:     "mol-refinery-patrol",
		AppendSteps: []*Step{{ID: "merge", Title: "dup"}},
	}
	_, err := ApplyPatch(base, patch)
	if err == nil {
		t.Fatal("expected error appending a step whose id already exists, got nil")
	}
	if !strings.Contains(err.Error(), "merge") {
		t.Errorf("error %q should name the duplicate step id", err)
	}
}

func TestApplyFormulaPatch_NameMismatchErrors(t *testing.T) {
	base := baseFormulaForPatch()
	patch := &Patch{Formula: "mol-other", Description: "x"}
	_, err := ApplyPatch(base, patch)
	if err == nil {
		t.Fatal("expected error when patch target name != base name")
	}
}

// TestParserResolve_AppliesFormulaPatchByName proves the patch is applied at
// resolve time, after extends composition, keyed by the unchanged formula name.
func TestParserResolve_AppliesFormulaPatchByName(t *testing.T) {
	dir := t.TempDir()
	writeLayerFile(t, dir, "mol-refinery-patrol.toml", `formula = "mol-refinery-patrol"
description = "base"

[[steps]]
id = "load"
title = "Load context"

[[steps]]
id = "merge"
title = "Merge the branch"
`)

	patch := Patch{
		Formula: "mol-refinery-patrol",
		Steps:   []*Step{{ID: "merge", Title: "Merge with ff-canonical"}},
		AppendSteps: []*Step{
			{ID: "codex-gate", Title: "Codex review gate"},
		},
		Vars: map[string]*VarDef{"review_required": {Default: fpStr("true")}},
	}

	parser := NewParser(dir).WithPatches(patch)
	loaded, err := parser.LoadByName("mol-refinery-patrol")
	if err != nil {
		t.Fatalf("LoadByName: %v", err)
	}
	resolved, err := parser.Resolve(loaded)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// Name MUST be unchanged — that is the whole point of the overlay.
	if resolved.Formula != "mol-refinery-patrol" {
		t.Fatalf("formula name changed to %q", resolved.Formula)
	}
	if len(resolved.Steps) != 3 {
		t.Fatalf("steps = %d, want 3 (2 base + 1 appended)", len(resolved.Steps))
	}
	if resolved.Steps[1].Title != "Merge with ff-canonical" {
		t.Errorf("override not applied: step[1].title = %q", resolved.Steps[1].Title)
	}
	if resolved.Steps[2].ID != "codex-gate" {
		t.Errorf("append not applied: step[2].id = %q", resolved.Steps[2].ID)
	}
	if _, ok := resolved.Vars["review_required"]; !ok {
		t.Errorf("var not added: %v", resolved.Vars)
	}
}

// TestParserResolve_LeavesNonTargetedFormulasUntouched ensures a patch only
// touches the formula it names.
func TestParserResolve_LeavesNonTargetedFormulasUntouched(t *testing.T) {
	dir := t.TempDir()
	writeLayerFile(t, dir, "mol-other.toml", `formula = "mol-other"
[[steps]]
id = "only"
title = "Only step"
`)
	patch := Patch{
		Formula: "mol-refinery-patrol",
		Steps:   []*Step{{ID: "anything", Title: "x"}},
	}
	parser := NewParser(dir).WithPatches(patch)
	loaded, err := parser.LoadByName("mol-other")
	if err != nil {
		t.Fatalf("LoadByName: %v", err)
	}
	resolved, err := parser.Resolve(loaded)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(resolved.Steps) != 1 || resolved.Steps[0].ID != "only" {
		t.Errorf("non-targeted formula was modified: %+v", resolved.Steps)
	}
}

// TestParserResolve_MultiplePatchesLastWins documents precedence: patches are
// applied in order; the later patch wins on an overlapping step override.
func TestParserResolve_MultiplePatchesLastWins(t *testing.T) {
	dir := t.TempDir()
	writeLayerFile(t, dir, "mol-p.toml", `formula = "mol-p"
[[steps]]
id = "s"
title = "original"
`)
	parser := NewParser(dir).WithPatches(
		Patch{Formula: "mol-p", Steps: []*Step{{ID: "s", Title: "first"}}},
		Patch{Formula: "mol-p", Steps: []*Step{{ID: "s", Title: "second"}}},
	)
	loaded, _ := parser.LoadByName("mol-p")
	resolved, err := parser.Resolve(loaded)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.Steps[0].Title != "second" {
		t.Errorf("precedence wrong: title = %q, want \"second\" (last patch wins)", resolved.Steps[0].Title)
	}
}

// TestParserResolve_UnknownStepIDErrorsAtResolve surfaces the hard error when a
// patch overrides a step that does not exist in the resolved base.
func TestParserResolve_UnknownStepIDErrorsAtResolve(t *testing.T) {
	dir := t.TempDir()
	writeLayerFile(t, dir, "mol-q.toml", `formula = "mol-q"
[[steps]]
id = "real"
title = "Real"
`)
	parser := NewParser(dir).WithPatches(
		Patch{Formula: "mol-q", Steps: []*Step{{ID: "ghost", Title: "x"}}},
	)
	loaded, _ := parser.LoadByName("mol-q")
	if _, err := parser.Resolve(loaded); err == nil {
		t.Fatal("expected resolve error for unknown step id in patch")
	}
}

// TestParserResolve_PatchAfterExtends proves the patch overlays the fully
// resolved (extends-merged) formula, not the raw child.
func TestParserResolve_PatchAfterExtends(t *testing.T) {
	dir := t.TempDir()
	writeLayerFile(t, dir, "mol-base.toml", `formula = "mol-base"
[[steps]]
id = "inherited"
title = "Inherited step"
`)
	writeLayerFile(t, dir, "mol-child.toml", `formula = "mol-child"
extends = ["mol-base"]
[[steps]]
id = "own"
title = "Own step"
`)
	// Patch overrides a step that only exists because of extends.
	parser := NewParser(dir).WithPatches(
		Patch{Formula: "mol-child", Steps: []*Step{{ID: "inherited", Title: "Overlaid inherited"}}},
	)
	loaded, _ := parser.LoadByName("mol-child")
	resolved, err := parser.Resolve(loaded)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var got string
	for _, s := range resolved.Steps {
		if s.ID == "inherited" {
			got = s.Title
		}
	}
	if got != "Overlaid inherited" {
		t.Errorf("patch did not overlay the extends-inherited step: %q", got)
	}
}

// TestResolvePatchDescriptionFiles_InlinesPackLocalFile proves a patch step's
// description_file is read relative to the pack directory and inlined into the
// step's Description, mirroring how ParseFile handles a formula file's prompts.
func TestResolvePatchDescriptionFiles_InlinesPackLocalFile(t *testing.T) {
	packDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(packDir, "merge-prompt.md"), []byte("PACK LOCAL MERGE PROMPT"), 0o644); err != nil {
		t.Fatal(err)
	}
	patch := &Patch{
		Formula:     "mol-refinery-patrol",
		Steps:       []*Step{{ID: "merge", DescriptionFile: "merge-prompt.md"}},
		AppendSteps: []*Step{{ID: "gate", DescriptionFile: "merge-prompt.md"}},
	}
	parser := NewParser(filepath.Join(packDir, "formulas"))
	if err := parser.ResolvePatchDescriptionFiles(patch, packDir); err != nil {
		t.Fatalf("ResolvePatchDescriptionFiles: %v", err)
	}
	for _, s := range append(patch.Steps, patch.AppendSteps...) {
		if s.DescriptionFile != "" {
			t.Errorf("step %q: description_file not consumed: %q", s.ID, s.DescriptionFile)
		}
		if s.Description != "PACK LOCAL MERGE PROMPT" {
			t.Errorf("step %q: description = %q, want inlined pack-local body", s.ID, s.Description)
		}
	}
}

// TestResolvePatchDescriptionFiles_MissingFileIsStrictError proves a patch step
// naming a description_file that does not resolve is a hard error, not a
// silently dropped prompt body — patch steps get no later parse pass to surface
// the omission.
func TestResolvePatchDescriptionFiles_MissingFileIsStrictError(t *testing.T) {
	packDir := t.TempDir()
	patch := &Patch{
		Formula:     "mol-refinery-patrol",
		AppendSteps: []*Step{{ID: "gate", DescriptionFile: "does-not-exist.md"}},
	}
	parser := NewParser(filepath.Join(packDir, "formulas"))
	err := parser.ResolvePatchDescriptionFiles(patch, packDir)
	if err == nil {
		t.Fatal("expected strict error for missing description_file")
	}
	if !strings.Contains(err.Error(), "does-not-exist.md") {
		t.Errorf("error %q should name the missing file", err)
	}
}

// TestResolvePatchDescriptionFiles_NoFilesIsNoOp proves the common overlay that
// only adjusts titles/vars is untouched (no spurious error, no mutation).
func TestResolvePatchDescriptionFiles_NoFilesIsNoOp(t *testing.T) {
	patch := &Patch{
		Formula: "mol-refinery-patrol",
		Steps:   []*Step{{ID: "merge", Title: "Merge with ff-canonical"}},
	}
	parser := NewParser(filepath.Join(t.TempDir(), "formulas"))
	if err := parser.ResolvePatchDescriptionFiles(patch, t.TempDir()); err != nil {
		t.Fatalf("no-op resolve errored: %v", err)
	}
	if patch.Steps[0].Title != "Merge with ff-canonical" || patch.Steps[0].Description != "" {
		t.Errorf("step mutated unexpectedly: %+v", patch.Steps[0])
	}
}
