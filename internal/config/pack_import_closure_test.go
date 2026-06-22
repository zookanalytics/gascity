package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

// writePackWithAsset creates an imported-pack directory named `name`,
// optionally materializes `asset` (a slash-separated subpath) under it with
// placeholder content, and returns the pack directory.
func writePackWithAsset(t *testing.T, name, asset string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pack.toml"),
		[]byte("[pack]\nname = \""+name+"\"\nschema = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if asset != "" {
		full := filepath.Join(dir, filepath.FromSlash(asset))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("body\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// A plain relative prompt_template that does not resolve city-root-relative
// falls through to the imported-pack closure and is rewritten to the
// absolute path inside the single pack that contains it.
func TestResolveAgentImportClosurePaths_Fallthrough(t *testing.T) {
	const sub = "agents/polecat/prompt.template.md"
	packDir := writePackWithAsset(t, "gastown", sub)
	cityRoot := t.TempDir() // empty: sub does not exist city-root-relative
	want := filepath.Join(packDir, filepath.FromSlash(sub))

	cfg := &City{
		PackDirs: []string{packDir},
		Agents:   []Agent{{Name: "polecat-codex", PromptTemplate: sub}},
	}
	if err := resolveAgentImportClosurePaths(fsys.OSFS{}, cfg, cityRoot); err != nil {
		t.Fatalf("resolveAgentImportClosurePaths: %v", err)
	}
	if got := cfg.Agents[0].PromptTemplate; got != want {
		t.Errorf("prompt_template = %q, want %q", got, want)
	}
}

// Precedence: city-root-relative wins over the import closure. When the same
// subpath exists both city-root-relative and in a pack, the path is left
// unchanged (it renders against the city root, as before).
func TestResolveAgentImportClosurePaths_CityRootPrecedence(t *testing.T) {
	const sub = "agents/polecat/prompt.template.md"
	packDir := writePackWithAsset(t, "gastown", sub)
	cityRoot := t.TempDir()
	cityFull := filepath.Join(cityRoot, filepath.FromSlash(sub))
	if err := os.MkdirAll(filepath.Dir(cityFull), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cityFull, []byte("city body\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &City{
		PackDirs: []string{packDir},
		Agents:   []Agent{{Name: "polecat", PromptTemplate: sub}},
	}
	if err := resolveAgentImportClosurePaths(fsys.OSFS{}, cfg, cityRoot); err != nil {
		t.Fatalf("resolveAgentImportClosurePaths: %v", err)
	}
	if got := cfg.Agents[0].PromptTemplate; got != sub {
		t.Errorf("city-root path rewritten: got %q, want unchanged %q", got, sub)
	}
}

// A subpath present in more than one imported pack is ambiguous and must be a
// hard config-load error rather than an arbitrary silent pick.
func TestResolveAgentImportClosurePaths_Ambiguous(t *testing.T) {
	const sub = "agents/polecat/prompt.template.md"
	packA := writePackWithAsset(t, "gastown", sub)
	packB := writePackWithAsset(t, "other", sub)
	cityRoot := t.TempDir()

	cfg := &City{
		PackDirs:    []string{packA},
		RigPackDirs: map[string][]string{"r": {packB}},
		Agents:      []Agent{{Name: "polecat", PromptTemplate: sub}},
	}
	if err := resolveAgentImportClosurePaths(fsys.OSFS{}, cfg, cityRoot); err == nil {
		t.Fatal("expected ambiguity error for subpath in multiple packs, got nil")
	}
}

// A relative path found neither city-root-relative nor in any imported pack is
// left unchanged (no error). This preserves the prior graceful behavior where
// an unreachable prompt simply renders empty; the change is purely additive.
func TestResolveAgentImportClosurePaths_NotFoundLeftUnchanged(t *testing.T) {
	const sub = "agents/missing/prompt.template.md"
	// The pack carries a different asset, so the requested subpath is absent
	// even though the pack itself is non-empty.
	packDir := writePackWithAsset(t, "gastown", "agents/other/present.template.md")
	cityRoot := t.TempDir()

	cfg := &City{
		PackDirs: []string{packDir},
		Agents:   []Agent{{Name: "native", PromptTemplate: sub}},
	}
	if err := resolveAgentImportClosurePaths(fsys.OSFS{}, cfg, cityRoot); err != nil {
		t.Fatalf("unexpected error for unresolved path: %v", err)
	}
	if got := cfg.Agents[0].PromptTemplate; got != sub {
		t.Errorf("unresolved path changed: got %q, want unchanged %q", got, sub)
	}
}

// Absolute paths are already resolved (e.g. by the "<pack>//<subpath>" pass or
// supplied absolute by the user) and must be skipped.
func TestResolveAgentImportClosurePaths_AbsoluteSkipped(t *testing.T) {
	const sub = "agents/polecat/prompt.template.md"
	packDir := writePackWithAsset(t, "gastown", sub)
	abs := filepath.Join(packDir, filepath.FromSlash(sub))
	cityRoot := t.TempDir()

	cfg := &City{
		PackDirs: []string{packDir},
		Agents:   []Agent{{Name: "x", PromptTemplate: abs}},
	}
	if err := resolveAgentImportClosurePaths(fsys.OSFS{}, cfg, cityRoot); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := cfg.Agents[0].PromptTemplate; got != abs {
		t.Errorf("absolute path changed: got %q, want %q", got, abs)
	}
}

// Coverage parity with the "<pack>//<subpath>" pass: prompt_template,
// overlay_dir, and namepool all fall through to the closure.
func TestResolveAgentImportClosurePaths_AllFields(t *testing.T) {
	const (
		promptSub  = "agents/polecat/prompt.template.md"
		overlaySub = "agents/polecat/overlay"
		poolSub    = "agents/polecat/names.txt"
	)
	packDir := writePackWithAsset(t, "gastown", promptSub)
	if err := os.MkdirAll(filepath.Join(packDir, filepath.FromSlash(overlaySub)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packDir, filepath.FromSlash(poolSub)), []byte("alpha\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cityRoot := t.TempDir()

	cfg := &City{
		PackDirs: []string{packDir},
		Agents: []Agent{{
			Name:           "polecat",
			PromptTemplate: promptSub,
			OverlayDir:     overlaySub,
			Namepool:       poolSub,
		}},
	}
	if err := resolveAgentImportClosurePaths(fsys.OSFS{}, cfg, cityRoot); err != nil {
		t.Fatalf("resolveAgentImportClosurePaths: %v", err)
	}
	a := cfg.Agents[0]
	if want := filepath.Join(packDir, filepath.FromSlash(promptSub)); a.PromptTemplate != want {
		t.Errorf("prompt_template = %q, want %q", a.PromptTemplate, want)
	}
	if want := filepath.Join(packDir, filepath.FromSlash(overlaySub)); a.OverlayDir != want {
		t.Errorf("overlay_dir = %q, want %q", a.OverlayDir, want)
	}
	if want := filepath.Join(packDir, filepath.FromSlash(poolSub)); a.Namepool != want {
		t.Errorf("namepool = %q, want %q", a.Namepool, want)
	}
}

// End-to-end through LoadWithIncludes: a native city agent that reuses an
// imported pack's prompt via a plain relative path resolves against the
// import closure. This mirrors the real "second polecat pool reusing the
// imported gastown prompt" scenario and guards the compose-pipeline wiring
// (the resolveAgentImportClosurePaths call), which the direct unit tests
// above do not exercise.
func TestLoadWithIncludes_AgentReusesImportedPromptViaRelativePath(t *testing.T) {
	dir := t.TempDir()
	cityDir := filepath.Join(dir, "city")
	gastownDir := filepath.Join(dir, "gastown")
	mustMkdirAll(t, gastownDir, 0o755)

	const sub = "agents/polecat/prompt.template.md"
	writeTestFile(t, gastownDir, "pack.toml", `
[pack]
name = "gastown"
schema = 2

[[agent]]
name = "polecat"
prompt_template = "agents/polecat/prompt.template.md"
scope = "city"
`)
	writeTestFile(t, gastownDir, sub, "# imported polecat prompt\n")

	// A native city agent referencing the imported prompt by a PLAIN relative
	// path (no "<pack>//" token). The file does not exist city-root-relative.
	writeTestFile(t, cityDir, "city.toml", `
[workspace]
name = "test"

[imports.gastown]
source = "../gastown"

[[agent]]
name = "polecat-codex"
prompt_template = "agents/polecat/prompt.template.md"
scope = "city"
`)

	cfg, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityDir, "city.toml"))
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}

	want := filepath.Join(gastownDir, filepath.FromSlash(sub))
	var found *Agent
	for i := range cfg.Agents {
		if cfg.Agents[i].Name == "polecat-codex" {
			found = &cfg.Agents[i]
			break
		}
	}
	if found == nil {
		t.Fatal("native agent polecat-codex not found")
	}
	if found.PromptTemplate != want {
		t.Errorf("polecat-codex prompt_template = %q, want %q (resolved against import closure)",
			found.PromptTemplate, want)
	}
}
