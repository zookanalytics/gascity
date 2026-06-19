package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSplitPackQualifiedPath(t *testing.T) {
	cases := []struct {
		in       string
		wantPack string
		wantSub  string
		wantOK   bool
	}{
		{"gastown//agents/mayor/prompt.template.md", "gastown", "agents/mayor/prompt.template.md", true},
		{"gastown//agents//mayor.md", "gastown", "agents//mayor.md", true},
		{"//.gc/system/packs/gastown/x", "", "", false},    // city-root, not pack-qualified
		{"agents/mayor/prompt.template.md", "", "", false}, // plain relative
		{"/abs/path/prompt.md", "", "", false},             // absolute
		{"", "", "", false},                                // empty
		{"https://example.com//x", "", "", false},          // URL (pack part has ':')
		{"gastown//", "", "", false},                       // empty subpath
		{"//", "", "", false},                              // bare city-root
	}
	for _, tc := range cases {
		gotPack, gotSub, gotOK := splitPackQualifiedPath(tc.in)
		if gotPack != tc.wantPack || gotSub != tc.wantSub || gotOK != tc.wantOK {
			t.Errorf("splitPackQualifiedPath(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.in, gotPack, gotSub, gotOK, tc.wantPack, tc.wantSub, tc.wantOK)
		}
	}
}

// writeGastownPackDir creates a minimal pack directory named "gastown" and
// returns its path.
func writeGastownPackDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pack.toml"),
		[]byte("[pack]\nname = \"gastown\"\nschema = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestResolvePackQualifiedAgentPaths(t *testing.T) {
	gastownDir := writeGastownPackDir(t)
	wantPrompt := filepath.Join(gastownDir, "agents", "mayor", "prompt.template.md")

	cfg := &City{
		PackDirs: []string{gastownDir},
		Agents: []Agent{
			{Name: "mayor-thread", PromptTemplate: "gastown//agents/mayor/prompt.template.md"},
			{Name: "native", PromptTemplate: "agents/native/prompt.template.md"}, // untouched
		},
	}
	if err := resolvePackQualifiedAgentPaths(cfg); err != nil {
		t.Fatalf("resolvePackQualifiedAgentPaths: %v", err)
	}
	if cfg.Agents[0].PromptTemplate != wantPrompt {
		t.Errorf("mayor-thread prompt = %q, want %q", cfg.Agents[0].PromptTemplate, wantPrompt)
	}
	if cfg.Agents[1].PromptTemplate != "agents/native/prompt.template.md" {
		t.Errorf("non-pack-qualified path was rewritten: %q", cfg.Agents[1].PromptTemplate)
	}
}

func TestResolvePackQualifiedAgentPaths_UnknownPack(t *testing.T) {
	cfg := &City{
		PackDirs: []string{writeGastownPackDir(t)},
		Agents: []Agent{
			{Name: "thread", PromptTemplate: "nope//agents/mayor/prompt.template.md"},
		},
	}
	if err := resolvePackQualifiedAgentPaths(cfg); err == nil {
		t.Fatal("expected error for unknown pack name, got nil")
	}
}

func TestResolvePackQualifiedAgentPaths_AmbiguousPack(t *testing.T) {
	// Two distinct dirs both named "gastown" (e.g. different pins across
	// rigs) must error rather than silently pick one.
	cfg := &City{
		PackDirs:    []string{writeGastownPackDir(t)},
		RigPackDirs: map[string][]string{"r": {writeGastownPackDir(t)}},
		Agents: []Agent{
			{Name: "thread", PromptTemplate: "gastown//agents/mayor/prompt.template.md"},
		},
	}
	if err := resolvePackQualifiedAgentPaths(cfg); err == nil {
		t.Fatal("expected error for ambiguous pack name, got nil")
	}
}
