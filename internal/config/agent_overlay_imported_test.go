package config

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

// findAgent returns the single explicit agent with the given name, failing
// the test if zero or more than one match.
func findAgent(t *testing.T, agents []Agent, name string) Agent {
	t.Helper()
	var matches []Agent
	for _, a := range explicitAgents(agents) {
		if a.Name == name {
			matches = append(matches, a)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly 1 %q agent, got %d: %+v", name, len(matches), matches)
	}
	return matches[0]
}

// TestImportingPackOverlayAttachesToImportedAgent reproduces gc-5uepp /
// furiosa Scenario A: an importing pack ("toolkit") imports a pack that
// DEFINES agents ("base" defines mayor and polecat), customizes mayor with a
// bare-name [[patches.agent]], and carries an agents/mayor/skills/ overlay to
// scope a skill to the imported mayor only.
//
// Before the fix the agents/mayor/ overlay dir made DiscoverPackAgents
// manufacture a native toolkit.mayor that collided with the imported mayor —
// `packs define duplicate agent` aborted the entire city load.
//
// After the fix the overlay is attached as agent-local assets to the
// imported+patched mayor (no phantom mint, no collision); the imported
// mayor's SkillsDir/MCPDir point at the importing pack's overlay, and the
// sibling imported polecat — imported but neither patched nor overlaid — is
// left untouched.
func TestImportingPackOverlayAttachesToImportedAgent(t *testing.T) {
	t.Setenv("GC_HOME", t.TempDir())
	dir := t.TempDir()

	// base pack DEFINES mayor and polecat via the v2 convention.
	base := filepath.Join(dir, "base")
	writeTestFile(t, base, "pack.toml", `
[pack]
name = "base"
schema = 2
`)
	writeTestFile(t, base, "agents/mayor/agent.toml", `scope = "city"`)
	writeTestFile(t, base, "agents/mayor/prompt.template.md", `Base mayor.`)
	writeTestFile(t, base, "agents/polecat/agent.toml", `scope = "city"`)
	writeTestFile(t, base, "agents/polecat/prompt.template.md", `Base polecat.`)

	// toolkit pack imports base, bare-name-patches mayor, and carries an
	// agents/mayor/{skills,mcp}/ overlay scoping assets to mayor only.
	toolkit := filepath.Join(dir, "toolkit")
	writeTestFile(t, toolkit, "pack.toml", `
[pack]
name = "toolkit"
schema = 2

[imports.base]
source = "../base"

[[patches.agent]]
name = "mayor"
nudge = "go"
`)
	writeTestFile(t, toolkit, "agents/mayor/skills/git-merge-pull-request/SKILL.md", `# git-merge-pull-request`)
	writeTestFile(t, toolkit, "agents/mayor/mcp/private.toml", `command = ["helper-mcp"]`)

	// city imports toolkit.
	city := filepath.Join(dir, "city")
	writeTestFile(t, city, "city.toml", `
[workspace]
name = "test"

[imports.toolkit]
source = "../toolkit"
`)

	cfg, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(city, "city.toml"))
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v — an overlay on an imported+patched agent must not abort the load", err)
	}

	// Exactly one mayor (no phantom mint) carrying the importing pack's
	// overlay asset dirs.
	mayor := findAgent(t, cfg.Agents, "mayor")
	wantSkills := filepath.Join("toolkit", "agents", "mayor", "skills")
	if !strings.HasSuffix(mayor.SkillsDir, wantSkills) {
		t.Errorf("mayor SkillsDir = %q, want suffix %q (the importing pack's overlay)", mayor.SkillsDir, wantSkills)
	}
	wantMCP := filepath.Join("toolkit", "agents", "mayor", "mcp")
	if !strings.HasSuffix(mayor.MCPDir, wantMCP) {
		t.Errorf("mayor MCPDir = %q, want suffix %q (the importing pack's overlay)", mayor.MCPDir, wantMCP)
	}

	// The sibling imported polecat must NOT pick up mayor's overlay — the
	// skill is scoped to mayor only.
	polecat := findAgent(t, cfg.Agents, "polecat")
	if polecat.SkillsDir != "" {
		t.Errorf("polecat SkillsDir = %q, want empty — mayor's overlay must not leak to siblings", polecat.SkillsDir)
	}
}

// TestImportingPackOverlayWithoutPatchStillCollides locks in the
// conservative gate: an agents/<name>/ overlay that collides with an
// imported agent but carries NO customizing [[patches.agent]] is treated as
// a genuine duplicate-agent definition, not an attachment overlay. The load
// must still fail rather than silently masking the collision. (gc-5uepp)
func TestImportingPackOverlayWithoutPatchStillCollides(t *testing.T) {
	t.Setenv("GC_HOME", t.TempDir())
	dir := t.TempDir()

	base := filepath.Join(dir, "base")
	writeTestFile(t, base, "pack.toml", `
[pack]
name = "base"
schema = 2
`)
	writeTestFile(t, base, "agents/mayor/agent.toml", `scope = "city"`)
	writeTestFile(t, base, "agents/mayor/prompt.template.md", `Base mayor.`)

	// toolkit imports base and drops an agents/mayor/ dir but declares NO
	// patch for mayor — the importer has not signaled intent to customize
	// the imported agent, so this stays a genuine collision.
	toolkit := filepath.Join(dir, "toolkit")
	writeTestFile(t, toolkit, "pack.toml", `
[pack]
name = "toolkit"
schema = 2

[imports.base]
source = "../base"
`)
	writeTestFile(t, toolkit, "agents/mayor/skills/git-merge-pull-request/SKILL.md", `# git-merge-pull-request`)

	city := filepath.Join(dir, "city")
	writeTestFile(t, city, "city.toml", `
[workspace]
name = "test"

[imports.toolkit]
source = "../toolkit"
`)

	_, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(city, "city.toml"))
	if err == nil {
		t.Fatal("expected a duplicate-agent error for an unpatched overlay colliding with an import")
	}
	if !strings.Contains(err.Error(), "duplicate agent") {
		t.Errorf("error = %q, want it to mention 'duplicate agent'", err.Error())
	}
}

// TestImportedOverlayAttachTargetsKeyedByIdentity locks in the gc-fbt9c fix
// for codex finding 1: when two imported agents share a bare name and only
// one is patched (binding-qualified), the agents/<name>/ overlay must attach
// to the patched agent's identity ONLY. Keying attach targets by bare name
// leaked the importing pack's private skills/mcp onto the unpatched sibling.
//
// This exercises the keying functions directly. Through LoadWithIncludes two
// same-named imports re-bind to a single outer binding and collide as a
// genuine duplicate before the leak is observable, so the bug lives at the
// function boundary — where the bindings are still distinct.
func TestImportedOverlayAttachTargetsKeyedByIdentity(t *testing.T) {
	packDir := t.TempDir()
	// The importing pack carries an agents/mayor/{skills,mcp}/ overlay.
	writeTestFile(t, packDir, "agents/mayor/skills/git-merge-pull-request/SKILL.md", `# git-merge-pull-request`)
	writeTestFile(t, packDir, "agents/mayor/mcp/private.toml", `command = ["helper-mcp"]`)

	// Two imported agents share the bare name "mayor" but come from distinct
	// bindings; only base1.mayor is patched.
	imported := []Agent{
		{Name: "mayor", BindingName: "base1", SourceDir: filepath.Join(packDir, "base1")},
		{Name: "mayor", BindingName: "base2", SourceDir: filepath.Join(packDir, "base2")},
	}
	patches := []AgentPatch{{Name: "base1.mayor"}}

	fs := fsys.OSFS{}
	targets := importedOverlayAttachTargets(fs, imported, patches, packDir)
	attachImportedAgentOverlays(fs, imported, targets, packDir)

	// base1.mayor (patched) receives the importing pack's overlay.
	wantSkills := filepath.Join(packDir, "agents", "mayor", "skills")
	if imported[0].SkillsDir != wantSkills {
		t.Errorf("patched base1.mayor SkillsDir = %q, want %q", imported[0].SkillsDir, wantSkills)
	}
	wantMCP := filepath.Join(packDir, "agents", "mayor", "mcp")
	if imported[0].MCPDir != wantMCP {
		t.Errorf("patched base1.mayor MCPDir = %q, want %q", imported[0].MCPDir, wantMCP)
	}

	// base2.mayor (unpatched) must NOT receive the overlay.
	if imported[1].SkillsDir != "" {
		t.Errorf("unpatched base2.mayor SkillsDir = %q, want empty — overlay leaked to an unpatched same-name import", imported[1].SkillsDir)
	}
	if imported[1].MCPDir != "" {
		t.Errorf("unpatched base2.mayor MCPDir = %q, want empty — overlay leaked to an unpatched same-name import", imported[1].MCPDir)
	}
}

// TestImportingPackOverlayTakesPrecedenceOverImportedCatalog locks in the
// gc-fbt9c fix for codex finding 2: when the imported+patched agent already
// carries its own agent-local skills/mcp catalog from its defining pack, the
// importing pack's agents/<name>/ overlay must still attach. The asset model
// holds a single SkillsDir/MCPDir per agent, so the composition rule is
// intentional precedence — the importing overlay wins — rather than the
// previous silent skip that dropped the overlay whenever the field was set.
func TestImportingPackOverlayTakesPrecedenceOverImportedCatalog(t *testing.T) {
	t.Setenv("GC_HOME", t.TempDir())
	dir := t.TempDir()

	// base DEFINES mayor AND gives it its own agent-local skills/mcp catalog.
	base := filepath.Join(dir, "base")
	writeTestFile(t, base, "pack.toml", `
[pack]
name = "base"
schema = 2
`)
	writeTestFile(t, base, "agents/mayor/agent.toml", `scope = "city"`)
	writeTestFile(t, base, "agents/mayor/prompt.template.md", `Base mayor.`)
	writeTestFile(t, base, "agents/mayor/skills/base-skill/SKILL.md", `# base-skill`)
	writeTestFile(t, base, "agents/mayor/mcp/base.toml", `command = ["base-mcp"]`)

	// toolkit imports base, bare-name-patches mayor, and carries its OWN
	// agents/mayor/{skills,mcp}/ overlay.
	toolkit := filepath.Join(dir, "toolkit")
	writeTestFile(t, toolkit, "pack.toml", `
[pack]
name = "toolkit"
schema = 2

[imports.base]
source = "../base"

[[patches.agent]]
name = "mayor"
nudge = "go"
`)
	writeTestFile(t, toolkit, "agents/mayor/skills/toolkit-skill/SKILL.md", `# toolkit-skill`)
	writeTestFile(t, toolkit, "agents/mayor/mcp/toolkit.toml", `command = ["toolkit-mcp"]`)

	city := filepath.Join(dir, "city")
	writeTestFile(t, city, "city.toml", `
[workspace]
name = "test"

[imports.toolkit]
source = "../toolkit"
`)

	cfg, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(city, "city.toml"))
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}

	mayor := findAgent(t, cfg.Agents, "mayor")
	// Importing overlay wins over the imported agent's own catalog: the
	// agents/<name>/skills overlay in the importing pack reliably attaches.
	wantSkills := filepath.Join("toolkit", "agents", "mayor", "skills")
	if !strings.HasSuffix(mayor.SkillsDir, wantSkills) {
		t.Errorf("mayor SkillsDir = %q, want suffix %q (importing overlay must win over imported catalog, not be silently skipped)", mayor.SkillsDir, wantSkills)
	}
	wantMCP := filepath.Join("toolkit", "agents", "mayor", "mcp")
	if !strings.HasSuffix(mayor.MCPDir, wantMCP) {
		t.Errorf("mayor MCPDir = %q, want suffix %q (importing overlay must win over imported catalog, not be silently skipped)", mayor.MCPDir, wantMCP)
	}
}
