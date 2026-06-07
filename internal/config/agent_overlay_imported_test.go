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

// TestImportedOverlayAttachTargetsBarePatchFirstMatchOnly locks in the
// gc-de5wp fix for the codex re-review finding on PR#37. The earlier
// gc-fbt9c fix only proved the BINDING-QUALIFIED patch path
// (TestImportedOverlayAttachTargetsKeyedByIdentity, patch "base1.mayor"),
// where agentPatchTargets matches a single import. The UNQUALIFIED bare
// patch path stayed broken: a legacy [[patches.agent]] name = "mayor"
// targets BOTH same-bare-name imports, but applyPackAgentPatches binds it to
// the FIRST match and stops (first-wins legacy semantics). The overlay attach
// targeting must mirror that identity and attach the importing pack's private
// skills/mcp to the first import ONLY — marking every same-name match leaked
// the assets onto the sibling that never received the patch.
func TestImportedOverlayAttachTargetsBarePatchFirstMatchOnly(t *testing.T) {
	packDir := t.TempDir()
	// The importing pack carries an agents/mayor/{skills,mcp}/ overlay.
	writeTestFile(t, packDir, "agents/mayor/skills/git-merge-pull-request/SKILL.md", `# git-merge-pull-request`)
	writeTestFile(t, packDir, "agents/mayor/mcp/private.toml", `command = ["helper-mcp"]`)

	// Two imported agents share the bare name "mayor" from distinct bindings.
	// The patch is UNQUALIFIED (no "binding." prefix), so agentPatchTargets
	// matches both by name — but applyPackAgentPatches patches only the first.
	imported := []Agent{
		{Name: "mayor", BindingName: "base1", SourceDir: filepath.Join(packDir, "base1")},
		{Name: "mayor", BindingName: "base2", SourceDir: filepath.Join(packDir, "base2")},
	}
	patches := []AgentPatch{{Name: "mayor"}}

	fs := fsys.OSFS{}
	targets := importedOverlayAttachTargets(fs, imported, patches, packDir)
	attachImportedAgentOverlays(fs, imported, targets, packDir)

	// The first import — the one applyPackAgentPatches actually patches —
	// receives the importing pack's overlay.
	wantSkills := filepath.Join(packDir, "agents", "mayor", "skills")
	if imported[0].SkillsDir != wantSkills {
		t.Errorf("first mayor import SkillsDir = %q, want %q", imported[0].SkillsDir, wantSkills)
	}
	wantMCP := filepath.Join(packDir, "agents", "mayor", "mcp")
	if imported[0].MCPDir != wantMCP {
		t.Errorf("first mayor import MCPDir = %q, want %q", imported[0].MCPDir, wantMCP)
	}

	// The second same-bare-name import never received the patch, so the
	// overlay must NOT attach to it — this is the asset leak the fix closes.
	if imported[1].SkillsDir != "" {
		t.Errorf("second mayor import SkillsDir = %q, want empty — overlay leaked to an unpatched same-bare-name import via the unqualified-patch path", imported[1].SkillsDir)
	}
	if imported[1].MCPDir != "" {
		t.Errorf("second mayor import MCPDir = %q, want empty — overlay leaked to an unpatched same-bare-name import via the unqualified-patch path", imported[1].MCPDir)
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

// TestImportedOverlayAttachTargetsRequiresAssetOnlyDir locks in the gc-l0t8a
// fix for the codex re-review finding on PR#37. importedOverlayAttachTargets
// suppresses convention discovery for the agents/<name>/ dirs it marks, but
// attachImportedAgentOverlays only re-homes the asset subdirs (skills, mcp).
// So a dir may be marked a target ONLY when it contains exclusively those
// asset subdirs. A dir that also carries a native-agent convention marker —
// agent.toml, a prompt file, overlay/, or namepool.txt — must NOT be marked:
// suppressing discovery for it would silently drop that marker instead of
// attaching it or colliding on it.
func TestImportedOverlayAttachTargetsRequiresAssetOnlyDir(t *testing.T) {
	patches := []AgentPatch{{Name: "mayor"}}

	assetOnly := func(t *testing.T, packDir string) {
		writeTestFile(t, packDir, "agents/mayor/skills/git-merge-pull-request/SKILL.md", `# git-merge-pull-request`)
		writeTestFile(t, packDir, "agents/mayor/mcp/private.toml", `command = ["helper-mcp"]`)
	}

	cases := []struct {
		name    string
		seed    func(t *testing.T, packDir string)
		wantHit bool
	}{
		{
			name:    "skills and mcp only attaches",
			seed:    assetOnly,
			wantHit: true,
		},
		{
			name: "incidental dotfile alongside assets still attaches",
			seed: func(t *testing.T, packDir string) {
				assetOnly(t, packDir)
				writeTestFile(t, packDir, "agents/mayor/.DS_Store", "junk")
			},
			wantHit: true,
		},
		{
			name: "agent.toml native definition disqualifies",
			seed: func(t *testing.T, packDir string) {
				assetOnly(t, packDir)
				writeTestFile(t, packDir, "agents/mayor/agent.toml", `scope = "city"`)
			},
			wantHit: false,
		},
		{
			name: "prompt template disqualifies",
			seed: func(t *testing.T, packDir string) {
				assetOnly(t, packDir)
				writeTestFile(t, packDir, "agents/mayor/prompt.template.md", `Custom mayor.`)
			},
			wantHit: false,
		},
		{
			name: "namepool disqualifies",
			seed: func(t *testing.T, packDir string) {
				assetOnly(t, packDir)
				writeTestFile(t, packDir, "agents/mayor/namepool.txt", "alpha\nbeta\n")
			},
			wantHit: false,
		},
		{
			name: "overlay dir disqualifies",
			seed: func(t *testing.T, packDir string) {
				assetOnly(t, packDir)
				writeTestFile(t, packDir, "agents/mayor/overlay/note.md", `overlay asset`)
			},
			wantHit: false,
		},
		{
			name: "no asset subdir at all disqualifies",
			seed: func(t *testing.T, packDir string) {
				writeTestFile(t, packDir, "agents/mayor/agent.toml", `scope = "city"`)
			},
			wantHit: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			packDir := t.TempDir()
			tc.seed(t, packDir)
			imported := []Agent{{Name: "mayor", BindingName: "base", SourceDir: filepath.Join(packDir, "base")}}

			targets := importedOverlayAttachTargets(fsys.OSFS{}, imported, patches, packDir)
			if got := targets[0]; got != tc.wantHit {
				t.Fatalf("importedOverlayAttachTargets marked mayor=%v, want %v", got, tc.wantHit)
			}
		})
	}
}

// TestImportingPackOverlayWithNativeDefinitionStillCollides locks in the
// gc-l0t8a fix end-to-end. An importing pack that patches an imported agent
// AND ships an agents/<name>/agent.toml native definition (alongside an asset
// overlay) is NOT a pure asset overlay. The native definition must not be
// silently dropped: the phantom mint is allowed through, collides with the
// import, and aborts the load — the conservative duplicate-agent behavior. The
// previous predicate keyed on mere directory existence and silently dropped
// the agent.toml (and any prompt/overlay/namepool) it never attached.
func TestImportingPackOverlayWithNativeDefinitionStillCollides(t *testing.T) {
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

	// toolkit imports base and bare-name-patches mayor (signaling intent to
	// customize the import), but its agents/mayor/ overlay carries a full
	// agent.toml native definition alongside the skills overlay — not a pure
	// asset overlay. The agent.toml must not be silently dropped.
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
	writeTestFile(t, toolkit, "agents/mayor/agent.toml", `scope = "city"`)
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
		t.Fatal("expected a duplicate-agent error: an agents/<name>/agent.toml native definition must not be silently dropped by the overlay-attach path")
	}
	if !strings.Contains(err.Error(), "duplicate agent") {
		t.Errorf("error = %q, want it to mention 'duplicate agent'", err.Error())
	}
}
