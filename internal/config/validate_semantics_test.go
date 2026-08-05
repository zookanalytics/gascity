package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

func TestValidateSemanticsNoWarnings(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "claude"},
		Providers: explicitBuiltins("claude", "codex"),
		Agents: []Agent{
			{Name: "mayor", Provider: "claude"},
			{Name: "worker", Provider: "codex"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestValidateSemanticsUnknownAgentProvider(t *testing.T) {
	cfg := &City{
		Agents: []Agent{
			{Name: "mayor", Provider: "cloude"}, // typo
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "cloude") {
		t.Errorf("warning should mention bad provider: %s", warnings[0])
	}
	if !strings.Contains(warnings[0], "mayor") {
		t.Errorf("warning should mention agent: %s", warnings[0])
	}
}

func TestValidateSemanticsCustomProviderOK(t *testing.T) {
	cfg := &City{
		Providers: map[string]ProviderSpec{
			"my-agent": {Command: "my-agent-cli"},
		},
		Agents: []Agent{
			{Name: "worker", Provider: "my-agent"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings for custom provider, got: %v", warnings)
	}
}

func TestValidateSemanticsUnknownWorkspaceProvider(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "bogus"},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "[workspace]") {
		t.Errorf("warning should mention workspace: %s", warnings[0])
	}
}

func TestValidateSemanticsUnknownAgentDefaultsProvider(t *testing.T) {
	cfg := &City{
		AgentDefaults: AgentDefaults{Provider: "cdoex"},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "[agent_defaults]") {
		t.Errorf("warning should mention agent_defaults: %s", warnings[0])
	}
	if !strings.Contains(warnings[0], "cdoex") {
		t.Errorf("warning should mention bad provider: %s", warnings[0])
	}
}

func TestValidateSemanticsAgentDefaultsCustomProviderOK(t *testing.T) {
	cfg := &City{
		AgentDefaults: AgentDefaults{Provider: "local-llm"},
		Providers: map[string]ProviderSpec{
			"local-llm": {Command: "local-llm"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings for custom agent default provider, got: %v", warnings)
	}
}

func TestValidateSemanticsStartCommandSkipsProviderCheck(t *testing.T) {
	cfg := &City{
		Agents: []Agent{
			{Name: "custom", Provider: "nonexistent", StartCommand: "my-binary"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("start_command should skip provider check, got: %v", warnings)
	}
}

func TestValidateSemanticsAgentSessionTransportAllowsTmux(t *testing.T) {
	cfg := &City{
		Providers: explicitBuiltins("claude"),
		Agents: []Agent{
			{Name: "worker", Provider: "claude", Session: "tmux"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings for tmux session transport, got: %v", warnings)
	}
}

func TestValidateSemanticsAgentSessionTransportRejectsUnknown(t *testing.T) {
	cfg := &City{
		Providers: explicitBuiltins("claude"),
		Agents: []Agent{
			{Name: "worker", Provider: "claude", Session: "stdio"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "stdio") || !strings.Contains(warnings[0], "tmux") {
		t.Fatalf("warning should mention bad value and allowed transports: %s", warnings[0])
	}
}

func TestValidateSemanticsProviderPromptModeBad(t *testing.T) {
	cfg := &City{
		Providers: map[string]ProviderSpec{
			"bad": {PromptMode: "pipe"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "pipe") {
		t.Errorf("warning should mention bad value: %s", warnings[0])
	}
}

func TestValidateSemanticsProviderPromptFlagRequired(t *testing.T) {
	cfg := &City{
		Providers: map[string]ProviderSpec{
			"needsflag": {PromptMode: "flag"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "prompt_flag") {
		t.Errorf("warning should mention prompt_flag: %s", warnings[0])
	}
}

func TestValidateSemanticsProviderPromptFlagOK(t *testing.T) {
	cfg := &City{
		Providers: map[string]ProviderSpec{
			"ok": {PromptMode: "flag", PromptFlag: "--prompt"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestValidateSemanticsMultipleIssues(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "nope"},
		Providers: map[string]ProviderSpec{
			"bad": {PromptMode: "pipe"},
		},
		Agents: []Agent{
			{Name: "a1", Provider: "missing1"},
			{Name: "a2", Provider: "missing2"},
		},
	}
	warnings := ValidateSemantics(cfg, "test.toml")
	// 1 workspace + 2 agents + 1 provider = 4
	if len(warnings) != 4 {
		t.Fatalf("expected 4 warnings, got %d: %v", len(warnings), warnings)
	}
}

func TestValidateSemanticsIncludesSource(t *testing.T) {
	cfg := &City{
		Agents: []Agent{
			{Name: "bad", Provider: "missing"},
		},
	}
	warnings := ValidateSemantics(cfg, "/path/to/city.toml")
	if len(warnings) == 0 {
		t.Fatal("expected warning")
	}
	if !strings.Contains(warnings[0], "/path/to/city.toml") {
		t.Errorf("warning should include source path: %s", warnings[0])
	}
}

func TestValidateAgentsScopeBadEnum(t *testing.T) {
	agents := []Agent{
		{Name: "bad", Scope: "global"},
	}
	err := ValidateAgents(agents)
	if err == nil {
		t.Fatal("expected error for bad scope")
	}
	if !strings.Contains(err.Error(), "global") {
		t.Errorf("error should mention bad value: %v", err)
	}
}

func TestValidateAgentsScopeValidValues(t *testing.T) {
	for _, scope := range []string{"", "city", "rig"} {
		agents := []Agent{
			{Name: "ok", Scope: scope},
		}
		if err := ValidateAgents(agents); err != nil {
			t.Errorf("scope %q should be valid, got: %v", scope, err)
		}
	}
}

func TestValidateAgentsPromptModeBadEnum(t *testing.T) {
	agents := []Agent{
		{Name: "bad", PromptMode: "pipe"},
	}
	err := ValidateAgents(agents)
	if err == nil {
		t.Fatal("expected error for bad prompt_mode")
	}
	if !strings.Contains(err.Error(), "pipe") {
		t.Errorf("error should mention bad value: %v", err)
	}
}

func TestValidateAgentsPromptModeValidValues(t *testing.T) {
	for _, mode := range []string{"", "arg", "flag", "none"} {
		agents := []Agent{
			{Name: "ok", PromptMode: mode, PromptFlag: "--p"},
		}
		if err := ValidateAgents(agents); err != nil {
			t.Errorf("prompt_mode %q should be valid, got: %v", mode, err)
		}
	}
}

func TestValidateAgentsLifecycleValues(t *testing.T) {
	for _, lifecycle := range []string{"", AgentLifecycleOneShot} {
		if err := ValidateAgents([]Agent{{Name: "ok", Lifecycle: lifecycle}}); err != nil {
			t.Errorf("lifecycle %q should be valid, got: %v", lifecycle, err)
		}
	}
	err := ValidateAgents([]Agent{{Name: "bad", Lifecycle: "short_lived"}})
	if err == nil {
		t.Fatal("expected error for bad lifecycle")
	}
	if !strings.Contains(err.Error(), "short_lived") {
		t.Errorf("error should mention bad value: %v", err)
	}
}

func TestValidateAgentsPromptFlagRequiredForFlagMode(t *testing.T) {
	agents := []Agent{
		{Name: "bad", PromptMode: "flag"},
	}
	err := ValidateAgents(agents)
	if err == nil {
		t.Fatal("expected error for missing prompt_flag")
	}
	if !strings.Contains(err.Error(), "prompt_flag") {
		t.Errorf("error should mention prompt_flag: %v", err)
	}
}

func TestValidateAgentsPromptFlagWithFlagModeOK(t *testing.T) {
	agents := []Agent{
		{Name: "ok", PromptMode: "flag", PromptFlag: "--prompt"},
	}
	if err := ValidateAgents(agents); err != nil {
		t.Errorf("should be valid: %v", err)
	}
}

// --- Semantic-warning source attribution (gc-qmr9) ---
//
// The source label on an agent-scoped warning must name the file that
// actually declares the agent, not the root city.toml that merely imports
// the pack it came from.

func TestValidateSemanticsAttributesV2ConventionAgentToItsAgentToml(t *testing.T) {
	cfg := &City{
		Agents: []Agent{{
			Name:        "refinery",
			Dir:         "gc-toolkit",
			BindingName: "gc-toolkit",
			Provider:    "bogus",
			SourceDir:   "/packs/gastown",
			layout:      layoutV2Convention,
		}},
	}
	warnings := ValidateSemantics(cfg, "/city/city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	want := "/packs/gastown/agents/refinery/agent.toml"
	if !strings.HasPrefix(warnings[0], want+":") {
		t.Errorf("warning should be attributed to %s, got: %s", want, warnings[0])
	}
	if strings.Contains(warnings[0], "/city/city.toml") {
		t.Errorf("warning must not name the root city.toml, got: %s", warnings[0])
	}
}

func TestValidateSemanticsAttributesV1InlineAgentToPackToml(t *testing.T) {
	cfg := &City{
		Agents: []Agent{{
			Name:      "refinery",
			Provider:  "bogus",
			SourceDir: "/packs/gastown",
			layout:    layoutV1Inline,
		}},
	}
	warnings := ValidateSemantics(cfg, "/city/city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	want := "/packs/gastown/pack.toml"
	if !strings.HasPrefix(warnings[0], want+":") {
		t.Errorf("warning should be attributed to %s, got: %s", want, warnings[0])
	}
}

func TestValidateSemanticsAttributesFragmentAgentToItsSourceDir(t *testing.T) {
	// Fragment agents carry SourceDir but no pack layout stamp. Naming the
	// directory is honest; naming a specific file there would be a guess.
	cfg := &City{
		Agents: []Agent{{
			Name:      "worker",
			Provider:  "bogus",
			SourceDir: "/city/fragments",
		}},
	}
	warnings := ValidateSemantics(cfg, "/city/city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.HasPrefix(warnings[0], "/city/fragments:") {
		t.Errorf("warning should be attributed to the fragment dir, got: %s", warnings[0])
	}
}

func TestValidateSemanticsAttributesInlineAgentToRootSource(t *testing.T) {
	// An agent declared inline in city.toml has no SourceDir; the root
	// source stays correct and must not change.
	cfg := &City{
		Agents: []Agent{{Name: "worker", Provider: "bogus"}},
	}
	warnings := ValidateSemantics(cfg, "/city/city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.HasPrefix(warnings[0], "/city/city.toml:") {
		t.Errorf("inline agent should keep the root source, got: %s", warnings[0])
	}
}

func TestValidateSemanticsKeepsRootSourceForNonAgentWarnings(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "bogus"},
	}
	warnings := ValidateSemantics(cfg, "/city/city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.HasPrefix(warnings[0], "/city/city.toml:") {
		t.Errorf("[workspace] warning should keep the root source, got: %s", warnings[0])
	}
}

func TestValidateSemanticsAttributesAllAgentScopedWarnings(t *testing.T) {
	// Every agent-scoped warning — not just the provider one — must follow
	// the agent's own declaring file.
	unlimited := -1
	cfg := &City{
		Agents: []Agent{{
			Name:              "refinery",
			Provider:          "bogus",
			Session:           "carrier-pigeon",
			Namepool:          "/packs/gastown/agents/refinery/namepool.txt",
			MaxActiveSessions: &unlimited,
			IdleTimeout:       "2h",
			SleepAfterIdle:    "300s",
			SourceDir:         "/packs/gastown",
			layout:            layoutV2Convention,
		}},
	}
	warnings := ValidateSemantics(cfg, "/city/city.toml")
	if len(warnings) != 4 {
		t.Fatalf("expected 4 agent-scoped warnings, got %d: %v", len(warnings), warnings)
	}
	want := "/packs/gastown/agents/refinery/agent.toml:"
	for _, w := range warnings {
		if !strings.HasPrefix(w, want) {
			t.Errorf("warning should be attributed to %s, got: %s", want, w)
		}
	}
}

// TestValidateSemanticsAttributesComposedPackAgentToItsAgentToml exercises the
// real composition path rather than a hand-built Agent: a city that imports a
// pack whose agents/<name>/agent.toml carries the offending keys must produce a
// warning naming that agent.toml, never the city.toml that only imports it.
// This is the shape the gastown pack ships (gc-qmr9).
func TestValidateSemanticsAttributesComposedPackAgentToItsAgentToml(t *testing.T) {
	root := t.TempDir()
	cityDir := filepath.Join(root, "city")
	packDir := filepath.Join(root, "helper")

	writeTestFile(t, packDir, "pack.toml", `
[pack]
name = "helper"
schema = 2
`)
	writeTestFile(t, packDir, "agents/refinery/agent.toml", `
scope = "city"
session = "carrier-pigeon"
idle_timeout = "2h"
sleep_after_idle = "300s"
`)
	writeTestFile(t, packDir, "agents/refinery/prompt.template.md", "You are the refinery.\n")
	writeTestFile(t, cityDir, "city.toml", `
[workspace]
name = "test"

[imports.helper]
source = "../helper"
`)

	cityPath := filepath.Join(cityDir, "city.toml")
	cfg, _, err := LoadWithIncludes(fsys.OSFS{}, cityPath)
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}

	warnings := ValidateSemantics(cfg, cityPath)
	if len(warnings) == 0 {
		t.Fatal("expected semantic warnings for the malformed pack agent")
	}
	want := filepath.Join(packDir, "agents", "refinery", "agent.toml")
	for _, w := range warnings {
		if !strings.HasPrefix(w, want+":") {
			t.Errorf("warning should name the declaring agent.toml, got: %s", w)
		}
		if strings.Contains(w, cityPath) {
			t.Errorf("warning must not name the importing city.toml, got: %s", w)
		}
	}
	// The named file must actually exist and hold the offending keys.
	data, readErr := os.ReadFile(want)
	if readErr != nil {
		t.Fatalf("attributed source %q is not readable: %v", want, readErr)
	}
	for _, key := range []string{"idle_timeout", "sleep_after_idle", "session"} {
		if !strings.Contains(string(data), key) {
			t.Errorf("attributed source %q does not contain %q", want, key)
		}
	}
}
