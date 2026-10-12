package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/hooks"
	"github.com/gastownhall/gascity/internal/shellquote"
)

func TestBuildProviderLaunchCommandAddsDefaultsAndSettings(t *testing.T) {
	dir := t.TempDir()
	runtimeDir := filepath.Join(dir, ".gc")
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runtimeDir, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	spec := BuiltinProviders()["claude"]
	rp := specToResolved("claude", &spec)

	got, err := BuildProviderLaunchCommand(dir, rp, nil, "")
	if err != nil {
		t.Fatalf("BuildProviderLaunchCommand: %v", err)
	}

	wantCommand := fmt.Sprintf("claude --dangerously-skip-permissions --effort max --settings %q", filepath.Join(dir, ".gc", "settings.json"))
	if got.Command != wantCommand {
		t.Fatalf("Command = %q, want %q", got.Command, wantCommand)
	}
	if got.SettingsPath != filepath.Join(dir, ".gc", "settings.json") {
		t.Fatalf("SettingsPath = %q, want %q", got.SettingsPath, filepath.Join(dir, ".gc", "settings.json"))
	}
	if got.SettingsRel != filepath.Join(".gc", "settings.json") {
		t.Fatalf("SettingsRel = %q, want %q", got.SettingsRel, filepath.Join(".gc", "settings.json"))
	}
}

func TestBuildProviderLaunchCommandAppliesOptionOverrides(t *testing.T) {
	spec := BuiltinProviders()["claude"]
	rp := specToResolved("claude", &spec)

	got, err := BuildProviderLaunchCommand("", rp, map[string]string{
		"permission_mode": "plan",
		"effort":          "low",
	}, "")
	if err != nil {
		t.Fatalf("BuildProviderLaunchCommand: %v", err)
	}

	want := "claude --permission-mode plan --effort low"
	if got.Command != want {
		t.Fatalf("Command = %q, want %q", got.Command, want)
	}
	if got.SettingsPath != "" || got.SettingsRel != "" {
		t.Fatalf("unexpected settings source: %#v", got)
	}
}

func TestBuildProviderLaunchCommandIgnoresInitialMessageOverride(t *testing.T) {
	spec := BuiltinProviders()["claude"]
	rp := specToResolved("claude", &spec)

	got, err := BuildProviderLaunchCommand("", rp, map[string]string{
		"initial_message": "hello",
		"effort":          "low",
	}, "")
	if err != nil {
		t.Fatalf("BuildProviderLaunchCommand: %v", err)
	}

	want := "claude --dangerously-skip-permissions --effort low"
	if got.Command != want {
		t.Fatalf("Command = %q, want %q", got.Command, want)
	}
}

func TestProviderOptionMapCapacity(t *testing.T) {
	tests := []struct {
		name         string
		defaultsLen  int
		overridesLen int
		want         int
	}{
		{
			name:         "adds safe override capacity",
			defaultsLen:  2,
			overridesLen: 3,
			want:         5,
		},
		{
			name:         "keeps defaults when overrides are empty",
			defaultsLen:  4,
			overridesLen: 0,
			want:         4,
		},
		{
			name:         "uses exact boundary when addition is safe",
			defaultsLen:  math.MaxInt - 1,
			overridesLen: 1,
			want:         math.MaxInt,
		},
		{
			name:         "skips override capacity when addition would overflow",
			defaultsLen:  math.MaxInt,
			overridesLen: 1,
			want:         math.MaxInt,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := providerOptionMapCapacity(tt.defaultsLen, tt.overridesLen); got != tt.want {
				t.Fatalf("providerOptionMapCapacity(%d, %d) = %d, want %d", tt.defaultsLen, tt.overridesLen, got, tt.want)
			}
		})
	}
}

func TestBuildProviderLaunchCommandUsesACPCommand(t *testing.T) {
	rp := &ResolvedProvider{
		Command: "custom-opencode",
		ACPArgs: []string{"acp"},
	}

	t.Run("acp transport uses ACPCommandString", func(t *testing.T) {
		got, err := BuildProviderLaunchCommand("", rp, nil, "acp")
		if err != nil {
			t.Fatalf("BuildProviderLaunchCommand: %v", err)
		}
		want := "custom-opencode acp"
		if got.Command != want {
			t.Fatalf("Command = %q, want %q", got.Command, want)
		}
	})

	t.Run("default transport uses CommandString", func(t *testing.T) {
		got, err := BuildProviderLaunchCommand("", rp, nil, "")
		if err != nil {
			t.Fatalf("BuildProviderLaunchCommand: %v", err)
		}
		want := "custom-opencode"
		if got.Command != want {
			t.Fatalf("Command = %q, want %q", got.Command, want)
		}
	})

	t.Run("tmux transport uses CommandString", func(t *testing.T) {
		got, err := BuildProviderLaunchCommand("", rp, nil, "tmux")
		if err != nil {
			t.Fatalf("BuildProviderLaunchCommand: %v", err)
		}
		want := "custom-opencode"
		if got.Command != want {
			t.Fatalf("Command = %q, want %q", got.Command, want)
		}
	})

	t.Run("unknown transport errors", func(t *testing.T) {
		_, err := BuildProviderLaunchCommand("", rp, nil, "stdio")
		if err == nil {
			t.Fatal("BuildProviderLaunchCommand() error = nil, want unknown transport error")
		}
	})
}

func TestBuildProviderLaunchCommandWithoutOptionsSkipsDefaultsButKeepsSettings(t *testing.T) {
	dir := t.TempDir()
	runtimeDir := filepath.Join(dir, ".gc")
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runtimeDir, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	spec := BuiltinProviders()["claude"]
	rp := specToResolved("claude", &spec)

	got, err := BuildProviderLaunchCommandWithoutOptions(dir, rp, "")
	if err != nil {
		t.Fatalf("BuildProviderLaunchCommandWithoutOptions: %v", err)
	}

	wantCommand := fmt.Sprintf("claude --settings %q", filepath.Join(dir, ".gc", "settings.json"))
	if got.Command != wantCommand {
		t.Fatalf("Command = %q, want %q", got.Command, wantCommand)
	}
	if got.SettingsPath != filepath.Join(dir, ".gc", "settings.json") {
		t.Fatalf("SettingsPath = %q, want %q", got.SettingsPath, filepath.Join(dir, ".gc", "settings.json"))
	}
	if got.SettingsRel != filepath.Join(".gc", "settings.json") {
		t.Fatalf("SettingsRel = %q, want %q", got.SettingsRel, filepath.Join(".gc", "settings.json"))
	}
}

func TestBuildProviderLaunchCommandWithoutOptionsUsesBuiltinAncestorForSettings(t *testing.T) {
	dir := t.TempDir()
	runtimeDir := filepath.Join(dir, ".gc")
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	settingsPath := filepath.Join(runtimeDir, "settings.json")
	if err := os.WriteFile(settingsPath, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	rp := &ResolvedProvider{
		Name:            "claude-max",
		BuiltinAncestor: "claude",
		Command:         "aimux",
		Args:            []string{"run", "claude", "--"},
	}

	got, err := BuildProviderLaunchCommandWithoutOptions(dir, rp, "")
	if err != nil {
		t.Fatalf("BuildProviderLaunchCommandWithoutOptions: %v", err)
	}

	if want := fmt.Sprintf("--settings %q", settingsPath); !strings.Contains(got.Command, want) {
		t.Fatalf("Command = %q, want settings arg %q", got.Command, want)
	}
	if count := strings.Count(got.Command, "--settings"); count != 1 {
		t.Fatalf("Command has %d --settings flags, want 1: %q", count, got.Command)
	}
	if got.SettingsPath != settingsPath {
		t.Fatalf("SettingsPath = %q, want %q", got.SettingsPath, settingsPath)
	}
}

func TestBuildProviderLaunchCommandWithoutOptionsIgnoresDeprecatedKindForSettings(t *testing.T) {
	dir := t.TempDir()
	runtimeDir := filepath.Join(dir, ".gc")
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runtimeDir, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}

	rp := &ResolvedProvider{
		Name:    "custom-provider",
		Kind:    "claude",
		Command: "custom-provider",
	}

	got, err := BuildProviderLaunchCommandWithoutOptions(dir, rp, "")
	if err != nil {
		t.Fatalf("BuildProviderLaunchCommandWithoutOptions: %v", err)
	}
	if strings.Contains(got.Command, "--settings") {
		t.Fatalf("Command = %q, want no settings from deprecated Kind fallback", got.Command)
	}
	if got.SettingsPath != "" || got.SettingsRel != "" {
		t.Fatalf("unexpected settings source from deprecated Kind fallback: %#v", got)
	}
}

// codexHookArgsIn returns the value of each -c hooks= argument in command.
func codexHookArgsIn(command string) []string {
	var values []string
	tokens := shellquote.Split(command)
	for i := 0; i+1 < len(tokens); i++ {
		if tokens[i] == "-c" && strings.HasPrefix(tokens[i+1], "hooks=") {
			values = append(values, tokens[i+1])
		}
	}
	return values
}

func TestBuildProviderLaunchCommandRegistersCodexHooks(t *testing.T) {
	const city = "/city with space"
	wantArgs, err := hooks.CodexLaunchArgs(city)
	if err != nil {
		t.Fatalf("CodexLaunchArgs: %v", err)
	}
	spec := BuiltinProviders()["codex"]
	rp := specToResolved("codex", &spec)

	got, err := BuildProviderLaunchCommand(city, rp, nil, "")
	if err != nil {
		t.Fatalf("BuildProviderLaunchCommand: %v", err)
	}
	if !strings.HasPrefix(got.Command, "codex --dangerously-bypass-approvals-and-sandbox ") {
		t.Fatalf("Command = %q, want the codex defaults first", got.Command)
	}
	if values := codexHookArgsIn(got.Command); !reflect.DeepEqual(values, wantArgs[1:]) {
		t.Fatalf("hooks overrides in %q = %q, want exactly %q", got.Command, values, wantArgs[1:])
	}
	if got.SettingsPath != "" || got.SettingsRel != "" {
		t.Fatalf("codex launch reported a settings file: %#v", got)
	}

	// Option overrides applied to the stored command later keep the
	// registration intact.
	reapplied := ReplaceSchemaFlags(got.Command, rp.OptionsSchema, []string{"-c", "model_reasoning_effort=high"})
	if values := codexHookArgsIn(reapplied); !reflect.DeepEqual(values, wantArgs[1:]) {
		t.Fatalf("hooks overrides after re-applying options to %q = %q, want exactly %q", reapplied, values, wantArgs[1:])
	}

	wrapped := &ResolvedProvider{Name: "codex-mini", BuiltinAncestor: "codex", Command: "codex"}
	base, err := BuildProviderLaunchCommandWithoutOptions(city, wrapped, "")
	if err != nil {
		t.Fatalf("BuildProviderLaunchCommandWithoutOptions: %v", err)
	}
	if want := "codex " + shellquote.Join(wantArgs); base.Command != want {
		t.Fatalf("wrapped codex Command = %q, want %q", base.Command, want)
	}
}

func TestProviderHookLaunchArgsRegisterOnlyCodex(t *testing.T) {
	for _, family := range []string{"claude", "gemini", "opencode", "custom", ""} {
		args, err := ProviderHookLaunchArgs("/city", family)
		if err != nil {
			t.Fatalf("ProviderHookLaunchArgs(%q): %v", family, err)
		}
		if len(args) != 0 {
			t.Errorf("ProviderHookLaunchArgs(%q) = %q, want none", family, args)
		}
	}
	args, err := ProviderHookLaunchArgs("/city", "codex")
	if err != nil {
		t.Fatalf("ProviderHookLaunchArgs(codex): %v", err)
	}
	if len(args) != 2 || args[0] != "-c" || !strings.HasPrefix(args[1], "hooks=") {
		t.Fatalf("ProviderHookLaunchArgs(codex) = %q, want [-c hooks=<table>]", args)
	}
}
