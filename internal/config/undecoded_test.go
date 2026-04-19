package config

import (
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestCheckUndecodedKeysDetectsTypo(t *testing.T) { //nolint:misspell // intentional typos in test data
	typo := "prompt_" + "tempalte" //nolint:misspell // intentional typo under test
	input := "[workspace]\nname = \"test\"\n\n[[agent]]\nname = \"mayor\"\n" + typo + " = \"prompts/mayor.md\"\n"
	var cfg City
	md, err := toml.Decode(input, &cfg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	warnings := CheckUndecodedKeys(md, "city.toml")
	if len(warnings) == 0 {
		t.Fatalf("expected warning for typo %s, got none", typo)
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, typo) {
			found = true
			if !strings.Contains(w, "prompt_template") {
				t.Errorf("warning should suggest prompt_template, got: %s", w)
			}
		}
	}
	if !found {
		t.Errorf("no warning about %s in: %v", typo, warnings)
	}
}

func TestCheckUndecodedKeysNoWarningsForValidConfig(t *testing.T) {
	input := `
[workspace]
name = "test"

[[agent]]
name = "mayor"
prompt_template = "prompts/mayor.md"
`
	var cfg City
	md, err := toml.Decode(input, &cfg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	warnings := CheckUndecodedKeys(md, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings for valid config, got: %v", warnings)
	}
}

func TestCheckUndecodedKeysMultipleTypos(t *testing.T) {
	input := `
[workspace]
name = "test"

[[agent]]
name = "mayor"
promtp_template = "bad"
idel_timeout = "5m"
`
	var cfg City
	md, err := toml.Decode(input, &cfg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	warnings := CheckUndecodedKeys(md, "test.toml")
	if len(warnings) < 2 {
		t.Fatalf("expected at least 2 warnings, got %d: %v", len(warnings), warnings)
	}
}

func TestCheckUndecodedKeysIncludesSource(t *testing.T) {
	input := `
[workspace]
name = "test"
bogus_field = true
`
	var cfg City
	md, err := toml.Decode(input, &cfg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	warnings := CheckUndecodedKeys(md, "/path/to/city.toml")
	if len(warnings) == 0 {
		t.Fatal("expected warning for bogus_field")
	}
	if !strings.Contains(warnings[0], "/path/to/city.toml") {
		t.Errorf("warning should include source path, got: %s", warnings[0])
	}
}

func TestCheckUndecodedKeysNoSuggestionForDistantTypo(t *testing.T) {
	input := `
[workspace]
name = "test"
completely_unknown_field_xyz = "val"
`
	var cfg City
	md, err := toml.Decode(input, &cfg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	warnings := CheckUndecodedKeys(md, "city.toml")
	if len(warnings) == 0 {
		t.Fatal("expected warning")
	}
	if strings.Contains(warnings[0], "did you mean") {
		t.Errorf("should not suggest for very distant key, got: %s", warnings[0])
	}
}

func TestEditDistance(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"abc", "abc", 0},
		{"abc", "abd", 1},
		{"abc", "abcd", 1},
		{"prompt_tempalte", "prompt_template", 2}, //nolint:misspell // intentional typo
		{"idle_timeout", "idel_timeout", 2},
		{"xyz", "abc", 3},
	}
	for _, tt := range tests {
		got := editDistance(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("editDistance(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestKnownTOMLKeysNotEmpty(t *testing.T) {
	keys := knownTOMLKeys()
	if len(keys) == 0 {
		t.Fatal("knownTOMLKeys returned empty list")
	}
	// Spot-check a few known keys.
	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, want := range []string{"name", "prompt_template", "provider", "idle_timeout", "pool"} {
		if !keySet[want] {
			t.Errorf("expected %q in known keys", want)
		}
	}
}

func TestParseWithMetaWarnings(t *testing.T) {
	input := `
[workspace]
name = "test"

[[agent]]
name = "mayor"
proivder = "claude"
`
	cfg, _, warnings, err := parseWithMeta([]byte(input), "test.toml")
	if err != nil {
		t.Fatalf("parseWithMeta: %v", err)
	}
	if cfg.Workspace.Name != "test" {
		t.Errorf("Name = %q, want %q", cfg.Workspace.Name, "test")
	}
	if len(warnings) == 0 {
		t.Fatal("expected warning for proivder typo")
	}
	if !strings.Contains(warnings[0], "proivder") {
		t.Errorf("warning should mention proivder, got: %s", warnings[0])
	}
	if !strings.Contains(warnings[0], "provider") {
		t.Errorf("warning should suggest provider, got: %s", warnings[0])
	}
}

func TestParseWithMetaWarnsForLegacyOrderGateAlias(t *testing.T) {
	input := `
[workspace]
name = "test"

[orders]

[[orders.overrides]]
name = "digest"
gate = "cooldown"
`
	cfg, _, warnings, err := parseWithMeta([]byte(input), "test.toml")
	if err != nil {
		t.Fatalf("parseWithMeta: %v", err)
	}
	if len(cfg.Orders.Overrides) != 1 {
		t.Fatalf("len(overrides) = %d, want 1", len(cfg.Orders.Overrides))
	}
	if cfg.Orders.Overrides[0].Trigger == nil || *cfg.Orders.Overrides[0].Trigger != "cooldown" {
		t.Fatalf("Trigger = %#v, want cooldown", cfg.Orders.Overrides[0].Trigger)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want 1 deprecation warning", warnings)
	}
	if !strings.Contains(warnings[0], `"orders.overrides.gate" is deprecated`) {
		t.Fatalf("warning = %q, want deprecation for orders.overrides.gate", warnings[0])
	}
	if !strings.Contains(warnings[0], `"orders.overrides.trigger"`) {
		t.Fatalf("warning = %q, want trigger replacement hint", warnings[0])
	}
}

func TestParseWithMetaWarnsForOrderOverrideTypos(t *testing.T) {
	input := `
[workspace]
name = "test"

[orders]

[[orders.overrides]]
name = "digest"
intervall = "24h"
`
	cfg, _, warnings, err := parseWithMeta([]byte(input), "test.toml")
	if err != nil {
		t.Fatalf("parseWithMeta: %v", err)
	}
	if len(cfg.Orders.Overrides) != 1 {
		t.Fatalf("len(overrides) = %d, want 1", len(cfg.Orders.Overrides))
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want 1 typo warning", warnings)
	}
	if !strings.Contains(warnings[0], "orders.overrides.intervall") {
		t.Fatalf("warning = %q, want typo path", warnings[0])
	}
	if !strings.Contains(warnings[0], `"interval"`) {
		t.Fatalf("warning = %q, want interval suggestion", warnings[0])
	}
}
