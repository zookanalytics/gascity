package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
)

// TestCmdInitFromTOMLFileTracksWorkspacePrefixInCityToml pins the contract that
// `gc init --from <file>` preserves the source workspace.prefix as a tracked
// city.toml field. Only workspace.name migrates into machine-local
// .gc/site.toml; the prefix (globally-invariant bead-ID identity) must never be
// stripped into the gitignored site binding, or a fresh clone of an
// init-produced city would silently rederive the wrong prefix.
func TestCmdInitFromTOMLFileTracksWorkspacePrefixInCityToml(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_DOLT", "skip")
	configureIsolatedRuntimeEnv(t)

	dir := t.TempDir()
	cityPath := filepath.Join(dir, "machine-city")
	if err := os.MkdirAll(cityPath, 0o755); err != nil {
		t.Fatal(err)
	}

	src := filepath.Join(dir, "source.toml")
	if err := os.WriteFile(src, []byte("[workspace]\nname = \"declared-city\"\nprefix = \"dc\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := cmdInitFromTOMLFileWithOptions(fsys.OSFS{}, src, cityPath, "machine-alias", &stdout, &stderr, true, false)
	if code != 0 {
		t.Fatalf("cmdInitFromTOMLFileWithOptions = %d, want 0; stderr: %s", code, stderr.String())
	}

	assertWorkspacePrefixTrackedInCityToml(t, cityPath)
}

// TestDoInitFromDirTracksWorkspacePrefixInCityToml is the directory-copy analog
// of TestCmdInitFromTOMLFileTracksWorkspacePrefixInCityToml: a source city with
// a pack.toml is copied via `gc init --from <dir>`, and the tracked
// workspace.prefix must survive in the destination city.toml.
func TestDoInitFromDirTracksWorkspacePrefixInCityToml(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_DOLT", "skip")
	configureIsolatedRuntimeEnv(t)

	dir := t.TempDir()
	srcDir := filepath.Join(dir, "template")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "city.toml"),
		[]byte(withBuiltinProviderAliasesTOMLForTest("[workspace]\nname = \"declared-city\"\nprefix = \"dc\"\nprovider = \"claude\"\n", "claude")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "pack.toml"),
		[]byte("[pack]\nname = \"declared-city\"\nschema = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cityPath := filepath.Join(dir, "machine-city")
	var stdout, stderr bytes.Buffer
	code := doInitFromDirWithOptions(srcDir, cityPath, "machine-alias", &stdout, &stderr, true)
	if code != 0 {
		t.Fatalf("doInitFromDirWithOptions = %d, want 0; stderr: %s", code, stderr.String())
	}

	assertWorkspacePrefixTrackedInCityToml(t, cityPath)
}

// assertWorkspacePrefixTrackedInCityToml verifies the post-init split-by-
// invariance contract for an init that started from source identity
// name="declared-city", prefix="dc" with name override "machine-alias":
//   - city.toml keeps the tracked prefix "dc" and drops the name.
//   - .gc/site.toml carries only workspace_name, never workspace_prefix.
//   - resolved identity is unchanged: tracked prefix wins, name from site.
func assertWorkspacePrefixTrackedInCityToml(t *testing.T, cityPath string) {
	t.Helper()

	cityData, err := os.ReadFile(filepath.Join(cityPath, "city.toml"))
	if err != nil {
		t.Fatalf("reading city.toml: %v", err)
	}
	raw, err := config.Parse(cityData)
	if err != nil {
		t.Fatalf("parsing city.toml: %v", err)
	}
	if raw.Workspace.Prefix != "dc" {
		t.Fatalf("city.toml workspace.prefix = %q, want %q (prefix must stay tracked in city.toml)", raw.Workspace.Prefix, "dc")
	}
	if got := strings.TrimSpace(raw.Workspace.Name); got != "" {
		t.Fatalf("city.toml workspace.name = %q, want empty (name migrates to site.toml)", got)
	}

	siteData, err := os.ReadFile(filepath.Join(cityPath, ".gc", "site.toml"))
	if err != nil {
		t.Fatalf("reading .gc/site.toml: %v", err)
	}
	if !strings.Contains(string(siteData), `workspace_name = "machine-alias"`) {
		t.Fatalf(".gc/site.toml missing workspace_name:\n%s", siteData)
	}
	if strings.Contains(string(siteData), "workspace_prefix") {
		t.Fatalf(".gc/site.toml must not carry workspace_prefix (prefix stays tracked in city.toml):\n%s", siteData)
	}

	cfg, _, err := config.LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityPath, "city.toml"))
	if err != nil {
		t.Fatalf("loading initialized config: %v", err)
	}
	if got := config.EffectiveHQPrefix(cfg); got != "dc" {
		t.Fatalf("EffectiveHQPrefix() = %q, want %q", got, "dc")
	}
	if got := config.EffectiveCityName(cfg, filepath.Base(cityPath)); got != "machine-alias" {
		t.Fatalf("EffectiveCityName() = %q, want %q", got, "machine-alias")
	}
}
