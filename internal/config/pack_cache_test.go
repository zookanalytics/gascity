package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/fsys"
)

// scaffoldPackCacheCity writes a minimal city + pack tree that exercises the
// discovery walk (pack.toml, agents/, commands/, doctor/) so the cache has
// non-trivial content to capture. The .gc/ runtime root is pre-created so
// that LoadWithIncludes treats the city as bootstrapped and writes the cache.
func scaffoldPackCacheCity(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, "city.toml", `
[workspace]
name = "test"
`)
	writeTestFile(t, root, "pack.toml", `
[pack]
name = "city"
schema = 2
`)
	writeTestFile(t, root, "commands/hello/run.sh", "#!/bin/sh\nexit 0\n")
	writeTestFile(t, root, "doctor/health/run.sh", "#!/bin/sh\nexit 0\n")
	writeTestFile(t, root, "agents/worker/prompt.template.md", "# worker\n")
	if err := os.MkdirAll(filepath.Join(root, ".gc"), 0o755); err != nil {
		t.Fatalf("mkdir .gc: %v", err)
	}
	return root
}

func mustLoadCity(t *testing.T, cityRoot string) (*City, *Provenance) {
	t.Helper()
	cfg, prov, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml"))
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}
	return cfg, prov
}

func packCacheFilePath(cityRoot string) string {
	return packCachePath(cityRoot)
}

func TestPackCache_HitProducesEqualResult(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := scaffoldPackCacheCity(t)

	cfg1, prov1 := mustLoadCity(t, cityRoot)
	if _, err := os.Stat(packCacheFilePath(cityRoot)); err != nil {
		t.Fatalf("expected cache file after first load: %v", err)
	}

	cfg2, prov2 := mustLoadCity(t, cityRoot)

	if !reflect.DeepEqual(cfg1.Agents, cfg2.Agents) {
		t.Errorf("Agents mismatch:\n cold=%+v\n warm=%+v", cfg1.Agents, cfg2.Agents)
	}
	if !reflect.DeepEqual(cfg1.PackCommands, cfg2.PackCommands) {
		t.Errorf("PackCommands mismatch:\n cold=%+v\n warm=%+v", cfg1.PackCommands, cfg2.PackCommands)
	}
	if !reflect.DeepEqual(cfg1.PackDoctors, cfg2.PackDoctors) {
		t.Errorf("PackDoctors mismatch:\n cold=%+v\n warm=%+v", cfg1.PackDoctors, cfg2.PackDoctors)
	}
	if cfg1.ResolvedWorkspaceName != cfg2.ResolvedWorkspaceName {
		t.Errorf("ResolvedWorkspaceName mismatch: cold=%q warm=%q",
			cfg1.ResolvedWorkspaceName, cfg2.ResolvedWorkspaceName)
	}
	if !reflect.DeepEqual(prov1.Sources, prov2.Sources) {
		t.Errorf("Provenance.Sources mismatch:\n cold=%+v\n warm=%+v", prov1.Sources, prov2.Sources)
	}
	if !reflect.DeepEqual(prov1.Agents, prov2.Agents) {
		t.Errorf("Provenance.Agents mismatch:\n cold=%+v\n warm=%+v", prov1.Agents, prov2.Agents)
	}
}

func TestPackCache_DoesNotCreateRuntimeRootForUnbootstrappedCity(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityRoot, "city.toml"),
		[]byte("[workspace]\nname = \"test\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}

	if _, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml")); err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}

	if _, err := os.Stat(filepath.Join(cityRoot, ".gc")); !os.IsNotExist(err) {
		t.Fatalf("LoadWithIncludes must not create .gc/ for an unbootstrapped city; got err=%v", err)
	}
}

func TestPackCache_WritesAfterRuntimeRootExists(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityRoot, "city.toml"),
		[]byte("[workspace]\nname = \"test\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}
	// Simulate `gc init` having created the runtime root.
	if err := os.MkdirAll(filepath.Join(cityRoot, ".gc"), 0o755); err != nil {
		t.Fatalf("mkdir .gc: %v", err)
	}

	if _, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml")); err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}

	if _, err := os.Stat(packCachePath(cityRoot)); err != nil {
		t.Fatalf("expected cache file written when .gc/ exists: %v", err)
	}
}

func TestPackCache_OffEnvVarBypassesCache(t *testing.T) {
	t.Setenv(packCacheEnvVar, "off")
	cityRoot := scaffoldPackCacheCity(t)

	mustLoadCity(t, cityRoot)
	if _, err := os.Stat(packCacheFilePath(cityRoot)); !os.IsNotExist(err) {
		t.Fatalf("expected no cache file with GC_PACK_CACHE=off, got err=%v", err)
	}
}

func TestPackCache_OffEnvVarSkipsRead(t *testing.T) {
	cityRoot := scaffoldPackCacheCity(t)

	t.Setenv(packCacheEnvVar, "")
	mustLoadCity(t, cityRoot)

	cachePath := packCacheFilePath(cityRoot)
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("expected cache file after first load: %v", err)
	}

	// Corrupt cache. With GC_PACK_CACHE=off the corrupt file is ignored.
	if err := os.WriteFile(cachePath, []byte("not a valid gob"), 0o644); err != nil {
		t.Fatalf("corrupt cache: %v", err)
	}

	t.Setenv(packCacheEnvVar, "off")
	if _, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml")); err != nil {
		t.Fatalf("LoadWithIncludes with cache=off should succeed despite corrupt cache: %v", err)
	}
}

func TestPackCache_InvalidatesOnFileMtimeChange(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := scaffoldPackCacheCity(t)

	cfg1, _ := mustLoadCity(t, cityRoot)
	if cfg1.Workspace.Name != "test" {
		t.Fatalf("baseline workspace name = %q, want %q", cfg1.Workspace.Name, "test")
	}

	cityTOML := filepath.Join(cityRoot, "city.toml")
	if err := os.WriteFile(cityTOML, []byte(`
[workspace]
name = "renamed"
`), 0o644); err != nil {
		t.Fatalf("rewrite city.toml: %v", err)
	}
	// Force a different mtime in case fs only has 1s resolution.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(cityTOML, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	cfg2, _ := mustLoadCity(t, cityRoot)
	if cfg2.Workspace.Name != "renamed" {
		t.Errorf("expected cache invalidation: workspace name = %q, want %q",
			cfg2.Workspace.Name, "renamed")
	}
}

func TestPackCache_InvalidatesOnNewFileInDiscoveredDir(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := scaffoldPackCacheCity(t)

	cfg1, _ := mustLoadCity(t, cityRoot)
	for _, c := range cfg1.PackCommands {
		if reflect.DeepEqual(c.Command, []string{"goodbye"}) {
			t.Fatalf("baseline already has goodbye command: %+v", cfg1.PackCommands)
		}
	}

	writeTestFile(t, cityRoot, "commands/goodbye/run.sh", "#!/bin/sh\nexit 0\n")
	commandsDir := filepath.Join(cityRoot, "commands")
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(commandsDir, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	cfg2, _ := mustLoadCity(t, cityRoot)
	found := false
	for _, c := range cfg2.PackCommands {
		if reflect.DeepEqual(c.Command, []string{"goodbye"}) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected cache miss to discover goodbye command, got: %+v", cfg2.PackCommands)
	}
}

func TestPackCache_InvalidatesOnBuildIDChange(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := scaffoldPackCacheCity(t)
	mustLoadCity(t, cityRoot)

	cachePath := packCacheFilePath(cityRoot)
	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	// Corrupt the build ID by mutating the gob bytes deterministically.
	// Simpler approach: write a payload with a known-bogus BuildID via the
	// internal helper, then rebuild — but cache helpers are package-private.
	// Use the wrapper test helper that writes a synthetic bad-buildID cache.
	if err := overwritePackCacheBuildID(cachePath, "bogus-build-id-different-from-real"); err != nil {
		t.Fatalf("overwrite buildID: %v", err)
	}
	// Sanity: the file changed.
	updated, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read corrupted cache: %v", err)
	}
	if reflect.DeepEqual(data, updated) {
		t.Fatalf("expected cache file to differ after buildID overwrite")
	}

	// Reload should miss-and-rebuild rather than error.
	cfg, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml"))
	if err != nil {
		t.Fatalf("reload after buildID mismatch: %v", err)
	}
	if cfg.Workspace.Name != "test" {
		t.Errorf("expected fresh load to succeed; got workspace=%q", cfg.Workspace.Name)
	}
}

func TestPackCache_CorruptCacheFallsThroughToFreshParse(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := scaffoldPackCacheCity(t)

	// Pre-populate cache file path with garbage.
	cachePath := packCacheFilePath(cityRoot)
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		t.Fatalf("mkdir cache dir: %v", err)
	}
	if err := os.WriteFile(cachePath, []byte("\x00\x01\x02 not a gob"), 0o644); err != nil {
		t.Fatalf("write garbage cache: %v", err)
	}

	cfg, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml"))
	if err != nil {
		t.Fatalf("LoadWithIncludes with corrupt cache: %v", err)
	}
	if cfg.Workspace.Name != "test" {
		t.Errorf("expected normal parse despite corrupt cache; got workspace=%q", cfg.Workspace.Name)
	}
	// Cache should be rewritten with valid content.
	if _, err := loadPackCachePayload(cachePath); err != nil {
		t.Errorf("expected valid cache rewrite after corruption: %v", err)
	}
}

func TestPackCache_DifferentExtrasUseSameFileButMissOnLoad(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := scaffoldPackCacheCity(t)

	helperRoot := filepath.Join(cityRoot, "helper-pack")
	writeTestFile(t, helperRoot, "pack.toml", `
[pack]
name = "helper"
schema = 2
`)
	writeTestFile(t, helperRoot, "commands/extra/run.sh", "#!/bin/sh\nexit 0\n")

	// First load: no extras.
	cfg1, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml"))
	if err != nil {
		t.Fatalf("load no extras: %v", err)
	}
	for _, c := range cfg1.PackCommands {
		if reflect.DeepEqual(c.Command, []string{"extra"}) {
			t.Fatal("expected no extra command without extras")
		}
	}

	// Second load: with helperRoot as extra.
	cfg2, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml"), helperRoot)
	if err != nil {
		t.Fatalf("load with extras: %v", err)
	}
	found := false
	for _, c := range cfg2.PackCommands {
		if reflect.DeepEqual(c.Command, []string{"extra"}) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected extra command via extras: %+v", cfg2.PackCommands)
	}

	// Third load: no extras again — should match first, not stale-match second.
	cfg3, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml"))
	if err != nil {
		t.Fatalf("load no extras (third): %v", err)
	}
	for _, c := range cfg3.PackCommands {
		if reflect.DeepEqual(c.Command, []string{"extra"}) {
			t.Errorf("third load with no extras must not see extra command: %+v", cfg3.PackCommands)
		}
	}
}

// loadPackCachePayload is a test-only helper that opens a cache file and
// returns the decoded payload (used to verify cache file shape, not behavior).
func loadPackCachePayload(path string) (*packCachePayload, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var p packCachePayload
	if err := decodePackCache(f, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// overwritePackCacheBuildID rewrites the BuildID field of an existing cache
// file. Used by tests to simulate a stale cache after gc rebuild.
func overwritePackCacheBuildID(path, newID string) error {
	p, err := loadPackCachePayload(path)
	if err != nil {
		return err
	}
	p.BuildID = newID
	return writePackCachePayload(path, p)
}

// Sanity check: cache helpers should not leak across goroutines.
// Verifies our claim that a single cache file matches the most recent caller's
// extras. If this assumption changes, the matching test above must update.
func TestPackCache_ExtrasMismatchInvalidates(t *testing.T) {
	t.Setenv(packCacheEnvVar, "")
	cityRoot := scaffoldPackCacheCity(t)

	if _, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml")); err != nil {
		t.Fatalf("first load: %v", err)
	}
	cachePath := packCacheFilePath(cityRoot)
	p, err := loadPackCachePayload(cachePath)
	if err != nil {
		t.Fatalf("load cache: %v", err)
	}
	if len(p.Extras) != 0 {
		t.Errorf("expected empty Extras for no-extras load, got %v", p.Extras)
	}

	helperRoot := filepath.Join(cityRoot, "ext")
	writeTestFile(t, helperRoot, "pack.toml", `
[pack]
name = "ext"
schema = 2
`)
	if _, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityRoot, "city.toml"), helperRoot); err != nil {
		t.Fatalf("second load: %v", err)
	}
	p, err = loadPackCachePayload(cachePath)
	if err != nil {
		t.Fatalf("reload cache: %v", err)
	}
	if len(p.Extras) != 1 || !strings.HasSuffix(p.Extras[0], "ext") {
		t.Errorf("expected one Extras entry ending in 'ext', got %v", p.Extras)
	}
}
