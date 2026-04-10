package bootstrap

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureBootstrapPopulatesCacheAndWritesImplicitFile(t *testing.T) {
	repo := initTaggedPackRepo(t, "gc-import", "v0.2.0")

	old := BootstrapPacks
	BootstrapPacks = []BootstrapEntry{{
		Name:    "import",
		Source:  repo,
		Version: "v0.2.0",
	}}
	t.Cleanup(func() { BootstrapPacks = old })

	gcHome := t.TempDir()
	if err := EnsureBootstrap(gcHome); err != nil {
		t.Fatalf("EnsureBootstrap: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(gcHome, "implicit-import.toml"))
	if err != nil {
		t.Fatalf("reading implicit-import.toml: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, `[imports.import]`) {
		t.Fatalf("implicit-import.toml missing import entry:\n%s", text)
	}
	if !strings.Contains(text, `version = "v0.2.0"`) {
		t.Fatalf("implicit-import.toml missing version:\n%s", text)
	}

	entries, err := readImplicitFile(filepath.Join(gcHome, "implicit-import.toml"))
	if err != nil {
		t.Fatalf("readImplicitFile: %v", err)
	}
	entry := entries["import"]
	cacheDir := filepath.Join(gcHome, "cache", "repos", CacheDir(entry.Source, entry.Commit))
	if _, err := os.Stat(filepath.Join(cacheDir, "pack.toml")); err != nil {
		t.Fatalf("bootstrap cache missing pack.toml: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, ".git")); !os.IsNotExist(err) {
		t.Fatalf("bootstrap cache should not keep .git directory, stat err = %v", err)
	}
}

func TestEnsureBootstrapPreservesExistingEntriesAndIsIdempotent(t *testing.T) {
	registryRepo := initTaggedPackRepo(t, "gc-registry", "v0.1.0")

	old := BootstrapPacks
	BootstrapPacks = []BootstrapEntry{{
		Name:    "registry",
		Source:  registryRepo,
		Version: "v0.1.0",
	}}
	t.Cleanup(func() { BootstrapPacks = old })

	gcHome := t.TempDir()
	if err := os.WriteFile(filepath.Join(gcHome, "implicit-import.toml"), []byte(`
schema = 1

[imports.custom]
source = "example.com/custom"
version = "1.0.0"
commit = "deadbeef"
`), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := EnsureBootstrap(gcHome); err != nil {
		t.Fatalf("first EnsureBootstrap: %v", err)
	}
	if err := EnsureBootstrap(gcHome); err != nil {
		t.Fatalf("second EnsureBootstrap: %v", err)
	}

	entries, err := readImplicitFile(filepath.Join(gcHome, "implicit-import.toml"))
	if err != nil {
		t.Fatalf("readImplicitFile: %v", err)
	}
	if _, ok := entries["custom"]; !ok {
		t.Fatal("custom implicit entry was removed")
	}
	if _, ok := entries["registry"]; !ok {
		t.Fatal("registry bootstrap entry missing")
	}
}

func initTaggedPackRepo(t *testing.T, packName, version string) string {
	t.Helper()

	dir := filepath.Join(t.TempDir(), packName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	runGitCmd(t, dir, "init")
	runGitCmd(t, dir, "config", "user.name", "Test User")
	runGitCmd(t, dir, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(dir, "pack.toml"), []byte(`
[pack]
name = "`+packName+`"
schema = 1

[[agent]]
name = "runner"
scope = "city"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	runGitCmd(t, dir, "add", "pack.toml")
	runGitCmd(t, dir, "commit", "-m", "init")
	runGitCmd(t, dir, "tag", version)
	return dir
}

func runGitCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
