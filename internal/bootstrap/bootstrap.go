// Package bootstrap installs user-global bootstrap packs used by implicit imports.
package bootstrap

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

const implicitImportSchema = 1

// BootstrapEntry describes a pack that gc init bootstraps into the global cache.
type BootstrapEntry struct {
	Name    string
	Source  string
	Version string
}

// BootstrapPacks is the hardcoded set of implicit packs bootstrapped by gc init.
var BootstrapPacks = []BootstrapEntry{
	{Name: "import", Source: "github.com/gastownhall/gc-import", Version: "0.2.0"},
	{Name: "registry", Source: "github.com/gastownhall/gc-registry", Version: "0.1.0"},
}

type implicitImport struct {
	Source  string `toml:"source"`
	Version string `toml:"version"`
	Commit  string `toml:"commit"`
}

type implicitImportFile struct {
	Schema  int                       `toml:"schema"`
	Imports map[string]implicitImport `toml:"imports"`
}

// CacheDir returns the cache directory name for a resolved source+commit pair.
func CacheDir(source, commit string) string {
	sum := sha256.Sum256([]byte(source + commit))
	return fmt.Sprintf("%x", sum[:])
}

// EnsureBootstrap populates the global cache and updates implicit-import.toml.
func EnsureBootstrap(gcHome string) error {
	if strings.TrimSpace(gcHome) == "" {
		gcHome = defaultGCHome()
	}
	if strings.TrimSpace(gcHome) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Join(gcHome, "cache", "repos"), 0o755); err != nil {
		return fmt.Errorf("creating bootstrap cache root: %w", err)
	}

	implicitPath := filepath.Join(gcHome, "implicit-import.toml")
	imports, err := readImplicitFile(implicitPath)
	if err != nil {
		return err
	}

	for _, entry := range BootstrapPacks {
		existing, ok := imports[entry.Name]
		if ok && existing.Source == entry.Source && existing.Version == entry.Version && existing.Commit != "" {
			cacheDir := filepath.Join(gcHome, "cache", "repos", CacheDir(existing.Source, existing.Commit))
			if _, err := os.Stat(filepath.Join(cacheDir, "pack.toml")); err == nil {
				continue
			}
		}

		commit, err := materializeBootstrapPack(gcHome, entry)
		if err != nil {
			return fmt.Errorf("bootstrapping %q: %w", entry.Name, err)
		}
		imports[entry.Name] = implicitImport{
			Source:  entry.Source,
			Version: entry.Version,
			Commit:  commit,
		}
	}

	if err := writeImplicitFile(implicitPath, imports); err != nil {
		return err
	}
	return nil
}

func defaultGCHome() string {
	if v := strings.TrimSpace(os.Getenv("GC_HOME")); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), ".gc")
	}
	return filepath.Join(home, ".gc")
}

func materializeBootstrapPack(gcHome string, entry BootstrapEntry) (string, error) {
	tmpDir, err := os.MkdirTemp(gcHome, "bootstrap-pack-*")
	if err != nil {
		return "", fmt.Errorf("creating temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir) //nolint:errcheck // best-effort cleanup

	if _, err := runGit("", "clone", "--quiet", "--depth", "1", "--branch", entry.Version, entry.Source, tmpDir); err != nil {
		return "", err
	}
	commit, err := runGit(tmpDir, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	commit = strings.TrimSpace(commit)
	cacheDir := filepath.Join(gcHome, "cache", "repos", CacheDir(entry.Source, commit))
	if _, err := os.Stat(filepath.Join(cacheDir, "pack.toml")); err == nil {
		return commit, nil
	}

	stageDir := cacheDir + ".tmp"
	_ = os.RemoveAll(stageDir)
	if err := copyTree(tmpDir, stageDir); err != nil {
		_ = os.RemoveAll(stageDir)
		return "", fmt.Errorf("copying bootstrap pack: %w", err)
	}
	if err := os.Rename(stageDir, cacheDir); err != nil {
		_ = os.RemoveAll(stageDir)
		if _, statErr := os.Stat(filepath.Join(cacheDir, "pack.toml")); statErr == nil {
			return commit, nil
		}
		return "", fmt.Errorf("moving bootstrap pack into cache: %w", err)
	}
	return commit, nil
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == ".git" || strings.HasPrefix(rel, ".git"+string(filepath.Separator)) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}

		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			in.Close() //nolint:errcheck // best effort
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close() //nolint:errcheck // best effort
			in.Close()  //nolint:errcheck // best effort
			return err
		}
		if err := out.Close(); err != nil {
			in.Close() //nolint:errcheck // best effort
			return err
		}
		if err := in.Close(); err != nil {
			return err
		}
		return nil
	})
}

func readImplicitFile(path string) (map[string]implicitImport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]implicitImport), nil
		}
		return nil, fmt.Errorf("reading implicit-import.toml: %w", err)
	}

	var file implicitImportFile
	if _, err := toml.Decode(string(data), &file); err != nil {
		return nil, fmt.Errorf("parsing implicit-import.toml: %w", err)
	}
	if file.Schema != 0 && file.Schema != implicitImportSchema {
		return nil, fmt.Errorf("unsupported implicit import schema %d", file.Schema)
	}
	if file.Imports == nil {
		file.Imports = make(map[string]implicitImport)
	}
	return file.Imports, nil
}

func writeImplicitFile(path string, imports map[string]implicitImport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating implicit-import dir: %w", err)
	}
	var names []string
	for name := range imports {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString("schema = 1\n")
	for _, name := range names {
		imp := imports[name]
		b.WriteString("\n")
		b.WriteString("[imports." + name + "]\n")
		b.WriteString(fmt.Sprintf("source = %q\n", imp.Source))
		if imp.Version != "" {
			b.WriteString(fmt.Sprintf("version = %q\n", imp.Version))
		}
		if imp.Commit != "" {
			b.WriteString(fmt.Sprintf("commit = %q\n", imp.Commit))
		}
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("writing implicit-import.toml: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replacing implicit-import.toml: %w", err)
	}
	return nil
}

func runGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), strings.TrimSpace(string(out)))
	}
	return string(out), nil
}
