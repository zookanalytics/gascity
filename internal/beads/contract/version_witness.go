// Reader/writer for bd's local version witness, the file bd consults to tell
// a current Dolt-server workspace from a pre-1.0 one it must refuse to open.
// gc seeds a scope's canonical bd files before the first `bd init` runs, so
// it is also the only party that can answer that question for a workspace bd
// has never seen.

package contract

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gastownhall/gascity/internal/fsys"
)

// localVersionWitnessFile is bd's gitignored record of the last bd version
// that ran against a scope. The name is bd's (cmd/bd/version_tracking.go);
// gc mirrors it rather than inventing a parallel marker.
const localVersionWitnessFile = ".local_version"

// LocalVersionWitnessPath returns the canonical witness path for a scope.
//
// scopeRoot is the parent of the .beads/ directory (city or rig root); the
// function joins scopeRoot/.beads/.local_version itself, so callers should
// not construct the path.
func LocalVersionWitnessPath(scopeRoot string) string {
	return filepath.Join(scopeRoot, ".beads", localVersionWitnessFile)
}

// ReadLocalVersionWitness reads the version recorded for a scope.
//
// The bool reports whether a usable version was found. An absent file and a
// present-but-blank file both return ("", false, nil): bd treats each as "no
// witness", so callers must not distinguish them either.
func ReadLocalVersionWitness(fs fsys.FS, scopeRoot string) (string, bool, error) {
	path := LocalVersionWitnessPath(scopeRoot)
	data, err := fs.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("read version witness %s: %w", path, err)
	}
	version := strings.TrimSpace(string(data))
	if version == "" {
		return "", false, nil
	}
	return version, true, nil
}

// EnsureLocalVersionWitness records version for a scope that has no witness
// yet, and reports whether it wrote one.
//
// Why gc writes a file bd owns: bd 1.2.1 refuses to open a workspace whose
// config says dolt.mode=server and whose .beads/dolt is a real directory
// unless a post-1.0 witness says which era created it (bd's
// guardLegacyUpgradeWorkspace). That is exactly the shape of a *fresh*
// gc-managed scope — the managed Dolt server's data root is .beads/dolt and
// it exists before the first bd init — so without this seed bd classifies a
// scope gc just created as a legacy workspace needing migration, and every
// fresh managed-bd city init fails.
//
// An existing witness is never overwritten. bd rewrites the file itself on
// every version change, and a pre-1.0 value there is a genuine
// migration-required signal that must keep reaching the operator.
//
// version must be one bd would accept as a post-1.0 witness: three numeric
// dot-separated components with a major of at least 1, optionally
// "v"-prefixed. Anything else is rejected rather than written, because a
// value bd will not accept either leaves the workspace refused anyway or
// records an era claim gc has no evidence for.
func EnsureLocalVersionWitness(fs fsys.FS, scopeRoot, version string) (bool, error) {
	normalized, err := normalizeVersionWitness(version)
	if err != nil {
		return false, err
	}
	if _, ok, err := ReadLocalVersionWitness(fs, scopeRoot); err != nil {
		return false, err
	} else if ok {
		return false, nil
	}

	path := LocalVersionWitnessPath(scopeRoot)
	dir := filepath.Dir(path)
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return false, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	// 0600 matches bd's own writeLocalVersion; the witness sits beside store
	// credentials in a .beads/ directory bd wants at 0700.
	if err := fsys.WriteFileIfChangedAtomic(fs, path, []byte(normalized+"\n"), 0o600); err != nil {
		return false, fmt.Errorf("write version witness %s: %w", path, err)
	}
	return true, nil
}

// normalizeVersionWitness validates version against bd's post-1.0 witness
// grammar and returns it in the bare (unprefixed) form bd writes itself.
func normalizeVersionWitness(version string) (string, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(version), "v")
	parts := strings.Split(trimmed, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("version witness %q is not a three-part version", version)
	}
	values := make([]int, len(parts))
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return "", fmt.Errorf("version witness %q has a non-numeric component %q", version, part)
		}
		values[i] = value
	}
	if values[0] < 1 {
		return "", fmt.Errorf("version witness %q is not post-1.0; bd would still refuse the workspace", version)
	}
	return trimmed, nil
}
