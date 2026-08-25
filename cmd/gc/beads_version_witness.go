package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/contract"
	"github.com/gastownhall/gascity/internal/fsys"
)

// resolveBDVersionForWitness reports the version of the bd binary gc is about
// to drive. It is the same PATH lookup gc-beads-bd.sh's run_bd_pinned makes,
// so the recorded version always names the binary that actually initializes
// the store. Overridden in tests.
var resolveBDVersionForWitness = beads.ProbeBDVersion

// ensureManagedScopeVersionWitness seeds bd's local version witness for a
// managed scope that bd would otherwise refuse to open.
//
// bd 1.2.1 refuses a workspace whose metadata says dolt_mode=server and whose
// .beads/dolt is a real directory unless a post-1.0 witness records which era
// created it. A fresh gc-managed scope is precisely that shape — the managed
// Dolt server's data root IS .beads/dolt and the server is up before the first
// `bd init` — so bd classifies a scope gc created moments earlier as a legacy
// workspace needing migration, and the init fails.
//
// gc is the only party that can answer for a workspace bd has never seen, and
// it answers narrowly: it writes only for the exact shape bd refuses, only
// when no witness exists, and only the version of the bd binary it is about to
// run. Every other case is left for bd to classify — including a pre-1.0
// witness, which is a real migration signal that must keep reaching the
// operator.
func ensureManagedScopeVersionWitness(fs fsys.FS, scopeRoot string) error {
	needed, err := scopeNeedsBdVersionWitness(fs, scopeRoot)
	if err != nil || !needed {
		return err
	}
	version, err := resolveBDVersionForWitness()
	if err != nil {
		return fmt.Errorf("resolving bd version to record in %s: %w", contract.LocalVersionWitnessPath(scopeRoot), err)
	}
	if _, err := contract.EnsureLocalVersionWitness(fs, scopeRoot, version); err != nil {
		return fmt.Errorf("recording bd version witness for %s: %w", scopeRoot, err)
	}
	return nil
}

// scopeNeedsBdVersionWitness reports whether scopeRoot has the shape bd
// refuses without a witness, and does not already carry one. It mirrors bd's
// guardLegacyUpgradeWorkspace server-mode branch; the probe in the caller is
// deliberately downstream of this check so an already-answered scope costs no
// subprocess.
func scopeNeedsBdVersionWitness(fs fsys.FS, scopeRoot string) (bool, error) {
	mode, ok, err := contract.ReadDoltMode(fs, scopeMetadataJSONPath(scopeRoot))
	if err != nil {
		return false, fmt.Errorf("reading dolt mode for %s: %w", scopeRoot, err)
	}
	if !ok || !strings.EqualFold(strings.TrimSpace(mode), "server") {
		return false, nil
	}
	if !hasRealDoltRootDir(fs, scopeRoot) {
		return false, nil
	}
	_, hasWitness, err := contract.ReadLocalVersionWitness(fs, scopeRoot)
	if err != nil {
		return false, err
	}
	return !hasWitness, nil
}

// hasRealDoltRootDir reports whether .beads/dolt is a regular directory rather
// than a symlink or absent. bd's hasLegacyDoltRoot draws the same distinction —
// a symlinked root never reaches its refusal — so gc must not seed a witness
// for a shape bd would have accepted anyway.
func hasRealDoltRootDir(fs fsys.FS, scopeRoot string) bool {
	info, err := fs.Lstat(filepath.Join(scopeRoot, ".beads", "dolt"))
	return err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0
}
