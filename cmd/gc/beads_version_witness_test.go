package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/beads/contract"
	"github.com/gastownhall/gascity/internal/fsys"
)

// writeManagedServerScope scaffolds the on-disk shape of a gc-managed
// server-mode scope: canonical metadata naming dolt_mode=server plus the
// managed Dolt server's data root at .beads/dolt.
func writeManagedServerScope(t *testing.T, scopeRoot, doltMode string, withDoltRoot bool) {
	t.Helper()
	beadsDir := filepath.Join(scopeRoot, ".beads")
	if err := os.MkdirAll(beadsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := contract.EnsureCanonicalMetadata(fsys.OSFS{}, filepath.Join(beadsDir, "metadata.json"), contract.MetadataState{
		Database:     "dolt",
		Backend:      "dolt",
		DoltMode:     doltMode,
		DoltDatabase: "hq",
	}); err != nil {
		t.Fatalf("EnsureCanonicalMetadata: %v", err)
	}
	if withDoltRoot {
		if err := os.MkdirAll(filepath.Join(beadsDir, "dolt", "hq"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

func pinBDVersionForWitness(t *testing.T, version string, err error) *int {
	t.Helper()
	calls := 0
	prev := resolveBDVersionForWitness
	resolveBDVersionForWitness = func() (string, error) {
		calls++
		return version, err
	}
	t.Cleanup(func() { resolveBDVersionForWitness = prev })
	return &calls
}

func TestEnsureManagedScopeVersionWitnessSeedsFreshServerScope(t *testing.T) {
	// The regression: a fresh managed server scope has dolt_mode=server and a
	// real .beads/dolt (the running server's data root) before bd has ever
	// run, which is exactly the shape bd 1.2.1 refuses as a legacy workspace.
	scope := t.TempDir()
	writeManagedServerScope(t, scope, "server", true)
	pinBDVersionForWitness(t, "1.2.1", nil)

	if err := ensureManagedScopeVersionWitness(fsys.OSFS{}, scope); err != nil {
		t.Fatalf("ensureManagedScopeVersionWitness() error = %v", err)
	}

	got, ok, err := contract.ReadLocalVersionWitness(fsys.OSFS{}, scope)
	if err != nil {
		t.Fatalf("ReadLocalVersionWitness: %v", err)
	}
	if !ok || got != "1.2.1" {
		t.Fatalf("version witness = %q ok=%v, want %q true", got, ok, "1.2.1")
	}
}

func TestEnsureManagedScopeVersionWitnessSkipsShapesBdDoesNotRefuse(t *testing.T) {
	// bd's guard only reaches the ambiguous-server branch when the config says
	// server mode AND .beads/dolt is a real directory. Every other shape needs
	// no witness, so gc must not write one.
	tests := []struct {
		name         string
		doltMode     string
		withDoltRoot bool
	}{
		{name: "embedded mode with dolt root", doltMode: "embedded", withDoltRoot: true},
		{name: "server mode without dolt root", doltMode: "server", withDoltRoot: false},
		{name: "unset mode with dolt root", doltMode: "", withDoltRoot: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scope := t.TempDir()
			writeManagedServerScope(t, scope, tc.doltMode, tc.withDoltRoot)
			calls := pinBDVersionForWitness(t, "1.2.1", nil)

			if err := ensureManagedScopeVersionWitness(fsys.OSFS{}, scope); err != nil {
				t.Fatalf("ensureManagedScopeVersionWitness() error = %v", err)
			}
			if _, ok, err := contract.ReadLocalVersionWitness(fsys.OSFS{}, scope); err != nil || ok {
				t.Fatalf("witness written for a shape bd does not refuse (ok=%v, err=%v)", ok, err)
			}
			if *calls != 0 {
				t.Fatalf("probed bd version %d times for a scope needing no witness, want 0", *calls)
			}
		})
	}
}

func TestEnsureManagedScopeVersionWitnessTreatsSymlinkedDoltRootAsAbsent(t *testing.T) {
	// bd's hasLegacyDoltRoot explicitly excludes symlinks, so a symlinked
	// .beads/dolt never reaches the refusal and needs no witness.
	scope := t.TempDir()
	writeManagedServerScope(t, scope, "server", false)
	target := filepath.Join(t.TempDir(), "dolt")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(scope, ".beads", "dolt")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	pinBDVersionForWitness(t, "1.2.1", nil)

	if err := ensureManagedScopeVersionWitness(fsys.OSFS{}, scope); err != nil {
		t.Fatalf("ensureManagedScopeVersionWitness() error = %v", err)
	}
	if _, ok, _ := contract.ReadLocalVersionWitness(fsys.OSFS{}, scope); ok {
		t.Fatal("witness written for a symlinked dolt root")
	}
}

func TestEnsureManagedScopeVersionWitnessPreservesLegacyWitness(t *testing.T) {
	// A pre-1.0 witness is a genuine migration-required signal. gc must leave
	// it in place so bd keeps refusing, and must not spend a version probe.
	scope := t.TempDir()
	writeManagedServerScope(t, scope, "server", true)
	witnessPath := contract.LocalVersionWitnessPath(scope)
	if err := os.WriteFile(witnessPath, []byte("0.58.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := pinBDVersionForWitness(t, "1.2.1", nil)

	if err := ensureManagedScopeVersionWitness(fsys.OSFS{}, scope); err != nil {
		t.Fatalf("ensureManagedScopeVersionWitness() error = %v", err)
	}
	data, err := os.ReadFile(witnessPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "0.58.0\n" {
		t.Fatalf("witness = %q, want legacy value preserved", string(data))
	}
	if *calls != 0 {
		t.Fatalf("probed bd version %d times with a witness already present, want 0", *calls)
	}
}

func TestEnsureManagedScopeVersionWitnessReportsProbeFailure(t *testing.T) {
	// Without a version there is no witness, and bd init fails moments later
	// with its cross-era refusal. Report the real cause instead.
	scope := t.TempDir()
	writeManagedServerScope(t, scope, "server", true)
	probeErr := errors.New("locate bd: executable file not found in $PATH")
	pinBDVersionForWitness(t, "", probeErr)

	err := ensureManagedScopeVersionWitness(fsys.OSFS{}, scope)
	if err == nil {
		t.Fatal("ensureManagedScopeVersionWitness() error = nil, want probe failure")
	}
	if !errors.Is(err, probeErr) {
		t.Fatalf("ensureManagedScopeVersionWitness() error = %v, want it to wrap the probe failure", err)
	}
}

func TestNormalizeCanonicalBdScopeFilesForInitSeedsVersionWitness(t *testing.T) {
	// The wiring that matters: the pre-init normalization pass is what runs
	// before `bd init`, so the witness must exist by the time it returns.
	cityPath := t.TempDir()
	writeManagedServerScope(t, cityPath, "server", true)
	if err := os.WriteFile(filepath.Join(cityPath, ".beads", "config.yaml"), []byte("issue_prefix: gc\nissue-prefix: gc\ndolt.auto-start: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pinBDVersionForWitness(t, "1.2.1", nil)

	if err := normalizeCanonicalBdScopeFilesForInit(cityPath, cityPath, "gc", "hq"); err != nil {
		t.Fatalf("normalizeCanonicalBdScopeFilesForInit: %v", err)
	}

	got, ok, err := contract.ReadLocalVersionWitness(fsys.OSFS{}, cityPath)
	if err != nil {
		t.Fatalf("ReadLocalVersionWitness: %v", err)
	}
	if !ok || got != "1.2.1" {
		t.Fatalf("version witness = %q ok=%v, want %q true", got, ok, "1.2.1")
	}
}
