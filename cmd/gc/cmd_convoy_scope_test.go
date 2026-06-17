package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// newConvoyScopeTestCity builds a city tempdir with a single bound rig
// "myrig" (prefix mp) at rigs/myrig and returns the city path, the rig's
// resolved store root, and a fresh config. A fresh config is returned per
// call because resolveBdScopeTarget mutates Rig.Path to an absolute path.
func newConvoyScopeTestCity(t *testing.T) (cityPath, rigRoot string, cfg *config.City) {
	t.Helper()
	cityPath = t.TempDir()
	rigRoot = filepath.Join(cityPath, "rigs", "myrig")
	if err := os.MkdirAll(rigRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg = &config.City{
		Workspace: config.Workspace{Name: "demo", Prefix: "gc"},
		Rigs: []config.Rig{
			{Name: "myrig", Path: filepath.Join("rigs", "myrig"), Prefix: "mp"},
		},
	}
	return cityPath, filepath.Clean(rigRoot), cfg
}

// TestResolveConvoyCreateScope exercises the rig-resolution contract for
// `gc convoy create` (gc-nm4d2h): the convoy store is resolved like `gc bd`
// (--rig flag, cwd, GC_RIG, then city scope), tracked issues anchor the
// scope, and the city-scope fall-through warns unless --city-scope opts in.
//
// Each subtest sets cwd and GC_RIG explicitly so resolution is deterministic
// regardless of where the test binary runs.
func TestResolveConvoyCreateScope(t *testing.T) {
	t.Run("rig flag wins from city cwd", func(t *testing.T) {
		cityPath, rigRoot, cfg := newConvoyScopeTestCity(t)
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "")

		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "myrig", false, nil)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, rigRoot) {
			t.Errorf("storeDir = %q, want rig root %q", got, rigRoot)
		}
		if warn != "" {
			t.Errorf("warning = %q, want none", warn)
		}
	})

	t.Run("GC_RIG env resolves from city cwd", func(t *testing.T) {
		cityPath, rigRoot, cfg := newConvoyScopeTestCity(t)
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "myrig")

		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "", false, nil)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, rigRoot) {
			t.Errorf("storeDir = %q, want rig root %q", got, rigRoot)
		}
		if warn != "" {
			t.Errorf("warning = %q, want none", warn)
		}
	})

	t.Run("cwd inside rig resolves to rig", func(t *testing.T) {
		cityPath, rigRoot, cfg := newConvoyScopeTestCity(t)
		t.Chdir(rigRoot)
		t.Setenv("GC_RIG", "")

		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "", false, nil)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, rigRoot) {
			t.Errorf("storeDir = %q, want rig root %q", got, rigRoot)
		}
		if warn != "" {
			t.Errorf("warning = %q, want none", warn)
		}
	})

	t.Run("no signal falls back to city scope with warning", func(t *testing.T) {
		cityPath, _, cfg := newConvoyScopeTestCity(t)
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "")

		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "", false, nil)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, cityPath) {
			t.Errorf("storeDir = %q, want city path %q", got, cityPath)
		}
		if !strings.Contains(warn, "city scope") {
			t.Errorf("warning = %q, want one mentioning city scope", warn)
		}
	})

	t.Run("city-scope flag forces city and silences warning", func(t *testing.T) {
		cityPath, _, cfg := newConvoyScopeTestCity(t)
		// Even from inside the rig, --city-scope wins.
		t.Chdir(filepath.Join(cityPath, "rigs", "myrig"))
		t.Setenv("GC_RIG", "myrig")

		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "", true, nil)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, cityPath) {
			t.Errorf("storeDir = %q, want city path %q", got, cityPath)
		}
		if warn != "" {
			t.Errorf("warning = %q, want none", warn)
		}
	})

	t.Run("city-scope conflicts with rig flag", func(t *testing.T) {
		cityPath, _, cfg := newConvoyScopeTestCity(t)
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "")

		_, _, err := resolveConvoyCreateScope(cfg, cityPath, "myrig", true, nil)
		if err == nil {
			t.Fatal("err = nil, want a conflict error")
		}
		if !strings.Contains(err.Error(), "city-scope") || !strings.Contains(err.Error(), "rig") {
			t.Errorf("err = %v, want mention of both --city-scope and --rig", err)
		}
	})

	t.Run("unknown rig flag errors", func(t *testing.T) {
		cityPath, _, cfg := newConvoyScopeTestCity(t)
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "")

		_, _, err := resolveConvoyCreateScope(cfg, cityPath, "nope", false, nil)
		if err == nil {
			t.Fatal("err = nil, want 'rig not found' error")
		}
	})

	t.Run("city issue anchors to city without warning", func(t *testing.T) {
		cityPath, _, cfg := newConvoyScopeTestCity(t)
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "")

		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "", false, []string{"gc-1"})
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, cityPath) {
			t.Errorf("storeDir = %q, want city path %q", got, cityPath)
		}
		if warn != "" {
			t.Errorf("warning = %q, want none", warn)
		}
	})

	t.Run("rig issue routes to rig store without warning", func(t *testing.T) {
		cityPath, rigRoot, cfg := newConvoyScopeTestCity(t)
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "")

		// No explicit signal: issue-prefix routing (the path that
		// historically worked) places the convoy with its issue.
		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "", false, []string{"mp-1"})
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, rigRoot) {
			t.Errorf("storeDir = %q, want rig root %q", got, rigRoot)
		}
		if warn != "" {
			t.Errorf("warning = %q, want none", warn)
		}
	})

	t.Run("cross-rig: issue store overrides resolved rig with warning", func(t *testing.T) {
		cityPath, _, cfg := newConvoyScopeTestCity(t)
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "")

		// --rig resolves to myrig, but the tracked issue lives in the city
		// store; the convoy must co-locate with the issue, and we warn.
		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "myrig", false, []string{"gc-1"})
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, cityPath) {
			t.Errorf("storeDir = %q, want city path %q (co-located with issue)", got, cityPath)
		}
		if !strings.Contains(warn, "tracked issues") {
			t.Errorf("warning = %q, want one mentioning tracked issues", warn)
		}
	})

	t.Run("rigless city does not warn on city scope", func(t *testing.T) {
		cityPath := t.TempDir()
		t.Chdir(cityPath)
		t.Setenv("GC_RIG", "")
		cfg := &config.City{Workspace: config.Workspace{Name: "solo", Prefix: "gc"}}

		got, warn, err := resolveConvoyCreateScope(cfg, cityPath, "", false, nil)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if !samePath(got, cityPath) {
			t.Errorf("storeDir = %q, want city path %q", got, cityPath)
		}
		if warn != "" {
			t.Errorf("warning = %q, want none (no rig to warn about)", warn)
		}
	})
}
