package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/rollout"
	"github.com/gastownhall/gascity/internal/rollout/gate"
)

func TestResolvedConditionalWritesMode(t *testing.T) {
	t.Run("nil config is unset", func(t *testing.T) {
		if got := resolvedConditionalWritesMode(nil); got != gate.ModeUnset {
			t.Fatalf("mode = %q, want unset", got)
		}
	})
	t.Run("resolved config value threads through", func(t *testing.T) {
		cfg, err := config.Parse([]byte("[workspace]\nname = \"t\"\n\n[beads]\nconditional_writes = \"require\"\n"))
		if err != nil {
			t.Fatal(err)
		}
		if got := resolvedConditionalWritesMode(cfg); got != gate.Require {
			t.Fatalf("mode = %q, want require", got)
		}
	})
	t.Run("resolve error degrades to unset, never raises", func(t *testing.T) {
		// config.Parse rejects the typo at load now; this defensive cell
		// covers an invalid value arriving through a non-Parse construction.
		cfg := &config.City{Beads: config.BeadsConfig{ConditionalWrites: "requre"}}
		if got := resolvedConditionalWritesMode(cfg); got != gate.ModeUnset {
			t.Fatalf("mode = %q, want unset (best-effort open paths cannot honor an invalid value)", got)
		}
	})
	t.Run("out-of-enum config fails to load at all", func(t *testing.T) {
		if _, err := config.Parse([]byte("[beads]\nconditional_writes = \"requre\"\n")); err == nil {
			t.Fatal("config.Parse accepted an out-of-enum conditional_writes — a typo must never silently mean off")
		}
	})
}

// TestOpenStoreResultAtForCityThreadsConditionalWrites is the entry-point
// test for the shared CLI/city open helper: a real temp city.toml declaring
// require must be observable on the store every command path receives —
// through the policy wrapper — without any per-command threading.
func TestOpenStoreResultAtForCityThreadsConditionalWrites(t *testing.T) {
	cityDir := t.TempDir()
	toml := "[workspace]\nname = \"t\"\nprefix = \"ga\"\n\n[beads]\nprovider = \"file\"\nconditional_writes = \"require\"\n"
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := openStoreResultAtForCity(cityDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreResultAtForCity: %v", err)
	}
	writer, diag, resolveErr := beads.ResolveConditionalWriter(result.Store)
	if resolveErr != nil || diag != nil {
		t.Fatalf("resolve = diag %v err %v, want the file store's writer under require", diag, resolveErr)
	}
	if writer == nil {
		t.Fatal("require in city.toml was not observed on the opened store: mode threading is broken")
	}
}

// TestOpenStoreResultWithConfigSkipsLoad pins the ga-237xpr fix at its most
// direct layer: openStoreResultAtForCityWithConfig must reuse an
// already-resolved *config.City instead of reloading city.toml + all pack
// includes, but must still fall back to a load when the caller has no config
// in hand (the nil branch every other pre-existing call site relies on).
func TestOpenStoreResultWithConfigSkipsLoad(t *testing.T) {
	cityDir := t.TempDir()
	toml := "[workspace]\nname = \"t\"\n\n[beads]\nprovider = \"file\"\n"
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadCityConfig(cityDir, io.Discard)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	before := loadCityConfigCalls.Load()
	if _, err := openStoreResultAtForCityWithConfig(cityDir, cityDir, cfg, gate.ModeUnset, false, false, false, nil); err != nil {
		t.Fatalf("openStoreResultAtForCityWithConfig(cfg): %v", err)
	}
	if grew := loadCityConfigCalls.Load() - before; grew != 0 {
		t.Fatalf("openStoreResultAtForCityWithConfig re-parsed city config %d times despite a non-nil cfg", grew)
	}

	before = loadCityConfigCalls.Load()
	if _, err := openStoreResultAtForCityWithConfig(cityDir, cityDir, nil, gate.ModeUnset, false, false, false, nil); err != nil {
		t.Fatalf("openStoreResultAtForCityWithConfig(nil): %v", err)
	}
	if grew := loadCityConfigCalls.Load() - before; grew != 1 {
		t.Fatalf("openStoreResultAtForCityWithConfig(nil cfg) parsed city config %d times, want exactly 1 (fallback load)", grew)
	}
}

// TestOpenStoreResultWithConfigSkipsLoad_ExecProvider is the exec-provider
// analog of TestOpenStoreResultWithConfigSkipsLoad, covering the gap left
// open by the original ga-237xpr fix (PR #4682 review round 1, BLOCKER):
// openStoreResultAtForCityWithConfig already threads its cfg into
// OpenExecStore (main.go), but openExecStoreAtForCity's own call to
// resolveConfiguredExecStoreTarget dropped it and re-parsed city.toml
// unconditionally on every call regardless of whether the caller had a
// resolved config in hand.
func TestOpenStoreResultWithConfigSkipsLoad_ExecProvider(t *testing.T) {
	cityDir := t.TempDir()
	toml := "[workspace]\nname = \"t\"\n\n[beads]\nprovider = \"exec:noop.sh\"\n"
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadCityConfig(cityDir, io.Discard)
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	before := loadCityConfigCalls.Load()
	if _, err := openStoreResultAtForCityWithConfig(cityDir, cityDir, cfg, gate.ModeUnset, false, false, false, nil); err != nil {
		t.Fatalf("openStoreResultAtForCityWithConfig(cfg): %v", err)
	}
	if grew := loadCityConfigCalls.Load() - before; grew != 0 {
		t.Fatalf("openStoreResultAtForCityWithConfig re-parsed city config %d times despite a non-nil cfg (exec provider)", grew)
	}

	before = loadCityConfigCalls.Load()
	if _, err := openStoreResultAtForCityWithConfig(cityDir, cityDir, nil, gate.ModeUnset, false, false, false, nil); err != nil {
		t.Fatalf("openStoreResultAtForCityWithConfig(nil): %v", err)
	}
	if grew := loadCityConfigCalls.Load() - before; grew != 1 {
		t.Fatalf("openStoreResultAtForCityWithConfig(nil cfg) parsed city config %d times, want exactly 1 (fallback load, exec provider)", grew)
	}
}

// TestOpenStoreResultNilConfigMatchesLegacy is a regression guard for the
// ga-237xpr refactor: openStoreResultAtForCityWithAuthority (the pre-existing
// entry point every non-dispatcher caller still uses) now delegates to
// openStoreResultAtForCityWithConfig with a nil config, and must keep both of
// its legacy characteristics — conditional-writes threading still works, and
// every call still reloads city.toml from disk (correct for callers like CLI
// commands, where the config may have changed since the last invocation).
func TestOpenStoreResultNilConfigMatchesLegacy(t *testing.T) {
	cityDir := t.TempDir()
	toml := "[workspace]\nname = \"t\"\n\n[beads]\nprovider = \"file\"\nconditional_writes = \"require\"\n"
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := openStoreResultAtForCityWithAuthority(cityDir, cityDir, gate.ModeUnset, false, false, false, nil)
	if err != nil {
		t.Fatalf("openStoreResultAtForCityWithAuthority: %v", err)
	}
	writer, diag, resolveErr := beads.ResolveConditionalWriter(result.Store)
	if resolveErr != nil || diag != nil {
		t.Fatalf("resolve = diag %v err %v, want the file store's writer under require", diag, resolveErr)
	}
	if writer == nil {
		t.Fatal("require in city.toml was not observed via the WithAuthority entry point after the WithConfig refactor")
	}

	before := loadCityConfigCalls.Load()
	if _, err := openStoreResultAtForCityWithAuthority(cityDir, cityDir, gate.ModeUnset, false, false, false, nil); err != nil {
		t.Fatalf("openStoreResultAtForCityWithAuthority (second call): %v", err)
	}
	if grew := loadCityConfigCalls.Load() - before; grew != 1 {
		t.Fatalf("openStoreResultAtForCityWithAuthority parsed city config %d times, want exactly 1 (legacy per-call reload preserved for non-dispatcher callers)", grew)
	}
}

// TestOpenRigStoreThreadsConditionalWrites drives the controller's rig-store
// open end-to-end with a file provider: the boot-latched rollout flags must
// reach the factory stamp, including on the file path (which previously
// bypassed the factory entirely via an early return).
func TestOpenRigStoreThreadsConditionalWrites(t *testing.T) {
	stubManagedDoltStoreOpeners(t)
	cityDir := t.TempDir()
	toml := "[workspace]\nname = \"t\"\n\n[beads]\nconditional_writes = \"require\"\n"
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(toml))
	if err != nil {
		t.Fatal(err)
	}
	cs := newControllerState(context.Background(), cfg, nil, nil, "t", cityDir)

	rigPath := filepath.Join(cityDir, "rigs", "r1")
	if err := os.MkdirAll(rigPath, 0o755); err != nil {
		t.Fatal(err)
	}
	store := cs.openRigStore("file", "r1", rigPath, "ga", cfg)
	writer, diag, resolveErr := beads.ResolveConditionalWriter(store)
	if resolveErr != nil || diag != nil {
		t.Fatalf("resolve = diag %v err %v, want the rig store's writer under require", diag, resolveErr)
	}
	if writer == nil {
		t.Fatal("boot-latched require was not observed on the rig store")
	}
}

// TestResolvedNativeTransportMode mirrors TestResolvedConditionalWritesMode
// for beads.native_transport.
func TestResolvedNativeTransportMode(t *testing.T) {
	t.Run("nil config is unset", func(t *testing.T) {
		if got := resolvedNativeTransportMode(nil); got != beads.NativeTransportUnset {
			t.Fatalf("mode = %q, want unset", got)
		}
	})
	t.Run("resolved config value threads through, normalized", func(t *testing.T) {
		cfg, err := config.Parse([]byte("[workspace]\nname = \"t\"\n\n[beads]\nnative_transport = \"off\"\n"))
		if err != nil {
			t.Fatal(err)
		}
		if got := resolvedNativeTransportMode(cfg); got != beads.NativeTransportOff {
			t.Fatalf("mode = %q, want off", got)
		}
	})
	t.Run("mixed case and whitespace normalize too", func(t *testing.T) {
		cfg := &config.City{Beads: config.BeadsConfig{NativeTransport: " OFF "}}
		if got := resolvedNativeTransportMode(cfg); got != beads.NativeTransportOff {
			t.Fatalf("mode = %q, want off: NormalizedNativeTransport did not fold case/whitespace before the cast to beads.NativeTransportMode", got)
		}
	})
}

// TestOpenStoreAtForCityThreadsNativeTransportFromLoadedConfig drives the
// shared, nil-cfg open path every hook-claim / convoy / order-dispatch / sweep
// call site uses (openStoreAtForCity -> ... -> openStoreResultAtForCityScoped,
// which loads cfg from disk itself because none of those callers has one in
// hand). A mutation that made openStoreResultAtForCityScoped ignore the cfg it
// just loaded — e.g. resolving native transport from a zero-value config
// instead — would leave beads.native_transport="off" unenforced on this exact
// path, silently, with no caller able to tell.
func TestOpenStoreAtForCityThreadsNativeTransportFromLoadedConfig(t *testing.T) {
	cityDir := t.TempDir()
	toml := "[workspace]\nname = \"t\"\n\n[beads]\nnative_transport = \"off\"\n"
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}

	var captured beads.StoreOpenOptions
	restore := openStoreFactoryForCity
	openStoreFactoryForCity = func(_ context.Context, opts beads.StoreOpenOptions) (beads.StoreOpenResult, error) {
		captured = opts
		return beads.StoreOpenResult{Store: beads.NewMemStore()}, nil
	}
	t.Cleanup(func() { openStoreFactoryForCity = restore })

	if _, err := openStoreAtForCity(cityDir, cityDir); err != nil {
		t.Fatalf("openStoreAtForCity: %v", err)
	}
	if captured.NativeTransport != beads.NativeTransportOff {
		t.Fatalf("StoreOpenOptions.NativeTransport = %q, want %q: beads.native_transport=\"off\" on disk did not reach the factory via the nil-cfg call path",
			captured.NativeTransport, beads.NativeTransportOff)
	}
}

// TestOpenStoreAtForCityConfigLoadError pins which opens fail when the
// city's city.toml exists but does not load. Only an open the city's
// native_transport value would decide fails: no boot-latched value is in hand
// and the provider reaches that decision, so an unreadable "off" must not
// open as "auto". Every other open proceeds on a nil config, best-effort,
// because the load error cannot change it.
func TestOpenStoreAtForCityConfigLoadError(t *testing.T) {
	const brokenInclude = "include = [\"broken.toml\"]\n\n[workspace]\nname = \"t\"\n"
	off := beads.NativeTransportOff
	cases := []struct {
		name     string
		cityTOML string
		override *beads.NativeTransportMode
		wantErr  bool
	}{
		{
			name:     "bd provider with an out-of-enum native_transport fails",
			cityTOML: "[workspace]\nname = \"t\"\n\n[beads]\nnative_transport = \"bogus\"\n",
			wantErr:  true,
		},
		{
			name:     "bd provider with a broken include fails",
			cityTOML: brokenInclude,
			wantErr:  true,
		},
		{
			name:     "bd-contract exec provider with a broken include fails",
			cityTOML: brokenInclude + "\n[beads]\nprovider = \"exec:gc-beads-bd\"\n",
			wantErr:  true,
		},
		{
			name:     "file provider with a broken include opens",
			cityTOML: brokenInclude + "\n[beads]\nprovider = \"file\"\n",
		},
		{
			name:     "file provider with an out-of-enum conditional_writes opens",
			cityTOML: "[workspace]\nname = \"t\"\n\n[beads]\nprovider = \"file\"\nconditional_writes = \"bogus\"\n",
		},
		{
			name:     "exec provider outside the bd contract with a broken include opens",
			cityTOML: brokenInclude + "\n[beads]\nprovider = \"exec:noop.sh\"\n",
		},
		{
			name:     "boot-latched native_transport with a broken include opens",
			cityTOML: brokenInclude,
			override: &off,
		},
		{
			name:     "boot-latched native_transport with an unparseable city.toml opens",
			cityTOML: "[workspace\n",
			override: &off,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cityDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(cityDir, "broken.toml"), []byte("["), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(tc.cityTOML), 0o644); err != nil {
				t.Fatal(err)
			}
			var captured *beads.StoreOpenOptions
			restore := openStoreFactoryForCity
			openStoreFactoryForCity = func(_ context.Context, opts beads.StoreOpenOptions) (beads.StoreOpenResult, error) {
				captured = &opts
				return beads.StoreOpenResult{Store: beads.NewMemStore()}, nil
			}
			t.Cleanup(func() { openStoreFactoryForCity = restore })

			_, err := openStoreResultAtForCityWithMode(cityDir, cityDir, gate.ModeUnset, false, false, tc.override)
			if tc.wantErr {
				if err == nil {
					t.Fatal("open succeeded; want the city.toml load error, because this city's unread native_transport would decide the open")
				}
				if captured != nil {
					t.Fatal("the store factory was reached after the city config failed to load")
				}
				return
			}
			if err != nil {
				t.Fatalf("open failed on a load error that cannot change it: %v", err)
			}
			if captured == nil {
				t.Fatal("the store factory was not reached")
			}
			if tc.override != nil && captured.NativeTransport != *tc.override {
				t.Fatalf("NativeTransport = %q, want the boot-latched %q", captured.NativeTransport, *tc.override)
			}
		})
	}
}

// TestCmdHookClaimUnderABrokenPackIncludeStopsAtItsOwnConfigLoad pins what a
// claim costs when a pack include does not load: gc hook --claim exits on its
// own config-load error, naming the include, before it runs a bd command or
// opens a store. The fail-closed load inside the store open
// (loadCityConfigForStoreOpen) is therefore never what refuses a claim.
func TestCmdHookClaimUnderABrokenPackIncludeStopsAtItsOwnConfigLoad(t *testing.T) {
	clearGCEnv(t)
	disableManagedDoltRecoveryForTest(t)
	cityDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityDir, ".gc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityDir, "broken.toml"), []byte("["), 0o644); err != nil {
		t.Fatal(err)
	}
	cityTOML := "include = [\"broken.toml\"]\n\n[workspace]\nname = \"test-city\"\n\n[[agent]]\nname = \"worker\"\n"
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(cityTOML), 0o644); err != nil {
		t.Fatal(err)
	}
	// Any bd subprocess, the work query and the claim alike, appends here.
	fakeBin := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "bd.log")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %q\nprintf '[]'\n", logPath)
	if err := os.WriteFile(filepath.Join(fakeBin, "bd"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_AGENT", "worker")
	opened := false
	restore := openStoreFactoryForCity
	openStoreFactoryForCity = func(context.Context, beads.StoreOpenOptions) (beads.StoreOpenResult, error) {
		opened = true
		return beads.StoreOpenResult{Store: beads.NewMemStore()}, nil
	}
	t.Cleanup(func() { openStoreFactoryForCity = restore })

	var stdout, stderr bytes.Buffer
	if code := cmdHookWithOptions(nil, hookCommandOptions{Claim: true}, &stdout, &stderr); code != 1 {
		t.Fatalf("gc hook --claim = %d, want 1; stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	_, loadErr := loadCityConfig(cityDir, io.Discard)
	if loadErr == nil || !strings.Contains(loadErr.Error(), "broken.toml") {
		t.Fatalf("loadCityConfig = %v, want an error naming broken.toml", loadErr)
	}
	if want := "gc hook: " + loadErr.Error() + "\n"; stderr.String() != want {
		t.Fatalf("stderr = %q, want the hook's own config-load error %q", stderr.String(), want)
	}
	if opened {
		t.Fatal("a store was opened under a city config that did not load")
	}
	if ran, err := os.ReadFile(logPath); err == nil {
		t.Fatalf("bd ran under a city config that did not load:\n%s", ran)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

// TestOpenStoreAtForCityToleratesNoCityTOMLAtAll pins that a path with no
// city.toml at all is not a load error: most callers of this shared open body
// (ad hoc store paths, rig/scope stores outside any city) pass one, and it
// keeps resolving to the nil-cfg best-effort default.
func TestOpenStoreAtForCityToleratesNoCityTOMLAtAll(t *testing.T) {
	storeDir := t.TempDir() // deliberately no city.toml anywhere above this.

	var captured beads.StoreOpenOptions
	restore := openStoreFactoryForCity
	openStoreFactoryForCity = func(_ context.Context, opts beads.StoreOpenOptions) (beads.StoreOpenResult, error) {
		captured = opts
		return beads.StoreOpenResult{Store: beads.NewMemStore()}, nil
	}
	t.Cleanup(func() { openStoreFactoryForCity = restore })

	if _, err := openStoreAtForCity(storeDir, storeDir); err != nil {
		t.Fatalf("openStoreAtForCity: want no-city.toml to stay non-fatal, got: %v", err)
	}
	if captured.NativeTransport != beads.NativeTransportUnset {
		t.Fatalf("NativeTransport = %q, want unset (no config to resolve from)", captured.NativeTransport)
	}
}

// TestOpenRigStoreThreadsNativeTransport proves the controller's per-rig open
// (api_state.go's openRigStore) threads the boot-latched native_transport
// value into StoreOpenOptions, the way TestOpenRigStoreThreadsConditionalWrites
// proves it for conditional_writes. A mutation that dropped the
// NativeTransport field from that call would leave every rig in an "off" city
// still eligible to open natively.
func TestOpenRigStoreThreadsNativeTransport(t *testing.T) {
	prevOpen := controllerStateOpenRigStoreAtForCity
	t.Cleanup(func() { controllerStateOpenRigStoreAtForCity = prevOpen })

	cityDir := t.TempDir()
	cfg := &config.City{Workspace: config.Workspace{Name: "t"}}
	// A directly-built controllerState, not newControllerState: the
	// constructor's own best-effort city-store open spawns a real managed
	// dolt process when unstubbed (~10s), which this test has no need to pay
	// — it exercises openRigStore in isolation, exactly like
	// TestControllerStateBuildStoresRoutesBdRigThroughStoreFactory does.
	cs := &controllerState{cityPath: cityDir, cfg: cfg, nativeTransport: beads.NativeTransportOff}

	rigPath := filepath.Join(cityDir, "rigs", "r1")
	if err := os.MkdirAll(rigPath, 0o755); err != nil {
		t.Fatal(err)
	}

	var captured beads.StoreOpenOptions
	controllerStateOpenRigStoreAtForCity = func(_ context.Context, opts beads.StoreOpenOptions) (beads.StoreOpenResult, error) {
		captured = opts
		return beads.StoreOpenResult{Store: beads.NewMemStore()}, nil
	}

	cs.openRigStore("bd", "r1", rigPath, "ga", cfg)
	if captured.NativeTransport != beads.NativeTransportOff {
		t.Fatalf("openRigStore StoreOpenOptions.NativeTransport = %q, want %q", captured.NativeTransport, beads.NativeTransportOff)
	}
}

// TestOpenBdStoreAtScopedConsultsTheDoltliteReadOptimizationOnlyOutsideOff
// pins, in the default build, which bd store opens may consult the
// GC_NATIVE_DOLTLITE_BEADS read optimization. Under "off", per city or through
// the deprecated GC_BEADS_FORCE_FALLBACK alias, no scope consults it and each
// gets the plain BdStore; otherwise every scope consults it and takes what it
// returns. The city scope, its one-shot variant and a rig scope are covered.
// The tagged TestOpenBdStoreAtScopedSkipsDoltliteOptimizationUnderNativeTransportOff
// proves the city refusal against the real optimization, which only a
// gascity_native_beads build has.
func TestOpenBdStoreAtScopedConsultsTheDoltliteReadOptimizationOnlyOutsideOff(t *testing.T) {
	clearGCEnv(t)
	cityDir := t.TempDir()
	writeMinimalCityToml(t, cityDir)
	rigDir := filepath.Join(cityDir, "rigs", "app")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatal(err)
	}

	optimized := beads.NewMemStore()
	available := true
	var consulted []string
	restore := openDoltliteReadOptimization
	openDoltliteReadOptimization = func(storePath string, _ *beads.BdStore) (beads.Store, bool) {
		consulted = append(consulted, storePath)
		if !available {
			return nil, false
		}
		return optimized, true
	}
	t.Cleanup(func() { openDoltliteReadOptimization = restore })

	scopes := []struct {
		name      string
		storePath string
		oneShot   bool
	}{
		{name: "city", storePath: cityDir},
		{name: "one-shot city", storePath: cityDir, oneShot: true},
		{name: "rig", storePath: rigDir},
	}
	for _, tc := range []struct {
		name          string
		mode          beads.NativeTransportMode
		forceFallback string
		available     bool
		wantConsulted bool
		wantOptimized bool
	}{
		{name: "unset", mode: beads.NativeTransportUnset, available: true, wantConsulted: true, wantOptimized: true},
		{name: "auto", mode: beads.NativeTransportAuto, available: true, wantConsulted: true, wantOptimized: true},
		{name: "auto without the optimization", mode: beads.NativeTransportAuto, wantConsulted: true},
		{name: "off", mode: beads.NativeTransportOff, available: true},
		{name: "auto under GC_BEADS_FORCE_FALLBACK", mode: beads.NativeTransportAuto, forceFallback: "1", available: true},
	} {
		for _, scope := range scopes {
			t.Run(tc.name+"/"+scope.name, func(t *testing.T) {
				t.Setenv("GC_BEADS_FORCE_FALLBACK", tc.forceFallback)
				available = tc.available
				consulted = nil

				store, err := openBdStoreAtScoped(scope.storePath, cityDir, &config.City{}, scope.oneShot, tc.mode)
				if err != nil {
					t.Fatalf("openBdStoreAtScoped: %v", err)
				}
				switch {
				case !tc.wantConsulted && len(consulted) != 0:
					t.Fatalf("the doltlite read optimization was consulted for %v; it must not be under this native_transport", consulted)
				case tc.wantConsulted && (len(consulted) != 1 || consulted[0] != scope.storePath):
					t.Fatalf("doltlite read optimization consulted for %v, want exactly [%s]", consulted, scope.storePath)
				}
				if tc.wantOptimized {
					if store != optimized {
						t.Fatalf("openBdStoreAtScoped returned %T, want the store the optimization returned", store)
					}
					return
				}
				if _, isPlainBd := store.(*beads.BdStore); !isPlainBd {
					t.Fatalf("openBdStoreAtScoped returned %T, want a plain *beads.BdStore", store)
				}
			})
		}
	}
}

// TestNewControllerStateOpenCityStoreThreadsTheLatchedNativeTransport proves
// the DEFAULT newControllerStateOpenCityStore closure — not a test stub of
// it — passes its nativeTransport parameter (the boot latch) through to the
// store-open options, by exercising the real var directly and capturing what
// reaches the factory. api_state_rollout_test.go's
// TestControllerStateNativeTransportDoesNotFlipOnReload stubs this var
// entirely, which proves newControllerState calls it with the right argument
// but can never catch a bug inside the default implementation itself — such
// as passing nil instead of &nativeTransport.
func TestNewControllerStateOpenCityStoreThreadsTheLatchedNativeTransport(t *testing.T) {
	cityDir := t.TempDir()
	writeMinimalCityToml(t, cityDir)

	var captured beads.StoreOpenOptions
	restore := openStoreFactoryForCity
	openStoreFactoryForCity = func(_ context.Context, opts beads.StoreOpenOptions) (beads.StoreOpenResult, error) {
		captured = opts
		return beads.StoreOpenResult{Store: beads.NewMemStore()}, nil
	}
	t.Cleanup(func() { openStoreFactoryForCity = restore })

	if _, err := newControllerStateOpenCityStore(cityDir, gate.ModeUnset, beads.NativeTransportOff); err != nil {
		t.Fatalf("newControllerStateOpenCityStore: %v", err)
	}
	if captured.NativeTransport != beads.NativeTransportOff {
		t.Fatalf("NativeTransport = %q, want %q: the default newControllerStateOpenCityStore did not thread its parameter through",
			captured.NativeTransport, beads.NativeTransportOff)
	}
}

// TestOpenControlBdStoreThroughFactoryStamps pins the control-dispatcher
// routing for a scope that does not pass the native-store preflight (here
// /city carries no .beads/metadata.json): the raw control-plane bd store must
// come back factory-stamped, and raw — control paths are deliberately
// unwrapped.
func TestOpenControlBdStoreThroughFactoryStamps(t *testing.T) {
	useControlStoreRegistryForTest(t)
	cfg, err := config.Parse([]byte("[workspace]\nname = \"t\"\n\n[beads]\nconditional_writes = \"require\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	capableHelp := []byte("Usage:\n  bd update [flags]\n\nFlags:\n  --if-revision int\n")
	raw := beads.NewBdStore("/city", func(_, _ string, _ ...string) ([]byte, error) {
		return capableHelp, nil
	})
	store, err := openControlBdStoreThroughFactory("/city", "/city", "bd", cfg,
		func() (beads.Store, error) { return raw, nil })
	if err != nil {
		t.Fatalf("openControlBdStoreThroughFactory: %v", err)
	}
	if store != beads.Store(raw) {
		t.Fatalf("store = %T, want the raw control bd store back (no policy wrap on control paths)", store)
	}
	writer, diag, resolveErr := beads.ResolveConditionalWriter(store)
	if resolveErr != nil || diag != nil {
		t.Fatalf("resolve = diag %v err %v, want the control store's writer under require", diag, resolveErr)
	}
	if writer == nil {
		t.Fatal("require was not stamped onto the control-plane bd store")
	}
}

// useControlStoreRegistryForTest gives the test an empty nativeControlStores
// on a clock the test moves through the returned pointer, and restores the
// process registry when the test ends.
func useControlStoreRegistryForTest(t *testing.T) *time.Time {
	t.Helper()
	clock := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	prev := nativeControlStores
	nativeControlStores = newControlStoreRegistry(func() time.Time { return clock })
	t.Cleanup(func() { nativeControlStores = prev })
	return &clock
}

// stubControlStoreFactory answers every factory open with answer(opts) and
// returns the options of each open, in order.
func stubControlStoreFactory(t *testing.T, answer func(beads.StoreOpenOptions) beads.Store) *[]beads.StoreOpenOptions {
	t.Helper()
	var opens []beads.StoreOpenOptions
	restore := openStoreFactoryForCity
	openStoreFactoryForCity = func(_ context.Context, opts beads.StoreOpenOptions) (beads.StoreOpenResult, error) {
		opens = append(opens, opts)
		return beads.StoreOpenResult{Store: answer(opts)}, nil
	}
	t.Cleanup(func() { openStoreFactoryForCity = restore })
	return &opens
}

// nativeUnlessOff answers a control open the way the factory does for a
// native-eligible scope: the native store when the transport allows it, the
// bd store when it is off.
func nativeUnlessOff(native, bd beads.Store) func(beads.StoreOpenOptions) beads.Store {
	return func(opts beads.StoreOpenOptions) beads.Store {
		if opts.NativeTransport == beads.NativeTransportOff {
			return bd
		}
		return native
	}
}

// TestOpenControlStoreKeepsTheFirstNativeStorePerScope pins why the control
// dispatcher stops forking bd: the first control open of a native-eligible
// scope asks the factory with the preflight checker and the native opener
// wired, and every later open of that scope (one per readiness scan, control
// bead and execution-fact flush) gets the same native store back without
// another preflight or connection pool. The per-call close each of those
// callers runs leaves the kept store open.
func TestOpenControlStoreKeepsTheFirstNativeStorePerScope(t *testing.T) {
	useControlStoreRegistryForTest(t)
	cfg := &config.City{Workspace: config.Workspace{Name: "demo"}}
	storage := &closeCountingStorage{}
	native := beads.NewNativeDoltStoreOverStorageForTest(storage)
	opens := stubControlStoreFactory(t, nativeUnlessOff(native, beads.NewMemStore()))
	openBd := func() (beads.Store, error) { return beads.NewMemStore(), nil }

	for i := 0; i < 3; i++ {
		store, err := openControlBdStoreThroughFactory("/city/rigs/repo", "/city", "bd", cfg, openBd)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if store != beads.Store(native) {
			t.Fatalf("open %d returned %T, want the native store the scope opened first", i, store)
		}
		if err := closeControlStoreHandle(store); err != nil {
			t.Fatalf("closeControlStoreHandle after open %d: %v", i, err)
		}
	}
	if len(*opens) != 1 {
		t.Fatalf("the factory was asked %d times, want once: a kept scope must not pay the preflight and a new pool per open", len(*opens))
	}
	if storage.closed != 0 {
		t.Fatalf("the kept native store was closed %d times by the per-call close; every later open would get a closed store", storage.closed)
	}
	opts := (*opens)[0]
	if opts.NativeTransport == beads.NativeTransportOff {
		t.Fatal("the control open asked for NativeTransportOff for a city that left native transport on")
	}
	if opts.PreflightChecker.BDContext == nil || opts.PreflightChecker.DatabaseProjectID == nil {
		t.Fatal("the control open passed no preflight checker, so the factory can never pick the native store")
	}
	if opts.OpenNativeStore == nil || opts.OpenBdStore == nil {
		t.Fatal("the control open must pass both the native opener and the bd fallback")
	}

	other, err := openControlBdStoreThroughFactory("/city/rigs/other", "/city", "bd", cfg, openBd)
	if err != nil {
		t.Fatalf("open other scope: %v", err)
	}
	if len(*opens) != 2 || other == nil {
		t.Fatalf("a second scope reused the first scope's store: factory asked %d times", len(*opens))
	}
}

// TestControlStoreNativeTransport pins which control opens ask the factory
// for the native store at all. The city's own beads.native_transport decides,
// except that an unread config and a library-extension scope ask for off: the
// first so an unread "off" never opens as "auto", the second because the
// library arm answers a failed open with an error instead of the bd store.
func TestControlStoreNativeTransport(t *testing.T) {
	doltScope := t.TempDir()
	if err := os.MkdirAll(filepath.Join(doltScope, ".beads"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(doltScope, ".beads", "metadata.json"),
		[]byte(`{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"demo"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	libraryScope := writeLibraryBackendScope(t)
	auto := &config.City{Workspace: config.Workspace{Name: "demo"}}
	off := &config.City{Workspace: config.Workspace{Name: "demo"}, Beads: config.BeadsConfig{NativeTransport: "off"}}

	for _, tc := range []struct {
		name  string
		scope string
		cfg   *config.City
		want  beads.NativeTransportMode
	}{
		{name: "the city's default", scope: doltScope, cfg: auto, want: beads.NativeTransportAuto},
		{name: "the city turned native off", scope: doltScope, cfg: off, want: beads.NativeTransportOff},
		{name: "unread config", scope: doltScope, cfg: nil, want: beads.NativeTransportOff},
		{name: "library-extension scope", scope: libraryScope, cfg: auto, want: beads.NativeTransportOff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := controlStoreNativeTransport(tc.scope, tc.cfg); got != tc.want {
				t.Fatalf("controlStoreNativeTransport() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestOpenControlStoreTurnedOffStopsHandingOutTheKeptNativeStore pins that
// beads.native_transport = "off" reaches a running control dispatcher on its
// next open, not at its next restart: the open bypasses the kept native store
// and asks the factory for the bd store. The kept store stays open, so turning
// native back on hands it out again.
func TestOpenControlStoreTurnedOffStopsHandingOutTheKeptNativeStore(t *testing.T) {
	useControlStoreRegistryForTest(t)
	auto := &config.City{Workspace: config.Workspace{Name: "demo"}}
	off := &config.City{Workspace: config.Workspace{Name: "demo"}, Beads: config.BeadsConfig{NativeTransport: "off"}}
	storage := &closeCountingStorage{}
	native := beads.NewNativeDoltStoreOverStorageForTest(storage)
	bd := beads.NewMemStore()
	opens := stubControlStoreFactory(t, nativeUnlessOff(native, bd))
	openBd := func() (beads.Store, error) { return bd, nil }
	const scope = "/city/rigs/repo"

	if store, err := openControlBdStoreThroughFactory(scope, "/city", "bd", auto, openBd); err != nil || store != beads.Store(native) {
		t.Fatalf("first open = (%T, %v), want the native store", store, err)
	}
	store, err := openControlBdStoreThroughFactory(scope, "/city", "bd", off, openBd)
	if err != nil {
		t.Fatalf("open under native_transport=off: %v", err)
	}
	if store != beads.Store(bd) {
		t.Fatalf("open under native_transport=off returned %T, want the bd store", store)
	}
	if got := (*opens)[len(*opens)-1].NativeTransport; got != beads.NativeTransportOff {
		t.Fatalf("open under native_transport=off asked the factory for %q, want off", got)
	}
	if err := closeControlStoreHandle(store); err != nil {
		t.Fatalf("closeControlStoreHandle(bd store): %v", err)
	}
	if storage.closed != 0 {
		t.Fatal("turning native transport off closed the kept native store")
	}
	if store, err := openControlBdStoreThroughFactory(scope, "/city", "bd", auto, openBd); err != nil || store != beads.Store(native) {
		t.Fatalf("open after native transport came back = (%T, %v), want the kept native store", store, err)
	}
}

// TestOpenControlStoreRechecksARefusedScopeOncePerWindow pins the cost of a
// scope the factory answers with the bd store. Without the window, every
// readiness scan would rerun the preflight (a bd context subprocess and two
// database probes) and log the refusal again. Inside the window the open asks
// for NativeTransportOff, which skips both; the first open after it asks again,
// so a refusal that has cleared reaches the native store.
func TestOpenControlStoreRechecksARefusedScopeOncePerWindow(t *testing.T) {
	clock := useControlStoreRegistryForTest(t)
	cfg := &config.City{Workspace: config.Workspace{Name: "demo"}}
	storage := &closeCountingStorage{}
	native := beads.NewNativeDoltStoreOverStorageForTest(storage)
	bd := beads.NewMemStore()
	eligible := false
	opens := stubControlStoreFactory(t, func(opts beads.StoreOpenOptions) beads.Store {
		if eligible && opts.NativeTransport != beads.NativeTransportOff {
			return native
		}
		return bd
	})
	const scope = "/city/rigs/repo"
	open := func() beads.Store {
		t.Helper()
		store, err := openControlBdStoreThroughFactory(scope, "/city", "bd", cfg,
			func() (beads.Store, error) { return bd, nil })
		if err != nil {
			t.Fatalf("openControlBdStoreThroughFactory: %v", err)
		}
		return store
	}
	lastTransport := func() beads.NativeTransportMode { return (*opens)[len(*opens)-1].NativeTransport }

	if store := open(); store != beads.Store(bd) || lastTransport() == beads.NativeTransportOff {
		t.Fatalf("first open = %T under %q, want the factory asked for native and answering bd", store, lastTransport())
	}
	*clock = clock.Add(controlStoreNativeRecheck - time.Second)
	if store := open(); store != beads.Store(bd) || lastTransport() != beads.NativeTransportOff {
		t.Fatalf("open inside the window = %T under %q, want the bd store under off", store, lastTransport())
	}

	eligible = true
	*clock = clock.Add(2 * time.Second)
	if store := open(); store != beads.Store(native) || lastTransport() == beads.NativeTransportOff {
		t.Fatalf("open after the window = %T under %q, want the factory asked again and the native store", store, lastTransport())
	}
	asked := len(*opens)
	if store := open(); store != beads.Store(native) || len(*opens) != asked {
		t.Fatalf("open after the scope turned native = %T with %d factory opens, want the kept store and no new open", store, len(*opens)-asked)
	}
}

// TestControlStoreRegistryKeepsTheWinnerOfAConcurrentFirstOpen pins the race
// between two first opens of one scope: both reach the factory, the first
// native store recorded is the one every caller gets, and the other is closed
// rather than leaked with its connection pool.
func TestControlStoreRegistryKeepsTheWinnerOfAConcurrentFirstOpen(t *testing.T) {
	registry := newControlStoreRegistry(time.Now)
	firstStorage, secondStorage := &closeCountingStorage{}, &closeCountingStorage{}
	first := beads.NewNativeDoltStoreOverStorageForTest(firstStorage)
	second := beads.NewNativeDoltStoreOverStorageForTest(secondStorage)

	if got := registry.retain("/scope", first); got != beads.Store(first) {
		t.Fatalf("retain(first) = %T, want the first store", got)
	}
	if got := registry.retain("/scope", second); got != beads.Store(first) {
		t.Fatalf("retain(second) = %T, want the store already kept", got)
	}
	if secondStorage.closed != 1 || firstStorage.closed != 0 {
		t.Fatalf("closes = first %d, second %d; want only the duplicate closed", firstStorage.closed, secondStorage.closed)
	}
	if got := registry.lookup("/scope"); got != beads.Store(first) {
		t.Fatalf("lookup = %T, want the kept store", got)
	}
}

// TestCloseControlStoreHandleClosesEveryStoreTheRegistryDoesNotKeep pins the
// other half of the close exemption: a native store the registry does not keep
// is closed like any other per-call handle, so the exemption cannot turn into
// a connection-pool leak.
func TestCloseControlStoreHandleClosesEveryStoreTheRegistryDoesNotKeep(t *testing.T) {
	useControlStoreRegistryForTest(t)
	keptStorage, looseStorage := &closeCountingStorage{}, &closeCountingStorage{}
	kept := beads.NewNativeDoltStoreOverStorageForTest(keptStorage)
	loose := beads.NewNativeDoltStoreOverStorageForTest(looseStorage)
	nativeControlStores.retain("/scope", kept)

	if err := closeControlStoreHandle(loose); err != nil {
		t.Fatalf("closeControlStoreHandle(loose): %v", err)
	}
	if err := closeControlStoreHandle(kept); err != nil {
		t.Fatalf("closeControlStoreHandle(kept): %v", err)
	}
	if looseStorage.closed != 1 {
		t.Fatalf("a native store the registry does not keep was closed %d times, want 1", looseStorage.closed)
	}
	if keptStorage.closed != 0 {
		t.Fatalf("the kept native store was closed %d times, want 0", keptStorage.closed)
	}
}

// TestOpenControlStoreAtForCityLogsAChosenBdStoreAtDebug pins the control
// path's logging. The control dispatcher opens a store on every readiness poll
// and dispatched control bead. A city that turned native transport off gets
// the bd store on each of those opens, a chosen fallback that must not reach
// the default handler as a WARN; it still logs at Debug under gate
// native_transport_off. A scope the factory refuses logs its refusal once,
// and the opens inside the recheck window log only the Debug pin. The control
// path passes no Logger, so the records land on slog.Default, which the test
// swaps for a recorder (so it cannot run in parallel).
func TestOpenControlStoreAtForCityLogsAChosenBdStoreAtDebug(t *testing.T) {
	const serverMetadata = `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"demo"}`
	const postgresMetadata = `{"database":"beads","backend":"postgres"}`
	type logCount struct{ warn, pinned int }
	countLogs := func(logs *captureHandler) logCount {
		var c logCount
		for _, r := range logs.records {
			if r.Message != "native_store_unavailable" {
				continue
			}
			if r.Level >= slog.LevelWarn {
				c.warn++
			}
			if r.Level == slog.LevelDebug && recordFields(r)["gate"].String() == "native_transport_off" {
				c.pinned++
			}
		}
		return c
	}
	setup := func(t *testing.T, scope, metadata, nativeTransport string) (string, string, *config.City, *captureHandler) {
		t.Helper()
		useControlStoreRegistryForTest(t)
		clearGCEnv(t)
		disableManagedDoltRecoveryForTest(t)
		t.Setenv("GC_BEADS", "bd")
		cityDir := t.TempDir()
		cfg := &config.City{
			Workspace: config.Workspace{Name: "demo"},
			Beads:     config.BeadsConfig{NativeTransport: nativeTransport},
			Rigs:      []config.Rig{{Name: "frontend", Path: "rigs/frontend"}},
		}
		scopeDir := cityDir
		if scope == "rig" {
			scopeDir = filepath.Join(cityDir, "rigs", "frontend")
		}
		if err := os.MkdirAll(filepath.Join(scopeDir, ".beads"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(scopeDir, ".beads", "metadata.json"), []byte(metadata), 0o644); err != nil {
			t.Fatalf("write %s metadata: %v", scope, err)
		}
		logs := &captureHandler{}
		prev := slog.Default()
		slog.SetDefault(slog.New(logs))
		t.Cleanup(func() { slog.SetDefault(prev) })
		return cityDir, scopeDir, cfg, logs
	}
	openBdStore := func(t *testing.T, scopeDir, cityDir string, cfg *config.City) {
		t.Helper()
		store, err := openControlStoreAtForCity(scopeDir, cityDir, cfg)
		if err != nil {
			t.Fatalf("openControlStoreAtForCity: %v", err)
		}
		if _, ok := store.(*beads.BdStore); !ok {
			t.Fatalf("store = %T, want the raw control bd store", store)
		}
	}

	for _, scope := range []string{"city", "rig"} {
		t.Run("native transport off/"+scope, func(t *testing.T) {
			cityDir, scopeDir, cfg, logs := setup(t, scope, serverMetadata, "off")
			openBdStore(t, scopeDir, cityDir, cfg)
			if got := countLogs(logs); got.warn != 0 || got.pinned != 1 {
				t.Fatalf("control open logged %d WARN+ and %d DEBUG native_transport_off records, want 0 and 1 (downgraded, not deleted)", got.warn, got.pinned)
			}
		})
		t.Run("refused scope/"+scope, func(t *testing.T) {
			cityDir, scopeDir, cfg, logs := setup(t, scope, postgresMetadata, "")
			openBdStore(t, scopeDir, cityDir, cfg)
			if got := countLogs(logs); got.warn != 1 || got.pinned != 0 {
				t.Fatalf("first open of a refused scope logged %d WARN+ and %d DEBUG pins, want the refusal once and no pin", got.warn, got.pinned)
			}
			openBdStore(t, scopeDir, cityDir, cfg)
			if got := countLogs(logs); got.warn != 1 || got.pinned != 1 {
				t.Fatalf("after a second open inside the recheck window: %d WARN+ and %d DEBUG pins, want the refusal still logged once and one pin", got.warn, got.pinned)
			}
		})
	}
}

func TestConditionalWritesDegradedRecorder(t *testing.T) {
	t.Run("nil recorder yields nil callback", func(t *testing.T) {
		if cb := conditionalWritesDegradedRecorder(nil, rollout.Flags{}, "rig/r1"); cb != nil {
			t.Fatal("want nil callback for busless paths")
		}
	})
	t.Run("records the typed event with wire vocabulary", func(t *testing.T) {
		fake := events.NewFake()
		cfg, err := config.Parse([]byte("[workspace]\nname = \"t\"\n\n[beads]\nconditional_writes = \"auto\"\n"))
		if err != nil {
			t.Fatal(err)
		}
		flags, err := rollout.Resolve(cfg, rollout.ResolveOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cb := conditionalWritesDegradedRecorder(fake, flags, "rig/r1")
		cb(beads.ConditionalWritesDegrade{StoreKind: "BdStore", Mode: "auto", Reason: "bd lacks --if-revision"})

		recorded, err := fake.List(events.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(recorded) != 1 || recorded[0].Type != events.BeadsConditionalWritesDegraded {
			t.Fatalf("recorded = %+v, want one beads.conditional_writes.degraded event", recorded)
		}
		var payload events.ConditionalWritesDegradedPayload
		if err := json.Unmarshal(recorded[0].Payload, &payload); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if payload.StoreID != "rig/r1" || payload.StoreKind != "bd" || payload.Mode != "auto" || payload.Origin != "config" {
			t.Fatalf("payload = %+v, want wire vocabulary (bd) + origin config", payload)
		}
	})
}

// TestConditionalWritesEventStoreKind pins the internal→wire vocabulary map,
// including the build-tagged DoltliteReadStore, which beads cannot name and
// therefore reaches this layer as its %T spelling.
func TestConditionalWritesEventStoreKind(t *testing.T) {
	for in, want := range map[string]string{
		beads.BeadsStoreNameBdStore:             "bd",
		beads.BeadsStoreNameNativeDoltStore:     "native",
		beads.BeadsStoreNameLibraryBackendStore: "native",
		beads.BeadsStoreNameFileStore:           "file",
		"MemStore":                              "mem",
		"CachingStore":                          "caching",
		"SQLiteStore":                           "sqlite-graph",
		"*beads.DoltliteReadStore":              "bd",
		"someFutureStore":                       "someFutureStore",
	} {
		if got := conditionalWritesEventStoreKind(in); got != want {
			t.Errorf("conditionalWritesEventStoreKind(%q) = %q, want %q", in, got, want)
		}
	}
}
