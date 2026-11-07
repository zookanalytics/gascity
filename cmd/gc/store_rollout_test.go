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
	"github.com/gastownhall/gascity/internal/beads/contract"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/rollout"
	"github.com/gastownhall/gascity/internal/rollout/gate"
)

func TestResolvedConditionalWritesMode(t *testing.T) {
	t.Run("nil config is unset", func(t *testing.T) {
		if got := resolvedConditionalWritesMode(nil); got != gate.ModeUnset {
			t.Fatalf("mode = %q, want unset", got)
		}
	})
	t.Run("unset config value resolves auto", func(t *testing.T) {
		cfg, err := config.Parse([]byte("[workspace]\nname = \"t\"\n"))
		if err != nil {
			t.Fatal(err)
		}
		if got := resolvedConditionalWritesMode(cfg); got != gate.Auto {
			t.Fatalf("mode = %q, want auto (the default since mc-dpo2b)", got)
		}
		cfg.Beads.ConditionalWrites = "off"
		if got := resolvedConditionalWritesMode(cfg); got != gate.Off {
			t.Fatalf("explicit off: mode = %q, want off", got)
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

// bindingAwareControlStoreFactory answers each control open the way the
// factory answers it from the scope's .beads/metadata.json at the time of the
// open: a Dolt server scope, under a transport that allows native, gets the
// native store of its database, made on the first open of that database, and
// any other scope gets bd. It returns each database's native store and the
// storage behind it, and the options of each open, in order.
func bindingAwareControlStoreFactory(t *testing.T, bd beads.Store) (map[string]*beads.NativeDoltStore, map[string]*closeCountingStorage, *[]beads.StoreOpenOptions) {
	t.Helper()
	natives := map[string]*beads.NativeDoltStore{}
	storages := map[string]*closeCountingStorage{}
	opens := stubControlStoreFactory(t, func(opts beads.StoreOpenOptions) beads.Store {
		data, err := os.ReadFile(filepath.Join(opts.ScopeRoot, ".beads", "metadata.json"))
		if err != nil {
			t.Fatalf("read %s metadata: %v", opts.ScopeRoot, err)
		}
		var meta struct {
			Backend      string `json:"backend"`
			DoltMode     string `json:"dolt_mode"`
			DoltDatabase string `json:"dolt_database"`
		}
		if err := json.Unmarshal(data, &meta); err != nil {
			t.Fatalf("parse %s metadata: %v", opts.ScopeRoot, err)
		}
		if opts.NativeTransport == beads.NativeTransportOff || meta.Backend != "dolt" || meta.DoltMode != "server" {
			return bd
		}
		if _, ok := natives[meta.DoltDatabase]; !ok {
			storages[meta.DoltDatabase] = &closeCountingStorage{}
			natives[meta.DoltDatabase] = beads.NewNativeDoltStoreOverStorageForTest(storages[meta.DoltDatabase])
		}
		return natives[meta.DoltDatabase]
	})
	return natives, storages, opens
}

const (
	// doltOneMetadata binds a scope to database one on a Dolt server.
	doltOneMetadata = `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"one"}`
	// postgresStorageMetadata binds a scope to a Postgres database through storage
	// fields, which the native store does not serve.
	postgresStorageMetadata = `{"database":"beads","backend":"postgres","storage_endpoint":"postgres://bd@db.example.test:5432","storage_database":"beads_pg"}`
)

// TestOpenControlStoreReopensAScopeWhoseBindingChanged pins that a kept native
// store serves only the binding its scope had when the store opened. A scope's
// .beads/metadata.json names its backend, its database and its Dolt mode. When
// it changes while the old database still answers, no read fails to show the
// change, so the next control open has to find it: it stops handing out the
// kept store and asks the factory for the new binding's store, as every other
// open in the city does. A caller that opened the old store before the change
// may still be using it, so that store stays open until the caller closes its
// handle, and is closed then.
func TestOpenControlStoreReopensAScopeWhoseBindingChanged(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata string
		// database is the Dolt database whose native store the factory answers
		// the new binding with, or "" when it answers with the bd store.
		database string
	}{
		{name: "another Dolt database", metadata: `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"two"}`, database: "two"},
		{name: "proxied-server", metadata: `{"database":"dolt","backend":"dolt","dolt_mode":"proxied-server","dolt_database":"one"}`},
		{name: "Postgres with storage fields", metadata: postgresStorageMetadata},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useControlStoreRegistryForTest(t)
			cfg := &config.City{Workspace: config.Workspace{Name: "demo"}}
			scope := t.TempDir()
			writeBackendMetadata(t, scope, doltOneMetadata)
			bd := beads.NewMemStore()
			natives, storages, opens := bindingAwareControlStoreFactory(t, bd)
			open := func() beads.Store {
				t.Helper()
				store, err := openControlBdStoreThroughFactory(scope, "/city", "bd", cfg,
					func() (beads.Store, error) { return bd, nil })
				if err != nil {
					t.Fatalf("openControlBdStoreThroughFactory: %v", err)
				}
				return store
			}

			inFlight := open()
			if inFlight != beads.Store(natives["one"]) {
				t.Fatalf("first open = %T, want the native store of database one", inFlight)
			}
			writeBackendMetadata(t, scope, tc.metadata)
			next := open()
			want := beads.Store(bd)
			if tc.database != "" {
				want = natives[tc.database]
			}
			if next != want || len(*opens) != 2 {
				t.Fatalf("open after the binding changed = %T with %d factory opens, want the new binding's store from a second factory open (the kept store serves the retired ledger)", next, len(*opens))
			}
			if storages["one"].closed != 0 {
				t.Fatal("the retired store was closed while a caller that opened it before the change still held it")
			}
			if err := closeControlStoreHandle(next); err != nil {
				t.Fatalf("closeControlStoreHandle(new store): %v", err)
			}
			if err := closeControlStoreHandle(inFlight); err != nil {
				t.Fatalf("closeControlStoreHandle(retired store): %v", err)
			}
			if storages["one"].closed != 1 {
				t.Fatalf("the retired store was closed %d times once its last handle came back, want 1", storages["one"].closed)
			}
			if tc.database == "" {
				return
			}
			if again := open(); again != next || len(*opens) != 2 || storages[tc.database].closed != 0 {
				t.Fatalf("open under the unchanged new binding = %T with %d factory opens, want the store kept for that binding", again, len(*opens))
			}
		})
	}
}

// endpointAwareControlStoreFactory answers each control open the way the
// factory answers a native-eligible scope: under a transport that allows
// native, the native store of the host and port the scope's native open env
// names at the time of the open, made on the first open of that endpoint, and
// the bd store otherwise. The env comes from the real resolver with no config
// passed, so it reads the scope's .beads/config.yaml and city.toml as they are
// at the open. It returns each endpoint's native store and the storage behind
// it, and the options of each open, in order.
func endpointAwareControlStoreFactory(t *testing.T, bd beads.Store) (map[string]*beads.NativeDoltStore, map[string]*closeCountingStorage, *[]beads.StoreOpenOptions) {
	t.Helper()
	natives := map[string]*beads.NativeDoltStore{}
	storages := map[string]*closeCountingStorage{}
	opens := stubControlStoreFactory(t, func(opts beads.StoreOpenOptions) beads.Store {
		if opts.NativeTransport == beads.NativeTransportOff {
			return bd
		}
		env, err := nativeDoltOpenEnvForScope(opts.CityPath, nil, opts.ScopeRoot)
		if err != nil {
			t.Fatalf("resolve the native open env of %s: %v", opts.ScopeRoot, err)
		}
		endpoint := env["BEADS_DOLT_SERVER_HOST"] + ":" + env["BEADS_DOLT_SERVER_PORT"]
		if _, ok := natives[endpoint]; !ok {
			storages[endpoint] = &closeCountingStorage{}
			natives[endpoint] = beads.NewNativeDoltStoreOverStorageForTest(storages[endpoint])
		}
		return natives[endpoint]
	})
	return natives, storages, opens
}

// TestOpenControlStoreReopensAScopeWhoseEndpointMoved pins that a kept native
// store serves only the endpoint its scope's native open env named when the
// store opened. gc rig set-endpoint moves a rig to another Dolt server by
// rewriting the rig's .beads/config.yaml and its endpoint in city.toml, and a
// rig can also carry its endpoint in city.toml alone. Neither move changes an
// already-canonical .beads/metadata.json, and a store on the old server that
// still answers never reconnects. So the next control open has to find the
// move in the env it resolves: it stops handing out the kept store and asks
// the factory for the new endpoint's store. A caller that opened the old store
// before the move may still be using it, so that store stays open until the
// caller closes its handle, and is closed then.
func TestOpenControlStoreReopensAScopeWhoseEndpointMoved(t *testing.T) {
	for _, tc := range []struct {
		name string
		// point moves the frontend rig to the Dolt server at host:port.
		point func(t *testing.T, cityDir, host, port string)
	}{
		{name: "gc rig set-endpoint", point: setFrontendRigEndpointForTest},
		{name: "a legacy rig endpoint in city.toml", point: writeLegacyFrontendRigEndpointForTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GC_BEADS", "bd")
			t.Setenv("GC_DOLT", "skip")
			useControlStoreRegistryForTest(t)
			cityDir := t.TempDir()
			rigDir := filepath.Join(t.TempDir(), "frontend")
			writeRigEndpointCityConfig(t, cityDir, rigDir)
			writeRigEndpointMetadata(t, rigDir, "fe")
			tc.point(t, cityDir, "db-one.example.com", "3307")
			bd := beads.NewMemStore()
			natives, storages, opens := endpointAwareControlStoreFactory(t, bd)
			// open reads city.toml for every open, as the control dispatcher
			// does for every readiness scan and every control bead.
			open := func() beads.Store {
				t.Helper()
				cfg, err := loadCityConfig(cityDir, io.Discard)
				if err != nil {
					t.Fatalf("loadCityConfig: %v", err)
				}
				resolveRigPaths(cityDir, cfg.Rigs)
				store, err := openControlBdStoreThroughFactory(rigDir, cityDir, "bd", cfg,
					func() (beads.Store, error) { return bd, nil })
				if err != nil {
					t.Fatalf("openControlBdStoreThroughFactory: %v", err)
				}
				return store
			}
			const one, two = "db-one.example.com:3307", "db-two.example.com:3308"

			inFlight := open()
			if inFlight != beads.Store(natives[one]) {
				t.Fatalf("first open = %T, want the native store of %s", inFlight, one)
			}
			metadata := scopeMetadataDigest(rigDir)
			tc.point(t, cityDir, "db-two.example.com", "3308")
			if got := scopeMetadataDigest(rigDir); got != metadata {
				t.Fatalf("the move rewrote .beads/metadata.json (%s, was %s); this case needs a move that leaves it as it was", got, metadata)
			}
			next := open()
			if next != beads.Store(natives[two]) || len(*opens) != 2 {
				t.Fatalf("open after the move = %T with %d factory opens, want the native store of %s from a second factory open (the kept store reads and writes the old server)", next, len(*opens), two)
			}
			if storages[one].closed != 0 {
				t.Fatal("the retired store was closed while a caller that opened it before the move still held it")
			}
			if err := closeControlStoreHandle(next); err != nil {
				t.Fatalf("closeControlStoreHandle(new store): %v", err)
			}
			if err := closeControlStoreHandle(inFlight); err != nil {
				t.Fatalf("closeControlStoreHandle(retired store): %v", err)
			}
			if storages[one].closed != 1 {
				t.Fatalf("the retired store was closed %d times once its last handle came back, want 1", storages[one].closed)
			}
			if again := open(); again != next || len(*opens) != 2 || storages[two].closed != 0 {
				t.Fatalf("open under the unchanged new endpoint = %T with %d factory opens, want the store kept for that endpoint", again, len(*opens))
			}
		})
	}
}

// setFrontendRigEndpointForTest points the frontend rig of a
// writeRigEndpointCityConfig city at the external Dolt server at host:port, as
// gc rig set-endpoint --external does, with the endpoint check passing.
func setFrontendRigEndpointForTest(t *testing.T, cityDir, host, port string) {
	t.Helper()
	origVerify := verifyRigExternalEndpoint
	verifyRigExternalEndpoint = func(contract.ConfigState, string, string) error { return nil }
	defer func() { verifyRigExternalEndpoint = origVerify }()
	var stdout, stderr bytes.Buffer
	if code := doRigSetEndpoint(fsys.OSFS{}, cityDir, "frontend", rigEndpointOptions{External: true, Host: host, Port: port}, &stdout, &stderr); code != 0 {
		t.Fatalf("doRigSetEndpoint(%s:%s) = %d, stderr = %s", host, port, code, stderr.String())
	}
}

// writeLegacyFrontendRigEndpointForTest points the frontend rig of a
// writeRigEndpointCityConfig city at the Dolt server at host:port through the
// rig's dolt_host and dolt_port in city.toml, leaving the rig with no
// .beads/config.yaml.
func writeLegacyFrontendRigEndpointForTest(t *testing.T, cityDir, host, port string) {
	t.Helper()
	content := fmt.Sprintf("[workspace]\n\n[[rigs]]\nname = \"frontend\"\nprefix = \"fe\"\ndolt_host = %q\ndolt_port = %q\n", host, port)
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestOpenControlStoreRetiresTheKeptStoreOfAScopeThatTurnsLibraryBacked pins
// the same rule for an open that asks for NativeTransportOff. A scope converted
// to a backend the beads library opens as an extension takes the bd store from
// then on, and the native store kept for its old Dolt database is retired. No
// caller holds it, so it is closed at once rather than keeping its connection
// pool to the old database open for the life of the process.
func TestOpenControlStoreRetiresTheKeptStoreOfAScopeThatTurnsLibraryBacked(t *testing.T) {
	useControlStoreRegistryForTest(t)
	registerLibraryBackendForTest(t, "http")
	cfg := &config.City{Workspace: config.Workspace{Name: "demo"}}
	scope := t.TempDir()
	writeBackendMetadata(t, scope, doltOneMetadata)
	bd := beads.NewMemStore()
	natives, storages, opens := bindingAwareControlStoreFactory(t, bd)
	openBd := func() (beads.Store, error) { return bd, nil }

	kept, err := openControlBdStoreThroughFactory(scope, "/city", "bd", cfg, openBd)
	if err != nil || kept != beads.Store(natives["one"]) {
		t.Fatalf("first open = (%T, %v), want the native store of database one", kept, err)
	}
	if err := closeControlStoreHandle(kept); err != nil {
		t.Fatalf("closeControlStoreHandle: %v", err)
	}
	writeBackendMetadata(t, scope, convertedHTTPMetadata)
	store, err := openControlBdStoreThroughFactory(scope, "/city", "bd", cfg, openBd)
	if err != nil || store != beads.Store(bd) {
		t.Fatalf("open of the library-backed scope = (%T, %v), want the bd store", store, err)
	}
	if got := (*opens)[len(*opens)-1].NativeTransport; got != beads.NativeTransportOff {
		t.Fatalf("open of the library-backed scope asked the factory for %q, want off", got)
	}
	if storages["one"].closed != 1 {
		t.Fatalf("the native store kept for the old binding was closed %d times, want 1", storages["one"].closed)
	}
}

// TestOpenControlStoreAsksAgainWhenARefusedScopeIsRebound pins that the recheck
// window holds a refusal only for the binding the factory refused. A scope
// moved onto a Dolt server inside the window asks the factory on its next open
// instead of taking the bd store until the window ends.
func TestOpenControlStoreAsksAgainWhenARefusedScopeIsRebound(t *testing.T) {
	useControlStoreRegistryForTest(t)
	cfg := &config.City{Workspace: config.Workspace{Name: "demo"}}
	scope := t.TempDir()
	writeBackendMetadata(t, scope, postgresStorageMetadata)
	bd := beads.NewMemStore()
	natives, _, opens := bindingAwareControlStoreFactory(t, bd)
	openBd := func() (beads.Store, error) { return bd, nil }

	if store, err := openControlBdStoreThroughFactory(scope, "/city", "bd", cfg, openBd); err != nil || store != beads.Store(bd) {
		t.Fatalf("open of the Postgres scope = (%T, %v), want the bd store", store, err)
	}
	writeBackendMetadata(t, scope, doltOneMetadata)
	store, err := openControlBdStoreThroughFactory(scope, "/city", "bd", cfg, openBd)
	if err != nil || store != beads.Store(natives["one"]) {
		t.Fatalf("open inside the recheck window after the scope moved onto Dolt = (%T, %v) under %q, want the factory asked again and the native store", store, err, (*opens)[len(*opens)-1].NativeTransport)
	}
}

// nativeEligibleMetadata binds a scope to database one on a Dolt server, with
// the project id passingControlPreflightForTest's probes report, so the real
// factory admits the native store for it.
const nativeEligibleMetadata = `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"one","project_id":"gc-local"}`

// foreignBdHook is a bd hook script gc did not install.
const foreignBdHook = "#!/bin/sh\nexit 0\n"

// passingControlPreflightForTest is a native-store preflight whose probes all
// answer in process for a nativeEligibleMetadata scope: bd reports a Dolt
// server, the database reports the scope's project id, and its schema sits at
// the library's ceiling.
func passingControlPreflightForTest() contract.PreflightChecker {
	return contract.PreflightChecker{
		Provider:            "bd",
		BeadsLibraryVersion: "1.0.4",
		BDContext: func(string) (contract.PreflightBDContext, error) {
			return contract.PreflightBDContext{Backend: "dolt", DoltMode: "server", BDVersion: "1.0.4"}, nil
		},
		DatabaseProjectID: func(string) (string, bool, error) {
			return "gc-local", true, nil
		},
		SchemaLatestVersion:        1,
		SchemaLatestIgnoredVersion: 1,
		DatabaseSchemaCursors: func(string) (contract.PreflightSchemaCursors, bool, error) {
			return contract.PreflightSchemaCursors{Main: 1, Ignored: 1, IgnoredChecked: true}, true, nil
		},
	}
}

// realControlStoreFactory answers each control open with the real beads
// factory. Only the preflight's probes and the native open are supplied: the
// preflight passes (passingControlPreflightForTest), and each native open makes
// a new native store. So whether an open gets a native store or the bd store is
// the factory's own decision, its executable-bd-hook rule included. It returns
// the native stores made and the storage behind each, in order, and the options
// of each open.
func realControlStoreFactory(t *testing.T) (*[]*beads.NativeDoltStore, *[]*closeCountingStorage, *[]beads.StoreOpenOptions) {
	t.Helper()
	var natives []*beads.NativeDoltStore
	var storages []*closeCountingStorage
	var opens []beads.StoreOpenOptions
	restore := openStoreFactoryForCity
	openStoreFactoryForCity = func(ctx context.Context, opts beads.StoreOpenOptions) (beads.StoreOpenResult, error) {
		opens = append(opens, opts)
		opts.PreflightChecker = passingControlPreflightForTest()
		opts.OpenNativeStore = func() (beads.Store, error) {
			storage := &closeCountingStorage{}
			storages = append(storages, storage)
			natives = append(natives, beads.NewNativeDoltStoreOverStorageForTest(storage))
			return natives[len(natives)-1], nil
		}
		return beads.OpenStoreAtForCity(ctx, opts)
	}
	t.Cleanup(func() { openStoreFactoryForCity = restore })
	return &natives, &storages, &opens
}

// bdHookPathForTest is the path of scope's bd on_update hook.
func bdHookPathForTest(scope string) string {
	return filepath.Join(scope, ".beads", "hooks", "on_update")
}

// writeBdHookForTest writes scope's bd on_update hook with content and mode
// perm.
func writeBdHookForTest(t *testing.T, scope, content string, perm os.FileMode) {
	t.Helper()
	path := bdHookPathForTest(scope)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), perm); err != nil {
		t.Fatal(err)
	}
	// WriteFile applies the umask, and leaves the mode of an existing file.
	if err := os.Chmod(path, perm); err != nil {
		t.Fatal(err)
	}
}

// TestControlStoreBindingCarriesTheFactorysNativeAdmission pins that a scope's
// binding changes when the factory's admission of the native store changes
// (beads.NativeStoreAdmitted), with nothing else about the scope changed. An
// executable bd hook gc did not install turns the factory away from the native
// store, so it changes the binding. A hook that is not executable, or one that
// carries gc's stamp, does not, because the factory still admits the native
// store for the scope.
func TestControlStoreBindingCarriesTheFactorysNativeAdmission(t *testing.T) {
	t.Setenv("GC_BEADS_FORCE_FALLBACK", "")
	for _, tc := range []struct {
		name    string
		content string
		perm    os.FileMode
		changes bool
	}{
		{name: "an executable hook gc did not install", content: foreignBdHook, perm: 0o755, changes: true},
		{name: "a hook that is not executable", content: foreignBdHook, perm: 0o644},
		{name: "an executable hook gc stamped", content: "#!/bin/sh\n# gc-hook-stamp: 2026-10-18 abc123\nexit 0\n", perm: 0o755},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := t.TempDir()
			writeBackendMetadata(t, scope, doltOneMetadata)
			before := controlStoreBinding(scope, "bd", nil, nil)
			writeBdHookForTest(t, scope, tc.content, tc.perm)
			if changed := controlStoreBinding(scope, "bd", nil, nil) != before; changed != tc.changes {
				t.Fatalf("the binding changed = %t, want %t", changed, tc.changes)
			}
		})
	}
}

// TestOpenControlStoreTakesTheBdStoreWhileAScopeHasAnExecutableBdHook pins that
// a kept native store is handed out only while the factory would still admit
// it. The native store writes in process and never runs bd's hook scripts, so
// the factory answers a scope that has an executable bd hook gc did not install
// with the bd store. Installing such a hook, or making one executable, after
// the scope's native store opened changes neither .beads/metadata.json nor the
// native open env, so the next control open has to find it through the
// factory's admission in the binding: it retires the kept store, and the
// factory answers with the bd store, whose writes run the hook. While the hook
// stands, the opens inside the recheck window take the bd store without the
// preflight. Taking the hook away lets the next open ask the factory for the
// native store at once. The factory here is the real one with only its probes
// and its native open supplied, so the hook rule the test exercises is the
// factory's own.
func TestOpenControlStoreTakesTheBdStoreWhileAScopeHasAnExecutableBdHook(t *testing.T) {
	for _, tc := range []struct {
		name string
		// before readies the scope's on_update hook before the first open,
		// which the factory still answers with the native store.
		before func(t *testing.T, scope string)
		// enable leaves the scope with an executable hook gc did not install.
		enable func(t *testing.T, scope string)
		// disable takes that hook away again.
		disable func(t *testing.T, scope string)
	}{
		{
			name:   "a hook installed",
			before: func(*testing.T, string) {},
			enable: func(t *testing.T, scope string) {
				writeBdHookForTest(t, scope, foreignBdHook, 0o755)
			},
			disable: func(t *testing.T, scope string) {
				if err := os.Remove(bdHookPathForTest(scope)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "a hook made executable",
			before: func(t *testing.T, scope string) {
				writeBdHookForTest(t, scope, foreignBdHook, 0o644)
			},
			enable: func(t *testing.T, scope string) {
				if err := os.Chmod(bdHookPathForTest(scope), 0o755); err != nil {
					t.Fatal(err)
				}
			},
			disable: func(t *testing.T, scope string) {
				if err := os.Chmod(bdHookPathForTest(scope), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useControlStoreRegistryForTest(t)
			t.Setenv("GC_BEADS_FORCE_FALLBACK", "")
			cfg := &config.City{Workspace: config.Workspace{Name: "demo"}}
			scope := t.TempDir()
			writeBackendMetadata(t, scope, nativeEligibleMetadata)
			tc.before(t, scope)
			natives, storages, opens := realControlStoreFactory(t)
			bd := beads.NewMemStore()
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

			inFlight := open()
			if len(*natives) != 1 || inFlight != beads.Store((*natives)[0]) {
				t.Fatalf("first open = %T, want the native store the factory opened", inFlight)
			}
			tc.enable(t, scope)
			next := open()
			if next != beads.Store(bd) || len(*opens) != 2 {
				t.Fatalf("open after the hook became executable = %T with %d factory opens, want the bd store from a second factory open (the kept native store would not run the hook)", next, len(*opens))
			}
			if (*storages)[0].closed != 0 {
				t.Fatal("the retired store was closed while a caller that opened it before the hook still held it")
			}
			if err := closeControlStoreHandle(next); err != nil {
				t.Fatalf("closeControlStoreHandle(bd store): %v", err)
			}
			if err := closeControlStoreHandle(inFlight); err != nil {
				t.Fatalf("closeControlStoreHandle(retired store): %v", err)
			}
			if (*storages)[0].closed != 1 {
				t.Fatalf("the retired store was closed %d times once its last handle came back, want 1", (*storages)[0].closed)
			}
			if store := open(); store != beads.Store(bd) || lastTransport() != beads.NativeTransportOff {
				t.Fatalf("open inside the recheck window = %T under %q, want the bd store under off, so no preflight runs while the hook stands", store, lastTransport())
			}

			tc.disable(t, scope)
			back := open()
			if len(*natives) != 2 || back != beads.Store((*natives)[1]) || lastTransport() == beads.NativeTransportOff {
				t.Fatalf("open after the hook went away = %T under %q, want the factory asked for the native store at once and a new native store", back, lastTransport())
			}
			asked := len(*opens)
			if again := open(); again != back || len(*opens) != asked {
				t.Fatalf("open under the unchanged binding = %T with %d more factory opens, want the kept native store", again, len(*opens)-asked)
			}
		})
	}
}

// TestStoreRefResolverReleaseGivesBackItsControlStoreHandles pins that a
// dispatch's store-ref resolver does not pin the native stores it opens. Each
// rig open through the control path takes a handle on the store the rig keeps,
// and the resolver's release, which the dispatch runs when it returns, gives
// each one back. A store retired while the dispatch used it then closes,
// instead of holding its connection pool for the life of the dispatcher.
func TestStoreRefResolverReleaseGivesBackItsControlStoreHandles(t *testing.T) {
	useControlStoreRegistryForTest(t)
	scope := t.TempDir()
	writeBackendMetadata(t, scope, doltOneMetadata)
	cfg := &config.City{
		Workspace: config.Workspace{Name: "demo"},
		Rigs:      []config.Rig{{Name: "repo", Path: scope}},
	}
	bd := beads.NewMemStore()
	natives, storages, _ := bindingAwareControlStoreFactory(t, bd)
	openRig := func(dir string) (beads.Store, error) {
		return openControlBdStoreThroughFactory(dir, "/city", "bd", cfg,
			func() (beads.Store, error) { return bd, nil })
	}
	resolve, release := makeStoreRefResolverWithOpen("/city", cfg, openRig)

	for i := 0; i < 2; i++ {
		store, err := resolve("rig:repo")
		if err != nil || store != beads.Store(natives["one"]) {
			t.Fatalf("resolve(rig:repo) #%d = (%T, %v), want the native store the rig keeps", i, store, err)
		}
	}
	writeBackendMetadata(t, scope, `{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"two"}`)
	if store, err := openRig(scope); err != nil || store != beads.Store(natives["two"]) {
		t.Fatalf("open after the rig moved to database two = (%T, %v), want its native store", store, err)
	}
	if storages["one"].closed != 0 {
		t.Fatal("the retired store was closed while the dispatch's resolver still held it")
	}
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if storages["one"].closed != 1 {
		t.Fatalf("the retired store was closed %d times after the resolver's release, want 1", storages["one"].closed)
	}
}

// TestControlStoreRegistryKeepsTheWinnerOfAConcurrentFirstOpen pins the race
// between two first opens of one scope under one binding: both reach the
// factory, the first native store recorded is the one every caller gets, and
// the other comes back for the caller to close rather than leak with its
// connection pool.
func TestControlStoreRegistryKeepsTheWinnerOfAConcurrentFirstOpen(t *testing.T) {
	registry := newControlStoreRegistry(time.Now)
	first := beads.NewNativeDoltStoreOverStorageForTest(&closeCountingStorage{})
	second := beads.NewNativeDoltStoreOverStorageForTest(&closeCountingStorage{})

	if got, toClose := registry.retain("/scope", "binding", first); got != beads.Store(first) || toClose != nil {
		t.Fatalf("retain(first) = (%T, %p), want the first store kept and nothing to close", got, toClose)
	}
	if got, toClose := registry.retain("/scope", "binding", second); got != beads.Store(first) || toClose != second {
		t.Fatalf("retain(second) = (%T, %p), want the store already kept and the second store to close", got, toClose)
	}
	if got, toClose := registry.acquire("/scope", "binding"); got != beads.Store(first) || toClose != nil {
		t.Fatalf("acquire = (%T, %p), want the kept store", got, toClose)
	}
}

// TestControlStoreRegistryRetiresTheStoreAnOpenUnderANewBindingReplaces pins
// two first opens of one scope whose bindings differ because the scope changed
// between them. The later retain is kept, and the store it replaces is retired,
// not closed: the open that recorded it handed it out with a handle, and the
// store is closed when that handle comes back.
func TestControlStoreRegistryRetiresTheStoreAnOpenUnderANewBindingReplaces(t *testing.T) {
	registry := newControlStoreRegistry(time.Now)
	old := beads.NewNativeDoltStoreOverStorageForTest(&closeCountingStorage{})
	current := beads.NewNativeDoltStoreOverStorageForTest(&closeCountingStorage{})

	if got, toClose := registry.retain("/scope", "old", old); got != beads.Store(old) || toClose != nil {
		t.Fatalf("retain under the old binding = (%T, %p), want the old store kept", got, toClose)
	}
	if got, toClose := registry.retain("/scope", "new", current); got != beads.Store(current) || toClose != nil {
		t.Fatalf("retain under the new binding = (%T, %p), want the new store kept and the old one left to its handle", got, toClose)
	}
	if got, toClose := registry.acquire("/scope", "new"); got != beads.Store(current) || toClose != nil {
		t.Fatalf("acquire under the new binding = (%T, %p), want the new store", got, toClose)
	}
	if tracked, toClose := registry.release(old); !tracked || toClose != old {
		t.Fatalf("release(old) = (%t, %p), want the retired store back to close on its last handle", tracked, toClose)
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
	nativeControlStores.retain("/scope", "binding", kept)

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
	t.Run("the builtin default records nothing", func(t *testing.T) {
		if cb := conditionalWritesDegradedRecorder(events.NewFake(), rollout.ForTest(), "rig/r1"); cb != nil {
			t.Fatal("want nil callback when auto is only the builtin default")
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

// TestDefaultConditionalWritesStayQuietOnAnExecStore pins that the builtin
// default auto treats a store that cannot carry the mode as off. An exec
// store cannot, and every CLI command opens one per scope, so a city that
// never set conditional_writes would otherwise log a WARN and record a
// beads.conditional_writes.degraded event on every command. An explicit
// "auto" stays loud. The open logs on slog.Default, which the test swaps for
// a recorder (so it cannot run in parallel).
func TestDefaultConditionalWritesStayQuietOnAnExecStore(t *testing.T) {
	for _, tc := range []struct {
		name, beads string
		wantLoud    bool
	}{
		{name: "unset", beads: "", wantLoud: false},
		{name: "explicit auto", beads: "conditional_writes = \"auto\"\n", wantLoud: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearGCEnv(t)
			cityDir := t.TempDir()
			toml := "[workspace]\nname = \"t\"\n\n[beads]\nprovider = \"exec:noop.sh\"\n" + tc.beads
			if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(toml), 0o644); err != nil {
				t.Fatal(err)
			}
			logs := &captureHandler{}
			prev := slog.Default()
			slog.SetDefault(slog.New(logs))
			t.Cleanup(func() { slog.SetDefault(prev) })

			for range 2 {
				if _, err := openStoreResultAtForCityWithConfig(cityDir, cityDir, nil, gate.ModeUnset, false, false, false, nil); err != nil {
					t.Fatalf("open: %v", err)
				}
			}

			warns := 0
			for _, r := range logs.records {
				if r.Message == "conditional_writes degraded at open" && r.Level >= slog.LevelWarn {
					warns++
				}
			}
			recorded, err := events.ReadFiltered(filepath.Join(cityDir, ".gc", "events.jsonl"), events.Filter{Type: events.BeadsConditionalWritesDegraded})
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("reading events: %v", err)
			}
			if tc.wantLoud {
				if warns != 2 || len(recorded) != 2 {
					t.Fatalf("explicit auto: %d WARNs and %d degraded events over two opens, want 2 and 2", warns, len(recorded))
				}
				return
			}
			if warns != 0 || len(recorded) != 0 {
				t.Fatalf("default auto: %d WARNs and %d degraded events over two opens, want none", warns, len(recorded))
			}
		})
	}
}
