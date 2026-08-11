package orderdiscovery

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

func TestScanAllNilConfigUsesDefaultCityRootsAndOSFS(t *testing.T) {
	cityPath, _ := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "heartbeat", `[order]
exec = "scripts/heartbeat.sh"
trigger = "cooldown"
interval = "5m"
`)

	aa, err := ScanAll(cityPath, nil, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if len(aa) != 1 {
		t.Fatalf("got %d orders, want 1", len(aa))
	}
	if aa[0].Name != "heartbeat" {
		t.Fatalf("Name = %q, want %q", aa[0].Name, "heartbeat")
	}
	if aa[0].Rig != "" {
		t.Fatalf("Rig = %q, want city-scoped order", aa[0].Rig)
	}
}

func TestScanAllScansRigExclusiveLayersInDeterministicRigOrder(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{},
		},
	}

	for _, rigName := range []string{"zeta", "alpha", "beta"} {
		rigLayer := orderDiscoveryRigLayer(t, rigName)
		writeOrderDiscoveryFile(t, filepath.Join(filepath.Dir(rigLayer), "orders"), rigName+"-health", `[order]
exec = "scripts/health.sh"
trigger = "cooldown"
interval = "5m"
`)
		cfg.FormulaLayers.Rigs[rigName] = []string{cityLayer, rigLayer}
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	got := make([]string, len(aa))
	for i, a := range aa {
		got[i] = a.Rig
	}
	want := []string{"alpha", "beta", "zeta"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("rig order = %v, want %v", got, want)
	}
}

func TestScanAllRigScanHandlerCanSkipFailedRig(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	brokenRigLayer := orderDiscoveryRigLayer(t, "broken")
	writeOrderDiscoveryFile(t, filepath.Join(filepath.Dir(brokenRigLayer), "orders"), "bad", "not toml")
	workingRigLayer := orderDiscoveryRigLayer(t, "working")
	writeOrderDiscoveryFile(t, filepath.Join(filepath.Dir(workingRigLayer), "orders"), "health", `[order]
exec = "scripts/health.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"broken":  {cityLayer, brokenRigLayer},
				"working": {cityLayer, workingRigLayer},
			},
		},
	}

	var skipped []string
	aa, err := ScanAll(cityPath, cfg, ScanOptions{
		OnRigScanError: func(rigName string, _ error) error {
			skipped = append(skipped, rigName)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if strings.Join(skipped, ",") != "broken" {
		t.Fatalf("skipped rigs = %v, want [broken]", skipped)
	}
	if len(aa) != 1 {
		t.Fatalf("got %d orders, want 1", len(aa))
	}
	if aa[0].Name != "health" || aa[0].Rig != "working" {
		t.Fatalf("order = %+v, want health scoped to working rig", aa[0])
	}
}

func TestScanAllRigScanHandlerCanAbortFailedRig(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	brokenRigLayer := orderDiscoveryRigLayer(t, "broken")
	writeOrderDiscoveryFile(t, filepath.Join(filepath.Dir(brokenRigLayer), "orders"), "bad", "not toml")
	handlerErr := errors.New("stop scanning rigs")

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"broken": {cityLayer, brokenRigLayer},
			},
		},
	}

	_, err := ScanAll(cityPath, cfg, ScanOptions{
		OnRigScanError: func(_ string, _ error) error {
			return handlerErr
		},
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("ScanAll error = %v, want handler error", err)
	}
}

func TestScanAllDefaultRigScanErrorPropagatesWithRigName(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	brokenRigLayer := orderDiscoveryRigLayer(t, "broken")
	writeOrderDiscoveryFile(t, filepath.Join(filepath.Dir(brokenRigLayer), "orders"), "bad", "not toml")

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"broken": {cityLayer, brokenRigLayer},
			},
		},
	}

	_, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err == nil {
		t.Fatal("ScanAll succeeded; want rig scan error")
	}
	if !strings.Contains(err.Error(), "rig broken:") {
		t.Fatalf("ScanAll error = %q, want rig name context", err.Error())
	}
}

func TestScanAllOverrideHandlerCanReturnPartiallyModifiedOrders(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "backup", `[order]
exec = "scripts/backup.sh"
trigger = "cooldown"
interval = "1h"
`)

	interval := "15m"
	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
		},
		Orders: config.OrdersConfig{
			Overrides: []config.OrderOverride{
				{Name: "backup", Interval: &interval},
				{Name: "missing"},
			},
		},
	}

	var handled string
	aa, err := ScanAll(cityPath, cfg, ScanOptions{
		OnOverrideError: func(err error) error {
			handled = err.Error()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if !strings.Contains(handled, `order "missing" not found`) {
		t.Fatalf("handled override error = %q, want missing-order error", handled)
	}
	if len(aa) != 1 {
		t.Fatalf("got %d orders, want 1", len(aa))
	}
	if aa[0].Interval != "15m" {
		t.Fatalf("Interval = %q, want partially applied override %q", aa[0].Interval, "15m")
	}
}

func TestScanAllOverrideHandlerStillValidatesPartiallyModifiedOrders(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "backup", `[order]
exec = "scripts/backup.sh"
trigger = "cooldown"
interval = "1h"
`)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "deploy", `[order]
formula = "mol-deploy"
trigger = "manual"

[order.env]
CUSTOM_ORDER_FLAG = "enabled"
`)

	interval := "15m"
	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
		},
		Orders: config.OrdersConfig{
			Overrides: []config.OrderOverride{
				{Name: "backup", Interval: &interval},
				{Name: "missing"},
			},
		},
	}

	var overrideHandled, validationHandled string
	aa, err := ScanAll(cityPath, cfg, ScanOptions{
		OnOverrideError: func(err error) error {
			overrideHandled = err.Error()
			return nil
		},
		OnValidateError: func(orderName string, err error) error {
			validationHandled = orderName + ": " + err.Error()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if !strings.Contains(overrideHandled, `order "missing" not found`) {
		t.Fatalf("handled override error = %q, want missing-order error", overrideHandled)
	}
	if !strings.Contains(validationHandled, `deploy`) || !strings.Contains(validationHandled, "env is supported only for exec orders") {
		t.Fatalf("handled validation error = %q, want deploy env-on-formula diagnostic", validationHandled)
	}
	if len(aa) != 1 {
		t.Fatalf("got %d orders, want only the valid order", len(aa))
	}
	if aa[0].Name != "backup" {
		t.Fatalf("remaining order = %q, want backup", aa[0].Name)
	}
	if aa[0].Interval != "15m" {
		t.Fatalf("Interval = %q, want partially applied override %q", aa[0].Interval, "15m")
	}
}

func TestScanAllOverrideHandlerCanAbortInvalidOverride(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "backup", `[order]
exec = "scripts/backup.sh"
trigger = "cooldown"
interval = "1h"
`)
	handlerErr := errors.New("stop applying overrides")

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
		},
		Orders: config.OrdersConfig{
			Overrides: []config.OrderOverride{{Name: "missing"}},
		},
	}

	_, err := ScanAll(cityPath, cfg, ScanOptions{
		OnOverrideError: func(error) error {
			return handlerErr
		},
	})
	if !errors.Is(err, handlerErr) {
		t.Fatalf("ScanAll error = %v, want handler error", err)
	}
}

func TestScanAllValidationHandlerSkipsInvalidOrderAfterOverrides(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "backup", `[order]
exec = "scripts/backup.sh"
trigger = "cooldown"
interval = "1h"
`)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "deploy", `[order]
formula = "mol-deploy"
trigger = "manual"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
		},
		Orders: config.OrdersConfig{
			Overrides: []config.OrderOverride{
				{Name: "deploy", Env: map[string]string{"CUSTOM_ORDER_FLAG": "enabled"}},
			},
		},
	}

	var handled string
	aa, err := ScanAll(cityPath, cfg, ScanOptions{
		OnValidateError: func(orderName string, err error) error {
			handled = orderName + ": " + err.Error()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if !strings.Contains(handled, `deploy`) || !strings.Contains(handled, "env is supported only for exec orders") {
		t.Fatalf("handled validation error = %q, want deploy env-on-formula diagnostic", handled)
	}
	if len(aa) != 1 {
		t.Fatalf("got %d orders, want only the valid order", len(aa))
	}
	if aa[0].Name != "backup" {
		t.Fatalf("remaining order = %q, want backup", aa[0].Name)
	}
}

func TestScanAllValidationHandlerSkipsInvalidCitySourceOrder(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "backup", `[order]
exec = "scripts/backup.sh"
trigger = "cooldown"
interval = "1h"
`)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "deploy", `[order]
formula = "mol-deploy"
trigger = "manual"

[order.env]
CUSTOM_ORDER_FLAG = "enabled"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
		},
	}

	var handled string
	aa, err := ScanAll(cityPath, cfg, ScanOptions{
		OnValidateError: func(orderName string, err error) error {
			handled = orderName + ": " + err.Error()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if !strings.Contains(handled, `deploy`) || !strings.Contains(handled, "env is supported only for exec orders") {
		t.Fatalf("handled validation error = %q, want deploy env-on-formula diagnostic", handled)
	}
	if len(aa) != 1 {
		t.Fatalf("got %d orders, want only the valid order", len(aa))
	}
	if aa[0].Name != "backup" {
		t.Fatalf("remaining order = %q, want backup", aa[0].Name)
	}
}

func TestScanAllRejectsSchema1PackLegacyOrderDirectory(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	if err := os.WriteFile(filepath.Join(cityPath, "pack.toml"), []byte("[pack]\nname = \"legacy-city\"\nschema = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	legacyOrderDir := filepath.Join(cityPath, "orders", "heartbeat")
	if err := os.MkdirAll(legacyOrderDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyOrderDir, "order.toml"), []byte(`[order]
exec = "scripts/heartbeat.sh"
trigger = "cooldown"
interval = "5m"
`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := ScanAll(cityPath, &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
		},
	}, ScanOptions{})
	if err == nil {
		t.Fatal("ScanAll succeeded, want schema-1 legacy order directory rejection")
	}
	if !strings.Contains(err.Error(), "rename to orders/heartbeat.toml") {
		t.Fatalf("ScanAll error = %v, want schema-1 flat-file migration guidance", err)
	}
}

func TestCityOrderRootsUsesLocalAndPackFormulaLayersOnce(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packLayer := filepath.Join(t.TempDir(), "formulas")

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer, packLayer, cityLayer},
		},
	}

	roots := CityOrderRoots(cityPath, cfg)
	if len(roots) != 2 {
		t.Fatalf("got %d roots, want 2: %#v", len(roots), roots)
	}
	if roots[0].Dir != filepath.Join(filepath.Dir(packLayer), "orders") || roots[0].FormulaLayer != packLayer {
		t.Fatalf("first root = %+v, want pack orders root", roots[0])
	}
	if roots[1].Dir != filepath.Join(cityPath, "orders") || roots[1].FormulaLayer != cityLayer {
		t.Fatalf("second root = %+v, want city orders root", roots[1])
	}
}

func TestScanAllCityLocalOrderOverridesOrdersOnlyPack(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "audit-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "audit", `[order]
exec = "scripts/pack.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "audit", `[order]
exec = "scripts/city.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
		},
		PackDirs: []string{packDir},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if len(aa) != 1 {
		t.Fatalf("got %d orders, want 1: %#v", len(aa), aa)
	}
	if aa[0].Exec != "scripts/city.sh" {
		t.Fatalf("Exec = %q, want city-local order to win", aa[0].Exec)
	}
}

func TestScanAllRigLocalOrderOverridesOrdersOnlyPack(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	rigLayer := orderDiscoveryRigLayer(t, "frontend")
	packDir := filepath.Join(t.TempDir(), "watch-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
exec = "scripts/pack.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(filepath.Dir(rigLayer), "orders"), "watch", `[order]
exec = "scripts/rig.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"frontend": {cityLayer, rigLayer},
			},
		},
		Rigs: []config.Rig{
			{Name: "frontend", FormulasDir: rigLayer},
		},
		RigPackDirs: map[string][]string{
			"frontend": {packDir},
		},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	for _, a := range aa {
		if a.Name != "watch" || a.Rig != "frontend" {
			continue
		}
		if a.Exec != "scripts/rig.sh" {
			t.Fatalf("Exec = %q, want rig-local order to win", a.Exec)
		}
		return
	}
	t.Fatalf("missing rig-scoped watch order in %#v", aa)
}

func TestScanAllSameOrdersOnlyPackImportedAtCityAndRigScopes(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "shared-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "sweep", `[order]
exec = "scripts/sweep.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"frontend": {cityLayer},
			},
		},
		PackDirs: []string{packDir},
		RigPackDirs: map[string][]string{
			"frontend": {packDir},
		},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	var cityFound, rigFound bool
	for _, a := range aa {
		if a.Name != "sweep" {
			continue
		}
		switch a.Rig {
		case "":
			cityFound = true
		case "frontend":
			rigFound = true
		}
	}
	if !cityFound || !rigFound {
		t.Fatalf("found city=%v rig=%v in %#v, want both city and rig orders", cityFound, rigFound, aa)
	}
}

func TestScanAllCityPackRootsPreserveTopoOrderAcrossOrdersOnlyPacks(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packA, layerA := orderDiscoveryPackLayer(t, "pack-a")
	packB := filepath.Join(t.TempDir(), "pack-b")
	packC, layerC := orderDiscoveryPackLayer(t, "pack-c")
	writeOrderDiscoveryFile(t, filepath.Join(packA, "orders"), "audit", `[order]
exec = "scripts/a.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(packB, "orders"), "audit", `[order]
exec = "scripts/b.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(packC, "orders"), "audit", `[order]
exec = "scripts/c.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{layerA, layerC, cityLayer},
		},
		PackDirs: []string{packA, packB, packC},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if len(aa) != 1 {
		t.Fatalf("got %d orders, want 1: %#v", len(aa), aa)
	}
	if aa[0].Exec != "scripts/c.sh" {
		t.Fatalf("Exec = %q, want later formula pack to override earlier orders-only pack", aa[0].Exec)
	}
}

func TestScanAllRigPackRootsPreserveTopoOrderAcrossOrdersOnlyPacks(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packA, layerA := orderDiscoveryPackLayer(t, "rig-pack-a")
	packB := filepath.Join(t.TempDir(), "rig-pack-b")
	packC, layerC := orderDiscoveryPackLayer(t, "rig-pack-c")
	writeOrderDiscoveryFile(t, filepath.Join(packA, "orders"), "watch", `[order]
exec = "scripts/a.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(packB, "orders"), "watch", `[order]
exec = "scripts/b.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(packC, "orders"), "watch", `[order]
exec = "scripts/c.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"frontend": {cityLayer, layerA, layerC},
			},
		},
		RigPackDirs: map[string][]string{
			"frontend": {packA, packB, packC},
		},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	for _, a := range aa {
		if a.Name != "watch" || a.Rig != "frontend" {
			continue
		}
		if a.Exec != "scripts/c.sh" {
			t.Fatalf("Exec = %q, want later rig formula pack to override earlier orders-only pack", a.Exec)
		}
		return
	}
	t.Fatalf("missing rig-scoped watch order in %#v", aa)
}

func TestScanAllRigLocalOrderUsesCanonicalFormulaLayer(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	rigLayer := orderDiscoveryRigLayer(t, "frontend")
	packDir := filepath.Join(t.TempDir(), "watch-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
exec = "scripts/pack.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(filepath.Dir(rigLayer), "orders"), "watch", `[order]
exec = "scripts/rig.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"frontend": {cityLayer, rigLayer},
			},
		},
		RigPackDirs: map[string][]string{
			"frontend": {packDir},
		},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	for _, a := range aa {
		if a.Name != "watch" || a.Rig != "frontend" {
			continue
		}
		if a.Exec != "scripts/rig.sh" {
			t.Fatalf("Exec = %q, want canonical rig-local formula layer to win", a.Exec)
		}
		return
	}
	t.Fatalf("missing rig-scoped watch order in %#v", aa)
}

func TestRigExclusiveLayersReturnsOnlyRigSuffix(t *testing.T) {
	cityLayers := []string{"/city/base", "/city/local"}
	rigLayers := []string{"/city/base", "/city/local", "/rig/base", "/rig/local"}

	got := RigExclusiveLayers(rigLayers, cityLayers)
	want := []string{"/rig/base", "/rig/local"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("RigExclusiveLayers = %v, want %v", got, want)
	}

	if got := RigExclusiveLayers(cityLayers, cityLayers); got != nil {
		t.Fatalf("RigExclusiveLayers for inherited-only rig = %v, want nil", got)
	}
}

func orderDiscoveryCity(t *testing.T) (cityPath, cityLayer string) {
	t.Helper()
	cityPath = t.TempDir()
	cityLayer = filepath.Join(cityPath, "formulas")
	if err := os.MkdirAll(cityLayer, 0o755); err != nil {
		t.Fatal(err)
	}
	return cityPath, cityLayer
}

func orderDiscoveryRigLayer(t *testing.T, rigName string) string {
	t.Helper()
	rigRoot := filepath.Join(t.TempDir(), rigName)
	rigLayer := filepath.Join(rigRoot, "formulas")
	if err := os.MkdirAll(rigLayer, 0o755); err != nil {
		t.Fatal(err)
	}
	return rigLayer
}

func orderDiscoveryPackLayer(t *testing.T, packName string) (packDir, layer string) {
	t.Helper()
	packDir = filepath.Join(t.TempDir(), packName)
	layer = filepath.Join(packDir, "formulas")
	if err := os.MkdirAll(layer, 0o755); err != nil {
		t.Fatal(err)
	}
	return packDir, layer
}

func writeOrderDiscoveryFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".toml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestScanAllCityScopedOrderRegistersOnceAcrossRigs(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "mixed-pack")
	// A scope=city order must register exactly once no matter how many rigs
	// import the pack.
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "city-sweep", `[order]
scope = "city"
exec = "scripts/sweep.sh"
trigger = "cooldown"
interval = "5m"
`)
	// An unscoped (rig-default) order still registers once per importing rig.
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "rig-health", `[order]
exec = "scripts/health.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"alpha": {cityLayer},
				"beta":  {cityLayer},
			},
		},
		RigPackDirs: map[string][]string{
			"alpha": {packDir},
			"beta":  {packDir},
		},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}

	citySweepCount := 0
	citySweepRig := "<unset>"
	rigHealth := map[string]int{}
	for _, a := range aa {
		switch a.Name {
		case "city-sweep":
			citySweepCount++
			citySweepRig = a.Rig
		case "rig-health":
			rigHealth[a.Rig]++
		}
	}
	if citySweepCount != 1 {
		t.Fatalf("city-scoped order registered %d times, want 1: %#v", citySweepCount, aa)
	}
	if citySweepRig != "" {
		t.Fatalf("city-scoped order Rig = %q, want \"\" (city-scoped)", citySweepRig)
	}
	if rigHealth["alpha"] != 1 || rigHealth["beta"] != 1 {
		t.Fatalf("rig-scoped order counts = %v, want one per importing rig", rigHealth)
	}
}

// The city-default [workspace] timezone is stamped onto orders that don't
// author their own tz, and never overrides an authored tz. This is how cron
// evaluation gets one explicit location without widening CheckTrigger.
func TestScanAllStampsWorkspaceTimezoneOntoOrdersWithoutTZ(t *testing.T) {
	cityPath, _ := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "inherits-tz", `[order]
exec = "scripts/digest.sh"
trigger = "cron"
schedule = "30 19 * * *"
`)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "owns-tz", `[order]
exec = "scripts/digest.sh"
trigger = "cron"
schedule = "30 19 * * *"
tz = "Europe/Berlin"
`)

	cfg := &config.City{Workspace: config.Workspace{Timezone: "America/New_York"}}
	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	got := make(map[string]string, len(aa))
	for _, a := range aa {
		got[a.Name] = a.TZ
	}
	if got["inherits-tz"] != "America/New_York" {
		t.Errorf("inherits-tz TZ = %q, want workspace default %q", got["inherits-tz"], "America/New_York")
	}
	if got["owns-tz"] != "Europe/Berlin" {
		t.Errorf("owns-tz TZ = %q, want authored %q kept over the workspace default", got["owns-tz"], "Europe/Berlin")
	}
}

// A bad [workspace] timezone fails order discovery loudly instead of
// silently moving every inheriting order onto a different wall clock.
func TestScanAllBadWorkspaceTimezoneFailsLoudly(t *testing.T) {
	cityPath, _ := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "digest", `[order]
exec = "scripts/digest.sh"
trigger = "cron"
schedule = "30 19 * * *"
`)

	cfg := &config.City{Workspace: config.Workspace{Timezone: "America/New_Yrok"}}
	_, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err == nil {
		t.Fatal("ScanAll should fail: bad [workspace] timezone")
	}
	if !strings.Contains(err.Error(), `timezone "America/New_Yrok"`) {
		t.Errorf("error = %q, want it to name the invalid timezone", err)
	}
}

// A bad order-authored tz fails validation during discovery (no handler).
func TestScanAllBadOrderTZFailsValidation(t *testing.T) {
	cityPath, _ := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "digest", `[order]
exec = "scripts/digest.sh"
trigger = "cron"
schedule = "30 19 * * *"
tz = "Amrica/New_York"
`)

	_, err := ScanAll(cityPath, &config.City{}, ScanOptions{})
	if err == nil {
		t.Fatal("ScanAll should fail: bad order tz")
	}
	if !strings.Contains(err.Error(), `invalid tz "Amrica/New_York"`) {
		t.Errorf("error = %q, want it to name the invalid tz", err)
	}
}

// A pack imported by rigs stays on the city layer list too, so ScanAll scans
// its orders/ twice: once on the city pass (Rig keeps "") and once per
// importing rig. A registration that EXPLICITLY declares scope = "rig" must
// not survive the city pass. The unbound copy names a rig pool that qualifies
// to nothing at city scope, so its wisp is poured into the city store where no
// agent can claim it — and because the order keeps its cooldown, it re-strands
// every interval.
func TestScanAllDropsUnboundRigScopedCityRegistration(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir, _ := orderDiscoveryPackLayer(t, "shared-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "liveness-sweep", `[order]
scope = "rig"
formula = "mol-liveness-sweep"
pool = "gc-toolkit.polecat"
trigger = "cooldown"
interval = "6h"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"alpha": {cityLayer},
				"beta":  {cityLayer},
			},
		},
		PackDirs: []string{packDir},
		RigPackDirs: map[string][]string{
			"alpha": {packDir},
			"beta":  {packDir},
		},
	}

	var droppedNames []string
	var droppedBoundRigs [][]string
	aa, err := ScanAll(cityPath, cfg, ScanOptions{
		OnUnboundRigScoped: func(orderName string, boundRigs []string) {
			droppedNames = append(droppedNames, orderName)
			droppedBoundRigs = append(droppedBoundRigs, boundRigs)
		},
	})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}

	rigs := map[string]int{}
	for _, a := range aa {
		if a.Name != "liveness-sweep" {
			continue
		}
		rigs[a.Rig]++
	}
	if rigs[""] != 0 {
		t.Fatalf("unbound (Rig == \"\") registration survived the city pass: %#v", aa)
	}
	if rigs["alpha"] != 1 || rigs["beta"] != 1 {
		t.Fatalf("rig-bound registrations = %v, want one per importing rig", rigs)
	}

	if len(droppedNames) != 1 || droppedNames[0] != "liveness-sweep" {
		t.Fatalf("dropped registrations reported = %v, want [liveness-sweep]", droppedNames)
	}
	if got := strings.Join(droppedBoundRigs[0], ","); got != "alpha,beta" {
		t.Errorf("bound rigs reported = %q, want %q", got, "alpha,beta")
	}
}

// THE guard against the catastrophic variant of this filter. Scope defaults to
// rig-scoped when EMPTY, but an empty field is not a declaration: most orders —
// including the builtin core set — never mention scope at all, and a filter
// keyed on !IsCityScoped() would delete every one of them. Only the literal
// "rig" is a declaration.
func TestScanAllKeepsCityOrdersThatDoNotDeclareRigScope(t *testing.T) {
	cityPath, _ := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "no-scope-key", `[order]
exec = "scripts/heartbeat.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "declares-city", `[order]
scope = "city"
exec = "scripts/sweep.sh"
trigger = "cooldown"
interval = "5m"
`)

	var dropped []string
	aa, err := ScanAll(cityPath, &config.City{}, ScanOptions{
		OnUnboundRigScoped: func(orderName string, _ []string) {
			dropped = append(dropped, orderName)
		},
	})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if len(dropped) != 0 {
		t.Fatalf("dropped %v, want nothing dropped: only an explicit scope = \"rig\" is a declaration", dropped)
	}

	kept := map[string]bool{}
	for _, a := range aa {
		kept[a.Name] = true
	}
	if !kept["no-scope-key"] || !kept["declares-city"] {
		t.Fatalf("kept orders = %v, want both no-scope-key and declares-city", kept)
	}
}

// A city-LOCAL order declaring scope = "rig" is in no rig's exclusive layers,
// so the guard removes it entirely and it runs nowhere. That is correct — it
// could only ever have stranded — but it is a config error, so the drop is
// reported with an empty rig list rather than passing silently.
func TestScanAllReportsCityLocalRigScopedOrderAsBoundNowhere(t *testing.T) {
	cityPath, _ := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "orphan-sweep", `[order]
scope = "rig"
exec = "scripts/sweep.sh"
trigger = "cooldown"
interval = "5m"
`)

	var droppedNames []string
	var droppedBoundRigs [][]string
	aa, err := ScanAll(cityPath, &config.City{}, ScanOptions{
		OnUnboundRigScoped: func(orderName string, boundRigs []string) {
			droppedNames = append(droppedNames, orderName)
			droppedBoundRigs = append(droppedBoundRigs, boundRigs)
		},
	})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if len(aa) != 0 {
		t.Fatalf("orders = %#v, want the unbound rig-scoped order dropped", aa)
	}
	if len(droppedNames) != 1 || droppedNames[0] != "orphan-sweep" {
		t.Fatalf("dropped registrations reported = %v, want [orphan-sweep]", droppedNames)
	}
	if len(droppedBoundRigs[0]) != 0 {
		t.Errorf("bound rigs reported = %v, want empty: the order now runs nowhere", droppedBoundRigs[0])
	}
}

// A nil handler must not turn the drop into a panic or an error — discovery
// still drops the unbound registration, the caller just gets no report.
func TestScanAllDropsUnboundRigScopedWithoutHandler(t *testing.T) {
	cityPath, _ := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "orphan-sweep", `[order]
scope = "rig"
exec = "scripts/sweep.sh"
trigger = "cooldown"
interval = "5m"
`)

	aa, err := ScanAll(cityPath, &config.City{}, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if len(aa) != 0 {
		t.Fatalf("orders = %#v, want the unbound rig-scoped order dropped", aa)
	}
}

// A rigless [[orders.overrides]] entry naming a rig-scoped order is the
// documented workaround for this very bug: it disables the unbound city copy
// so the copy stops stranding a wisp every cooldown. It is in city.toml on
// every deployment that hit the bug before this guard existed. ApplyOverrides
// errors on an override that matches nothing and returns at the FIRST miss, so
// a drop that ran before the overrides would turn that workaround into a hard
// scan failure for the callers that leave OnOverrideError nil (gc order, the
// doctor order-firing check) and would strand every LATER override unapplied
// for the ones that log and continue — silently re-enabling orders the
// operator had disabled.
func TestScanAllRiglessOverrideStillMatchesDroppedRigScopedOrder(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir, _ := orderDiscoveryPackLayer(t, "shared-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "liveness-sweep", `[order]
scope = "rig"
formula = "mol-liveness-sweep"
pool = "gc-toolkit.polecat"
trigger = "cooldown"
interval = "6h"
`)
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "triage-recurrence", `[order]
scope = "rig"
formula = "mol-triage-recurrence"
pool = "gc-toolkit.polecat"
trigger = "cooldown"
interval = "6h"
`)

	disabled := false
	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{
				"alpha": {cityLayer},
				"beta":  {cityLayer},
			},
		},
		PackDirs: []string{packDir},
		RigPackDirs: map[string][]string{
			"alpha": {packDir},
			"beta":  {packDir},
		},
		Orders: config.OrdersConfig{
			Overrides: []config.OrderOverride{
				// The workaround, exactly as deployments wrote it.
				{Name: "liveness-sweep", Enabled: &disabled},
				// A later, valid entry that must still be applied.
				{Name: "triage-recurrence", Rig: "alpha", Enabled: &disabled},
			},
		},
	}

	// OnOverrideError stays nil on purpose: that is the gc order / doctor
	// shape, where an unmatched override fails the whole scan.
	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}

	for _, a := range aa {
		if a.Rig == "" {
			t.Fatalf("unbound (Rig == \"\") registration survived: %#v", a)
		}
	}

	enabled := map[string]bool{}
	for _, a := range aa {
		enabled[a.Name+"@"+a.Rig] = a.IsEnabled()
	}
	if got, ok := enabled["triage-recurrence@alpha"]; !ok || got {
		t.Errorf("triage-recurrence@alpha enabled = %v (present=%v), want the later override applied", got, ok)
	}
	if got, ok := enabled["triage-recurrence@beta"]; !ok || !got {
		t.Errorf("triage-recurrence@beta enabled = %v (present=%v), want untouched by a rig-scoped override", got, ok)
	}
	// The rigless override reached only the dropped copy, so the rig-bound
	// registrations it never named keep running.
	if got, ok := enabled["liveness-sweep@alpha"]; !ok || !got {
		t.Errorf("liveness-sweep@alpha enabled = %v (present=%v), want the rig-bound registration untouched", got, ok)
	}
	if got, ok := enabled["liveness-sweep@beta"]; !ok || !got {
		t.Errorf("liveness-sweep@beta enabled = %v (present=%v), want the rig-bound registration untouched", got, ok)
	}
}

// An override cannot resurrect a dropped registration. The guard refuses the
// unbound copy because nothing at city scope can claim its wisp, and that is
// not a preference an [[orders.overrides]] entry gets to reverse — matching one
// only consumes the entry, it never puts the registration back.
func TestScanAllRiglessOverrideCannotResurrectDroppedRigScopedOrder(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir, _ := orderDiscoveryPackLayer(t, "shared-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "liveness-sweep", `[order]
scope = "rig"
formula = "mol-liveness-sweep"
pool = "gc-toolkit.polecat"
trigger = "cooldown"
interval = "6h"
`)

	enabled := true
	interval := "5m"
	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{"alpha": {cityLayer}},
		},
		PackDirs:    []string{packDir},
		RigPackDirs: map[string][]string{"alpha": {packDir}},
		Orders: config.OrdersConfig{
			Overrides: []config.OrderOverride{
				{Name: "liveness-sweep", Enabled: &enabled, Interval: &interval},
			},
		},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	for _, a := range aa {
		if a.Rig == "" {
			t.Fatalf("unbound (Rig == \"\") registration survived an enabling override: %#v", a)
		}
	}
	if len(aa) != 1 || aa[0].Rig != "alpha" {
		t.Fatalf("orders = %#v, want only the rig-bound registration", aa)
	}
	if aa[0].Interval != "6h" {
		t.Errorf("Interval = %q, want %q: a rigless override reaches only the city-level copy", aa[0].Interval, "6h")
	}
}
