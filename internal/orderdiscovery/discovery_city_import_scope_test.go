package orderdiscovery

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// An order that declares scope = "rig" in a pack imported at CITY scope
// instantiates once per configured rig, the way an agent or named session
// declaring the same in the same pack does (config's
// expandCityImportedAgentsForRigs). Before the fix the declaration was
// validated and then silently dropped: the order registered once,
// city-wide, and resolveOrderStoreTarget routed it to the city store. An
// order that declares nothing keeps its contract: once, city-wide.
func TestScanAllCityImportedRigScopedOrderExpandsPerRig(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
scope = "rig"
exec = "scripts/watch.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "sweep", `[order]
exec = "scripts/sweep.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{City: []string{cityLayer}},
		PackDirs:      []string{packDir},
		Rigs:          []config.Rig{{Name: "alpha"}, {Name: "beta"}},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	watch := map[string]int{}
	sweep := map[string]int{}
	for _, a := range aa {
		switch a.Name {
		case "watch":
			watch[a.Rig]++
		case "sweep":
			sweep[a.Rig]++
		}
	}
	if len(watch) != 2 || watch["alpha"] != 1 || watch["beta"] != 1 {
		t.Fatalf("rig-scoped order in a city import registered as %v, want one instance per rig (alpha, beta) and no city copy", watch)
	}
	if len(sweep) != 1 || sweep[""] != 1 {
		t.Fatalf("unscoped order in a city import registered as %v, want once city-wide", sweep)
	}
}

// Importing the same pack at both scopes must not double the rig-scoped
// order: the rig that imports the pack itself keeps the instance from its
// own scan, and only the rigs that do not import it get a fan-out copy.
func TestScanAllCityImportedRigScopedOrderYieldsToRigImport(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
scope = "rig"
exec = "scripts/watch.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{"alpha": {cityLayer}},
		},
		PackDirs:    []string{packDir},
		RigPackDirs: map[string][]string{"alpha": {packDir}},
		Rigs:        []config.Rig{{Name: "alpha"}, {Name: "beta"}},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	watch := map[string]int{}
	for _, a := range aa {
		if a.Name == "watch" {
			watch[a.Rig]++
		}
	}
	if len(watch) != 2 || watch["alpha"] != 1 || watch["beta"] != 1 {
		t.Fatalf("rig-scoped order imported at both scopes registered as %v, want exactly one instance per rig", watch)
	}
}

// Within one rig a local order replaces a pack's of the same name, and that
// holds for the copy a city import adds: a rig whose own orders/ defines the
// name keeps its own order and gets no copy, so the rig does not hold two
// instances under one scoped name.
func TestScanAllCityImportedRigScopedOrderYieldsToRigLocalOrder(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	rigLayer := orderDiscoveryRigLayer(t, "alpha")
	packDir := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
scope = "rig"
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
			Rigs: map[string][]string{"alpha": {cityLayer, rigLayer}},
		},
		PackDirs: []string{packDir},
		Rigs:     []config.Rig{{Name: "alpha", FormulasDir: rigLayer}, {Name: "beta"}},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	execs := map[string][]string{}
	for _, a := range aa {
		if a.Name == "watch" {
			execs[a.Rig] = append(execs[a.Rig], a.Exec)
		}
	}
	if len(execs) != 2 || strings.Join(execs["alpha"], ",") != "scripts/rig.sh" || strings.Join(execs["beta"], ",") != "scripts/pack.sh" {
		t.Fatalf("watch registered as %v (rig: execs), want alpha's own scripts/rig.sh alone and the city import's scripts/pack.sh on beta", execs)
	}
}

// A local definition with enabled = false replaces the pack's within the rig
// too, so a rig turns a city import's order off for itself the way it turns
// off a pack order it imports: the scan leaves the disabled order out, and
// the rig gets no copy in its place.
func TestScanAllCityImportedRigScopedOrderYieldsToRigLocalDisabledOrder(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	rigLayer := orderDiscoveryRigLayer(t, "alpha")
	packDir := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
scope = "rig"
exec = "scripts/pack.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(filepath.Dir(rigLayer), "orders"), "watch", `[order]
exec = "scripts/rig.sh"
trigger = "cooldown"
interval = "5m"
enabled = false
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{"alpha": {cityLayer, rigLayer}},
		},
		PackDirs: []string{packDir},
		Rigs:     []config.Rig{{Name: "alpha", FormulasDir: rigLayer}, {Name: "beta"}},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	execs := map[string][]string{}
	for _, a := range aa {
		if a.Name == "watch" {
			execs[a.Rig] = append(execs[a.Rig], a.Exec)
		}
	}
	if len(execs) != 1 || strings.Join(execs["beta"], ",") != "scripts/pack.sh" {
		t.Fatalf("watch registered as %v (rig: execs), want the city import's scripts/pack.sh on beta alone and nothing on alpha, whose own definition is disabled", execs)
	}
}

// A rig that imports the same pack from another directory, such as another
// version of it, holds its own instance of the order, and the city import's
// copy yields to that instance as it does to one from the same directory.
func TestScanAllCityImportedRigScopedOrderYieldsToRigImportFromAnotherDir(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	cityPack := filepath.Join(t.TempDir(), "review-pack")
	rigPack := filepath.Join(t.TempDir(), "review-pack")
	for dir, exec := range map[string]string{cityPack: "scripts/v1.sh", rigPack: "scripts/v2.sh"} {
		writeOrderDiscoveryFile(t, filepath.Join(dir, "orders"), "watch", `[order]
scope = "rig"
exec = "`+exec+`"
trigger = "cooldown"
interval = "5m"
`)
	}

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{"alpha": {cityLayer}},
		},
		PackDirs:    []string{cityPack},
		RigPackDirs: map[string][]string{"alpha": {rigPack}},
		Rigs:        []config.Rig{{Name: "alpha"}, {Name: "beta"}},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	execs := map[string][]string{}
	for _, a := range aa {
		if a.Name == "watch" {
			execs[a.Rig] = append(execs[a.Rig], a.Exec)
		}
	}
	if len(execs) != 2 || strings.Join(execs["alpha"], ",") != "scripts/v2.sh" || strings.Join(execs["beta"], ",") != "scripts/v1.sh" {
		t.Fatalf("watch registered as %v (rig: execs), want alpha's own import alone and the city import's copy on beta", execs)
	}
}

// When the rig's own copy of the pack disables the order, the rig's import
// still decides it: the city import's copy stays off that rig.
func TestScanAllCityImportedRigScopedOrderYieldsToDisabledRigImportFromAnotherDir(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	cityPack := filepath.Join(t.TempDir(), "review-pack")
	rigPack := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(cityPack, "orders"), "watch", `[order]
scope = "rig"
exec = "scripts/v1.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(rigPack, "orders"), "watch", `[order]
scope = "rig"
exec = "scripts/v2.sh"
trigger = "cooldown"
interval = "5m"
enabled = false
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{"alpha": {cityLayer}},
		},
		PackDirs:    []string{cityPack},
		RigPackDirs: map[string][]string{"alpha": {rigPack}},
		Rigs:        []config.Rig{{Name: "alpha"}, {Name: "beta"}},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	execs := map[string][]string{}
	for _, a := range aa {
		if a.Name == "watch" {
			execs[a.Rig] = append(execs[a.Rig], a.Exec)
		}
	}
	if len(execs) != 1 || strings.Join(execs["beta"], ",") != "scripts/v1.sh" {
		t.Fatalf("watch registered as %v (rig: execs), want the city import's copy on beta alone and nothing on alpha, whose own import disables it", execs)
	}
}

// A pack imported at city scope and by every rig expands to no copy: each
// rig's own scan already holds the rig-scoped order, so no rig gets a second
// one and none is left registered city-wide.
func TestScanAllRigScopedOrderImportedAtCityAndByEveryRigRegistersOncePerRig(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
scope = "rig"
exec = "scripts/watch.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{
			City: []string{cityLayer},
			Rigs: map[string][]string{"alpha": {cityLayer}, "beta": {cityLayer}},
		},
		PackDirs:    []string{packDir},
		RigPackDirs: map[string][]string{"alpha": {packDir}, "beta": {packDir}},
		Rigs:        []config.Rig{{Name: "alpha"}, {Name: "beta"}},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	watch := map[string]int{}
	for _, a := range aa {
		if a.Name == "watch" {
			watch[a.Rig]++
		}
	}
	if len(watch) != 2 || watch["alpha"] != 1 || watch["beta"] != 1 {
		t.Fatalf("rig-scoped order imported at city scope and by every rig registered as %v, want one instance per rig and none city-wide", watch)
	}
}

// Only an explicit scope = "rig" expands. Scope defaults to rig-scoped when
// it is empty, but an empty field is not a declaration: most orders never
// mention scope, and a split keyed on !IsCityScoped() would fan every one of
// them out per rig.
func TestScanAllCityImportedOrderWithoutRigDeclarationStaysCityWide(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "no-scope-key", `[order]
exec = "scripts/heartbeat.sh"
trigger = "cooldown"
interval = "5m"
`)
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "declares-city", `[order]
scope = "city"
exec = "scripts/sweep.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{City: []string{cityLayer}},
		PackDirs:      []string{packDir},
		Rigs:          []config.Rig{{Name: "alpha"}, {Name: "beta"}},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	registered := map[string]map[string]int{}
	for _, a := range aa {
		if registered[a.Name] == nil {
			registered[a.Name] = map[string]int{}
		}
		registered[a.Name][a.Rig]++
	}
	for _, name := range []string{"no-scope-key", "declares-city"} {
		if got := registered[name]; len(got) != 1 || got[""] != 1 {
			t.Errorf("order %q registered as %v, want once city-wide", name, got)
		}
	}
}

// The per-rig expansion covers the orders of a pack imported at city scope.
// An order in the city's own orders/ is not one of them: it registers once,
// city-wide, whatever scope it declares.
func TestScanAllCityLocalRigScopedOrderRegistersOnceCityWide(t *testing.T) {
	cityPath, _ := orderDiscoveryCity(t)
	writeOrderDiscoveryFile(t, filepath.Join(cityPath, "orders"), "local-sweep", `[order]
scope = "rig"
exec = "scripts/sweep.sh"
trigger = "cooldown"
interval = "5m"
`)

	cfg := &config.City{Rigs: []config.Rig{{Name: "alpha"}, {Name: "beta"}}}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	if len(aa) != 1 || aa[0].Name != "local-sweep" || aa[0].Rig != "" {
		t.Fatalf("orders = %#v, want local-sweep registered once, city-wide", aa)
	}
}

// Each rig's copy is its own instance to [[orders.overrides]]: an override
// naming one rig reaches that rig's copy alone, the env it merges included,
// and rig = "*" reaches every copy.
func TestScanAllOverridesReachEachRigsCopyOfACityImportedOrder(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
scope = "rig"
exec = "scripts/watch.sh"
trigger = "cooldown"
interval = "5m"

[order.env]
WATCH_MODE = "lenient"
`)

	interval := "1m"
	timeout := "90s"
	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{City: []string{cityLayer}},
		PackDirs:      []string{packDir},
		Rigs:          []config.Rig{{Name: "alpha"}, {Name: "beta"}},
		Orders: config.OrdersConfig{
			Overrides: []config.OrderOverride{
				{Name: "watch", Rig: "alpha", Interval: &interval, Env: map[string]string{"WATCH_MODE": "strict"}},
				{Name: "watch", Rig: "*", Timeout: &timeout},
			},
		},
	}

	aa, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err != nil {
		t.Fatalf("ScanAll returned error: %v", err)
	}
	intervals := map[string]string{}
	modes := map[string]string{}
	timeouts := map[string]string{}
	for _, a := range aa {
		if a.Name != "watch" {
			continue
		}
		intervals[a.Rig] = a.Interval
		modes[a.Rig] = a.Env["WATCH_MODE"]
		timeouts[a.Rig] = a.Timeout
	}
	if len(intervals) != 2 {
		t.Fatalf("watch registered on %v, want one copy each on alpha and beta", intervals)
	}
	if intervals["alpha"] != "1m" || modes["alpha"] != "strict" {
		t.Errorf("alpha's copy: interval %q, WATCH_MODE %q, want the override's 1m and strict", intervals["alpha"], modes["alpha"])
	}
	if intervals["beta"] != "5m" || modes["beta"] != "lenient" {
		t.Errorf("beta's copy: interval %q, WATCH_MODE %q, want the definition's 5m and lenient, untouched by alpha's override", intervals["beta"], modes["beta"])
	}
	if timeouts["alpha"] != "90s" || timeouts["beta"] != "90s" {
		t.Errorf("timeouts = %v, want rig = \"*\" to set 90s on both copies", timeouts)
	}
}

// A rigless [[orders.overrides]] entry targets the city-wide instance, and a
// city import's rig-scoped order has none, so the entry matches nothing. The
// error names the rigs the order runs on, which is what the entry has to
// name, and a scan that goes on past it leaves every copy as defined.
func TestScanAllRiglessOverrideOfACityImportedRigScopedOrderNamesItsRigs(t *testing.T) {
	cityPath, cityLayer := orderDiscoveryCity(t)
	packDir := filepath.Join(t.TempDir(), "review-pack")
	writeOrderDiscoveryFile(t, filepath.Join(packDir, "orders"), "watch", `[order]
scope = "rig"
exec = "scripts/watch.sh"
trigger = "cooldown"
interval = "5m"
`)

	disabled := false
	cfg := &config.City{
		FormulaLayers: config.FormulaLayers{City: []string{cityLayer}},
		PackDirs:      []string{packDir},
		Rigs:          []config.Rig{{Name: "alpha"}, {Name: "beta"}},
		Orders: config.OrdersConfig{
			Overrides: []config.OrderOverride{{Name: "watch", Enabled: &disabled}},
		},
	}

	_, err := ScanAll(cityPath, cfg, ScanOptions{})
	if err == nil {
		t.Fatal("ScanAll returned no error for a rigless override of an order that has no city-wide instance")
	}
	for _, want := range []string{`"watch"`, `rig = "alpha"`, `rig = "beta"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to name %s", err, want)
		}
	}

	var overrideErr error
	aa, err := ScanAll(cityPath, cfg, ScanOptions{
		OnOverrideError: func(err error) error {
			overrideErr = err
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ScanAll with an override handler returned error: %v", err)
	}
	if overrideErr == nil {
		t.Fatal("override handler was not called for the unmatched rigless override")
	}
	enabled := map[string]bool{}
	for _, a := range aa {
		if a.Name == "watch" {
			enabled[a.Rig] = a.IsEnabled()
		}
	}
	if len(enabled) != 2 || !enabled["alpha"] || !enabled["beta"] {
		t.Fatalf("watch copies = %v (rig: enabled), want both alpha and beta present and enabled", enabled)
	}
}
