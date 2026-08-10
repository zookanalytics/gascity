// Package orderdiscovery scans configured city and rig order roots.
package orderdiscovery

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/citylayout"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/orders"
)

// RigScanErrorHandler handles a failed rig-exclusive order scan.
// Returning nil skips that rig and continues scanning remaining rigs.
type RigScanErrorHandler func(rigName string, err error) error

// OverrideErrorHandler handles a failed [orders.ApplyOverrides] call.
// Returning nil preserves the scanned orders without applying the invalid
// override set.
type OverrideErrorHandler func(err error) error

// ValidateErrorHandler handles an order validation failure after config
// layering. Returning nil drops that order and continues scanning.
type ValidateErrorHandler func(orderName string, err error) error

// OrderValidator performs caller-specific post-layering validation.
type OrderValidator func(order orders.Order) error

// UnboundRigScopedHandler reports a city-pass registration dropped because the
// order file explicitly declares scope = "rig". boundRigs names the rigs that
// did register the order, sorted; when it is empty the order now runs nowhere
// and the declaration is a config error the caller should surface loudly.
type UnboundRigScopedHandler func(orderName string, boundRigs []string)

// UnboundRigScopedMessage renders the operator-facing explanation for a
// registration dropped by the rig-scope guard, so every caller's log reads the
// same and the two cases stay distinguishable: a drop that changed nothing
// observable, versus one that removed the order's only registration.
func UnboundRigScopedMessage(orderName string, boundRigs []string) string {
	if len(boundRigs) == 0 {
		return fmt.Sprintf("order %q declares scope = \"rig\" but no rig imports it, so it now runs nowhere — move it into a pack a rig imports, or drop the scope key to run it at city scope", orderName)
	}
	return fmt.Sprintf("order %q declares scope = \"rig\": dropped the unbound city-scope registration nothing could claim (still registered on rig(s) %s)", orderName, strings.Join(boundRigs, ", "))
}

// ScanOptions controls shared order discovery behavior.
type ScanOptions struct {
	FS              fsys.FS
	OnRigScanError  RigScanErrorHandler
	OnOverrideError OverrideErrorHandler
	OnValidateError ValidateErrorHandler
	ValidateOrder   OrderValidator
	// OnUnboundRigScoped reports city-pass registrations dropped by the
	// rig-scope guard. Optional: a nil handler still drops them, silently.
	OnUnboundRigScoped UnboundRigScopedHandler
}

// ScanAll scans city-level and rig-exclusive order roots, stamps rig orders,
// and applies configured order overrides. The returned slice includes orders
// disabled by overrides; callers choose whether to filter them.
func ScanAll(cityPath string, cfg *config.City, opts ScanOptions) ([]orders.Order, error) {
	if cfg == nil {
		cfg = &config.City{}
	}
	fsysImpl := opts.FS
	if fsysImpl == nil {
		fsysImpl = fsys.OSFS{}
	}

	cityLayers := cityFormulaLayers(cityPath, cfg)
	cityOrders, err := orders.ScanRoots(fsysImpl, CityOrderRoots(cityPath, cfg), cfg.Orders.Skip)
	if err != nil {
		return nil, err
	}
	cityOrders, unboundRigScoped := dropUnboundRigScoped(cityOrders)

	rigNames := make(map[string]struct{}, len(cfg.FormulaLayers.Rigs)+len(cfg.RigPackDirs))
	for rigName := range cfg.FormulaLayers.Rigs {
		rigNames[rigName] = struct{}{}
	}
	for rigName := range cfg.RigPackDirs {
		rigNames[rigName] = struct{}{}
	}

	// City-scoped orders register exactly once regardless of how many rigs
	// import the pack, so dedup them across the rig loop by name. Seed the set
	// with city-level orders so a city-local order of the same name wins.
	cityScopedSeen := make(map[string]bool, len(cityOrders))
	for _, o := range cityOrders {
		cityScopedSeen[o.Name] = true
	}

	var promotedCityOrders, rigOrders []orders.Order
	for _, rigName := range sortedRigNames(rigNames) {
		exclusive := RigExclusiveLayers(cfg.FormulaLayers.Rigs[rigName], cityLayers)
		exclusivePackDirs := cfg.RigPackDirs[rigName]
		if len(exclusive) == 0 && len(exclusivePackDirs) == 0 {
			continue
		}
		aa, err := orders.ScanRoots(fsysImpl, rigOrderRoots(exclusive, exclusivePackDirs, rigLocalFormulaLayer(exclusive, exclusivePackDirs)), cfg.Orders.Skip)
		if err != nil {
			if opts.OnRigScanError != nil {
				if handlerErr := opts.OnRigScanError(rigName, err); handlerErr != nil {
					return nil, handlerErr
				}
				continue
			}
			return nil, fmt.Errorf("rig %s: %w", rigName, err)
		}
		for i := range aa {
			if aa[i].IsCityScoped() {
				// Keep the first occurrence (rigs are scanned in deterministic
				// order) and leave Rig empty so it registers city-wide once.
				if cityScopedSeen[aa[i].Name] {
					continue
				}
				cityScopedSeen[aa[i].Name] = true
				promotedCityOrders = append(promotedCityOrders, aa[i])
				continue
			}
			aa[i].Rig = rigName
			rigOrders = append(rigOrders, aa[i])
		}
	}

	if len(unboundRigScoped) > 0 && opts.OnUnboundRigScoped != nil {
		reportUnboundRigScoped(unboundRigScoped, rigOrders, opts.OnUnboundRigScoped)
	}

	allOrders := make([]orders.Order, 0, len(cityOrders)+len(promotedCityOrders)+len(rigOrders))
	allOrders = append(allOrders, cityOrders...)
	allOrders = append(allOrders, promotedCityOrders...)
	allOrders = append(allOrders, rigOrders...)
	// Stamp the city-default cron timezone onto orders that don't author
	// their own tz, so trigger evaluation sees one explicit location without
	// widening the CheckTrigger signature. A bad [workspace] timezone fails
	// the whole scan loudly — a silent fallback would move every inheriting
	// order's schedule onto a different wall clock.
	if tz := cfg.Workspace.Timezone; tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return nil, fmt.Errorf("[workspace] timezone %q: %w", tz, err)
		}
		for i := range allOrders {
			if allOrders[i].TZ == "" {
				allOrders[i].TZ = tz
			}
		}
	}
	if len(cfg.Orders.Overrides) > 0 {
		if err := orders.ApplyOverrides(allOrders, overridesFromConfig(cfg.Orders.Overrides)); err != nil {
			if opts.OnOverrideError == nil {
				return nil, err
			}
			if handlerErr := opts.OnOverrideError(err); handlerErr != nil {
				return nil, handlerErr
			}
		}
	}
	allOrders, err = validateOrders(allOrders, opts.ValidateOrder, opts.OnValidateError)
	if err != nil {
		return nil, err
	}
	return allOrders, nil
}

// dropUnboundRigScoped removes city-pass registrations for orders that
// explicitly declare scope = "rig", returning the survivors and the drops.
//
// A pack imported by rigs is still on the city layer list, so its orders/ is
// scanned twice: once on the city pass, where Rig stays empty, and once per
// importing rig. For a rig-scoped order that extra unbound copy is pure
// breakage — its pool is a rig pool name that qualifies to nothing without a
// rig, so its wisp is poured into the city store where nothing can claim it,
// and the order's own cooldown re-strands it every interval. Refusing the
// registration here is cheaper and quieter than detecting the strand later.
//
// The filter keys on Order.DeclaresRigScope (the literal "rig") rather than on
// !IsCityScoped(), which would also match every order that never mentions
// scope at all — nearly all of them.
func dropUnboundRigScoped(cityOrders []orders.Order) (kept, dropped []orders.Order) {
	kept = cityOrders[:0]
	for _, order := range cityOrders {
		if order.DeclaresRigScope() {
			dropped = append(dropped, order)
			continue
		}
		kept = append(kept, order)
	}
	return kept, dropped
}

// reportUnboundRigScoped hands each dropped city-pass registration to the
// caller along with the rigs that did register that order name.
//
// The rig list is what tells the two cases apart. Non-empty: the pack is
// imported by rigs, every real registration survives, and the drop removed
// only the copy that could never have run. Empty: the order is city-local, so
// it is in no rig's exclusive layers and the guard removed it everywhere. That
// second case is a correct removal — the order could only ever have stranded —
// but it is a config error, which is why the drop is reported rather than
// silently applied.
func reportUnboundRigScoped(dropped, rigOrders []orders.Order, report UnboundRigScopedHandler) {
	bound := make(map[string]map[string]bool, len(dropped))
	for _, order := range dropped {
		bound[order.Name] = map[string]bool{}
	}
	for _, order := range rigOrders {
		if rigs, ok := bound[order.Name]; ok && order.Rig != "" {
			rigs[order.Rig] = true
		}
	}
	for _, order := range dropped {
		rigs := make([]string, 0, len(bound[order.Name]))
		for rig := range bound[order.Name] {
			rigs = append(rigs, rig)
		}
		sort.Strings(rigs)
		report(order.Name, rigs)
	}
}

func validateOrders(allOrders []orders.Order, extraValidate OrderValidator, onError ValidateErrorHandler) ([]orders.Order, error) {
	valid := allOrders[:0]
	for _, order := range allOrders {
		if err := validateOrder(order, extraValidate); err != nil {
			if onError == nil {
				return nil, err
			}
			if handlerErr := onError(order.ScopedName(), err); handlerErr != nil {
				return nil, handlerErr
			}
			continue
		}
		valid = append(valid, order)
	}
	return valid, nil
}

func validateOrder(order orders.Order, extraValidate OrderValidator) error {
	if err := orders.Validate(order); err != nil {
		return err
	}
	if extraValidate != nil {
		if err := extraValidate(order); err != nil {
			return err
		}
	}
	return nil
}

// cityFormulaLayers returns the formula directory layers for city-level order
// scanning.
func cityFormulaLayers(cityPath string, cfg *config.City) []string {
	if len(cfg.FormulaLayers.City) > 0 {
		return cfg.FormulaLayers.City
	}
	return []string{citylayout.ResolveFormulasDir(cityPath, cfg.FormulasDir())}
}

// CityOrderRoots returns the order roots used for city-level discovery.
func CityOrderRoots(cityPath string, cfg *config.City) []orders.ScanRoot {
	formulaLayers := cityFormulaLayers(cityPath, cfg)
	localFormulas := citylayout.ResolveFormulasDir(cityPath, cfg.FormulasDir())

	// Formula layers include system packs (via LoadWithIncludes extraIncludes)
	// and user packs (via workspace.includes). City-local formulas are highest
	// priority and override pack formulas when order names collide.
	return orderRoots(formulaLayers, cfg.PackDirs, localFormulas, orders.ScanRoot{
		Dir:          citylayout.OrdersPath(cityPath),
		FormulaLayer: localFormulas,
	})
}

func rigOrderRoots(formulaLayers []string, packDirs []string, localFormulas string) []orders.ScanRoot {
	localRoot := orders.ScanRoot{}
	if localFormulas != "" {
		localRoot = formulaLayerRoot(localFormulas)
	}
	return orderRoots(formulaLayers, packDirs, localFormulas, localRoot)
}

func orderRoots(formulaLayers []string, packDirs []string, localFormulas string, localRoot orders.ScanRoot) []orders.ScanRoot {
	roots := make([]orders.ScanRoot, 0, len(formulaLayers)+len(packDirs)+1)
	seen := make(map[string]bool, len(formulaLayers)+len(packDirs)+1)
	appendRoot := func(root orders.ScanRoot) {
		key := scanRootKey(root)
		if seen[key] {
			return
		}
		seen[key] = true
		roots = append(roots, root)
	}

	for _, packDir := range packDirs {
		appendRoot(packRoot(packDir))
	}

	localFound := false
	for _, layer := range formulaLayers {
		if samePath(layer, localFormulas) {
			if !localFound {
				if localRoot.Dir == "" {
					localRoot = formulaLayerRoot(layer)
				}
				localFound = true
			}
			continue
		}
		appendRoot(formulaLayerRoot(layer))
	}

	if localFound {
		appendRoot(localRoot)
	}
	return roots
}

func formulaLayerRoot(layer string) orders.ScanRoot {
	return orders.ScanRoot{
		Dir:          filepath.Join(filepath.Dir(layer), "orders"),
		FormulaLayer: layer,
	}
}

func packRoot(packDir string) orders.ScanRoot {
	return orders.ScanRoot{
		Dir:          filepath.Join(packDir, "orders"),
		FormulaLayer: filepath.Join(packDir, "formulas"),
	}
}

func scanRootKey(root orders.ScanRoot) string {
	return filepath.Clean(root.Dir) + "\n" + filepath.Clean(root.FormulaLayer)
}

func samePath(a, b string) bool {
	return a != "" && b != "" && filepath.Clean(a) == filepath.Clean(b)
}

func rigLocalFormulaLayer(formulaLayers []string, packDirs []string) string {
	packFormulaLayers := make(map[string]bool, len(packDirs))
	for _, packDir := range packDirs {
		packFormulaLayers[filepath.Clean(filepath.Join(packDir, "formulas"))] = true
	}
	for i := len(formulaLayers) - 1; i >= 0; i-- {
		layer := formulaLayers[i]
		if !packFormulaLayers[filepath.Clean(layer)] {
			return layer
		}
	}
	return ""
}

// RigExclusiveLayers returns the suffix of rig layers that is not inherited
// from the city formula layers.
func RigExclusiveLayers(rigLayers, cityLayers []string) []string {
	if len(rigLayers) <= len(cityLayers) {
		return nil
	}
	return rigLayers[len(cityLayers):]
}

func sortedRigNames(rigs map[string]struct{}) []string {
	names := make([]string, 0, len(rigs))
	for name := range rigs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func overridesFromConfig(cfgOverrides []config.OrderOverride) []orders.Override {
	out := make([]orders.Override, len(cfgOverrides))
	for i, override := range cfgOverrides {
		out[i] = orders.Override{
			Name:         override.Name,
			Rig:          override.Rig,
			Enabled:      override.Enabled,
			Trigger:      override.Trigger,
			Interval:     override.Interval,
			Schedule:     override.Schedule,
			Check:        override.Check,
			On:           override.On,
			Pool:         override.Pool,
			Timeout:      override.Timeout,
			CheckTimeout: override.CheckTimeout,
			Idempotent:   override.Idempotent,
			Env:          override.Env,
		}
	}
	return out
}
