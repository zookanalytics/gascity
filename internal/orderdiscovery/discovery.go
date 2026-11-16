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

// OverrideErrorHandler handles a failed [orders.ApplyOverrides] call, whose
// error names every override that has no name or matches no order. Returning
// nil keeps the scanned orders with every other override applied.
type OverrideErrorHandler func(err error) error

// ValidateErrorHandler handles an order validation failure after config
// layering. Returning nil drops that order and continues scanning.
type ValidateErrorHandler func(orderName string, err error) error

// OrderValidator performs caller-specific post-layering validation.
type OrderValidator func(order orders.Order) error

// ScanOptions controls shared order discovery behavior.
type ScanOptions struct {
	FS              fsys.FS
	OnRigScanError  RigScanErrorHandler
	OnOverrideError OverrideErrorHandler
	OnValidateError ValidateErrorHandler
	ValidateOrder   OrderValidator
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
	// An order the city scan registers that declares scope = "rig", from a
	// pack imported at city scope or from the city's own orders/,
	// instantiates once per configured rig and never city-wide, the way a
	// city import's agent and an inline named session declaring the same do
	// (config's expandCityImportedAgentsForRigs and
	// ExpandGenericRigNamedSessions). An order that declares nothing keeps
	// its contract: once, city-wide.
	cityOrders, cityRigScoped := splitRigScopedOrders(cityOrders)

	rigNames := make(map[string]struct{}, len(cfg.FormulaLayers.Rigs)+len(cfg.RigPackDirs))
	for rigName := range cfg.FormulaLayers.Rigs {
		rigNames[rigName] = struct{}{}
	}
	for rigName := range cfg.RigPackDirs {
		rigNames[rigName] = struct{}{}
	}

	// City-scoped orders register exactly once regardless of how many rigs
	// import the pack, so dedup them across the rig loop by name. Seed the set
	// with the city scan's city-wide orders, so such an order wins over a
	// rig's order of the same name.
	cityScopedSeen := make(map[string]bool, len(cityOrders))
	for _, o := range cityOrders {
		cityScopedSeen[o.Name] = true
	}

	var promotedCityOrders, rigOrders []orders.Order
	rigDefined := make(map[string]map[string]bool)
	for _, rigName := range sortedRigNames(rigNames) {
		exclusive := RigExclusiveLayers(cfg.FormulaLayers.Rigs[rigName], cityLayers)
		exclusivePackDirs := cfg.RigPackDirs[rigName]
		if len(exclusive) == 0 && len(exclusivePackDirs) == 0 {
			continue
		}
		aa, disabled, err := orders.ScanRootsWithDisabledNames(fsysImpl, rigOrderRoots(exclusive, exclusivePackDirs, rigLocalFormulaLayer(exclusive, exclusivePackDirs)), cfg.Orders.Skip)
		if err != nil {
			if opts.OnRigScanError != nil {
				if handlerErr := opts.OnRigScanError(rigName, err); handlerErr != nil {
					return nil, handlerErr
				}
				continue
			}
			return nil, fmt.Errorf("rig %s: %w", rigName, err)
		}
		rigDefined[rigName] = definedOrderNames(aa, disabled)
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

	// Each configured rig gets a copy of each city-level order that declares
	// scope = "rig", unless the rig's own layers decide that order for it. A
	// rig that imports the pack itself is skipped, since its own scan in the
	// loop above decides that pack's orders for it. So is a rig whose own
	// layers define an order of the same name, from another copy of the pack
	// or in its own orders/: within one rig a local order replaces a pack's.
	// A disabled definition there turns the order off on that rig, and one
	// that declares scope = "city" runs once, city-wide, under the dedup
	// above, with no copy on the rig.
	for _, rig := range cfg.Rigs {
		for _, o := range cityRigScoped {
			if orderUnderPackDirs(o.Source, cfg.RigPackDirs[rig.Name]) || rigDefined[rig.Name][o.Name] {
				continue
			}
			rigOrders = append(rigOrders, cloneOrderForRig(o, rig.Name))
		}
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

// splitRigScopedOrders partitions the city-level scan into the orders that
// stay city-wide and those that declare scope = "rig", whichever city-level
// root defined them.
func splitRigScopedOrders(cityOrders []orders.Order) (city, rigScoped []orders.Order) {
	for _, o := range cityOrders {
		if o.IsRigScoped() {
			rigScoped = append(rigScoped, o)
			continue
		}
		city = append(city, o)
	}
	return city, rigScoped
}

// definedOrderNames returns the order names one rig scan defines: those of
// the orders it returns, whatever scope they declare, and the names whose
// highest-priority definition is disabled, which the scan leaves out.
func definedOrderNames(scanned []orders.Order, disabled []string) map[string]bool {
	names := make(map[string]bool, len(scanned)+len(disabled))
	for _, o := range scanned {
		names[o.Name] = true
	}
	for _, name := range disabled {
		names[name] = true
	}
	return names
}

// orderUnderPackDirs reports whether an order file was scanned from the
// orders/ directory of one of the given pack dirs.
func orderUnderPackDirs(source string, packDirs []string) bool {
	for _, dir := range packDirs {
		if pathWithin(source, packRoot(dir).Dir) {
			return true
		}
	}
	return false
}

func pathWithin(path, dir string) bool {
	if path == "" || dir == "" {
		return false
	}
	rel, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(path))
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

// cloneOrderForRig stamps a copy of a city-scanned order for one rig. The
// maps are copied: overrides and env projection mutate them per instance.
func cloneOrderForRig(o orders.Order, rig string) orders.Order {
	o.Rig = rig
	if o.Env != nil {
		env := make(map[string]string, len(o.Env))
		for k, v := range o.Env {
			env[k] = v
		}
		o.Env = env
	}
	if o.Params != nil {
		params := make(map[string]orders.OrderParam, len(o.Params))
		for k, v := range o.Params {
			params[k] = v
		}
		o.Params = params
	}
	return o
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
