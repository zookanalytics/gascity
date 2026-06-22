package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/config"
)

// MEASUREMENT-ONLY instrumentation for gc-k8r4y.1 (controller connection-storm
// empirical validation). This file isolates a throwaway, env-gated probe that
// counts and classifies the native bead-store opens order dispatch performs per
// tick. It does NOT change dispatch behavior: every hook is a no-op unless the
// GC_ORDER_DISPATCH_INSTRUMENT environment switch is set, so an uninstrumented
// build (and every existing test) is byte-for-byte identical to main. The
// branch carrying it is not meant to merge — it exists to produce data.

// orderStoreOpenLogPrefix tags every per-open instrumentation line so a
// measurement run can grep them out of controller stderr and aggregate them.
const orderStoreOpenLogPrefix = "gc: order-dispatch instrument: store-open"

// orderDispatchInstrumentEnvVar gates the per-store-open instrumentation. It is
// OFF unless explicitly enabled, keeping production dispatch — and the existing
// test suite — unchanged.
const orderDispatchInstrumentEnvVar = "GC_ORDER_DISPATCH_INSTRUMENT"

// orderDispatchInstrumentEnabled reports whether per-open instrumentation is
// switched on via the environment. It is read once at dispatcher construction
// so the per-open path costs nothing to evaluate when disabled.
func orderDispatchInstrumentEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(orderDispatchInstrumentEnvVar))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// boundRigNameSet returns the set of rig names the controller builds a cached
// bead-store handle for: every rig declared with a non-empty path. It mirrors
// controllerState.buildStores, which skips unbound rigs (empty path) — so a rig
// in this set is exactly one for which the controller holds beadStores[rig].
// Used to classify whether an order-dispatch store open targets a scope the
// controller already has cached (an "eliminable" open).
func boundRigNameSet(cfg *config.City) map[string]struct{} {
	if cfg == nil {
		return nil
	}
	set := make(map[string]struct{}, len(cfg.Rigs))
	for _, rig := range cfg.Rigs {
		if strings.TrimSpace(rig.Path) == "" {
			continue
		}
		set[rig.Name] = struct{}{}
	}
	return set
}

// targetControllerCached reports whether the controller already holds a cached
// bead-store handle for target's scope — cityBeadStore for the city scope, or
// beadStores[rig] for a bound rig. An open whose target is controller-cached is
// "eliminable": the cached-store-reuse fix (upstream #2750 wiring) would remove
// it. The city scope is always treated as cached because the controller holds
// cityBeadStore for the city it runs.
func (m *memoryOrderDispatcher) targetControllerCached(target execStoreTarget) bool {
	switch target.ScopeKind {
	case "city":
		return true
	case "rig":
		_, ok := m.boundRigNames[target.RigName]
		return ok
	default:
		return false
	}
}

// recordStoreOpen emits one structured instrumentation line per actual native
// store open performed by dispatch (i.e. per storeFn call — a per-tick
// cache-miss, never a dedup hit). It is a no-op unless instrumentation is
// enabled, so it adds nothing to production dispatch. Each line carries the
// target scope and whether that scope is already controller-cached, letting an
// offline pass compute opens/min and the eliminable fraction from a run's
// stderr.
func (m *memoryOrderDispatcher) recordStoreOpen(target execStoreTarget, openErr error) {
	if m == nil || !m.instrumentOpens || m.stderr == nil {
		return
	}
	fmt.Fprintf(m.stderr, "%s scope=%s root=%s rig=%s cached=%t err=%t ts=%s\n", //nolint:errcheck // best-effort stderr
		orderStoreOpenLogPrefix,
		target.ScopeKind,
		target.ScopeRoot,
		target.RigName,
		m.targetControllerCached(target),
		openErr != nil,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
}
