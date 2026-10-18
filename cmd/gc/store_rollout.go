package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/rollout"
	"github.com/gastownhall/gascity/internal/rollout/gate"
)

// resolvedConditionalWritesFlags resolves the rollout flags from an
// already-loaded config for store-open threading. A nil config or a resolve
// error yields (zero, false) — the factory maps the unset mode to off with a
// defaulted marker, so a best-effort open can never RAISE enforcement. The
// loud surfaces for a resolve error are the controller boot latch and gc
// doctor; store-open helpers stay best-effort, matching their existing
// config-error tolerance. Resolution is per-process by design (the env
// break-glass is per-process; the supported whole-city change is config edit
// plus restart).
func resolvedConditionalWritesFlags(cfg *config.City) (rollout.Flags, bool) {
	if cfg == nil {
		return rollout.Flags{}, false
	}
	flags, err := rollout.Resolve(cfg, rollout.ResolveOptions{})
	if err != nil {
		return rollout.Flags{}, false
	}
	return flags, true
}

// resolvedConditionalWritesMode is the mode-only view of
// resolvedConditionalWritesFlags for store-open threading.
func resolvedConditionalWritesMode(cfg *config.City) gate.Mode {
	flags, ok := resolvedConditionalWritesFlags(cfg)
	if !ok {
		return gate.ModeUnset
	}
	return flags.BeadsConditionalWrites()
}

// resolvedNativeTransportMode is the store-open view of a city's
// beads.native_transport, for threading into beads.StoreOpenOptions. A nil cfg
// yields NativeTransportUnset, which every consumer treats as "auto". The
// deprecated GC_BEADS_FORCE_FALLBACK alias is not resolved here: it is
// process-wide, and each consumer checks it ahead of this per-city value.
func resolvedNativeTransportMode(cfg *config.City) beads.NativeTransportMode {
	if cfg == nil {
		return beads.NativeTransportUnset
	}
	return beads.NativeTransportMode(cfg.Beads.NormalizedNativeTransport())
}

// lazyConditionalWritesDegradeEmitter builds the factory degrade callback for
// open paths that have no live event provider in hand (the shared CLI open
// helper and the control dispatcher). The recorder is constructed INSIDE the
// callback — which the factory latches to at most one invocation per store —
// so routine opens pay nothing and an auto-degrade still lands in the city's
// event log instead of persisting unnoticed.
func lazyConditionalWritesDegradeEmitter(cityPath, storeID string, flags rollout.Flags, resolved bool) func(beads.ConditionalWritesDegrade) {
	if !resolved || strings.TrimSpace(cityPath) == "" {
		return nil
	}
	return func(d beads.ConditionalWritesDegrade) {
		cb := conditionalWritesDegradedRecorder(openCityRecorderAt(cityPath, io.Discard), flags, storeID)
		if cb != nil {
			cb(d)
		}
	}
}

// conditionalWritesStoreID labels a store scope for the degraded event
// (matching the DESIGN examples: "city", "rig/<name>").
func conditionalWritesStoreID(scopeRoot, cityPath string) string {
	if samePath(scopeRoot, cityPath) {
		return "city"
	}
	return "rig/" + filepath.Base(scopeRoot)
}

// openControlBdStoreThroughFactory routes a control-plane store through the
// beads factory so it carries the conditional-writes stamp, and so a scope
// that passes the native-store preflight answers the control dispatcher's
// reads and writes in process instead of through one bd subprocess per call.
// The store comes back raw, with no policy or cache wrapper, matching the
// control path's deliberately unwrapped handles.
//
// The control dispatcher opens its scope store for every readiness scan,
// every control bead it processes and every execution-fact flush. A native
// store opened per call would pay the preflight (a bd context subprocess and
// two database probes) and a new connection pool each time, so
// nativeControlStores keeps the first native store each scope opens for the
// life of the process, and a scope the factory answered with the bd store
// skips the preflight until controlStoreNativeRecheck has passed. The proxied
// lane is not wired here, so a proxied-server scope keeps the bd store.
func openControlBdStoreThroughFactory(scopeRoot, cityPath, provider string, cfg *config.City, openBd func() (beads.Store, error)) (beads.Store, error) {
	transport := controlStoreNativeTransport(scopeRoot, cfg)
	if transport != beads.NativeTransportOff {
		if store := nativeControlStores.lookup(scopeRoot); store != nil {
			return store, nil
		}
		if nativeControlStores.refusedRecently(scopeRoot) {
			transport = beads.NativeTransportOff
		}
	}
	flags, resolved := resolvedConditionalWritesFlags(cfg)
	mode := gate.ModeUnset
	if resolved {
		mode = flags.BeadsConditionalWrites()
	}
	result, err := openStoreFactoryForCity(context.Background(), beads.StoreOpenOptions{
		ScopeRoot:         scopeRoot,
		CityPath:          cityPath,
		Provider:          provider,
		PreflightChecker:  newBeadsPreflightChecker(cityPath, provider, cfg),
		ConditionalWrites: mode,
		NativeTransport:   transport,
		OnConditionalWritesDegraded: lazyConditionalWritesDegradeEmitter(
			cityPath, conditionalWritesStoreID(scopeRoot, cityPath), flags, resolved),
		OpenBdStore: openBd,
		OpenNativeStore: func() (beads.Store, error) {
			return openNativeControlStore(scopeRoot, cityPath, cfg)
		},
	})
	if err != nil {
		return nil, err
	}
	if transport == beads.NativeTransportOff {
		return result.Store, nil
	}
	return nativeControlStores.retain(scopeRoot, result.Store), nil
}

// controlStoreNativeTransport is the native transport a control-plane open
// of scopeRoot asks the factory for: the city's beads.native_transport, read
// from the config the caller loaded for this open, so a city that turns native
// off stops handing out its retained native stores on the next open. Two cases
// ask for NativeTransportOff instead:
//   - a nil cfg, because the caller could not read city.toml, and an unread
//     "off" must not open as "auto" (loadCityConfigForStoreOpen holds the same
//     line for the shared open path);
//   - a scope whose metadata names a backend the linked beads library opens as
//     an extension. The factory's library arm returns a failed open as an
//     error rather than the bd store, and on the rig: store-ref resolver that
//     error quarantines the workflow finalizer.
func controlStoreNativeTransport(scopeRoot string, cfg *config.City) beads.NativeTransportMode {
	if cfg == nil || beads.ScopeUsesLibraryExtensionBackend(scopeRoot) {
		return beads.NativeTransportOff
	}
	return resolvedNativeTransportMode(cfg)
}

// openNativeControlStore opens the native store a control-plane scope keeps
// for the life of the process, with the scoped Dolt env and the library's
// long-lived connection pool, as controllerState.openRigStore opens the
// controller's rig stores. Its reopen hook re-resolves the current env on
// every reconnect, because a dispatcher can outlive a managed-Dolt restart or
// rebind by days and the env it opened with pins the port as of the open. The
// hook passes no config, so a reconnect also re-reads city.toml for a rig's
// explicit Dolt endpoint and the city's beads.allow_schema_behind_migrate
// decision. The controller's hook reuses the config it opened with.
func openNativeControlStore(scopeRoot, cityPath string, cfg *config.City) (beads.Store, error) {
	env, err := nativeDoltOpenEnvForScope(cityPath, cfg, scopeRoot)
	if err != nil {
		return nil, fmt.Errorf("project native control store env %s: %w", scopeRoot, err)
	}
	reopen := func(ctx context.Context) (beads.NativeStorage, error) {
		freshEnv, rerr := nativeDoltOpenEnvForScopeContext(ctx, cityPath, nil, scopeRoot)
		if rerr != nil {
			return nil, fmt.Errorf("re-resolve native control store env %s: %w", scopeRoot, rerr)
		}
		return beads.OpenNativeStorage(ctx, scopeRoot, freshEnv)
	}
	return beads.OpenNativeDoltStoreAt(context.Background(), scopeRoot, env, beads.WithNativeReopen(reopen))
}

// controlStoreNativeRecheck is how long a scope the factory answered with the
// bd store keeps that answer for control-plane opens. Opens inside the window
// take the bd store without the preflight and without logging the refusal
// again; the first open after it asks the factory again. A refusal that has
// cleared, such as a Dolt server that was down at the first open, reaches the
// native store within one window, and a standing one costs one preflight per
// window instead of one per readiness scan.
const controlStoreNativeRecheck = 5 * time.Minute

// nativeControlStores records the transport each control-plane scope settled
// on in this process. See openControlBdStoreThroughFactory.
var nativeControlStores = newControlStoreRegistry(time.Now)

// controlStoreRegistry holds the native store each control-plane scope opened,
// kept for the life of the process, and the time the factory last answered a
// scope with the bd store.
type controlStoreRegistry struct {
	mu      sync.Mutex
	now     func() time.Time
	native  map[string]*beads.NativeDoltStore
	refused map[string]time.Time
}

func newControlStoreRegistry(now func() time.Time) *controlStoreRegistry {
	return &controlStoreRegistry{
		now:     now,
		native:  map[string]*beads.NativeDoltStore{},
		refused: map[string]time.Time{},
	}
}

// lookup returns the native store retained for scopeRoot, or nil.
func (r *controlStoreRegistry) lookup(scopeRoot string) beads.Store {
	r.mu.Lock()
	defer r.mu.Unlock()
	if store, ok := r.native[scopeRoot]; ok {
		return store
	}
	return nil
}

// refusedRecently reports whether the factory answered scopeRoot with the bd
// store within the last controlStoreNativeRecheck.
func (r *controlStoreRegistry) refusedRecently(scopeRoot string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	at, ok := r.refused[scopeRoot]
	return ok && r.now().Sub(at) < controlStoreNativeRecheck
}

// retain records what the factory answered for scopeRoot under a transport
// that allowed the native store, and returns the store to hand out. A native
// store is kept and returned, unless a concurrent first open of the same scope
// already kept one: that one is returned and this one is closed. Any other
// store is returned as it came, and the refusal is recorded.
func (r *controlStoreRegistry) retain(scopeRoot string, store beads.Store) beads.Store {
	r.mu.Lock()
	defer r.mu.Unlock()
	native, ok := store.(*beads.NativeDoltStore)
	if !ok {
		r.refused[scopeRoot] = r.now()
		return store
	}
	delete(r.refused, scopeRoot)
	if kept, ok := r.native[scopeRoot]; ok {
		if err := native.CloseStore(); err != nil {
			log.Printf("control store: closing the duplicate native store for %s: %v", scopeRoot, err)
		}
		return kept
	}
	r.native[scopeRoot] = native
	return native
}

// holds reports whether store is a native store this registry keeps.
func (r *controlStoreRegistry) holds(store beads.Store) bool {
	native, ok := store.(*beads.NativeDoltStore)
	if !ok {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, kept := range r.native {
		if kept == native {
			return true
		}
	}
	return false
}

// closeControlStoreHandle releases a control-plane store the caller opened
// for one readiness scan, control dispatch or execution-fact flush. A native
// store nativeControlStores keeps is shared by every later open of its scope,
// so it stays open: CloseStore is a one-way latch, and closing it here would
// fail every later control open of the scope with ErrStoreClosed.
func closeControlStoreHandle(store beads.Store) error {
	if nativeControlStores.holds(store) {
		return nil
	}
	return closeBeadStoreHandle(store)
}

// conditionalWritesEventStoreKind maps internal store-kind names onto the
// beads.conditional_writes.degraded wire vocabulary
// (bd | native | sqlite-graph | caching | mem | file).
func conditionalWritesEventStoreKind(kind string) string {
	switch kind {
	case beads.BeadsStoreNameBdStore:
		return "bd"
	case beads.BeadsStoreNameNativeDoltStore, beads.BeadsStoreNameLibraryBackendStore:
		// A library-backend store is the native store the linked beads library
		// opened through a registered extension backend.
		return "native"
	case beads.BeadsStoreNameFileStore:
		return "file"
	case "MemStore":
		return "mem"
	case "CachingStore":
		return "caching"
	case "SQLiteStore":
		// The SQLite engine a relocated class binding opens.
		return "sqlite-graph"
	case "*beads.DoltliteReadStore":
		// DoltliteReadStore only exists under the gascity_native_beads build
		// tag, so beads.conditionalStoreKind cannot name it and it arrives as
		// the %T spelling. It embeds *BdStore and its entire conditional-write
		// surface IS bd's, so on the wire it is a bd store.
		return "bd"
	default:
		return kind
	}
}

// conditionalWritesDegradedRecorder converts the beads factory's degrade
// notification into the typed beads.conditional_writes.degraded event,
// attaching what only the composition root knows: the store scope and the
// resolved mode's origin. The factory latches invocation once per store
// instance, so this cannot storm.
func conditionalWritesDegradedRecorder(rec events.Recorder, flags rollout.Flags, storeID string) func(beads.ConditionalWritesDegrade) {
	if rec == nil {
		return nil
	}
	return func(d beads.ConditionalWritesDegrade) {
		payload, err := json.Marshal(events.ConditionalWritesDegradedPayload{
			StoreID:   storeID,
			StoreKind: conditionalWritesEventStoreKind(d.StoreKind),
			Mode:      d.Mode,
			Origin:    string(flags.OriginOf(rollout.KeyBeadsConditionalWrites)),
			Reason:    d.Reason,
		})
		if err != nil {
			return
		}
		rec.Record(events.Event{
			Type:    events.BeadsConditionalWritesDegraded,
			Actor:   "gc",
			Subject: storeID,
			Message: fmt.Sprintf("conditional_writes degraded: store=%s mode=%s reason=%q", storeID, d.Mode, d.Reason),
			Payload: payload,
		})
	}
}
