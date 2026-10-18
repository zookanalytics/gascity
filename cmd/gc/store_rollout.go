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
// nativeControlStores keeps the first native store each scope opens, and a
// scope the factory answered with the bd store skips the preflight until
// controlStoreNativeRecheck has passed. Both answers hold only for the binding
// the scope had when the factory gave them (controlStoreBinding). Every open
// resolves the env a native open of the scope projects, as the bd store
// resolves its env for every bd call, and an open that finds the binding
// changed retires the kept store and asks the factory again. A change to the
// scope's .beads/metadata.json changes the binding, such as a move to another
// database, to proxied-server or to another backend. So does a change to the
// server, the credentials or the open options the env projects, such as gc rig
// set-endpoint pointing the scope at another server while the old one still
// answers. The proxied lane is not wired here, so a proxied-server scope keeps
// the bd store.
func openControlBdStoreThroughFactory(scopeRoot, cityPath, provider string, cfg *config.City, openBd func() (beads.Store, error)) (beads.Store, error) {
	// The binding is read before the factory opens anything, and a native open
	// uses the env the binding names, so a store is never recorded under a
	// binding newer than the one it opened under.
	env, envErr := nativeDoltOpenEnvForScope(cityPath, cfg, scopeRoot)
	binding := controlStoreBinding(scopeRoot, env, envErr)
	transport := controlStoreNativeTransport(scopeRoot, cfg)
	if transport == beads.NativeTransportOff {
		closeUnheldControlStore(scopeRoot, nativeControlStores.retireRebound(scopeRoot, binding))
	} else {
		store, unheld := nativeControlStores.acquire(scopeRoot, binding)
		closeUnheldControlStore(scopeRoot, unheld)
		if store != nil {
			return store, nil
		}
		if nativeControlStores.refusedRecently(scopeRoot, binding) {
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
			return openNativeControlStore(scopeRoot, cityPath, env, envErr)
		},
	})
	if err != nil {
		return nil, err
	}
	if transport == beads.NativeTransportOff {
		return result.Store, nil
	}
	store, unheld := nativeControlStores.retain(scopeRoot, binding, result.Store)
	closeUnheldControlStore(scopeRoot, unheld)
	return store, nil
}

// controlStoreBinding names what a control-plane open of scopeRoot binds a
// native store to. It joins the digest of the scope's .beads/metadata.json,
// which names the backend, the database and the Dolt mode, with the digest of
// what a native open projects from env, the scope's native open env: the
// server, the credentials and the open options. The env carries them whichever
// input supplied them, such as the scope's canonical .beads/config.yaml, a
// rig's Dolt host and port in city.toml, or the managed Dolt port. envErr is
// the error resolving env. An env that could not be resolved is named as
// unresolved, without the error's text, so a failure that stands keeps one
// binding and its refusal stays held for controlStoreNativeRecheck.
func controlStoreBinding(scopeRoot string, env map[string]string, envErr error) string {
	endpoint := "unresolved"
	if envErr == nil {
		endpoint = beads.NativeDoltOpenEnvDigest(env)
	}
	return "metadata:" + scopeMetadataDigest(scopeRoot) + " env:" + endpoint
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
// while its binding holds, with the scoped Dolt env the binding names and the
// library's long-lived connection pool, as controllerState.openRigStore opens
// the controller's rig stores. env is the scope's native open env, and envErr,
// the error resolving it, fails the open. Its reopen hook re-resolves the
// current env on every reconnect, because a dispatcher can outlive a
// managed-Dolt restart or rebind by days and the env it opened with pins the
// port as of the open. A reconnect that reaches another endpoint leaves the
// store recorded under the binding it opened with, so the next control open
// retires it and asks the factory again. The hook passes no config, so a
// reconnect also re-reads city.toml for a rig's explicit Dolt endpoint and the
// city's beads.allow_schema_behind_migrate decision. The controller's hook
// reuses the config it opened with.
func openNativeControlStore(scopeRoot, cityPath string, env map[string]string, envErr error) (beads.Store, error) {
	if envErr != nil {
		return nil, fmt.Errorf("project native control store env %s: %w", scopeRoot, envErr)
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
// bd store keeps that answer for control-plane opens while its binding stays
// the same. Opens inside the window take the bd store without the preflight
// and without logging the refusal again; the first open after it asks the
// factory again. A refusal that has cleared, such as a Dolt server that was
// down at the first open, reaches the native store within one window, and a
// standing one costs one preflight per window instead of one per readiness
// scan.
const controlStoreNativeRecheck = 5 * time.Minute

// nativeControlStores records what the factory answered each control-plane
// scope in this process. See openControlBdStoreThroughFactory.
var nativeControlStores = newControlStoreRegistry(time.Now)

// controlStoreRegistry holds the native store each control-plane scope opened
// and the time the factory last answered a scope with the bd store, each with
// the scope's binding (controlStoreBinding) when the factory answered. An open
// that finds the scope under another binding retires the kept store and does
// not honor the refusal.
//
// A retired store is handed out no more, but a caller that opened it before
// the binding changed may still be using it. So every open that hands out a
// kept store takes a handle on it, closeControlStoreHandle and
// releaseControlStoreHandle give a handle back, and a retired store is closed
// when its last handle comes back. A caller that never gives its handle back
// keeps a retired store open until the process exits.
type controlStoreRegistry struct {
	mu      sync.Mutex
	now     func() time.Time
	native  map[string]keptControlStore
	refused map[string]controlStoreRefusal
	// handles counts the handles out on each native store the registry keeps
	// or has retired.
	handles map[*beads.NativeDoltStore]int
	// retired holds the stores retired while a handle was out on them.
	retired map[*beads.NativeDoltStore]bool
}

// keptControlStore is the native store a scope opened and the scope's binding
// when it did.
type keptControlStore struct {
	store   *beads.NativeDoltStore
	binding string
}

// controlStoreRefusal is when the factory answered a scope with the bd store,
// and the scope's binding then.
type controlStoreRefusal struct {
	at      time.Time
	binding string
}

func newControlStoreRegistry(now func() time.Time) *controlStoreRegistry {
	return &controlStoreRegistry{
		now:     now,
		native:  map[string]keptControlStore{},
		refused: map[string]controlStoreRefusal{},
		handles: map[*beads.NativeDoltStore]int{},
		retired: map[*beads.NativeDoltStore]bool{},
	}
}

// acquire returns the native store kept for scopeRoot, with a handle taken on
// it, when the scope still has the binding the store opened under. A store
// kept under another binding is retired instead, and no store is returned.
// The second result is a retired store no handle holds, for the caller to
// close.
func (r *controlStoreRegistry) acquire(scopeRoot, binding string) (beads.Store, *beads.NativeDoltStore) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if unheld := r.retireReboundLocked(scopeRoot, binding); unheld != nil {
		return nil, unheld
	}
	kept, ok := r.native[scopeRoot]
	if !ok {
		return nil, nil
	}
	r.handles[kept.store]++
	return kept.store, nil
}

// retireRebound retires the native store kept for scopeRoot when the scope's
// binding has changed since the store opened, for an open that hands out no
// native store. It returns the retired store when no handle holds it, for the
// caller to close.
func (r *controlStoreRegistry) retireRebound(scopeRoot, binding string) *beads.NativeDoltStore {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retireReboundLocked(scopeRoot, binding)
}

func (r *controlStoreRegistry) retireReboundLocked(scopeRoot, binding string) *beads.NativeDoltStore {
	kept, ok := r.native[scopeRoot]
	if !ok || kept.binding == binding {
		return nil
	}
	delete(r.native, scopeRoot)
	return r.retireLocked(kept.store)
}

// retireLocked stops handing out store. It returns store when no handle holds
// it, for the caller to close outside the lock; otherwise the release of its
// last handle closes it.
func (r *controlStoreRegistry) retireLocked(store *beads.NativeDoltStore) *beads.NativeDoltStore {
	if r.handles[store] > 0 {
		r.retired[store] = true
		return nil
	}
	delete(r.handles, store)
	return store
}

// refusedRecently reports whether the factory answered scopeRoot with the bd
// store under binding within the last controlStoreNativeRecheck.
func (r *controlStoreRegistry) refusedRecently(scopeRoot, binding string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	refusal, ok := r.refused[scopeRoot]
	return ok && refusal.binding == binding && r.now().Sub(refusal.at) < controlStoreNativeRecheck
}

// retain records what the factory answered for scopeRoot under binding, with
// a transport that allowed the native store, and returns the store to hand
// out. A native store is kept and returned with a handle taken on it, and a
// store kept for the scope under another binding is retired. When a
// concurrent first open under the same binding kept a store already, that one
// is returned with a handle taken on it, and this one is not kept. Any other
// store is returned as it came, and the refusal is recorded. The second
// result is a store for the caller to close: the one not kept, or a retired
// store no handle holds.
func (r *controlStoreRegistry) retain(scopeRoot, binding string, store beads.Store) (beads.Store, *beads.NativeDoltStore) {
	r.mu.Lock()
	defer r.mu.Unlock()
	native, ok := store.(*beads.NativeDoltStore)
	if !ok {
		r.refused[scopeRoot] = controlStoreRefusal{at: r.now(), binding: binding}
		return store, nil
	}
	delete(r.refused, scopeRoot)
	if kept, ok := r.native[scopeRoot]; ok && kept.binding == binding {
		r.handles[kept.store]++
		return kept.store, native
	}
	unheld := r.retireReboundLocked(scopeRoot, binding)
	r.native[scopeRoot] = keptControlStore{store: native, binding: binding}
	r.handles[native]++
	return native, unheld
}

// release gives back a handle on store. It reports whether store is a native
// store the registry keeps or has retired, and returns store when it is
// retired and this was its last handle, for the caller to close.
func (r *controlStoreRegistry) release(store beads.Store) (bool, *beads.NativeDoltStore) {
	native, ok := store.(*beads.NativeDoltStore)
	if !ok {
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.retired[native] && !r.keepsLocked(native) {
		return false, nil
	}
	if r.handles[native] > 1 {
		r.handles[native]--
		return true, nil
	}
	delete(r.handles, native)
	if !r.retired[native] {
		return true, nil
	}
	delete(r.retired, native)
	return true, native
}

// keepsLocked reports whether store is the native store kept for a scope.
func (r *controlStoreRegistry) keepsLocked(store *beads.NativeDoltStore) bool {
	for _, kept := range r.native {
		if kept.store == store {
			return true
		}
	}
	return false
}

// closeUnheldControlStore closes a native store the registry no longer hands
// out and no handle holds.
func closeUnheldControlStore(scopeRoot string, store *beads.NativeDoltStore) {
	if store == nil {
		return
	}
	if err := store.CloseStore(); err != nil {
		log.Printf("control store: closing a native store no longer kept for %s: %v", scopeRoot, err)
	}
}

// releaseControlStoreHandle gives back a handle on a native store the control
// path keeps or has retired, and closes a retired store whose last handle it
// was. It reports whether store is such a store, and leaves any other store
// as it is.
func releaseControlStoreHandle(store beads.Store) (bool, error) {
	tracked, unheld := nativeControlStores.release(store)
	if unheld != nil {
		return tracked, unheld.CloseStore()
	}
	return tracked, nil
}

// closeControlStoreHandle releases a control-plane store the caller opened
// for one readiness scan, control dispatch or execution-fact flush. A native
// store nativeControlStores keeps is shared by every later open of its scope,
// so it stays open: CloseStore is a one-way latch, and closing it here would
// fail every later control open of the scope with ErrStoreClosed. A store the
// registry retired is closed when its last handle comes back, and any other
// store is closed here.
func closeControlStoreHandle(store beads.Store) error {
	if tracked, err := releaseControlStoreHandle(store); tracked {
		return err
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
