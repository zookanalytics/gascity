package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/citylayout"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/fsys"
)

// runAutocloseCores runs the convoy, wisp, and molecule autoclose cores
// for a single just-closed bead against store, in the same order and with
// the same semantics as the bd on_close hook's three `gc … autoclose`
// invocations (cmd/gc/hooks.go: closeHookScript). It is the in-process
// equivalent of that subprocess cascade: closing a work bead must auto-
// close its now-complete parent convoy, force-close any wisp/molecule
// children attached to it, and auto-close its parent molecule once every
// step is terminal (gastownhall/gascity#1039).
//
// All three cores are idempotent and best-effort: they no-op when the
// target is already terminal, has open siblings, or does not exist, and
// they never return errors. Re-running on the same bead is therefore safe,
// which is what lets the event consumer process a bead.closed even when the
// hook (in hook mode) already handled the same close.
//
// storeRef is the "city:<name>" / "rig:<name>" label of store; the molecule
// core uses it to scope source-bead reverse lookups so a same-ID bead in a
// different store is never auto-closed. Pass "" to match on bead ID alone
// (single-store behavior).
func runAutocloseCores(store beads.Store, rec events.Recorder, storeRef, beadID string) {
	if store == nil || strings.TrimSpace(beadID) == "" {
		return
	}
	if rec == nil {
		rec = events.Discard
	}
	// Order mirrors closeHookScript: convoy → wisp → molecule.
	doConvoyAutocloseWith(store, rec, beadID, io.Discard, io.Discard)
	doWispAutocloseWith(store, beadID, io.Discard)
	doMoleculeAutocloseWith(store, storeRef, rec, beadID, io.Discard)
}

// autocloseTarget pairs a bead store with its store-ref label so the
// molecule core's cross-store source-bead scoping resolves correctly.
type autocloseTarget struct {
	store    beads.Store
	storeRef string
}

// cityAutocloseStoreRef returns the "city:<name>" store-ref label for the
// city-level store, mirroring workflowStoreRefForDir's city branch
// (defaulting the name to "city" when unset).
func cityAutocloseStoreRef(cityName string) string {
	cityName = strings.TrimSpace(cityName)
	if cityName == "" {
		cityName = "city"
	}
	return "city:" + cityName
}

// resolveAutocloseTargets resolves which store(s) a closed bead's autoclose
// cores should run against, routing by bead-ID prefix exactly as gc bd does
// (longest matching prefix wins). A matched bead resolves to its single
// owning store, so a close in one rig never scans another rig's beads. An
// unrecognized ID falls back to every configured store — each core bails
// when the bead is absent — mirroring beadEventStoresLocked's unknown-ID
// fan-out so legacy/un-prefixed beads are still handled.
func resolveAutocloseTargets(cfg *config.City, cityName string, cityStore beads.Store, rigStores map[string]beads.Store, id string) []autocloseTarget {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	if cfg != nil {
		var matched autocloseTarget
		matchedLen := -1
		consider := func(prefix, ref string, store beads.Store) {
			if prefix == "" || store == nil || !strings.HasPrefix(id, prefix+"-") {
				return
			}
			if len(prefix) > matchedLen {
				matchedLen = len(prefix)
				matched = autocloseTarget{store: store, storeRef: ref}
			}
		}
		consider(config.EffectiveHQPrefix(cfg), cityAutocloseStoreRef(cityName), cityStore)
		for i := range cfg.Rigs {
			rig := cfg.Rigs[i]
			consider(rig.EffectivePrefix(), "rig:"+rig.Name, rigStores[rig.Name])
		}
		if matchedLen >= 0 {
			return []autocloseTarget{matched}
		}
	}

	// Unknown prefix: fan out to every configured store. The cores bail
	// when the bead is absent, so a mis-scoped store is a no-op.
	targets := make([]autocloseTarget, 0, len(rigStores)+1)
	if cfg != nil {
		for i := range cfg.Rigs {
			rig := cfg.Rigs[i]
			if store := rigStores[rig.Name]; store != nil {
				targets = append(targets, autocloseTarget{store: store, storeRef: "rig:" + rig.Name})
			}
		}
	}
	if cityStore != nil {
		targets = append(targets, autocloseTarget{store: cityStore, storeRef: cityAutocloseStoreRef(cityName)})
	}
	return targets
}

// autocloseTargetsLocked resolves the autoclose target store(s) for a bead
// ID against the current config and stores. Callers hold cs.mu (read).
func (cs *controllerState) autocloseTargetsLocked(id string) []autocloseTarget {
	return resolveAutocloseTargets(cs.cfg, cs.cityName, cs.cityBeadStore, cs.beadStores, id)
}

// runAutocloseForClosedBead routes a bead.closed event to its owning store
// and runs the autoclose cores against it. The store is a CachingStore, so
// any cascade close it performs re-enters the event stream (via the cache's
// change notification in native mode, or the bd on_close hook in hook mode),
// which this same watcher then picks up — so multi-level cascades (a closed
// convoy completing its parent convoy) converge without special handling.
func (cs *controllerState) runAutocloseForClosedBead(evt events.Event) {
	id := beadEventID(evt)
	if id == "" {
		return
	}
	cs.mu.RLock()
	targets := cs.autocloseTargetsLocked(id)
	rec := cs.eventProv
	cs.mu.RUnlock()
	for _, tgt := range targets {
		runAutocloseCores(tgt.store, rec, tgt.storeRef, id)
	}
}

// startAutocloseWatcher subscribes to the city event stream and runs the
// convoy/wisp/molecule autoclose cores for every bead.closed event,
// in-process. This replaces the bd on_close hook as the autoclose driver
// when the event-forwarding hooks are removed (native store mode,
// [beads] event_hooks=false): bead.closed still reaches the stream — emitted
// by CachingStore's change notifier in native mode — so the cascade survives
// even with no hook present. In hook mode it runs alongside the hook as an
// idempotent redundant observer (the cores no-op on already-closed targets).
//
// Unlike startBeadEventWatcher (which feeds caches and so skips its own
// "cache-reconcile" emissions), this watcher acts on every bead.closed
// regardless of actor: native-mode closes are emitted with the
// "cache-reconcile" actor and must drive autoclose too.
//
// The last processed sequence is checkpointed to a per-city runtime file so a
// controller restart resumes from the last handled close rather than from
// "now" — without it, closes that landed while the controller was down would
// never be auto-closed. Idempotency makes the bounded replay after the
// checkpoint harmless.
func (cs *controllerState) startAutocloseWatcher(ctx context.Context) {
	ep := cs.EventProvider()
	if ep == nil {
		return
	}
	cs.mu.RLock()
	cityPath := cs.cityPath
	cs.mu.RUnlock()

	seq, ok := loadAutocloseCursor(cityPath)
	if !ok {
		// First run for this city: start from the stream head captured at
		// construction rather than replaying all history. Pre-existing
		// closes were already handled by the hook that was active before
		// this consumer shipped; from here forward the consumer owns them.
		seq = cs.beadEventStartSeq
		if err := saveAutocloseCursor(cityPath, seq); err != nil {
			fmt.Fprintf(os.Stderr, "api: autoclose watcher: seed cursor seq %d: %v\n", seq, err)
		}
	}

	go func() {
		for {
			watcher, err := ep.Watch(ctx, seq)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Fprintf(os.Stderr, "api: autoclose watcher: watch from seq %d: %v\n", seq, err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(beadEventWatcherRetryDelay):
					continue
				}
			}
			for {
				evt, err := watcher.Next()
				if err != nil {
					_ = watcher.Close()
					break
				}
				seq = evt.Seq
				if evt.Type != events.BeadClosed {
					continue
				}
				cs.runAutocloseForClosedBead(evt)
				if err := saveAutocloseCursor(cityPath, seq); err != nil {
					fmt.Fprintf(os.Stderr, "api: autoclose watcher: persist cursor seq %d: %v\n", seq, err)
				}
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
}

// autocloseCursorState is the on-disk shape of the autoclose checkpoint.
type autocloseCursorState struct {
	// Seq is the highest event-stream sequence whose bead.closed the
	// autoclose watcher has processed.
	Seq uint64 `json:"seq"`
}

// loadAutocloseCursor reads the persisted autoclose checkpoint sequence for
// cityPath. The second return is false when no valid cursor exists yet (first
// run, missing or unreadable file), signaling the caller to seed from the
// live stream head instead.
func loadAutocloseCursor(cityPath string) (uint64, bool) {
	if strings.TrimSpace(cityPath) == "" {
		return 0, false
	}
	data, err := os.ReadFile(citylayout.AutocloseCursorFile(cityPath))
	if err != nil {
		return 0, false
	}
	var st autocloseCursorState
	if err := json.Unmarshal(data, &st); err != nil {
		return 0, false
	}
	return st.Seq, true
}

// saveAutocloseCursor atomically persists the autoclose checkpoint sequence
// for cityPath. A no-op (nil) when cityPath is empty.
func saveAutocloseCursor(cityPath string, seq uint64) error {
	if strings.TrimSpace(cityPath) == "" {
		return nil
	}
	p := citylayout.AutocloseCursorFile(cityPath)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("creating autoclose cursor dir: %w", err)
	}
	data, err := json.Marshal(autocloseCursorState{Seq: seq})
	if err != nil {
		return fmt.Errorf("marshaling autoclose cursor: %w", err)
	}
	if err := fsys.WriteFileAtomic(fsys.OSFS{}, p, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("writing autoclose cursor %s: %w", p, err)
	}
	return nil
}
