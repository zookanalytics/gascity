package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	convoycore "github.com/gastownhall/gascity/internal/convoy"
	"github.com/gastownhall/gascity/internal/sourceworkflow"
)

// carriedPoolRoute returns the pool route a bead already declares for itself and
// that the controller may safely restore to gc.routed_to, or "" when the bead
// carries no recoverable route. Two bead shapes carry a legacy gc.run_target
// pool route: a plain (kind-less) standalone work bead — this fork's dominant
// work shape — and a pre-ga-eld2x workflow root (recognized by
// legacyWorkflowRunTarget).
//
// Control-dispatcher (retry, ralph, …) and other workflow-topology (scope, spec)
// beads also carry a bare gc.run_target, but there it is a dispatch/structure
// target an agent never claims from a pool; restoring gc.routed_to on one would
// mis-route it into pool demand, so they yield "". The choice is judgment-free
// (ZFC): it copies a route the bead already declares and never invents a target.
// Idempotent: a bead that already carries gc.routed_to yields "".
func carriedPoolRoute(b beads.Bead) string {
	// Legacy pre-ga-eld2x workflow root: gc.run_target is the root's pool route
	// only while gc.routed_to is empty — exactly legacyWorkflowRunTarget's rule.
	if route := legacyWorkflowRunTarget(b); route != "" {
		return route
	}
	// Broaden beyond workflow roots to plain standalone work beads. Any non-empty
	// gc.kind reaching here is a control-dispatcher or workflow-topology construct
	// (legacyWorkflowRunTarget already consumed the lone claimable kind,
	// "workflow"), so its gc.run_target is not a recoverable pool route.
	if strings.TrimSpace(b.Metadata[beadmeta.KindMetadataKey]) != "" {
		return ""
	}
	if strings.TrimSpace(b.Metadata[beadmeta.RoutedToMetadataKey]) != "" {
		return ""
	}
	return strings.TrimSpace(b.Metadata[beadmeta.RunTargetMetadataKey])
}

// restoreCarriedWorkRoutes re-stamps gc.routed_to from the route a bead already
// carries (its legacy gc.run_target route, via carriedPoolRoute) for open,
// unassigned work whose canonical pool route was lost or never written. It
// returns the number of beads whose route it restored.
//
// The pool autoscaler keys exclusively on gc.routed_to, so an open work bead
// that carries a gc.run_target hint but an empty gc.routed_to is invisible to
// pool demand and never spawns a worker — the post-restart stall in ga-n2d.4
// (ready beads, 0 routed, 0 workers, until a manual `gc sling`). The controller
// runs this on startup and on every patrol tick so such work re-enters demand
// on its own. It is the automatic, broader-scoped promotion of the manual
// `gc doctor --fix` run-target-routed-to-backfill check.
//
// The recovery is judgment-free and cannot mis-route (ZFC): carriedPoolRoute
// only copies a route the bead already declares and skips control-dispatcher and
// topology beads. A bead with no carried route is left for its owner to sling —
// which pool ad-hoc work belongs to is the owner's judgment, not the
// controller's. Idempotent: an already-routed bead yields no route and is
// skipped.
//
// TOCTOU-narrowing (not eliminating): the open-bead List is a snapshot, so
// before writing, each bead is re-read through the store's authoritative,
// cache-bypassing live handle and skipped unless it is still open, unassigned,
// and carries the same recoverable route. This shrinks — but does not close —
// the window in which the re-stamp could clobber a route a polecat consumed by
// claiming the bead after the snapshot (ga-bgu): a claim landing between the
// live re-read and SetMetadata is still possible. The re-stamp stays monotonic
// (never worse than the prior blind write), so the residual window degrades to
// the pre-guard behavior rather than a new failure.
//
// That re-read guards claims but cannot guard blocks: a claim flips the bead to
// in_progress, which mapBdStatus preserves, while a block flips it to a status
// that collapses to "open" (gc-4zb). Blocked work is therefore excluded at the
// snapshot, by the Live query below, and not here.
//
// It also cannot guard a graph workflow, which drives its work without claiming
// or blocking the bead: the bead stays open and unassigned for the workflow's
// whole life. liveGraphWorkflowDrivesBead is the guard for that shape, and
// graphStore is where it reads roots on a city that relocates the graph class
// (nil where graph collapses onto the work store).
func restoreCarriedWorkRoutes(store, graphStore beads.Store) (int, error) {
	if store == nil {
		return 0, nil
	}
	// Open work is the only place a lost pool route can be recovered: closed or
	// in-progress beads need no route restored. Scanning open beads (not the
	// whole store) keeps the hot reconcile path cheap while still seeing both
	// carriers of a legacy route — plain work beads and workflow roots — which a
	// gc.kind=workflow query would miss. Mirrors sweepDetachedHandoffOrphans'
	// open-bead scan (AllowScan acknowledges the intentional population read).
	//
	// Live is what makes Status:"open" mean open (gc-4zb). mapBdStatus folds
	// bd's blocked/deferred/review/testing into Gas City's three statuses, so a
	// blocked bead decodes with Status "open" and is indistinguishable from
	// ready work in every beads.Bead this function can read. A cached List
	// filters with ListQuery.Matches against that collapsed status and so hands
	// back blocked beads; only the backing store filters on the raw status, by
	// passing --status=open to bd. Live bypasses the CachingStore to get there.
	// Without it a blocked root that carries gc.run_target is re-stamped on
	// every patrol tick — the blocked-routed-reaper's recurring offenders. The
	// workflow-root spawn path selects on gc.routed_to without re-checking
	// status, so each re-stamp respawns a worker that drains no-op.
	items, err := store.List(beads.ListQuery{Status: "open", AllowScan: true, Live: true})
	if err != nil {
		return 0, fmt.Errorf("listing open work: %w", err)
	}
	var (
		restored int
		errs     []error
	)
	// Resolve the authoritative, cache-bypassing read handle once. Production
	// stores are CachingStore-wrapped (see wrapWithCachingStore), so a plain
	// store.Get can return a cached bead that predates a cross-process claim;
	// handles.Live reads the backing store directly. For a plain store this
	// degrades to store.Get.
	handles := beads.HandlesFor(store)
	for _, b := range items {
		route := carriedPoolRoute(b)
		if route == "" {
			continue
		}
		// Only re-route open, unassigned work: an assigned bead is already
		// claimed. (Belt-and-braces with the Status:"open" query so the guarantee
		// holds regardless of store-level filtering semantics.)
		if b.Status != "open" || strings.TrimSpace(b.Assignee) != "" {
			continue
		}
		// Re-read the live bead immediately before writing, through the
		// authoritative cache-bypassing handle. The open-bead List is a snapshot;
		// a polecat — often in another process — may have claimed this bead in the
		// window since, which atomically flips it open->in_progress, records
		// gc.run_target, and consumes gc.routed_to in one update (ga-sa0). A plain
		// store.Get would go through the wrapping CachingStore and could return a
		// stale cached copy that predates a cross-process claim not yet absorbed
		// into this process's cache; handles.Live reads the backing store and sees
		// the claim. A blind SetMetadata keyed on the stale snapshot would re-stamp
		// gc.routed_to onto the now-claimed bead, undoing that consumption and
		// handing the dispatcher a phantom pool-demand bead that flaps
		// open<->in_progress and thrashes owners (ga-bgu). Recomputing
		// carriedPoolRoute on the live bead also yields "" once another restore has
		// already re-stamped it, so concurrent passes stay idempotent.
		live, getErr := handles.Live.Get(b.ID)
		if getErr != nil {
			errs = append(errs, fmt.Errorf("bead %s: re-reading before route restore: %w", b.ID, getErr))
			continue
		}
		if live.Status != "open" || strings.TrimSpace(live.Assignee) != "" || carriedPoolRoute(live) != route {
			continue // claimed, closed, or already routed since the snapshot — don't clobber
		}
		// A started graph workflow is the single live dispatch surface for the
		// work it drives, and it establishes that by retiring gc.routed_to on its
		// bead (internal/sling: retireClaimRoute). gc.run_target survives that
		// retire on purpose — it is the archived route reopen-source and this
		// recovery restore the bead from once the workflow is gone (ga-20zd) — so
		// without this gate the retire lasts exactly until the next patrol tick,
		// which re-promotes the archived route and hands the pool a bead the
		// workflow is already dispatching. That is the double-dispatch the retire
		// exists to prevent, reintroduced one tick later.
		driven, drivenErr := liveGraphWorkflowDrivesBead(store, graphStore, live)
		if drivenErr != nil {
			errs = append(errs, fmt.Errorf("bead %s: checking for a live workflow before route restore: %w", b.ID, drivenErr))
			continue // fail closed: an unproven bead keeps its retired route
		}
		if driven {
			continue
		}
		if setErr := store.SetMetadata(b.ID, beadmeta.RoutedToMetadataKey, route); setErr != nil {
			errs = append(errs, fmt.Errorf("bead %s: restoring gc.routed_to=%q: %w", b.ID, route, setErr))
			continue
		}
		restored++
	}
	return restored, errors.Join(errs...)
}

// workflowIDMetadataKey is the BARE (non-gc-prefixed) key a graph workflow
// launch stamps on its source bead to point at the root now driving it, and
// that every path returning the bead to the pool clears. It is deliberately not
// beadmeta.WorkflowIDMetadataKey ("gc.workflow_id"), which names a different
// key on a different bead — see the same distinction in
// internal/api/handler_beads.go.
const workflowIDMetadataKey = "workflow_id"

// liveGraphWorkflowDrivesBead reports whether a graph workflow is currently
// driving b's work, in which case b's retired pool route must stay retired.
//
// Liveness — not the mere presence of a link — is the gate, so the answer stops
// being true the moment the workflow reaches a terminal status and the bead
// becomes recoverable again. A marker that outlived its workflow would strand
// the bead in the one state this recovery exists to heal.
//
// Both launch shapes are checked because they leave different links behind. A
// convoy-first pour (the graph.v2 shape: `gc sling <bead> --on <formula>` mints
// a synthetic input convoy over the bead) clears gc.source_bead_id and links
// back only through gc.input_convoy_id -> the convoy -> its tracked members,
// which is the reverse walk. A workflow attached to a source bead instead
// stamps that bead's workflow_id with the root id, and carries no input convoy
// to walk. Neither link alone sees the other's shape.
//
// graphStore is where workflow roots live on a city that relocates the graph
// coordination class; pass nil where graph collapses onto the work store.
func liveGraphWorkflowDrivesBead(store, graphStore beads.Store, b beads.Bead) (bool, error) {
	rootStore := graphStore
	if rootStore == nil {
		rootStore = store
	}
	if rootID := strings.TrimSpace(b.Metadata[workflowIDMetadataKey]); rootID != "" {
		root, err := rootStore.Get(rootID)
		switch {
		case errors.Is(err, beads.ErrNotFound):
			// The root is gone, so nothing dispatches through it. Fall through
			// rather than return: a bead can carry a stale attachment link and
			// still be a live convoy-first workflow's member.
		case err != nil:
			return false, fmt.Errorf("reading workflow %s: %w", rootID, err)
		case !convoycore.IsTerminalStatus(root.Status):
			return true, nil
		}
	}
	roots, err := sourceworkflow.ListLiveInputConvoyRootsForItem(store, rootStore, b.ID, "")
	if err != nil {
		return false, err
	}
	return len(roots) > 0, nil
}

// routeRecoveryScope pairs a bead store with a human label for logging.
type routeRecoveryScope struct {
	label string
	store beads.Store
}

// recoverUnroutedWorkRoutes restores gc.routed_to from each bead's own carried
// route across the city store and every active rig store, so ready work
// re-enters pool demand after a controller restart without a manual `gc sling`
// (ga-n2d.4). Best-effort: a per-store failure is logged and the remaining
// stores still run.
func (cr *CityRuntime) recoverUnroutedWorkRoutes() {
	scopes := []routeRecoveryScope{{label: "city", store: cr.cityBeadStore()}}
	for name, store := range cr.rigBeadStores() {
		scopes = append(scopes, routeRecoveryScope{label: "rig " + name, store: store})
	}
	// Where the graph class is relocated every scope's workflow roots live in the
	// one binding; where it is not, they live beside the work in each scope's own
	// store, which is what a nil graphStore selects.
	graphStore := cr.relocatedGraphStore()
	for _, sc := range scopes {
		if sc.store == nil {
			continue
		}
		restored, err := restoreCarriedWorkRoutes(sc.store, graphStore)
		if err != nil {
			fmt.Fprintf(cr.stderr, "%s: route recovery (%s): %v\n", cr.logPrefix, sc.label, err) //nolint:errcheck // best-effort stderr
		}
		if restored > 0 {
			fmt.Fprintf(cr.stderr, "%s: route recovery (%s): restored gc.routed_to on %d ready bead(s) from gc.run_target\n", cr.logPrefix, sc.label, restored) //nolint:errcheck // best-effort stderr
		}
	}
}
