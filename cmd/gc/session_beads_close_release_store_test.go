package main

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
)

// newCloseReleaseSessionBead creates a pool session bead carrying the template
// metadata retiredSessionFallbackRoute recovers the owning pool route from.
func newCloseReleaseSessionBead(t *testing.T, store beads.Store, sessionName, template string) beads.Bead {
	t.Helper()
	b, err := store.Create(beads.Bead{
		Title:  "worker",
		Type:   sessionBeadType,
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"session_name": sessionName,
			"template":     template,
			"state":        "active",
		},
	})
	if err != nil {
		t.Fatalf("create session bead: %v", err)
	}
	return b
}

// newCloseReleaseWork creates a work bead assigned to assignee and forces its
// status, because Store.Create always mints "open" — without the follow-up
// Update the in_progress -> open reset would never be exercised.
func newCloseReleaseWork(t *testing.T, store beads.Store, title, assignee, status string) beads.Bead {
	t.Helper()
	b, err := store.Create(beads.Bead{Title: title, Assignee: assignee})
	if err != nil {
		t.Fatalf("create work %q: %v", title, err)
	}
	if err := store.Update(b.ID, beads.UpdateOpts{Status: &status}); err != nil {
		t.Fatalf("set status %q on %q: %v", status, title, err)
	}
	got, err := store.Get(b.ID)
	if err != nil {
		t.Fatalf("re-fetch work %q: %v", title, err)
	}
	if got.Status != status {
		t.Fatalf("work %q status = %q, want %q", title, got.Status, status)
	}
	return got
}

// TestCloseBeadReleasesInProgressWorkInRigStore is the regression test for
// gc-d9qnh: a pool session whose session bead lives in the city store but whose
// work lives in a RIG store. Before the fix, releaseWorkFromClosedSessionBead
// scanned only the store the session bead came from, found nothing, and ran none
// of its recovery — leaving the work in_progress with a cleared assignee and no
// route. Nothing picked it up (the pool demand probe keys on gc.routed_to, and
// releaseOrphanedPoolAssignments skips empty-routed beads), so the parent
// workflow stayed in_progress forever.
func TestCloseBeadReleasesInProgressWorkInRigStore(t *testing.T) {
	store := beads.NewMemStore()
	rigStore := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, store, "gascity__polecat-lx-psy7", "gascity/gc-toolkit.polecat")

	// A graph.v2 step bead: assigned to the session, in_progress, and carrying
	// an empty gc.routed_to — the shape releaseOrphanedPoolAssignments skips.
	work := newCloseReleaseWork(t, rigStore, "rig-store step bead", sessionBead.ID, "in_progress")

	var stderr bytes.Buffer
	if !closeBead(store, workAssignmentStores(store, map[string]beads.Store{"gascity": rigStore}), sessionBead.ID, "orphaned", time.Now().UTC(), &stderr) {
		t.Fatalf("closeBead() = false, want true; stderr=%q", stderr.String())
	}

	got, err := rigStore.Get(work.ID)
	if err != nil {
		t.Fatalf("get rig-store work: %v", err)
	}
	if got.Assignee != "" {
		t.Errorf("rig-store work assignee = %q, want empty — close-release must reach the rig store", got.Assignee)
	}
	if got.Status != "open" {
		t.Errorf("rig-store work status = %q, want open — in_progress work is invisible to the work_query until reopened", got.Status)
	}
	if got.Metadata[beadmeta.RunTargetMetadataKey] != "gascity/gc-toolkit.polecat" {
		t.Errorf("gc.run_target = %q, want gascity/gc-toolkit.polecat — the owning pool route must be restored so the work re-enters pool demand", got.Metadata[beadmeta.RunTargetMetadataKey])
	}
}

// TestCloseBeadReleasesWorkAcrossEveryRigStore proves the scan is a fan-out, not
// a first-hit lookup: work assigned to the closing session is released in the
// city store and in every attached rig store in one pass.
func TestCloseBeadReleasesWorkAcrossEveryRigStore(t *testing.T) {
	store := beads.NewMemStore()
	rigA := beads.NewMemStore()
	rigB := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, store, "worker-3", "pool/worker")

	// rig-b's work is assigned by session_name rather than bead ID, so the
	// fan-out is exercised alongside the identity union.
	cityWork := newCloseReleaseWork(t, store, "city work", sessionBead.ID, "in_progress")
	rigAWork := newCloseReleaseWork(t, rigA, "rig-a work", sessionBead.ID, "in_progress")
	rigBWork := newCloseReleaseWork(t, rigB, "rig-b work", "worker-3", "open")

	var stderr bytes.Buffer
	if !closeBead(store, workAssignmentStores(store, map[string]beads.Store{"a": rigA, "b": rigB}), sessionBead.ID, "orphaned", time.Now().UTC(), &stderr) {
		t.Fatalf("closeBead() = false, want true; stderr=%q", stderr.String())
	}

	for _, tc := range []struct {
		name       string
		store      beads.Store
		id         string
		wantStatus string
	}{
		{"city", store, cityWork.ID, "open"},
		{"rig-a", rigA, rigAWork.ID, "open"},
		{"rig-b", rigB, rigBWork.ID, "open"},
	} {
		got, err := tc.store.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s work: %v", tc.name, err)
		}
		if got.Assignee != "" {
			t.Errorf("%s work assignee = %q, want empty", tc.name, got.Assignee)
		}
		if got.Status != tc.wantStatus {
			t.Errorf("%s work status = %q, want %q", tc.name, got.Status, tc.wantStatus)
		}
	}
}

// TestCloseBeadReleasesSameBeadIDInTwoStores pins the per-store dedupe key. Bead
// IDs are only unique within a store, so two stores can hand back the same ID;
// a bare-ID dedupe would release the first and silently skip the second.
func TestCloseBeadReleasesSameBeadIDInTwoStores(t *testing.T) {
	store := beads.NewMemStore()
	rigStore := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, store, "worker-1", "pool/worker")

	// Burn one id in the rig store so both stores mint the SAME id for the work
	// beads below — the collision this dedupe key has to survive.
	if _, err := rigStore.Create(beads.Bead{Title: "id-aligning filler"}); err != nil {
		t.Fatalf("create filler bead: %v", err)
	}
	cityWork := newCloseReleaseWork(t, store, "city work", sessionBead.ID, "in_progress")
	rigWork := newCloseReleaseWork(t, rigStore, "rig work", sessionBead.ID, "in_progress")
	if cityWork.ID != rigWork.ID {
		t.Fatalf("fixture did not produce an id collision: city=%q rig=%q", cityWork.ID, rigWork.ID)
	}

	var stderr bytes.Buffer
	if !closeBead(store, workAssignmentStores(store, map[string]beads.Store{"gascity": rigStore}), sessionBead.ID, "orphaned", time.Now().UTC(), &stderr) {
		t.Fatalf("closeBead() = false, want true; stderr=%q", stderr.String())
	}

	got, err := rigStore.Get(rigWork.ID)
	if err != nil {
		t.Fatalf("get rig work: %v", err)
	}
	if got.Assignee != "" || got.Status != "open" {
		t.Errorf("rig work assignee=%q status=%q, want empty/open — a same-ID city bead must not dedupe the rig release away", got.Assignee, got.Status)
	}
}

// TestCloseBeadWithoutRigStoresReleasesCityWorkUnchanged pins the single-store
// city: with no rigs attached the release scope collapses to the city store
// alone, and the release behaves exactly as it did before the fan-out was added.
func TestCloseBeadWithoutRigStoresReleasesCityWorkUnchanged(t *testing.T) {
	store := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, store, "worker-1", "pool/worker")
	work := newCloseReleaseWork(t, store, "city work", sessionBead.ID, "in_progress")

	var stderr bytes.Buffer
	if !closeBead(store, workAssignmentStores(store, nil), sessionBead.ID, "orphaned", time.Now().UTC(), &stderr) {
		t.Fatalf("closeBead() = false, want true; stderr=%q", stderr.String())
	}

	got, err := store.Get(work.ID)
	if err != nil {
		t.Fatalf("get city work: %v", err)
	}
	if got.Assignee != "" {
		t.Errorf("city work assignee = %q, want empty", got.Assignee)
	}
	if got.Status != "open" {
		t.Errorf("city work status = %q, want open", got.Status)
	}
	if got.Metadata[beadmeta.RunTargetMetadataKey] != "pool/worker" {
		t.Errorf("gc.run_target = %q, want pool/worker", got.Metadata[beadmeta.RunTargetMetadataKey])
	}
}

// TestReleaseWorkFromClosedSessionBeadSkipsRigSessionBeads guards the fan-out
// against the one class it must never touch: a session bead that happens to live
// in a rig store. Releasing one would strip a live session's own identity record.
func TestReleaseWorkFromClosedSessionBeadSkipsRigSessionBeads(t *testing.T) {
	store := beads.NewMemStore()
	rigStore := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, store, "worker-1", "pool/worker")

	// Another session bead in the rig store, assigned to the closing session's
	// identity. IsSessionBeadOrRepairable must filter it out.
	rigSessionBead, err := rigStore.Create(beads.Bead{
		Title:    "nested session bead",
		Type:     sessionBeadType,
		Labels:   []string{sessionBeadLabel},
		Assignee: sessionBead.ID,
		Metadata: map[string]string{"session_name": "worker-2"},
	})
	if err != nil {
		t.Fatalf("create rig session bead: %v", err)
	}
	inProgress := "in_progress"
	if err := rigStore.Update(rigSessionBead.ID, beads.UpdateOpts{Status: &inProgress}); err != nil {
		t.Fatalf("mark rig session bead in_progress: %v", err)
	}

	var stderr bytes.Buffer
	releaseWorkFromClosedSessionBead(workAssignmentStores(store, map[string]beads.Store{"gascity": rigStore}), sessionBead, &stderr)

	got, err := rigStore.Get(rigSessionBead.ID)
	if err != nil {
		t.Fatalf("get rig session bead: %v", err)
	}
	if got.Assignee != sessionBead.ID {
		t.Errorf("rig session bead assignee = %q, want %q — session beads must not be released as work", got.Assignee, sessionBead.ID)
	}
	if got.Status != "in_progress" {
		t.Errorf("rig session bead status = %q, want in_progress", got.Status)
	}
}

// newReachableCloseCity builds a two-rig city whose "worker" agent is scoped to
// riga. reachableStoresForSessionInfo therefore resolves a riga/worker session's
// assigned-work scope to the riga store alone — the city store and the rigb store
// are UNREACHABLE for it.
func newReachableCloseCity(t *testing.T) (string, *config.City) {
	t.Helper()
	cityPath := t.TempDir()
	return cityPath, &config.City{
		Rigs: []config.Rig{
			{Name: "riga", Path: filepath.Join(cityPath, "riga")},
			{Name: "rigb", Path: filepath.Join(cityPath, "rigb")},
		},
		Agents: []config.Agent{{Name: "worker", Dir: "riga"}},
	}
}

// TestCloseSessionBeadIfReachableStoreUnassignedLeavesUnreachableStoreWorkAlone
// pins the close-release scope to the gate's proof scope.
//
// closeSessionBeadIfReachableStoreUnassigned only proves that the ONE store the
// session's configured agent can query holds no work assigned to it; that is the
// whole point of the reachable-store gate, which deliberately lets a rig-scoped
// session close while work in other stores stays put because that work may be
// unrelated and merely share an assignment token. Handing the close-release scan
// the full city+rig fan-out broke that: it cleared the assignee, reset
// in_progress -> open and stamped the closing session's pool route onto beads
// nobody proved it owned.
func TestCloseSessionBeadIfReachableStoreUnassignedLeavesUnreachableStoreWorkAlone(t *testing.T) {
	cityPath, cfg := newReachableCloseCity(t)
	cityStore := beads.NewMemStore()
	rigA := beads.NewMemStore()
	rigB := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, cityStore, "worker-session", "riga/worker")

	// Work in the two stores this riga-scoped session cannot query, assigned
	// under the same token. riga — the reachable store — stays empty, so the
	// gate sees no assigned work and the close proceeds.
	cityWork := newCloseReleaseWork(t, cityStore, "city work", sessionBead.ID, "in_progress")
	rigBWork := newCloseReleaseWork(t, rigB, "rig-b work", sessionBead.ID, "in_progress")

	var stderr bytes.Buffer
	if !closeSessionBeadIfReachableStoreUnassigned(
		cityPath, cfg, cityStore,
		map[string]beads.Store{"riga": rigA, "rigb": rigB},
		seedSessionInfo(sessionBead), "drained", time.Now().UTC(), &stderr, false,
	) {
		t.Fatalf("closeSessionBeadIfReachableStoreUnassigned() = false, want true; stderr=%q", stderr.String())
	}

	for _, tc := range []struct {
		name  string
		store beads.Store
		id    string
	}{
		{"city-store", cityStore, cityWork.ID},
		{"rigb-store", rigB, rigBWork.ID},
	} {
		got, err := tc.store.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s work: %v", tc.name, err)
		}
		if got.Assignee != sessionBead.ID {
			t.Errorf("%s work assignee = %q, want %q — close-release must not reach a store the gate never proved", tc.name, got.Assignee, sessionBead.ID)
		}
		if got.Status != "in_progress" {
			t.Errorf("%s work status = %q, want in_progress — close-release must not reopen unproven work", tc.name, got.Status)
		}
		if got.Metadata[beadmeta.RunTargetMetadataKey] != "" {
			t.Errorf("%s work gc.run_target = %q, want empty — the closing session's pool route must not be stamped onto unproven work", tc.name, got.Metadata[beadmeta.RunTargetMetadataKey])
		}
	}
}

// TestCloseSessionBeadIfReachableStoreUnassignedReleasesInsideProvenScope is the
// other half of the contract: scoping the scan to the gate's proof must not turn
// the release into a no-op.
//
// The release identity set is strictly broader than the gate's — the gate matches
// {ID, session_name, configured_named_identity} while the release also matches
// the session's alias and alias history — so work assigned to a reachable-store
// bead under the alias passes the gate unseen and is exactly what the release
// still has to clean up.
func TestCloseSessionBeadIfReachableStoreUnassignedReleasesInsideProvenScope(t *testing.T) {
	cityPath, cfg := newReachableCloseCity(t)
	cityStore := beads.NewMemStore()
	rigA := beads.NewMemStore()
	rigB := beads.NewMemStore()

	sessionBead, err := cityStore.Create(beads.Bead{
		Title:  "worker",
		Type:   sessionBeadType,
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"session_name": "worker-session",
			"template":     "riga/worker",
			"state":        "active",
			"alias":        "worker-alias",
		},
	})
	if err != nil {
		t.Fatalf("create session bead: %v", err)
	}

	reachableWork := newCloseReleaseWork(t, rigA, "riga work", "worker-alias", "in_progress")

	var stderr bytes.Buffer
	if !closeSessionBeadIfReachableStoreUnassigned(
		cityPath, cfg, cityStore,
		map[string]beads.Store{"riga": rigA, "rigb": rigB},
		seedSessionInfo(sessionBead), "drained", time.Now().UTC(), &stderr, false,
	) {
		t.Fatalf("closeSessionBeadIfReachableStoreUnassigned() = false, want true; stderr=%q", stderr.String())
	}

	got, err := rigA.Get(reachableWork.ID)
	if err != nil {
		t.Fatalf("get riga work: %v", err)
	}
	if got.Assignee != "" {
		t.Errorf("riga work assignee = %q, want empty — the reachable store is inside the proven scope and must still be released", got.Assignee)
	}
	if got.Status != "open" {
		t.Errorf("riga work status = %q, want open — in_progress work is invisible to the work_query until reopened", got.Status)
	}
	if got.Metadata[beadmeta.RunTargetMetadataKey] != "riga/worker" {
		t.Errorf("riga work gc.run_target = %q, want riga/worker — the owning pool route must be restored so the work re-enters pool demand", got.Metadata[beadmeta.RunTargetMetadataKey])
	}
}

// TestReleaseWorkFromClosedSessionBeadToleratesNilRigStoreEntry guards the
// scan's nil-entry filter: a rig whose store failed to open reaches the release
// scope as a nil element, and the scan must skip it and keep going rather than
// panic and drop every store behind it.
func TestReleaseWorkFromClosedSessionBeadToleratesNilRigStoreEntry(t *testing.T) {
	store := beads.NewMemStore()
	rigStore := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, store, "worker-1", "pool/worker")
	work := newCloseReleaseWork(t, rigStore, "rig work", sessionBead.ID, "in_progress")

	var stderr bytes.Buffer
	releaseWorkFromClosedSessionBead([]beads.Store{store, nil, rigStore}, sessionBead, &stderr)

	got, err := rigStore.Get(work.ID)
	if err != nil {
		t.Fatalf("get rig work: %v", err)
	}
	if got.Status != "open" || got.Assignee != "" {
		t.Errorf("rig work assignee=%q status=%q, want empty/open — a nil rig store must not abort the fan-out", got.Assignee, got.Status)
	}
}
