package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
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
	if !closeBead(store, map[string]beads.Store{"gascity": rigStore}, sessionBead.ID, "orphaned", time.Now().UTC(), &stderr) {
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
	if !closeBead(store, map[string]beads.Store{"a": rigA, "b": rigB}, sessionBead.ID, "orphaned", time.Now().UTC(), &stderr) {
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
	if !closeBead(store, map[string]beads.Store{"gascity": rigStore}, sessionBead.ID, "orphaned", time.Now().UTC(), &stderr) {
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
// city: nil rigStores must behave exactly as before the fan-out was added.
func TestCloseBeadWithoutRigStoresReleasesCityWorkUnchanged(t *testing.T) {
	store := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, store, "worker-1", "pool/worker")
	work := newCloseReleaseWork(t, store, "city work", sessionBead.ID, "in_progress")

	var stderr bytes.Buffer
	if !closeBead(store, nil, sessionBead.ID, "orphaned", time.Now().UTC(), &stderr) {
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
	releaseWorkFromClosedSessionBead(store, map[string]beads.Store{"gascity": rigStore}, sessionBead, &stderr)

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

// TestReleaseWorkFromClosedSessionBeadToleratesNilRigStoreEntry guards the
// candidate builder's nil-entry filter: a rig whose store failed to open is
// registered with a nil value, and the scan must skip it rather than panic.
func TestReleaseWorkFromClosedSessionBeadToleratesNilRigStoreEntry(t *testing.T) {
	store := beads.NewMemStore()
	rigStore := beads.NewMemStore()

	sessionBead := newCloseReleaseSessionBead(t, store, "worker-1", "pool/worker")
	work := newCloseReleaseWork(t, rigStore, "rig work", sessionBead.ID, "in_progress")

	var stderr bytes.Buffer
	releaseWorkFromClosedSessionBead(store, map[string]beads.Store{"broken": nil, "gascity": rigStore}, sessionBead, &stderr)

	got, err := rigStore.Get(work.ID)
	if err != nil {
		t.Fatalf("get rig work: %v", err)
	}
	if got.Status != "open" || got.Assignee != "" {
		t.Errorf("rig work assignee=%q status=%q, want empty/open — a nil rig store must not abort the fan-out", got.Assignee, got.Status)
	}
}
