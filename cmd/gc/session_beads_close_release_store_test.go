package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
)

// Cross-store close-release regression tests (gc-d9qnh): a pool session whose
// session bead lives in the city store but whose work lives in a RIG store.
// Before upstream's WorkLegs/ReleaseClaims (#7487, #7525, #7561) the close
// cascade scanned only the session bead's own store, left the rig-store work
// in_progress against a closed session, and the parent workflow never
// finished. The cascade now reads every serving leg; these pin that, including
// the same-ID-in-two-stores case that a bare-ID dedupe would skip.

func closeReleaseSessionBead(t *testing.T, store beads.Store, sessionName, template string) beads.Bead {
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

func closeReleaseWork(t *testing.T, store beads.Store, title, assignee, status string) beads.Bead {
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

// closeReleaseCity declares rigs (so they are serving legs) and agents serving routes.
func closeReleaseCity(rigs []string, routes ...string) *config.City {
	cfg := servingCity(routes...)
	for _, r := range rigs {
		cfg.Rigs = append(cfg.Rigs, config.Rig{Name: r, Path: "rigs/" + r})
	}
	return cfg
}

func TestCloseBeadReleasesInProgressWorkInRigStore(t *testing.T) {
	store := beads.NewMemStore()
	rigStore := beads.NewMemStore()

	sessionBead := closeReleaseSessionBead(t, store, "gascity__polecat-lx-psy7", "gascity/gc-toolkit.polecat")
	work := closeReleaseWork(t, rigStore, "rig-store step bead", sessionBead.ID, "in_progress")

	cfg := closeReleaseCity([]string{"gascity"}, "gascity/gc-toolkit.polecat")
	rigs := map[string]beads.Store{"gascity": rigStore}
	var stderr bytes.Buffer
	if !closeBead(store, testSeatWork("", cfg, store, rigs), decidedSessionInfo(store, sessionBead.ID), "orphaned", time.Now().UTC(), &stderr) {
		t.Fatalf("closeBead() = false, want true; stderr=%q", stderr.String())
	}

	got, err := rigStore.Get(work.ID)
	if err != nil {
		t.Fatalf("get rig-store work: %v", err)
	}
	if got.Assignee != "" {
		t.Errorf("rig-store work assignee = %q, want empty; stderr=%q", got.Assignee, stderr.String())
	}
	if got.Status != "open" {
		t.Errorf("rig-store work status = %q, want open", got.Status)
	}
	if got.Metadata[beadmeta.RunTargetMetadataKey] != "gascity/gc-toolkit.polecat" {
		t.Errorf("gc.run_target = %q, want gascity/gc-toolkit.polecat", got.Metadata[beadmeta.RunTargetMetadataKey])
	}
}

func TestCloseBeadReleasesWorkAcrossEveryRigStore(t *testing.T) {
	store := beads.NewMemStore()
	rigA := beads.NewMemStore()
	rigB := beads.NewMemStore()

	sessionBead := closeReleaseSessionBead(t, store, "worker-3", "pool/worker")
	cityWork := closeReleaseWork(t, store, "city work", sessionBead.ID, "in_progress")
	rigAWork := closeReleaseWork(t, rigA, "rig-a work", sessionBead.ID, "in_progress")
	rigBWork := closeReleaseWork(t, rigB, "rig-b work", "worker-3", "open")

	cfg := closeReleaseCity([]string{"a", "b"}, "pool/worker")
	rigs := map[string]beads.Store{"a": rigA, "b": rigB}
	var stderr bytes.Buffer
	if !closeBead(store, testSeatWork("", cfg, store, rigs), decidedSessionInfo(store, sessionBead.ID), "orphaned", time.Now().UTC(), &stderr) {
		t.Fatalf("closeBead() = false, want true; stderr=%q", stderr.String())
	}
	for _, tc := range []struct {
		name  string
		store beads.Store
		id    string
	}{{"city", store, cityWork.ID}, {"rig-a", rigA, rigAWork.ID}, {"rig-b", rigB, rigBWork.ID}} {
		got, err := tc.store.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s work: %v", tc.name, err)
		}
		if got.Assignee != "" || got.Status != "open" {
			t.Errorf("%s work assignee=%q status=%q, want empty/open; stderr=%q", tc.name, got.Assignee, got.Status, stderr.String())
		}
	}
}

func TestCloseBeadReleasesSameBeadIDInTwoStores(t *testing.T) {
	store := beads.NewMemStore()
	rigStore := beads.NewMemStore()

	sessionBead := closeReleaseSessionBead(t, store, "worker-1", "pool/worker")
	if _, err := rigStore.Create(beads.Bead{Title: "id-aligning filler"}); err != nil {
		t.Fatalf("create filler bead: %v", err)
	}
	cityWork := closeReleaseWork(t, store, "city work", sessionBead.ID, "in_progress")
	rigWork := closeReleaseWork(t, rigStore, "rig work", sessionBead.ID, "in_progress")
	if cityWork.ID != rigWork.ID {
		t.Fatalf("fixture did not produce an id collision: city=%q rig=%q", cityWork.ID, rigWork.ID)
	}

	cfg := closeReleaseCity([]string{"gascity"}, "pool/worker")
	rigs := map[string]beads.Store{"gascity": rigStore}
	var stderr bytes.Buffer
	if !closeBead(store, testSeatWork("", cfg, store, rigs), decidedSessionInfo(store, sessionBead.ID), "orphaned", time.Now().UTC(), &stderr) {
		t.Fatalf("closeBead() = false, want true; stderr=%q", stderr.String())
	}
	for _, tc := range []struct {
		name  string
		store beads.Store
	}{{"city", store}, {"rig", rigStore}} {
		got, err := tc.store.Get(rigWork.ID)
		if err != nil {
			t.Fatalf("get %s work: %v", tc.name, err)
		}
		if got.Assignee != "" || got.Status != "open" {
			t.Errorf("%s work assignee=%q status=%q, want empty/open; stderr=%q", tc.name, got.Assignee, got.Status, stderr.String())
		}
	}
}
