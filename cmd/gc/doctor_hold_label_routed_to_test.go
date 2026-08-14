package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
)

// TestHoldLabelRoutedToCheck covers ga-fm2vgd.1: a bead carrying a
// hold:<value> label but whose gc.routed_to metadata is missing or does not
// match <value> has silently drifted from its intended route. The check must
// flag such beads across both city and rig stores, --fix must backfill
// gc.routed_to from the label value, hold:external must never be flagged,
// and beads with no hold:* label must be left untouched.
func TestHoldLabelRoutedToCheck(t *testing.T) {
	cityDir := t.TempDir()
	rigDir := t.TempDir()
	cfg := &config.City{Rigs: []config.Rig{{Name: "repo", Path: rigDir}}}

	cityStore := beads.NewMemStoreFrom(0, []beads.Bead{
		// Mismatch: hold:mayor label, no gc.routed_to at all.
		{ID: "H-1", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"}},
		// Mismatch: hold:mayor label, gc.routed_to set to something else.
		{
			ID: "H-2", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"},
			Metadata: map[string]string{"gc.routed_to": "reviewer"},
		},
		// Healthy: hold:mayor label, gc.routed_to already matches — must be left alone.
		{
			ID: "H-3", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"},
			Metadata: map[string]string{"gc.routed_to": "mayor"},
		},
		// Excluded by definition: hold:external names a human/out-of-system
		// dependency, never a routing gap, regardless of gc.routed_to state.
		{ID: "H-4", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:external"}},
		// No hold:* label at all — must be ignored even with empty gc.routed_to.
		{ID: "T-1", Title: "work", Type: "task", Status: "open"},
		// Generic over label value — must not require any special-casing of
		// "mayor" specifically; an arbitrary hold value must be caught too.
		{ID: "H-5", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:qa-lead"}},
		// Mismatch on a non-open bead: the scan must not filter to
		// Status=="open" only, or in_progress/blocked/deferred hold-labeled
		// beads silently escape it (ga-fm2vgd.2).
		{ID: "H-6", Title: "held", Type: "task", Status: "in_progress", Labels: []string{"hold:mayor"}},
	}, nil)
	rigStore := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "RH-1", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"}},
	}, nil)
	stores := map[string]beads.Store{cityDir: cityStore, rigDir: rigStore}
	factory := func(path string) (beads.Store, error) {
		store, ok := stores[path]
		if !ok {
			return nil, fmt.Errorf("unexpected store path %q", path)
		}
		return store, nil
	}

	check := newHoldLabelRoutedToCheck(cfg, cityDir, factory)

	res := check.Run(&doctor.CheckContext{})
	if res.Status != doctor.StatusWarning {
		t.Fatalf("Run status = %v, want warning: %#v", res.Status, res)
	}
	details := strings.Join(res.Details, "\n")
	for _, want := range []string{"H-1", "H-2", "H-5", "H-6", "RH-1"} {
		if !strings.Contains(details, want) {
			t.Fatalf("details missing %q:\n%s", want, details)
		}
	}
	for _, notWant := range []string{"H-3", "H-4", "T-1"} {
		if strings.Contains(details, notWant) {
			t.Fatalf("details should not mention %q:\n%s", notWant, details)
		}
	}

	if err := check.Fix(&doctor.CheckContext{}); err != nil {
		t.Fatalf("Fix: %v", err)
	}

	// Idempotency: a second Run after Fix must report clean.
	if res2 := check.Run(&doctor.CheckContext{}); res2.Status != doctor.StatusOK {
		t.Fatalf("post-fix Run status = %v, want OK: %#v", res2.Status, res2)
	}

	h1, err := cityStore.Get("H-1")
	if err != nil {
		t.Fatalf("get H-1: %v", err)
	}
	if got := h1.Metadata["gc.routed_to"]; got != "mayor" {
		t.Errorf("H-1 gc.routed_to = %q, want mayor (backfilled from hold:mayor label)", got)
	}
	h2, err := cityStore.Get("H-2")
	if err != nil {
		t.Fatalf("get H-2: %v", err)
	}
	if got := h2.Metadata["gc.routed_to"]; got != "mayor" {
		t.Errorf("H-2 gc.routed_to = %q, want mayor (corrected from stale reviewer)", got)
	}
	h5, err := cityStore.Get("H-5")
	if err != nil {
		t.Fatalf("get H-5: %v", err)
	}
	if got := h5.Metadata["gc.routed_to"]; got != "qa-lead" {
		t.Errorf("H-5 gc.routed_to = %q, want qa-lead (arbitrary hold value, not special-cased)", got)
	}
	h6, err := cityStore.Get("H-6")
	if err != nil {
		t.Fatalf("get H-6: %v", err)
	}
	if got := h6.Metadata["gc.routed_to"]; got != "mayor" {
		t.Errorf("H-6 gc.routed_to = %q, want mayor (backfilled despite in_progress status)", got)
	}
	h4, err := cityStore.Get("H-4")
	if err != nil {
		t.Fatalf("get H-4: %v", err)
	}
	if got := h4.Metadata["gc.routed_to"]; got != "" {
		t.Errorf("H-4 (hold:external) gc.routed_to = %q, want untouched empty", got)
	}
	t1, err := cityStore.Get("T-1")
	if err != nil {
		t.Fatalf("get T-1: %v", err)
	}
	if got := t1.Metadata["gc.routed_to"]; got != "" {
		t.Errorf("T-1 (no hold label) gc.routed_to = %q, want untouched empty", got)
	}
	rh1, err := rigStore.Get("RH-1")
	if err != nil {
		t.Fatalf("get RH-1: %v", err)
	}
	if got := rh1.Metadata["gc.routed_to"]; got != "mayor" {
		t.Errorf("RH-1 gc.routed_to = %q, want mayor", got)
	}
}

// TestHoldLabelRoutedToCheckCleanStore confirms a store with no mismatches
// reports OK and CanFix advertises remediation.
func TestHoldLabelRoutedToCheckCleanStore(t *testing.T) {
	cityDir := t.TempDir()
	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{
			ID: "H-9", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"},
			Metadata: map[string]string{"gc.routed_to": "mayor"},
		},
		{ID: "H-10", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:external"}},
	}, nil)
	check := newHoldLabelRoutedToCheck(nil, cityDir, func(path string) (beads.Store, error) {
		if path != cityDir {
			return nil, fmt.Errorf("unexpected store path %q", path)
		}
		return store, nil
	})
	if !check.CanFix() {
		t.Fatal("CanFix() = false, want true")
	}
	if res := check.Run(&doctor.CheckContext{}); res.Status != doctor.StatusOK {
		t.Fatalf("Run status = %v, want OK: %#v", res.Status, res)
	}
}

func TestHoldLabelRoutedToFixReportsOpenFailures(t *testing.T) {
	cityDir := t.TempDir()
	rigDir := t.TempDir()
	cfg := &config.City{Rigs: []config.Rig{{Name: "repo", Path: rigDir}}}
	cityStore := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "H-1", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"}},
	}, nil)
	check := newHoldLabelRoutedToCheck(cfg, cityDir, func(path string) (beads.Store, error) {
		if path == rigDir {
			return nil, errors.New("permission denied")
		}
		return cityStore, nil
	})

	err := check.Fix(&doctor.CheckContext{})
	if err == nil {
		t.Fatal("Fix error = nil, want skipped scope error")
	}
	if got := err.Error(); !strings.Contains(got, "rig repo skipped") || !strings.Contains(got, "permission denied") {
		t.Fatalf("Fix error = %q, want rig open failure detail", got)
	}
	h1, getErr := cityStore.Get("H-1")
	if getErr != nil {
		t.Fatalf("get H-1: %v", getErr)
	}
	if got := h1.Metadata["gc.routed_to"]; got != "mayor" {
		t.Fatalf("H-1 gc.routed_to = %q, want available repair applied despite skipped rig", got)
	}
}

func TestHoldLabelRoutedToFixReportsListFailures(t *testing.T) {
	cityDir := t.TempDir()
	check := newHoldLabelRoutedToCheck(nil, cityDir, func(path string) (beads.Store, error) {
		if path != cityDir {
			return nil, fmt.Errorf("unexpected store path %q", path)
		}
		return holdLabelListErrorStore{Store: beads.NewMemStore()}, nil
	})

	err := check.Fix(&doctor.CheckContext{})
	if err == nil {
		t.Fatal("Fix error = nil, want skipped scope error")
	}
	if got := err.Error(); !strings.Contains(got, "city skipped") || !strings.Contains(got, "listing failed") {
		t.Fatalf("Fix error = %q, want list failure detail", got)
	}
}

type holdLabelListErrorStore struct {
	beads.Store
}

func (s holdLabelListErrorStore) List(beads.ListQuery) ([]beads.Bead, error) {
	return nil, errors.New("listing failed")
}

// TestHoldLabelRoutedToAcceptsBoundAliasRoute covers gc-yzxra: in a city that
// binds its route targets, the canonical gc.routed_to value is
// binding-qualified ("gastown.mayor") while the canonical hold label value
// stays short ("hold:mayor" — hold-label-conventions.md sanctions only "mayor"
// and "external"). Comparing the two by exact string equality reports that
// canonical pairing as drift, so a gc.routed_to holding a bound alias of the
// hold value must count as a match, and --fix must backfill the qualified form
// so the repair does not immediately re-trip v2-routed-to-namespace.
func TestHoldLabelRoutedToAcceptsBoundAliasRoute(t *testing.T) {
	cityDir := t.TempDir()
	cfg := &config.City{Agents: []config.Agent{{Name: "mayor", BindingName: "gastown"}}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{
		// Canonical pairing: short hold label, binding-qualified route.
		{
			ID: "B-1", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"},
			Metadata: map[string]string{"gc.routed_to": "gastown.mayor"},
		},
		// Missing route: --fix must backfill the qualified form, not "mayor".
		{ID: "B-2", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"}},
		// Short-form route matches the label exactly, so this check has no
		// quarrel with it; rewriting it to the qualified form belongs to
		// v2-routed-to-namespace, which owns that finding.
		{
			ID: "B-3", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"},
			Metadata: map[string]string{"gc.routed_to": "mayor"},
		},
		// Unbound hold value: no alias exists, so the short-form backfill stands.
		{ID: "B-4", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:qa-lead"}},
		// A different bound agent's route is still drift, alias or not.
		{
			ID: "B-5", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"},
			Metadata: map[string]string{"gc.routed_to": "gastown.reviewer"},
		},
	}, nil)
	check := newHoldLabelRoutedToCheck(cfg, cityDir, func(path string) (beads.Store, error) {
		if path != cityDir {
			return nil, fmt.Errorf("unexpected store path %q", path)
		}
		return store, nil
	})

	res := check.Run(&doctor.CheckContext{})
	if res.Status != doctor.StatusWarning {
		t.Fatalf("Run status = %v, want warning: %#v", res.Status, res)
	}
	details := strings.Join(res.Details, "\n")
	for _, want := range []string{"B-2", "B-4", "B-5"} {
		if !strings.Contains(details, want) {
			t.Fatalf("details missing %q:\n%s", want, details)
		}
	}
	for _, notWant := range []string{"B-1", "B-3"} {
		if strings.Contains(details, notWant) {
			t.Fatalf("details should not mention %q (routed_to already denotes the hold target):\n%s", notWant, details)
		}
	}

	if err := check.Fix(&doctor.CheckContext{}); err != nil {
		t.Fatalf("Fix: %v", err)
	}
	if res2 := check.Run(&doctor.CheckContext{}); res2.Status != doctor.StatusOK {
		t.Fatalf("post-fix Run status = %v, want OK: %#v", res2.Status, res2)
	}

	for _, tc := range []struct{ id, want string }{
		{"B-1", "gastown.mayor"},
		{"B-2", "gastown.mayor"},
		{"B-3", "mayor"},
		{"B-4", "qa-lead"},
		{"B-5", "gastown.mayor"},
	} {
		bead, err := store.Get(tc.id)
		if err != nil {
			t.Fatalf("get %s: %v", tc.id, err)
		}
		if got := bead.Metadata["gc.routed_to"]; got != tc.want {
			t.Errorf("%s gc.routed_to = %q, want %q", tc.id, got, tc.want)
		}
	}
}

// TestHoldLabelAndV2RoutedToChecksConverge is the regression test for the
// oscillation gc-yzxra and gc-sci79 reported from opposite directions against
// the same bead: v2-routed-to-namespace --fix rewrote gc.routed_to to the
// binding-qualified form, hold-label-routed-to --fix rewrote it straight back
// to the short form, and each pass left the other check red. Both must report
// OK in the same pass once either has run.
func TestHoldLabelAndV2RoutedToChecksConverge(t *testing.T) {
	cityDir := t.TempDir()
	cfg := &config.City{Agents: []config.Agent{{Name: "mayor", BindingName: "gastown"}}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{
			ID: "C-1", Title: "held", Type: "task", Status: "open", Labels: []string{"hold:mayor"},
			Metadata: map[string]string{"gc.routed_to": "mayor"},
		},
	}, nil)
	factory := func(path string) (beads.Store, error) {
		if path != cityDir {
			return nil, fmt.Errorf("unexpected store path %q", path)
		}
		return store, nil
	}
	namespaceCheck := newV2RoutedToNamespaceCheck(cfg, cityDir, factory)
	holdCheck := newHoldLabelRoutedToCheck(cfg, cityDir, factory)

	if res := namespaceCheck.Run(&doctor.CheckContext{}); res.Status != doctor.StatusWarning {
		t.Fatalf("v2-routed-to-namespace status = %v, want warning: %#v", res.Status, res)
	}
	if err := namespaceCheck.Fix(&doctor.CheckContext{}); err != nil {
		t.Fatalf("v2-routed-to-namespace Fix: %v", err)
	}

	bead, err := store.Get("C-1")
	if err != nil {
		t.Fatalf("get C-1: %v", err)
	}
	if got := bead.Metadata["gc.routed_to"]; got != "gastown.mayor" {
		t.Fatalf("C-1 gc.routed_to = %q, want gastown.mayor after namespace fix", got)
	}

	// The qualified form must not read as hold-label drift, and running the
	// hold check's --fix must not undo it.
	if res := holdCheck.Run(&doctor.CheckContext{}); res.Status != doctor.StatusOK {
		t.Fatalf("hold-label-routed-to status = %v, want OK: %#v", res.Status, res)
	}
	if err := holdCheck.Fix(&doctor.CheckContext{}); err != nil {
		t.Fatalf("hold-label-routed-to Fix: %v", err)
	}
	bead, err = store.Get("C-1")
	if err != nil {
		t.Fatalf("get C-1: %v", err)
	}
	if got := bead.Metadata["gc.routed_to"]; got != "gastown.mayor" {
		t.Fatalf("C-1 gc.routed_to = %q, want gastown.mayor preserved by hold-label fix", got)
	}
	if res := namespaceCheck.Run(&doctor.CheckContext{}); res.Status != doctor.StatusOK {
		t.Fatalf("v2-routed-to-namespace status = %v, want OK after hold-label fix: %#v", res.Status, res)
	}
}
