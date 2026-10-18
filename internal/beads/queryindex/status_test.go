package queryindex

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCheckStoreSortsMissingHeldAndExposed(t *testing.T) {
	status := ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	root := MetadataIndex("issues", "gc.root_bead_id")
	anchor := MetadataIndex("issues", "anchor_bead")
	want := []Index{status, root, anchor}

	passing := &fakeStore{label: "city", present: map[string]bool{root.Name: true}}
	got, err := CheckStore(context.Background(), passing.store(), want, &Verdicts{})
	if err != nil {
		t.Fatalf("CheckStore: %v", err)
	}
	if !reflect.DeepEqual(got.Missing, []Index{status, anchor}) || got.Held != nil || got.Exposed != nil || !got.Verdict.OK {
		t.Fatalf("on a passing server: %+v", got)
	}

	failing := &fakeStore{label: "rig/tk", present: map[string]bool{root.Name: true}, version: "2.4.2", unsafe: "an upsert that updates a row shifted columns"}
	got, err = CheckStore(context.Background(), failing.store(), want, &Verdicts{})
	if err != nil {
		t.Fatalf("CheckStore: %v", err)
	}
	if !reflect.DeepEqual(got.Missing, []Index{status}) {
		t.Errorf("Missing = %v, want only the column index", got.Missing)
	}
	if !reflect.DeepEqual(got.Held, []Index{anchor}) {
		t.Errorf("Held = %v, want the absent metadata index", got.Held)
	}
	if !reflect.DeepEqual(got.Exposed, []Index{root}) {
		t.Errorf("Exposed = %v, want the metadata index present on a failing server", got.Exposed)
	}
	if got.Version != "2.4.2" || got.Verdict.Failure != "an upsert that updates a row shifted columns" {
		t.Errorf("Version, Verdict = %q, %+v", got.Version, got.Verdict)
	}
}

func TestCheckStoreSkipsTheSelfTestWithoutMetadataIndexes(t *testing.T) {
	status := ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	store := &fakeStore{label: "city", present: map[string]bool{}}
	got, err := CheckStore(context.Background(), store.store(), []Index{status}, &Verdicts{})
	if err != nil {
		t.Fatalf("CheckStore: %v", err)
	}
	if store.selfTests != 0 || got.Version != "" {
		t.Fatalf("ran the self-test (%d) or read a version (%q) with no metadata index wanted", store.selfTests, got.Version)
	}
}

func TestVerdictsShareOneRunPerVersion(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	a := &fakeStore{label: "city", present: map[string]bool{}, version: "2.4.2"}
	b := &fakeStore{label: "rig/gascity", present: map[string]bool{}, version: "2.4.2"}
	var verdicts Verdicts
	for _, s := range []*fakeStore{a, b} {
		if _, err := CheckStore(context.Background(), s.store(), []Index{root}, &verdicts); err != nil {
			t.Fatalf("CheckStore: %v", err)
		}
	}
	if a.selfTests+b.selfTests != 1 {
		t.Fatalf("ran %d self-tests for one Dolt version, want 1", a.selfTests+b.selfTests)
	}
	broken := &fakeStore{label: "rig/down", present: map[string]bool{}, version: "2.5.0", selfTestErr: errors.New("access denied")}
	if _, err := CheckStore(context.Background(), broken.store(), []Index{root}, &verdicts); err == nil {
		t.Fatal("a self-test that could not run returned no error")
	}
	broken.selfTestErr = nil
	if _, err := CheckStore(context.Background(), broken.store(), []Index{root}, &verdicts); err != nil {
		t.Fatalf("a failed self-test run was cached: %v", err)
	}
}
