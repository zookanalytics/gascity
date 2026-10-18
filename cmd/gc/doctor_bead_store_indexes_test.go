package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads/queryindex"
	"github.com/gastownhall/gascity/internal/doctor"
)

func TestBeadStoreIndexesResult(t *testing.T) {
	status := queryindex.ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	root := queryindex.MetadataIndex("issues", "gc.root_bead_id")
	anchor := queryindex.MetadataIndex("issues", "anchor_bead")
	failing := queryindex.SelfTestResult{Failure: "an upsert that updates a row shifted columns"}
	cases := []struct {
		name       string
		status     queryindex.Status
		err        error
		wantStatus doctor.CheckStatus
		wantIn     []string
		wantHint   string
	}{
		{
			name:       "unreadable store",
			err:        errors.New("reading indexes: connection refused"),
			wantStatus: doctor.StatusWarning,
			wantIn:     []string{"query indexes unknown for rig/tk", "connection refused"},
		},
		{
			name:       "metadata index on a failing server",
			status:     queryindex.Status{Exposed: []queryindex.Index{root}, Version: "2.4.2", Verdict: failing},
			wantStatus: doctor.StatusError,
			wantIn:     []string{"rig/tk carries 1 query index (issues(metadata gc.root_bead_id)) on Dolt 2.4.2", "shifted columns"},
			wantHint:   "DROP INDEX `" + root.Name + "` ON `issues`;",
		},
		{
			name:       "missing column index with metadata held",
			status:     queryindex.Status{Missing: []queryindex.Index{status}, Held: []queryindex.Index{root, anchor}, Version: "2.4.2", Verdict: failing},
			wantStatus: doctor.StatusWarning,
			wantIn:     []string{"rig/tk lacks 1 query index (wisps(status, issue_type))", "held back on Dolt 2.4.2", "2 query indexes (issues(metadata gc.root_bead_id), issues(metadata anchor_bead))"},
			wantHint:   "bead-store-index",
		},
		{
			name:       "everything present",
			status:     queryindex.Status{Verdict: queryindex.SelfTestResult{OK: true}, Version: "2.9.0"},
			wantStatus: doctor.StatusOK,
			wantIn:     []string{"rig/tk carries its query indexes"},
		},
		{
			name:       "metadata indexes held, nothing else missing",
			status:     queryindex.Status{Held: []queryindex.Index{root}, Version: "2.4.2", Verdict: failing},
			wantStatus: doctor.StatusOK,
			wantIn:     []string{"rig/tk carries its query indexes; held back on Dolt 2.4.2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := beadStoreIndexesResult("bead-store-indexes:rig/tk", "rig/tk", tc.status, tc.err)
			if res.Status != tc.wantStatus {
				t.Errorf("Status = %v, want %v (%s)", res.Status, tc.wantStatus, res.Message)
			}
			if res.Severity != doctor.SeverityAdvisory {
				t.Errorf("Severity = %v, want advisory", res.Severity)
			}
			for _, want := range tc.wantIn {
				if !strings.Contains(res.Message, want) {
					t.Errorf("Message %q lacks %q", res.Message, want)
				}
			}
			if !strings.Contains(res.FixHint, tc.wantHint) {
				t.Errorf("FixHint %q lacks %q", res.FixHint, tc.wantHint)
			}
		})
	}
}

func TestBeadStoreIndexesCheckSkipsAScopeWithoutADoltTarget(t *testing.T) {
	city := t.TempDir()
	check := newBeadStoreIndexesCheck(city, city, "city", nil, &queryindex.Verdicts{})
	if got := check.Name(); got != "bead-store-indexes:city" {
		t.Fatalf("Name() = %q", got)
	}
	res := check.Run(&doctor.CheckContext{CityPath: city})
	if res.Status != doctor.StatusOK || !strings.Contains(res.Message, "not checked") {
		t.Fatalf("Run on a scope with no Dolt target = %v %q, want an OK not-checked line", res.Status, res.Message)
	}
	if _, ok, err := beadStoreIndexStore(city, city, "city"); ok || err != nil {
		t.Fatalf("beadStoreIndexStore = ok %v, err %v; want no store and no error", ok, err)
	}
}
