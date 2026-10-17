package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/api"
	"github.com/gastownhall/gascity/internal/beads"
)

// waitPartialListStore returns its seeded rows alongside a beads.PartialResultError
// from List, modeling the degraded read the CLI fallback must tolerate.
type waitPartialListStore struct {
	beads.Store
	rows []beads.Bead
}

func (s waitPartialListStore) List(_ beads.ListQuery) ([]beads.Bead, error) {
	return s.rows, &beads.PartialResultError{Op: "bd list", Err: errors.New("skipped 1 corrupt wait")}
}

// TestRouteWaitList_APIIncompleteReadNotices drives both API rungs of `gc wait
// list` against reads the server reports as incomplete and against complete
// ones. An incomplete read still renders its rows and prints a notice on stderr
// instead of failing: "showing partial results" for a degraded store read (the
// generic /beads partial UX), "showing capped results" for a read that hit the
// wait lookup limit. A complete read prints neither.
func TestRouteWaitList_APIIncompleteReadNotices(t *testing.T) {
	const (
		typedRoute    = "cmd=wait list route=api\n"
		legacyRoute   = "cmd=wait list route=api-legacy reason=route-missing\n"
		partialNotice = "gc wait list: bd list: skipped 1 corrupt wait; showing partial results\n"
	)
	cappedNotice := fmt.Sprintf("gc wait list: wait lookup hit limit %d; showing capped results\n", waitLookupLimit)
	tests := []struct {
		name        string
		handler     waitMatrixHandler
		wantRoute   string
		wantStdout  string
		wantNotices []string
	}{
		{name: "typed-partial", handler: typedWaitListBodyHandler(false, true), wantRoute: typedRoute, wantStdout: "w-typed", wantNotices: []string{partialNotice}},
		{name: "typed-capped", handler: typedWaitListBodyHandler(true, false), wantRoute: typedRoute, wantStdout: "w-typed", wantNotices: []string{cappedNotice}},
		{name: "typed-complete", handler: typedWaitListBodyHandler(false, false), wantRoute: typedRoute, wantStdout: "w-typed"},
		{name: "legacy-capped", handler: legacyWaitBeadPagesHandler(waitLookupLimit + 1), wantRoute: legacyRoute, wantStdout: "ga-wait-0000", wantNotices: []string{cappedNotice}},
		{name: "legacy-at-limit", handler: legacyWaitBeadPagesHandler(waitLookupLimit), wantRoute: legacyRoute, wantStdout: "ga-wait-0000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GC_DEBUG", "1")
			srv := httptest.NewServer(tc.handler(t))
			defer srv.Close()
			c := api.NewCityScopedClient(srv.URL, "test-city")

			var stdout, stderr bytes.Buffer
			if code := routeWaitList(t.TempDir(), c, "", "", "", false, &stdout, &stderr); code != 0 {
				t.Fatalf("routeWaitList exit = %d, want 0; stderr=%q", code, stderr.String())
			}
			if !strings.Contains(stdout.String(), tc.wantStdout) {
				t.Errorf("stdout missing wait row %q:\n%s", tc.wantStdout, stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.wantRoute) {
				t.Errorf("stderr missing route line %q:\n%s", tc.wantRoute, stderr.String())
			}
			for _, notice := range tc.wantNotices {
				if !strings.Contains(stderr.String(), notice) {
					t.Errorf("stderr missing notice %q:\n%s", notice, stderr.String())
				}
			}
			if got := strings.Count(stderr.String(), "; showing "); got != len(tc.wantNotices) {
				t.Errorf("stderr carries %d read notices, want %d:\n%s", got, len(tc.wantNotices), stderr.String())
			}
		})
	}
}

// typedWaitListBodyHandler serves the typed /waits route with one wait and the
// given capped and partial flags.
func typedWaitListBodyHandler(capped, partial bool) waitMatrixHandler {
	return func(_ *testing.T) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasSuffix(r.URL.Path, "/waits") {
				http.NotFound(w, r)
				return
			}
			body := map[string]any{
				"waits": []map[string]any{
					{"id": "w-typed", "session_id": "s-1", "kind": "deps", "state": "ready", "status": "open"},
				},
				"capped": capped,
			}
			if partial {
				body["partial"] = true
				body["partial_errors"] = []string{"bd list: skipped 1 corrupt wait"}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(body)
		})
	}
}

// legacyWaitBeadPagesHandler emulates a server that predates /waits: the typed
// route is a plain 404, and the generic /beads endpoint pages through n wait
// beads, serving at most limit per request and continuing with next_cursor.
func legacyWaitBeadPagesHandler(n int) waitMatrixHandler {
	return func(t *testing.T) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasSuffix(r.URL.Path, "/beads") {
				http.NotFound(w, r)
				return
			}
			start := 0
			if cursor := r.URL.Query().Get("cursor"); cursor != "" {
				var err error
				if start, err = strconv.Atoi(cursor); err != nil {
					t.Errorf("cursor %q is not one this server issued: %v", cursor, err)
				}
			}
			limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
			if err != nil || limit <= 0 {
				t.Errorf("limit = %q, want a positive page size", r.URL.Query().Get("limit"))
				limit = n
			}
			end := min(start+limit, n)
			items := make([]map[string]any, 0, end-start)
			for i := start; i < end; i++ {
				item := legacyWaitBeadItem()
				item["id"] = fmt.Sprintf("ga-wait-%04d", i)
				items = append(items, item)
			}
			body := map[string]any{"items": items, "total": n}
			if end < n {
				body["next_cursor"] = strconv.Itoa(end)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(body)
		})
	}
}

// TestWaitListFallbackDataFoldsPartialRows exercises the exact store call the
// local fallback (doWaitListFallback) makes: on a PartialResultError the front
// door returns the surviving rows so the fallback can display them instead of
// exiting 1.
func TestWaitListFallbackDataFoldsPartialRows(t *testing.T) {
	wait := beads.Bead{
		ID:       "w-partial",
		Type:     waitBeadType,
		Status:   "open",
		Labels:   []string{waitBeadLabel, "session:s-1"},
		Metadata: map[string]string{"session_id": "s-1", "state": waitStateReady, "kind": "deps"},
	}
	store := waitPartialListStore{Store: beads.NewMemStore(), rows: []beads.Bead{wait}}

	got, err := sessionFrontDoor(store).ListWaits("", "")
	if !beads.IsPartialResult(err) {
		t.Fatalf("err = %v, want PartialResultError folded through", err)
	}
	if len(got) != 1 || got[0].ID != "w-partial" {
		t.Fatalf("waits = %+v, want the surviving w-partial row preserved", got)
	}
}
