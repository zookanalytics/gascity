package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gastownhall/gascity/internal/api/genclient"
	"github.com/gastownhall/gascity/internal/bazeltest"
	"github.com/gastownhall/gascity/internal/beads"
)

// handlerClient returns a city-scoped Client whose requests h serves in
// process through an http.RoundTripper, with no listener.
func handlerClient(t *testing.T, h http.Handler, cityName string) *Client {
	t.Helper()
	serve := rtFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Result(), nil
	})
	cw, err := genclient.NewClientWithResponses("http://example.com", genclient.WithHTTPClient(&http.Client{Transport: serve}))
	if err != nil {
		t.Fatalf("build in-process client: %v", err)
	}
	return &Client{cw: cw, baseURL: "http://example.com", cityName: cityName}
}

// writeListJSON answers a list request with body as JSON.
func writeListJSON(w http.ResponseWriter, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(body) //nolint:errcheck
}

// assertListPageError checks that err is a listPageError whose message holds
// every part, and that read paths fall back on it the way they do on a 5xx.
func assertListPageError(t *testing.T, err error, parts ...string) {
	t.Helper()
	var lpe *listPageError
	if !errors.As(err, &lpe) {
		t.Fatalf("err = %v (%T), want a *listPageError", err, err)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("err = %q, want it to contain %q", err, part)
		}
	}
	if !ShouldFallbackForRead(nil, err) {
		t.Errorf("ShouldFallbackForRead = false for %v, want a read fallback", err)
	}
	if got := FallbackReason(nil, err); got != "conn-refused" {
		t.Errorf("FallbackReason = %q, want conn-refused (an unhealthy server)", got)
	}
	if ShouldFallback(nil, err) {
		t.Errorf("ShouldFallback = true for %v; an unusable list page must not license a mutation fallback", err)
	}
}

// stringRow is a walk row whose identity is the string itself.
func stringRow(s string) string { return s }

// TestWalkKeysetPagesRejectsCursorCycle proves the walk remembers every cursor
// it requested, not only the last one: a server alternating between two
// cursors would otherwise be walked forever. The error names the request and
// the page.
func TestWalkKeysetPagesRejectsCursorCycle(t *testing.T) {
	next := map[string]string{"": "a", "a": "b", "b": "a"}
	var requested []string
	_, err := walkKeysetPages("GET /v0/city/alpha/things", 0, maxKeysetWalkPages, func(cursor string, _ int64) (keysetPage[string], error) {
		requested = append(requested, cursor)
		if len(requested) > 10 {
			return keysetPage[string]{ok: true}, nil // end a walk that lost its guard instead of hanging
		}
		return keysetPage[string]{ok: true, items: []string{"row-" + cursor}, next: next[cursor]}, nil
	}, stringRow)
	assertListPageError(t, err, "GET /v0/city/alpha/things: page 3:", `next_cursor "a" repeated`)
	if want := []string{"", "a", "b"}; !slices.Equal(requested, want) {
		t.Errorf("requested cursors = %q, want %q", requested, want)
	}
}

// TestWalkKeysetPagesStopsAtThePageCap proves the page cap bounds a server
// that mints a new cursor on every page. No cursor ever repeats, so the
// repeated-cursor guard cannot end that walk.
func TestWalkKeysetPagesStopsAtThePageCap(t *testing.T) {
	const maxPages = 5
	fetches := 0
	_, err := walkKeysetPages("GET /v0/city/alpha/things", 0, maxPages, func(string, int64) (keysetPage[string], error) {
		fetches++
		if fetches > maxPages {
			return keysetPage[string]{ok: true}, nil // end a walk that lost its cap instead of spinning
		}
		return keysetPage[string]{ok: true, next: strconv.Itoa(fetches)}, nil
	}, stringRow)
	assertListPageError(t, err, "page 5:", fmt.Sprintf("exceeded %d pages", maxPages))
	if fetches != maxPages {
		t.Errorf("fetches = %d, want %d (the cap bounds the walk)", fetches, maxPages)
	}
}

// TestWalkKeysetPagesKeepsTheFirstCopyOfARow proves a row two pages both
// return is listed once. The server rebuilds the list for every page, so a row
// whose sort key moved between requests lands on both sides of a boundary.
func TestWalkKeysetPagesKeepsTheFirstCopyOfARow(t *testing.T) {
	type row struct{ id, page string }
	pages := map[string]keysetPage[row]{
		"":   {items: []row{{"a", "1"}, {"b", "1"}}, next: "p2"},
		"p2": {items: []row{{"b", "2"}, {"c", "2"}}},
	}
	got, err := walkKeysetPages("GET /v0/city/alpha/things", 0, maxKeysetWalkPages, func(cursor string, pageLimit int64) (keysetPage[row], error) {
		if pageLimit != maxPaginationLimit {
			t.Errorf("pageLimit = %d, want the %d server cap on every page of an unlimited walk", pageLimit, maxPaginationLimit)
		}
		p := pages[cursor]
		p.ok = true
		p.http = &http.Response{Header: http.Header{}}
		p.http.Header.Set(cacheAgeHeader, map[string]string{"": "1.25", "p2": "9.9"}[cursor])
		return p, nil
	}, func(r row) string { return r.id })
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if want := []row{{"a", "1"}, {"b", "1"}, {"c", "2"}}; !slices.Equal(got.items, want) {
		t.Errorf("rows = %v, want %v (each id once, the first copy kept)", got.items, want)
	}
	if got.ageSeconds != 1.25 {
		t.Errorf("ageSeconds = %v, want 1.25 (the first page's cache age)", got.ageSeconds)
	}
}

// TestWalkKeysetPagesFailsOnAPageWithNoListBody proves a 2xx page that decoded
// no list body fails the whole read. Ending the walk there would hand back the
// pages already read as the complete list.
func TestWalkKeysetPagesFailsOnAPageWithNoListBody(t *testing.T) {
	got, err := walkKeysetPages("GET /v0/city/alpha/things", 0, maxKeysetWalkPages, func(cursor string, _ int64) (keysetPage[string], error) {
		if cursor == "" {
			return keysetPage[string]{ok: true, items: []string{"a"}, next: "p2"}, nil
		}
		return keysetPage[string]{http: &http.Response{Header: http.Header{"Content-Type": {"text/html"}}}}, nil
	}, stringRow)
	assertListPageError(t, err, "GET /v0/city/alpha/things: page 2:", `decoded no list body (Content-Type "text/html")`)
	if len(got.items) != 0 {
		t.Errorf("rows = %q, want none from a failed read", got.items)
	}
}

// TestWalkKeysetPagesMergesPartialReads proves one partial page makes the whole
// list partial, and that a backend failing on several pages is reported once
// even when its message text differs from page to page.
func TestWalkKeysetPagesMergesPartialReads(t *testing.T) {
	pages := map[string]keysetPage[string]{
		"":   {items: []string{"a"}, next: "p2"},
		"p2": {items: []string{"b"}, next: "p3", partial: true, partialErrors: []string{"rig alpha: read timed out after 8.01s"}},
		"p3": {items: []string{"c"}, partial: true, partialErrors: []string{"rig alpha: read timed out after 8.03s", "rig beta: store closed"}},
	}
	got, err := walkKeysetPages("GET /v0/city/alpha/things", 0, maxKeysetWalkPages, func(cursor string, _ int64) (keysetPage[string], error) {
		p := pages[cursor]
		p.ok = true
		return p, nil
	}, stringRow)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if !got.partial {
		t.Error("partial = false, want true when any page was partial")
	}
	if want := []string{"rig alpha: read timed out after 8.01s", "rig beta: store closed"}; !slices.Equal(got.partialErrors, want) {
		t.Errorf("partialErrors = %q, want %q (one entry per failed backend)", got.partialErrors, want)
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(got.items, want) {
		t.Errorf("rows = %q, want %q", got.items, want)
	}
}

// TestWalkKeysetPagesStopsAtTheRowLimit proves a positive limit bounds the
// rows: each page asks only for the rows still wanted, and the walk stops once
// it has them without requesting the advertised next page.
func TestWalkKeysetPagesStopsAtTheRowLimit(t *testing.T) {
	var asked []int64
	got, err := walkKeysetPages("GET /v0/city/alpha/things", 3, maxKeysetWalkPages, func(cursor string, pageLimit int64) (keysetPage[string], error) {
		asked = append(asked, pageLimit)
		switch cursor {
		case "":
			return keysetPage[string]{ok: true, items: []string{"a", "b"}, next: "p2"}, nil
		case "p2":
			return keysetPage[string]{ok: true, items: []string{"c"}, next: "p3"}, nil
		}
		t.Errorf("requested cursor %q past the limit", cursor)
		return keysetPage[string]{ok: true}, nil
	}, stringRow)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(got.items, want) {
		t.Errorf("rows = %q, want %q", got.items, want)
	}
	if want := []int64{3, 1}; !slices.Equal(asked, want) {
		t.Errorf("page limits asked = %v, want %v (only the rows still wanted)", asked, want)
	}
}

// keysetListCase is one client keyset list read, for tests that run the same
// server behavior against every walked list.
type keysetListCase struct {
	name string
	path string
	item func(id string) map[string]any
	list func(*Client) ([]string, error)
}

// keysetListCases are the client lists that walk keyset pages.
func keysetListCases() []keysetListCase {
	sessionItem := func(id string) map[string]any {
		return map[string]any{"id": id, "template": "worker", "state": "active", "title": "Worker", "session_name": "worker", "provider": "claude", "created_at": "2026-04-23T10:00:00Z", "attached": false, "running": true}
	}
	beadItem := func(kind string) func(string) map[string]any {
		return func(id string) map[string]any {
			return map[string]any{"id": id, "title": "t", "issue_type": kind, "status": "open", "created_at": "2026-04-23T10:00:00Z"}
		}
	}
	beadIDs := func(bs []beads.Bead) []string {
		ids := make([]string, 0, len(bs))
		for _, b := range bs {
			ids = append(ids, b.ID)
		}
		return ids
	}
	return []keysetListCase{
		{
			name: "sessions",
			path: "/v0/city/alpha/sessions",
			item: sessionItem,
			list: func(c *Client) ([]string, error) {
				got, err := c.ListSessions("", "", false)
				ids := make([]string, 0, len(got.Body))
				for _, s := range got.Body {
					ids = append(ids, s.ID)
				}
				return ids, err
			},
		},
		{
			name: "convoys",
			path: "/v0/city/alpha/convoys",
			item: beadItem("convoy"),
			list: func(c *Client) ([]string, error) {
				got, err := c.ListConvoys()
				return beadIDs(got.Body.Items), err
			},
		},
		{
			name: "beads",
			path: "/v0/city/alpha/beads",
			item: beadItem("task"),
			list: func(c *Client) ([]string, error) {
				got, err := c.ListBeads(ListBeadsOpts{})
				return beadIDs(got.Body), err
			},
		},
	}
}

// TestClientKeysetListsFailOnAPageWithNoListBody proves every walked list
// fails, naming its request, when a later page answers 200 with a body no list
// decode accepts, such as a proxy's HTML page. The read falls back instead of
// returning the first page as the whole list.
func TestClientKeysetListsFailOnAPageWithNoListBody(t *testing.T) {
	for _, tc := range keysetListCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.path {
					t.Errorf("path = %q, want %q", r.URL.Path, tc.path)
				}
				if r.URL.Query().Get("cursor") == "" {
					writeListJSON(w, map[string]any{"items": []map[string]any{tc.item("gc-1")}, "total": 2, "next_cursor": "p2"})
					return
				}
				w.Header().Set("Content-Type", "text/html")
				fmt.Fprint(w, "<html>upstream unavailable</html>") //nolint:errcheck
			})
			ids, err := tc.list(handlerClient(t, h, "alpha"))
			assertListPageError(t, err, "GET "+tc.path+": page 2:", "decoded no list body")
			if len(ids) != 0 {
				t.Errorf("rows = %q, want none from a failed read", ids)
			}
		})
	}
}

// TestClientKeysetListsRejectRepeatedCursor proves every walked list fails
// loudly when the server hands back a cursor it already served. Requesting it
// again would return the same rows forever, and stopping there would return
// them twice. The failure is a read fallback, like any unhealthy server.
func TestClientKeysetListsRejectRepeatedCursor(t *testing.T) {
	for _, tc := range keysetListCases() {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				// A server that ignores the cursor serves the first page and the
				// same next_cursor every time. After a few requests the page
				// stops minting one, so a walk without the guard fails here
				// instead of hanging.
				body := map[string]any{"items": []map[string]any{tc.item("gc-1")}, "total": 2}
				if requests.Add(1) <= 5 {
					body["next_cursor"] = "stuck"
				}
				writeListJSON(w, body)
			})
			_, err := tc.list(handlerClient(t, h, "alpha"))
			assertListPageError(t, err, "GET "+tc.path+": page 2:", `next_cursor "stuck" repeated`)
			if got := requests.Load(); got != 2 {
				t.Errorf("requests = %d, want 2 (the first page, then the repeated cursor once)", got)
			}
		})
	}
}

// TestClientKeysetListsKeepTheFirstCopyOfARowTwoPagesReturn proves every walked
// list returns a row once when its sort key moved between two page requests
// and it landed on both pages.
func TestClientKeysetListsKeepTheFirstCopyOfARowTwoPagesReturn(t *testing.T) {
	for _, tc := range keysetListCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("cursor") == "" {
					writeListJSON(w, map[string]any{"items": []map[string]any{tc.item("gc-3"), tc.item("gc-2")}, "total": 3, "next_cursor": "p2"})
					return
				}
				writeListJSON(w, map[string]any{"items": []map[string]any{tc.item("gc-2"), tc.item("gc-1")}, "total": 3})
			})
			ids, err := tc.list(handlerClient(t, h, "alpha"))
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if want := []string{"gc-3", "gc-2", "gc-1"}; !slices.Equal(ids, want) {
				t.Errorf("ids = %q, want %q (gc-2 once)", ids, want)
			}
		})
	}
}

// TestClientListConvoysFollowsNextCursor proves ListConvoys walks every keyset
// page. One request returns only the server's first page, 100 rows by default,
// so the walk is what lists a city with more open convoys than that in full.
func TestClientListConvoysFollowsNextCursor(t *testing.T) {
	var cursors []string
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/city/alpha/convoys" {
			t.Errorf("path = %q, want /v0/city/alpha/convoys", r.URL.Path)
		}
		// Every page asks for the 1000-row server cap so the walk makes as few
		// round trips as the server allows.
		if got, want := r.URL.Query().Get("limit"), "1000"; got != want {
			t.Errorf("limit query = %q, want %q", got, want)
		}
		cursor := r.URL.Query().Get("cursor")
		cursors = append(cursors, cursor)
		switch cursor {
		case "":
			w.Header().Set(cacheAgeHeader, "1.25")
			writeListJSON(w, map[string]any{
				"items":       []map[string]any{{"id": "gc-2", "title": "release", "issue_type": "convoy", "status": "open", "created_at": "2026-04-23T11:00:00Z"}},
				"total":       2,
				"next_cursor": "page2",
			})
		case "page2":
			// A different age on the later page proves the merged read reports
			// the first page's age.
			w.Header().Set(cacheAgeHeader, "9.9")
			writeListJSON(w, map[string]any{
				"items": []map[string]any{{"id": "gc-1", "title": "deploy", "issue_type": "convoy", "status": "open", "created_at": "2026-04-23T10:00:00Z"}},
				"total": 2,
			})
		default:
			t.Errorf("unexpected page request, cursor = %q", cursor)
			writeListJSON(w, map[string]any{"items": []map[string]any{}, "total": 2})
		}
	})

	got, err := handlerClient(t, h, "alpha").ListConvoys()
	if err != nil {
		t.Fatalf("ListConvoys: %v", err)
	}
	items := got.Body.Items
	if len(items) != 2 || items[0].ID != "gc-2" || items[1].ID != "gc-1" {
		t.Fatalf("convoys = %+v, want gc-2 then gc-1 (both pages, in server order)", items)
	}
	if got.AgeSeconds != 1.25 {
		t.Errorf("AgeSeconds = %v, want 1.25 (first page's age)", got.AgeSeconds)
	}
	if want := []string{"", "page2"}; !slices.Equal(cursors, want) {
		t.Errorf("requested cursors = %q, want %q", cursors, want)
	}
}

// TestClientListConvoysReportsAPartialPage proves a rig read that failed on
// any one page marks the whole convoy list partial. Its cursor was cut from a
// set without that rig, so rows can be missing from the middle of the list.
func TestClientListConvoysReportsAPartialPage(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "" {
			writeListJSON(w, map[string]any{
				"items":       []map[string]any{{"id": "gc-2", "title": "release", "issue_type": "convoy", "status": "open", "created_at": "2026-04-23T11:00:00Z"}},
				"total":       2,
				"next_cursor": "page2",
			})
			return
		}
		writeListJSON(w, map[string]any{
			"items":          []map[string]any{{"id": "gc-1", "title": "deploy", "issue_type": "convoy", "status": "open", "created_at": "2026-04-23T10:00:00Z"}},
			"total":          1,
			"partial":        true,
			"partial_errors": []string{"rig beta: store closed"},
		})
	})
	got, err := handlerClient(t, h, "alpha").ListConvoys()
	if err != nil {
		t.Fatalf("ListConvoys: %v", err)
	}
	if !got.Body.Partial {
		t.Error("Partial = false, want true when a later page was partial")
	}
	if want := []string{"rig beta: store closed"}; !slices.Equal(got.Body.PartialErrors, want) {
		t.Errorf("PartialErrors = %q, want %q", got.Body.PartialErrors, want)
	}
}

// TestClientListConvoysLaterPageErrorFailsTheList proves a failure on a later
// page fails the whole read instead of returning the pages already fetched. A
// short list would read as the complete set; the error keeps the caller's
// fallback reachable.
func TestClientListConvoysLaterPageErrorFailsTheList(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "" {
			writeListJSON(w, map[string]any{
				"items":       []map[string]any{{"id": "gc-2", "title": "release", "issue_type": "convoy", "status": "open", "created_at": "2026-04-23T11:00:00Z"}},
				"total":       2,
				"next_cursor": "page2",
			})
			return
		}
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"title":  "Service Unavailable",
			"status": http.StatusServiceUnavailable,
			"detail": "cache_not_live: supervisor cache is priming",
		})
	})
	got, err := handlerClient(t, h, "alpha").ListConvoys()
	if err == nil {
		t.Fatalf("ListConvoys = %d convoys and no error, want the second page's error", len(got.Body.Items))
	}
	if !ShouldFallback(nil, err) {
		t.Errorf("ShouldFallback = false for a cache-not-live later page: %v", err)
	}
}

// countingHandler serves h and counts the requests whose path ends in suffix,
// so a test can prove how many pages a client read.
func countingHandler(h http.Handler, suffix string, n *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, suffix) {
			n.Add(1)
		}
		h.ServeHTTP(w, r)
	})
}

// sortedIDs returns the ids in ascending order, for set comparison.
func sortedIDs(ids []string) []string {
	out := slices.Clone(ids)
	sort.Strings(out)
	return out
}

// TestClientListConvoysReturnsEveryServerPage runs ListConvoys against the real
// convoy list handler holding more open convoys than one 1000-row page, so the
// walk follows a cursor the server minted. The client must return exactly the
// convoys the store holds, each once.
func TestClientListConvoysReturnsEveryServerPage(t *testing.T) {
	state := newFakeMutatorState(t)
	store := state.stores["myrig"]
	const n = maxPaginationLimit + 25
	for i := 0; i < n; i++ {
		if _, err := store.Create(beads.Bead{Title: "c", Type: "convoy"}); err != nil {
			t.Fatalf("create convoy %d: %v", i, err)
		}
	}
	direct, err := store.List(beads.ListQuery{Type: "convoy"})
	if err != nil {
		t.Fatalf("list convoys from the store: %v", err)
	}
	var wantIDs []string
	for _, b := range direct {
		wantIDs = append(wantIDs, b.ID)
	}
	if len(wantIDs) != n {
		t.Fatalf("store holds %d convoys, want %d", len(wantIDs), n)
	}

	var pages atomic.Int32
	c := handlerClient(t, countingHandler(newTestCityHandler(t, state), "/convoys", &pages), state.CityName())
	got, err := c.ListConvoys()
	if err != nil {
		t.Fatalf("ListConvoys: %v", err)
	}
	var gotIDs []string
	for _, b := range got.Body.Items {
		gotIDs = append(gotIDs, b.ID)
	}
	if !slices.Equal(sortedIDs(gotIDs), sortedIDs(wantIDs)) {
		t.Fatalf("ListConvoys returned %d convoys (%d distinct), want the store's %d", len(gotIDs), len(slices.Compact(sortedIDs(gotIDs))), n)
	}
	if got.Body.Partial {
		t.Errorf("Partial = true with errors %q, want a complete read", got.Body.PartialErrors)
	}
	if p := pages.Load(); p != 2 {
		t.Errorf("convoy list requests = %d, want 2 (a full page, then the remainder)", p)
	}
}

// TestClientMailInboxSummaryCountsTheWholeInboxInOneRequest runs
// MailInboxSummary against the real mail list handler holding more unread
// messages than one 1000-row page. One single-row request must count all of
// them.
func TestClientMailInboxSummaryCountsTheWholeInboxInOneRequest(t *testing.T) {
	state := newFakeState(t)
	mp := state.cityMailProv
	const n = maxPaginationLimit + 25
	for i := 0; i < n; i++ {
		if _, err := mp.Send("alice", "worker", "s", "b"); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	var requests atomic.Int32
	c := handlerClient(t, countingHandler(newTestCityHandler(t, state), "/mail", &requests), state.CityName())
	got, err := c.MailInboxSummary("worker", "")
	if err != nil {
		t.Fatalf("MailInboxSummary: %v", err)
	}
	if got.Body.Total != n {
		t.Errorf("Total = %d, want %d", got.Body.Total, n)
	}
	if got.Body.Partial || len(got.Body.PartialErrors) != 0 {
		t.Errorf("partial = %v %q, want a complete read", got.Body.Partial, got.Body.PartialErrors)
	}
	if r := requests.Load(); r != 1 {
		t.Errorf("mail list requests = %d, want 1", r)
	}
}

// TestClientMailInboxSummaryFailsOnAResponseWithNoListBody proves a 200 whose
// body no list decode accepts fails the summary instead of reading as an empty
// inbox.
func TestClientMailInboxSummaryFailsOnAResponseWithNoListBody(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html>upstream unavailable</html>") //nolint:errcheck
	})
	got, err := handlerClient(t, h, "alpha").MailInboxSummary("worker", "")
	assertListPageError(t, err, "GET /v0/city/alpha/mail: page 1:", "decoded no list body")
	if got.Body.Total != 0 {
		t.Errorf("Total = %d from a failed read, want 0", got.Body.Total)
	}
}

// keysetListMethods returns the generated client methods whose params carry a
// keyset Cursor: the endpoints that cut their result into pages. The set holds
// the WithResponse methods and the raw ClientInterface methods that
// ClientWithResponses embeds.
func keysetListMethods(t *testing.T) map[string]bool {
	t.Helper()
	methods := map[string]bool{}
	client := reflect.TypeOf((*genclient.ClientWithResponses)(nil))
	for i := 0; i < client.NumMethod(); i++ {
		m := client.Method(i)
		for j := 0; j < m.Type.NumIn(); j++ {
			in := m.Type.In(j)
			if in.Kind() != reflect.Pointer || in.Elem().Kind() != reflect.Struct {
				continue
			}
			if _, ok := in.Elem().FieldByName("Cursor"); ok {
				methods[m.Name] = true
			}
		}
	}
	// A generator rename that hid every keyset list would leave the guard
	// passing on nothing, so require the lists the module is known to read.
	for _, name := range []string{
		"GetV0CityByCityNameBeadsWithResponse",
		"GetV0CityByCityNameConvoysWithResponse",
		"GetV0CityByCityNameEventsWithResponse",
		"GetV0CityByCityNameMailWithResponse",
		"GetV0CityByCityNameSessionsWithResponse",
	} {
		if !methods[name] {
			t.Fatalf("generated client method %s no longer takes a Cursor param; update keysetListMethods", name)
		}
	}
	return methods
}

// keysetCallAllowance permits one function to call a keyset list endpoint
// outside a walkKeysetPages fetch.
type keysetCallAllowance struct {
	// firstPage marks a read of one page's metadata, such as its total, its
	// partial-read state or a header, never its rows. The guard checks that
	// such a function names no Items field and never hands the decoded body
	// (JSON200) on whole.
	firstPage bool
	reason    string
}

// keysetCallAllowances names, by "<repo-relative file>:<function>", the
// functions that may call a keyset list endpoint outside a walkKeysetPages
// fetch, each with the reason it needs no walk.
var keysetCallAllowances = map[string]keysetCallAllowance{
	"internal/api/client.go:MailInboxSummary": {
		firstPage: true,
		reason:    "reads the inbox's total and partial-read state from one single-row page",
	},
	"cmd/gc/cmd_events.go:probeCityEventsReachable": {
		firstPage: true,
		reason:    "a single-row reachability probe that reads only the response status",
	},
	"cmd/gc/cmd_events.go:fetchCityHeadIndex": {
		firstPage: true,
		reason:    "reads the X-GC-Index header of a single-row page",
	},
	"cmd/gc/cmd_events.go:fetchCityEventsPage": {
		reason: "one page of fetchCityEvents' event walk, which follows next_cursor across a --since window under a walk budget and prints a truncation notice when it stops early",
	},
	"cmd/gc/cmd_events.go:fetchCityEventsAfterSeq": {
		reason: "the --watch replay walk, which follows next_cursor until a page reaches the resume seq",
	},
}

// keysetCallScan is what keysetListCallViolations found.
type keysetCallScan struct {
	violations []string        // one line per offending reference
	allowed    map[string]bool // allowance keys a reference matched
	walked     int             // calls inside walkKeysetPages fetches
}

// keysetListCallViolations checks every reference in files to a method in
// methods. A reference passes when it is the callee of a call inside the fetch
// closure passed to walkKeysetPages, and that closure uses its cursor
// parameter. Otherwise its function must hold an allowance, and a first-page
// allowance also requires that the function never touches the page's rows.
// files maps repo-relative paths to parsed files.
func keysetListCallViolations(fset *token.FileSet, files map[string]*ast.File, methods map[string]bool, allowances map[string]keysetCallAllowance) keysetCallScan {
	scan := keysetCallScan{allowed: map[string]bool{}}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		for _, decl := range files[path].Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := path + ":" + fn.Name.Name
			allowance, allowed := allowances[key]
			var stack []ast.Node
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				if sel, ok := n.(*ast.SelectorExpr); ok {
					pos := fset.Position(sel.Pos())
					switch {
					case methods[sel.Sel.Name]:
						switch problem := walkFetchProblem(stack, sel); {
						case problem == "":
							scan.walked++
						case allowed:
							scan.allowed[key] = true
						default:
							scan.violations = append(scan.violations, fmt.Sprintf("%s: %s references %s %s, so it can read one page of a keyset list as the whole list; walk it with walkKeysetPages, or add an allowance with its reason to keysetCallAllowances", pos, fn.Name.Name, sel.Sel.Name, problem))
						}
					case allowed && allowance.firstPage && sel.Sel.Name == "Items":
						scan.violations = append(scan.violations, fmt.Sprintf("%s: %s is allowed as a first-page read but reads a page's Items, which hold one page of the list", pos, fn.Name.Name))
					case allowed && allowance.firstPage && sel.Sel.Name == "JSON200" && !metadataUse(stack, sel):
						scan.violations = append(scan.violations, fmt.Sprintf("%s: %s is allowed as a first-page read but hands the decoded page body on whole; read only its metadata fields", pos, fn.Name.Name))
					}
				}
				stack = append(stack, n)
				return true
			})
		}
	}
	return scan
}

// walkFetchProblem returns "" when sel is the callee of a call inside the fetch
// closure of a walkKeysetPages call, a closure that uses its cursor parameter.
// Otherwise it says what is missing. stack holds sel's ancestors, outermost
// first.
func walkFetchProblem(stack []ast.Node, sel *ast.SelectorExpr) string {
	if len(stack) == 0 {
		return "outside any call"
	}
	if call, ok := stack[len(stack)-1].(*ast.CallExpr); !ok || call.Fun != sel {
		return "as a function value instead of calling it"
	}
	for i := len(stack) - 1; i > 0; i-- {
		lit, ok := stack[i].(*ast.FuncLit)
		if !ok {
			continue
		}
		call, ok := stack[i-1].(*ast.CallExpr)
		if !ok || !isWalkKeysetPages(call.Fun) {
			return "inside a closure that is not a walkKeysetPages fetch"
		}
		if len(call.Args) < 4 || call.Args[3] != lit {
			return "inside a walkKeysetPages closure that is not its fetch argument"
		}
		if !usesFirstParam(lit) {
			return "inside a walkKeysetPages fetch that never uses its cursor parameter"
		}
		return ""
	}
	return "outside a walkKeysetPages fetch"
}

// isWalkKeysetPages reports whether fun names walkKeysetPages, with or without
// explicit type arguments.
func isWalkKeysetPages(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name == "walkKeysetPages"
	case *ast.IndexExpr:
		return isWalkKeysetPages(f.X)
	case *ast.IndexListExpr:
		return isWalkKeysetPages(f.X)
	}
	return false
}

// usesFirstParam reports whether lit's body refers to its first parameter.
func usesFirstParam(lit *ast.FuncLit) bool {
	params := lit.Type.Params.List
	if len(params) == 0 || len(params[0].Names) == 0 || params[0].Names[0].Name == "_" {
		return false
	}
	name := params[0].Names[0].Name
	used := false
	ast.Inspect(lit.Body, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == name {
			used = true
		}
		return !used
	})
	return used
}

// metadataUse reports whether the JSON200 selector sel only reaches a field
// of the decoded body, or is compared with nil, rather than handing the body
// on whole.
func metadataUse(stack []ast.Node, sel *ast.SelectorExpr) bool {
	switch parent := stack[len(stack)-1].(type) {
	case *ast.SelectorExpr:
		return parent.X == sel
	case *ast.BinaryExpr:
		return (parent.Op == token.EQL || parent.Op == token.NEQ) && (isNilIdent(parent.X) || isNilIdent(parent.Y))
	}
	return false
}

func isNilIdent(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "nil"
}

// TestKeysetListGuardFlagsEveryUnwalkedRead feeds the guard sources that each
// read a keyset list in a way that can return one page as the whole list, and
// two that read correctly, so the guard is shown to tell them apart: two lists
// in one function with only one walked, a single read of next_cursor, a
// method value, a fetch that ignores its cursor, a call in the walk's key
// function, a call from another package, and first-page reads that touch rows.
func TestKeysetListGuardFlagsEveryUnwalkedRead(t *testing.T) {
	sources := map[string]string{
		"internal/api/walk.go": `package api

func (c *Client) Walked() {
	walkKeysetPages(req, 0, maxPages, func(cursor string, n int64) (keysetPage[int], error) {
		p.Cursor = &cursor
		resp, err := c.cw.GetV0CityByCityNameBeadsWithResponse(ctx, c.cityName, p)
		return page(resp, err)
	}, key)
}

func (c *Client) WalksOneOfTwo() {
	walkKeysetPages(req, 0, maxPages, func(cursor string, n int64) (keysetPage[int], error) {
		p.Cursor = &cursor
		return page(c.cw.GetV0CityByCityNameBeadsWithResponse(ctx, c.cityName, p))
	}, key)
	resp, _ := c.cw.GetV0CityByCityNameMailWithResponse(ctx, c.cityName, q)
	_ = resp
}

func (c *Client) ReadsNextCursorOnce() {
	resp, _ := c.cw.GetV0CityByCityNameMailWithResponse(ctx, c.cityName, q)
	_ = resp.JSON200.NextCursor
}

func (c *Client) MethodValue() {
	walkKeysetPages(req, 0, maxPages, func(cursor string, n int64) (keysetPage[int], error) {
		p.Cursor = &cursor
		get := c.cw.GetV0CityByCityNameMailWithResponse
		return page(get(ctx, c.cityName, p))
	}, key)
}

func (c *Client) IgnoresCursor() {
	walkKeysetPages(req, 0, maxPages, func(cursor string, n int64) (keysetPage[int], error) {
		return page(c.cw.GetV0CityByCityNameMailWithResponse(ctx, c.cityName, p))
	}, key)
}

func (c *Client) InTheKeyFunc() {
	walkKeysetPages(req, 0, maxPages, fetch, func(r int) string {
		c.cw.GetV0CityByCityNameMailWithResponse(ctx, c.cityName, p)
		return ""
	})
}
`,
		"internal/api/summary.go": `package api

func (c *Client) Summary() {
	resp, _ := c.cw.GetV0CityByCityNameMailWithResponse(ctx, c.cityName, one)
	if resp.JSON200 == nil {
		return
	}
	_ = resp.JSON200.Total
}

func (c *Client) SummaryReadsRows() {
	resp, _ := c.cw.GetV0CityByCityNameMailWithResponse(ctx, c.cityName, one)
	_ = resp.JSON200.Items
}

func (c *Client) SummaryHandsBodyOn() {
	resp, _ := c.cw.GetV0CityByCityNameMailWithResponse(ctx, c.cityName, one)
	decode(resp.JSON200)
}
`,
		"cmd/gc/direct.go": `package main

func listDirect(client *genclient.ClientWithResponses) {
	client.GetV0CityByCityNameMailWithResponse(ctx, "alpha", p)
}
`,
	}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for path, src := range sources {
		f, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		files[path] = f
	}
	methods := map[string]bool{"GetV0CityByCityNameBeadsWithResponse": true, "GetV0CityByCityNameMailWithResponse": true}
	allowances := map[string]keysetCallAllowance{
		"internal/api/summary.go:Summary":            {firstPage: true, reason: "test"},
		"internal/api/summary.go:SummaryReadsRows":   {firstPage: true, reason: "test"},
		"internal/api/summary.go:SummaryHandsBodyOn": {firstPage: true, reason: "test"},
	}

	scan := keysetListCallViolations(fset, files, methods, allowances)
	flagged := map[string]bool{}
	for _, v := range scan.violations {
		for _, fn := range []string{"Walked", "WalksOneOfTwo", "ReadsNextCursorOnce", "MethodValue", "IgnoresCursor", "InTheKeyFunc", "Summary", "SummaryReadsRows", "SummaryHandsBodyOn", "listDirect"} {
			if strings.Contains(v, ": "+fn+" ") {
				flagged[fn] = true
			}
		}
	}
	for _, fn := range []string{"WalksOneOfTwo", "ReadsNextCursorOnce", "MethodValue", "IgnoresCursor", "InTheKeyFunc", "SummaryReadsRows", "SummaryHandsBodyOn", "listDirect"} {
		if !flagged[fn] {
			t.Errorf("guard did not flag %s; violations:\n%s", fn, strings.Join(scan.violations, "\n"))
		}
	}
	for _, fn := range []string{"Walked", "Summary"} {
		if flagged[fn] {
			t.Errorf("guard flagged %s, which reads correctly; violations:\n%s", fn, strings.Join(scan.violations, "\n"))
		}
	}
	if scan.walked != 2 {
		t.Errorf("walked calls = %d, want 2 (Walked and the walked half of WalksOneOfTwo)", scan.walked)
	}
}

// repoKeysetCallFiles parses every non-test Go file under root that names a
// method in methods, keyed by repo-relative path. It skips the generated client,
// which defines those methods, and the directories that are not this module's
// source: hidden directories, vendor, node_modules, testdata, nested modules
// and nested git worktrees.
func repoKeysetCallFiles(t *testing.T, root string, methods map[string]bool) (*token.FileSet, map[string]*ast.File) {
	t.Helper()
	names := make([][]byte, 0, len(methods))
	for name := range methods {
		names = append(names, []byte(name))
	}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == root {
				return nil
			}
			base := d.Name()
			if strings.HasPrefix(base, ".") || base == "vendor" || base == "node_modules" || base == "testdata" {
				return filepath.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir
			}
			if fi, err := os.Stat(filepath.Join(path, ".git")); err == nil && !fi.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "internal/api/genclient/") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(names, func(name []byte) bool { return bytes.Contains(src, name) }) {
			return nil
		}
		f, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("parse %s: %w", rel, err)
		}
		files[rel] = f
		return nil
	})
	if err != nil {
		t.Fatalf("scan %s for keyset list calls: %v", root, err)
	}
	return fset, files
}

// TestKeysetListCallsWalkEveryPage fails when non-test code anywhere in the
// module reads a keyset-paginated list in a way that can return one page as
// the whole list. Such a read gets only the server's first page, 100 rows by
// default, and hands its caller a truncated list that looks complete. Every
// call to a keyset list endpoint is the fetch of a walkKeysetPages walk, or
// its function holds an allowance in keysetCallAllowances that says why it
// needs no walk.
func TestKeysetListCallsWalkEveryPage(t *testing.T) {
	methods := keysetListMethods(t)
	root := bazeltest.RepoRoot(t)
	fset, files := repoKeysetCallFiles(t, root, methods)
	scan := keysetListCallViolations(fset, files, methods, keysetCallAllowances)
	for _, v := range scan.violations {
		t.Error(v)
	}
	keys := make([]string, 0, len(keysetCallAllowances))
	for key := range keysetCallAllowances {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !scan.allowed[key] {
			t.Errorf("keysetCallAllowances[%q] matched no keyset list call; remove the stale entry", key)
		}
	}
	if scan.walked == 0 {
		t.Fatalf("found no walked keyset list calls under %s; the guard is scanning the wrong files", root)
	}
}
