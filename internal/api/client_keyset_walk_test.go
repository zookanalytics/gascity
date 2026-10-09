package api

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gastownhall/gascity/internal/api/genclient"
	"github.com/gastownhall/gascity/internal/beads"
)

// TestWalkKeysetPagesRejectsCursorCycle proves the walk remembers every cursor
// it requested, not only the last one: a server alternating between two
// cursors would otherwise be walked forever.
func TestWalkKeysetPagesRejectsCursorCycle(t *testing.T) {
	next := map[string]string{"": "a", "a": "b", "b": "a"}
	var requested []string
	err := walkKeysetPages(func(cursor string) (string, error) {
		requested = append(requested, cursor)
		if len(requested) > 10 {
			return "", nil // end a walk that lost its guard instead of hanging
		}
		return next[cursor], nil
	})
	if err == nil || !strings.Contains(err.Error(), `cursor repeated ("a")`) {
		t.Fatalf("err = %v, want a repeated-cursor error naming %q", err, "a")
	}
	if want := []string{"", "a", "b"}; !slices.Equal(requested, want) {
		t.Errorf("requested cursors = %q, want %q", requested, want)
	}
}

// TestClientKeysetListsRejectRepeatedCursor proves each walked list fails
// loudly when the server hands back a cursor it already served. Requesting it
// again would return the same rows forever, and stopping there would return
// them twice.
func TestClientKeysetListsRejectRepeatedCursor(t *testing.T) {
	cases := []struct {
		name string
		path string
		item map[string]any
		list func(*Client) error
	}{
		{
			name: "sessions",
			path: "/v0/city/alpha/sessions",
			item: map[string]any{"id": "gc-abc", "template": "worker", "state": "active", "title": "Worker", "session_name": "worker", "provider": "claude", "created_at": "2026-04-23T10:00:00Z", "attached": false, "running": true},
			list: func(c *Client) error { _, err := c.ListSessions("", "", false); return err },
		},
		{
			name: "convoys",
			path: "/v0/city/alpha/convoys",
			item: map[string]any{"id": "gc-1", "title": "deploy", "issue_type": "convoy", "status": "open", "created_at": "2026-04-23T10:00:00Z"},
			list: func(c *Client) error { _, err := c.ListConvoys(); return err },
		},
		{
			name: "mail",
			path: "/v0/city/alpha/mail",
			item: map[string]any{"id": "msg-1", "from": "alice", "to": "worker", "subject": "hi", "body": "hello", "created_at": "2026-04-23T10:00:00Z", "read": false},
			list: func(c *Client) error { _, err := c.ListMailInbox("worker", ""); return err },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.path {
					t.Errorf("path = %q, want %q", r.URL.Path, tc.path)
				}
				// A server that ignores the cursor serves the first page and the
				// same next_cursor every time. After a few requests the page
				// stops minting one, so a walk without the guard fails here
				// instead of hanging.
				body := map[string]any{"items": []map[string]any{tc.item}, "total": 2}
				if requests.Add(1) <= 5 {
					body["next_cursor"] = "stuck"
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(body) //nolint:errcheck
			}))
			defer ts.Close()

			err := tc.list(NewCityScopedClient(ts.URL, "alpha"))
			if err == nil || !strings.Contains(err.Error(), "cursor repeated") {
				t.Fatalf("err = %v, want a repeated-cursor error", err)
			}
			if got := requests.Load(); got != 2 {
				t.Errorf("requests = %d, want 2 (the first page, then the repeated cursor once)", got)
			}
		})
	}
}

// countingHandler serves h and counts the requests whose path ends in suffix,
// so a test can prove a client walk crossed a page boundary.
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
	ts := httptest.NewServer(countingHandler(newTestCityHandler(t, state), "/convoys", &pages))
	t.Cleanup(ts.Close)

	got, err := NewCityScopedClient(ts.URL, state.CityName()).ListConvoys()
	if err != nil {
		t.Fatalf("ListConvoys: %v", err)
	}
	var gotIDs []string
	for _, b := range got.Body {
		gotIDs = append(gotIDs, b.ID)
	}
	if !slices.Equal(sortedIDs(gotIDs), sortedIDs(wantIDs)) {
		t.Fatalf("ListConvoys returned %d convoys (%d distinct), want the store's %d", len(gotIDs), len(slices.Compact(sortedIDs(gotIDs))), n)
	}
	if p := pages.Load(); p != 2 {
		t.Errorf("convoy list requests = %d, want 2 (a full page, then the remainder)", p)
	}
}

// TestClientListMailInboxReturnsEveryServerPage runs ListMailInbox against the
// real mail list handler holding more unread messages than one 1000-row page.
// The client must return every message once, under the server's full-set
// total.
func TestClientListMailInboxReturnsEveryServerPage(t *testing.T) {
	state := newFakeState(t)
	mp := state.cityMailProv
	const n = maxPaginationLimit + 25
	var wantIDs []string
	for i := 0; i < n; i++ {
		m, err := mp.Send("alice", "worker", "s", "b")
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		wantIDs = append(wantIDs, m.ID)
	}

	var pages atomic.Int32
	ts := httptest.NewServer(countingHandler(newTestCityHandler(t, state), "/mail", &pages))
	t.Cleanup(ts.Close)

	got, err := NewCityScopedClient(ts.URL, state.CityName()).ListMailInbox("worker", "")
	if err != nil {
		t.Fatalf("ListMailInbox: %v", err)
	}
	var gotIDs []string
	for _, m := range got.Body.Items {
		gotIDs = append(gotIDs, m.ID)
	}
	if !slices.Equal(sortedIDs(gotIDs), sortedIDs(wantIDs)) {
		t.Fatalf("ListMailInbox returned %d messages (%d distinct), want the %d sent", len(gotIDs), len(slices.Compact(sortedIDs(gotIDs))), n)
	}
	if got.Body.Total != n {
		t.Errorf("Total = %d, want %d", got.Body.Total, n)
	}
	if p := pages.Load(); p != 2 {
		t.Errorf("mail list requests = %d, want 2 (a full page, then the remainder)", p)
	}
}

// keysetListMethods returns the generated client methods whose params carry a
// keyset Cursor, which are the endpoints that cut their result into pages.
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
	// A generator rename that hid every keyset list would leave this guard
	// passing on nothing, so require the lists the client is known to read.
	for _, name := range []string{
		"GetV0CityByCityNameBeadsWithResponse",
		"GetV0CityByCityNameConvoysWithResponse",
		"GetV0CityByCityNameMailWithResponse",
		"GetV0CityByCityNameSessionsWithResponse",
	} {
		if !methods[name] {
			t.Fatalf("generated client method %s no longer takes a Cursor param; update keysetListMethods", name)
		}
	}
	return methods
}

// TestClientKeysetListCallsFollowNextCursor fails when a function in this
// package calls a keyset-paginated list endpoint and never reads next_cursor.
// Such a call gets only the server's first page, 100 rows by default, and
// hands its caller a truncated list that looks complete. Follow the cursor
// with walkKeysetPages, or return it to a caller that does.
func TestClientKeysetListCallsFollowNextCursor(t *testing.T) {
	methods := keysetListMethods(t)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob package sources: %v", err)
	}
	fset := token.NewFileSet()
	calls := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var listCalls []*ast.SelectorExpr
			readsNextCursor := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if sel.Sel.Name == "NextCursor" {
					readsNextCursor = true
				}
				if methods[sel.Sel.Name] {
					listCalls = append(listCalls, sel)
				}
				return true
			})
			calls += len(listCalls)
			if readsNextCursor {
				continue
			}
			for _, sel := range listCalls {
				t.Errorf("%s: %s calls %s but never reads NextCursor, so it returns only the first page of a keyset list", fset.Position(sel.Pos()), fn.Name.Name, sel.Sel.Name)
			}
		}
	}
	if calls == 0 {
		t.Fatal("found no calls to keyset list endpoints in this package; the guard is scanning the wrong files")
	}
}
