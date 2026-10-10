package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/api/genclient"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/session"
)

// TestRouteMissingClassification pins the new-CLI/old-server hazard model: a 404
// with no problem+json body is a route-missing signal (old server's SPA / bare-
// mux catch-all), while a 404 that carries a problem+json body is a domain 404.
func TestRouteMissingClassification(t *testing.T) {
	pd := &genclient.ErrorModel{}
	cases := []struct {
		name   string
		status int
		pd     *genclient.ErrorModel
		wantRM bool
	}{
		{"404-no-body-route-missing", http.StatusNotFound, nil, true},
		{"404-with-problem-body-domain", http.StatusNotFound, pd, false},
		{"200-ok", http.StatusOK, nil, false},
		{"500-not-route-missing", http.StatusInternalServerError, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := routeMissingFromResponse(tc.status, tc.pd, "/waits")
			if got := err != nil; got != tc.wantRM {
				t.Fatalf("routeMissingFromResponse -> err=%v, want route-missing=%v", err, tc.wantRM)
			}
			if tc.wantRM {
				if !IsRouteMissing(err) {
					t.Errorf("IsRouteMissing=false for %v", err)
				}
				if !ShouldFallbackForRead(nil, err) {
					t.Errorf("ShouldFallbackForRead=false for route-missing")
				}
				if r := FallbackReason(nil, err); r != "route-missing" {
					t.Errorf("FallbackReason=%q, want route-missing", r)
				}
			}
		})
	}
}

// TestWaitInfoWireRoundTrip proves WaitInfo -> WaitView -> (JSON) ->
// genclient.WaitView -> WaitInfo preserves the fields the CLI renders, including
// the nil-vs-empty DepIDs and zero-CreatedAt distinctions.
func TestWaitInfoWireRoundTrip(t *testing.T) {
	cases := map[string]session.WaitInfo{
		"full": {
			ID:              "gc-wait-1",
			SessionID:       "gc-sess-1",
			SessionName:     "worker",
			Kind:            "deps",
			State:           "ready",
			DepIDs:          []string{"gc-1", "gc-2"},
			DepMode:         "all",
			RegisteredEpoch: "3",
			DeliveryAttempt: "2",
			NudgeID:         "wait-gc-wait-1-3-2",
			ExpiresAt:       "2026-05-16T09:30:00Z",
			Note:            "Continue.",
			Status:          "open",
			CreatedAt:       time.Date(2026, 3, 2, 4, 5, 6, 0, time.UTC),
			Labels:          []string{session.WaitBeadLabel, "session:gc-sess-1"},
		},
		"nil-deps-zero-created": {
			ID:        "gc-wait-2",
			SessionID: "gc-sess-2",
			Kind:      "deps",
			State:     "pending",
			Status:    "open",
			// DepIDs nil, CreatedAt zero
		},
		// Sub-second CreatedAt must survive the wire so the CLI's created-time
		// sort key stays precise across rungs (RFC3339Nano out, RFC3339 parse in).
		"sub-second-created": {
			ID:        "gc-wait-3",
			SessionID: "gc-sess-3",
			Kind:      "deps",
			State:     "ready",
			Status:    "open",
			CreatedAt: time.Date(2026, 3, 2, 4, 5, 6, 123456789, time.UTC),
		},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			view := waitViewFromInfo(in)
			raw, err := json.Marshal(view)
			if err != nil {
				t.Fatalf("marshal WaitView: %v", err)
			}
			var g genclient.WaitView
			if err := json.Unmarshal(raw, &g); err != nil {
				t.Fatalf("unmarshal genclient.WaitView: %v", err)
			}
			out := waitInfoFromGen(g)
			if !reflect.DeepEqual(out, in) {
				t.Fatalf("round-trip mismatch:\n got  %#v\n want %#v", out, in)
			}
		})
	}
}

// TestNotAWaitErrorMessage locks the CLI-facing text the inspect ladder renders.
func TestNotAWaitErrorMessage(t *testing.T) {
	e := &NotAWaitError{ID: "gc-9"}
	if e.Error() != "gc-9 is not a wait" {
		t.Fatalf("NotAWaitError = %q", e.Error())
	}
}

// subSecondWaitBead builds an IsWaitBead-satisfying gate bead with an explicit
// CreatedAt so the created-time ordering is deterministic and sub-second.
func subSecondWaitBead(id string, created time.Time) beads.Bead {
	return beads.Bead{
		ID:        id,
		Type:      session.WaitBeadType,
		Status:    "open",
		Labels:    []string{session.WaitBeadLabel, "session:s-1"},
		Metadata:  map[string]string{"session_id": "s-1", "state": "ready", "kind": "deps"},
		CreatedAt: created,
	}
}

// TestWaitList_TypedRungPreservesSubSecondOrder is the source-side guard for the
// cross-rung byte-identity contract: the real /v0/waits handler (waitViewFromInfo)
// must carry sub-second CreatedAt so the CLI's ascending created-time sort orders
// two same-second waits identically to the legacy/local rungs (which see full
// time.Time precision). If waitViewFromInfo truncated to whole seconds, the two
// waits would compare equal and the stable sort would keep the server's DESC
// order (newest first) instead of chronological.
func TestWaitList_TypedRungPreservesSubSecondOrder(t *testing.T) {
	base := time.Date(2026, 3, 2, 4, 5, 6, 0, time.UTC)
	early := subSecondWaitBead("w-early", base.Add(100*time.Millisecond))
	late := subSecondWaitBead("w-late", base.Add(900*time.Millisecond))

	state := newFakeState(t)
	// Seed both waits verbatim (NewMemStoreFrom preserves the explicit CreatedAt
	// that Create would otherwise overwrite). SessionsBeadStore falls back to
	// cityBeadStore, which the /v0/waits handler reads.
	state.cityBeadStore = beads.NewMemStoreFrom(2, []beads.Bead{late, early}, nil)

	ts := httptest.NewServer(newTestCityHandler(t, state))
	t.Cleanup(ts.Close)
	c := NewCityScopedClient(ts.URL, state.CityName())

	cr, err := c.ListWaits("", "")
	if err != nil {
		t.Fatalf("ListWaits: %v", err)
	}
	got := cr.Body.Waits
	if len(got) != 2 {
		t.Fatalf("wait count = %d, want 2", len(got))
	}
	for _, w := range got {
		if w.CreatedAt.Nanosecond() == 0 {
			t.Fatalf("wait %s lost sub-second precision on the typed rung: %v", w.ID, w.CreatedAt)
		}
	}
	// Apply the CLI's ascending stable created-time sort and assert chronological
	// order (oldest first) — the same ordering the legacy/local rungs produce.
	sort.SliceStable(got, func(i, j int) bool { return got[i].CreatedAt.Before(got[j].CreatedAt) })
	if got[0].ID != "w-early" || got[1].ID != "w-late" {
		t.Fatalf("post-sort order = [%s %s], want [w-early w-late]", got[0].ID, got[1].ID)
	}
}

// inProcessCityClient returns a city-scoped Client whose requests h serves
// in-process, so a test drives the real read routes without opening a loopback
// listener. It sends no X-GC-Request header, so mutating routes refuse it.
// example.com is the host wrapTestSupervisorMiddleware allows.
func inProcessCityClient(t *testing.T, h http.Handler, cityName string) *Client {
	t.Helper()
	transport := rtFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Result(), nil
	})
	const baseURL = "http://example.com"
	cw, err := genclient.NewClientWithResponses(baseURL, genclient.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("genclient.NewClientWithResponses: %v", err)
	}
	return &Client{cw: cw, baseURL: baseURL, cityName: cityName}
}

// TestListWaitsViaBeadsReportsCapAtBound drives the legacy leg through the real
// /beads handler on both sides of session.SessionWaitLookupLimit. Exactly the
// limit is a complete read. One wait more is a capped read that keeps the newest
// SessionWaitLookupLimit waits, the same bound the /waits handler reports.
func TestListWaitsViaBeadsReportsCapAtBound(t *testing.T) {
	limit := session.SessionWaitLookupLimit
	base := time.Date(2026, 3, 2, 4, 5, 6, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		seeded     int
		wantCapped bool
	}{
		{name: "exactly-the-limit", seeded: limit, wantCapped: false},
		{name: "one-past-the-limit", seeded: limit + 1, wantCapped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Wait i is created i seconds after base, so w-0000 is the oldest.
			seed := make([]beads.Bead, 0, tc.seeded)
			for i := 0; i < tc.seeded; i++ {
				seed = append(seed, subSecondWaitBead(fmt.Sprintf("w-%04d", i), base.Add(time.Duration(i)*time.Second)))
			}
			state := newFakeState(t)
			state.stores["myrig"] = beads.NewMemStoreFrom(tc.seeded, seed, nil)
			c := inProcessCityClient(t, newTestCityHandler(t, state), state.CityName())

			cr, err := c.ListWaitsViaBeads()
			if err != nil {
				t.Fatalf("ListWaitsViaBeads: %v", err)
			}
			if cr.Body.Capped != tc.wantCapped {
				t.Errorf("Capped = %v, want %v", cr.Body.Capped, tc.wantCapped)
			}
			if got := len(cr.Body.Waits); got != limit {
				t.Fatalf("wait count = %d, want %d", got, limit)
			}
			kept := make(map[string]bool, len(cr.Body.Waits))
			for _, w := range cr.Body.Waits {
				kept[w.ID] = true
			}
			if newest := fmt.Sprintf("w-%04d", tc.seeded-1); !kept[newest] {
				t.Errorf("newest wait %s missing from the read", newest)
			}
			if kept["w-0000"] == tc.wantCapped {
				t.Errorf("oldest wait w-0000 kept = %v, want %v (a capped read drops it)", kept["w-0000"], !tc.wantCapped)
			}
		})
	}
}
