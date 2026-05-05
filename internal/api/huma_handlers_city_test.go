package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/events"
)

// TestHumaHandleCityPatchSuspendEmitsEvent guards the API path that gc
// suspend now takes whenever a controller is up. The bug (gc-k2yqq) was
// that the API mutation succeeded silently — events.jsonl never recorded
// a city.suspended event, so dashboards/SSE consumers and anyone tailing
// the log had no visibility into the state change.
func TestHumaHandleCityPatchSuspendEmitsEvent(t *testing.T) {
	state := newFakeMutatorState(t)
	fakeProv, ok := state.eventProv.(*events.Fake)
	if !ok {
		t.Fatalf("eventProv = %T, want *events.Fake", state.eventProv)
	}

	h := newTestCityHandler(t, state)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, cityURL(state, ""), strings.NewReader(`{"suspended":true}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", "true")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	if !state.cfg.Workspace.Suspended {
		t.Error("workspace.suspended = false after PATCH suspended=true")
	}
	if len(fakeProv.Events) != 1 {
		t.Fatalf("recorded %d events, want 1; events = %+v", len(fakeProv.Events), fakeProv.Events)
	}
	ev := fakeProv.Events[0]
	if ev.Type != events.CitySuspended {
		t.Errorf("event type = %q, want %q", ev.Type, events.CitySuspended)
	}
	if ev.Actor == "" {
		t.Error("event actor empty, want non-empty (api distinguishes from cli/human)")
	}
}

func TestHumaHandleCityPatchResumeEmitsEvent(t *testing.T) {
	state := newFakeMutatorState(t)
	state.cfg.Workspace.Suspended = true
	fakeProv := state.eventProv.(*events.Fake)

	h := newTestCityHandler(t, state)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, cityURL(state, ""), strings.NewReader(`{"suspended":false}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", "true")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	if state.cfg.Workspace.Suspended {
		t.Error("workspace.suspended = true after PATCH suspended=false")
	}
	if len(fakeProv.Events) != 1 || fakeProv.Events[0].Type != events.CityResumed {
		t.Fatalf("expected one city.resumed event; got %+v", fakeProv.Events)
	}
}

// TestListCitiesIncludesSuspended ensures the /v0/cities response
// reflects workspace.suspended so operators can see at-a-glance why a
// session that "successfully" attaches drains immediately. The default
// stateCityResolver in test_helpers_test.go now copies the suspended
// flag through; this test pins that contract.
func TestListCitiesIncludesSuspended(t *testing.T) {
	state := newFakeMutatorState(t)
	state.cfg.Workspace.Suspended = true

	h := newTestCityHandler(t, state)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v0/cities", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"suspended":true`) {
		t.Errorf("response missing suspended=true; body = %s", body)
	}
}
