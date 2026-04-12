package dashboard

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
)

func TestScopedPath(t *testing.T) {
	tests := []struct {
		path      string
		cityScope string
		want      string
	}{
		// Standalone mode — no rewriting.
		{"/v0/sessions", "", "/v0/sessions"},
		{"/v0/events/stream", "", "/v0/events/stream"},
		{"/v0/bead/abc123", "", "/v0/bead/abc123"},
		{"/health", "", "/health"},

		// Supervisor mode — /v0/ paths get city prefix.
		{"/v0/sessions", "bright-lights", "/v0/city/bright-lights/sessions"},
		{"/v0/events/stream", "bright-lights", "/v0/city/bright-lights/events/stream"},
		{"/v0/bead/abc123", "bright-lights", "/v0/city/bright-lights/bead/abc123"},
		{"/v0/session/abc123/transcript", "mytown", "/v0/city/mytown/session/abc123/transcript"},
		{"/v0/beads?status=open&limit=50", "mytown", "/v0/city/mytown/beads?status=open&limit=50"},

		// Non-/v0/ paths are never rewritten.
		{"/health", "bright-lights", "/health"},
		{"", "bright-lights", ""},
	}

	for _, tt := range tests {
		got := scopedPath(tt.path, tt.cityScope)
		if got != tt.want {
			t.Errorf("scopedPath(%q, %q) = %q, want %q", tt.path, tt.cityScope, got, tt.want)
		}
	}
}

func TestDetectSupervisor(t *testing.T) {
	t.Run("supervisor with cities", func(t *testing.T) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v0/ws" {
				http.NotFound(w, r)
				return
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Fatalf("upgrade: %v", err)
			}
			defer conn.Close()
			_ = conn.WriteJSON(map[string]any{"type": "hello"})
			var req struct {
				Action string `json:"action"`
			}
			if err := conn.ReadJSON(&req); err != nil {
				t.Fatalf("read request: %v", err)
			}
			if req.Action != "cities.list" {
				t.Fatalf("action = %q, want cities.list", req.Action)
			}
			_ = conn.WriteJSON(map[string]any{
				"type": "response",
				"id":   "cli-1",
				"result": map[string]any{
					"items": []map[string]any{
						{"name": "bright-lights"},
						{"name": "test-city"},
					},
				},
			})
		}))
		defer srv.Close()

		if !detectSupervisor(srv.URL) {
			t.Error("detectSupervisor() = false, want true")
		}
	})

	t.Run("standalone mode (404)", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "not found", http.StatusNotFound)
		}))
		defer srv.Close()

		if detectSupervisor(srv.URL) {
			t.Error("detectSupervisor() = true, want false")
		}
	})

	t.Run("unreachable server", func(t *testing.T) {
		if detectSupervisor("http://127.0.0.1:1") {
			t.Error("detectSupervisor() = true, want false")
		}
	})
}

func TestFetchCityTabs(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v0/ws" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"type": "hello"})
		var req struct {
			Action string `json:"action"`
		}
		if err := conn.ReadJSON(&req); err != nil {
			t.Fatalf("read request: %v", err)
		}
		if req.Action != "cities.list" {
			t.Fatalf("action = %q, want cities.list", req.Action)
		}
		_ = conn.WriteJSON(map[string]any{
			"type": "response",
			"id":   "cli-1",
			"result": map[string]any{
				"items": []map[string]any{
					{"name": "bright-lights", "running": true},
					{"name": "stopped-city", "running": false},
				},
			},
		})
	}))
	defer srv.Close()

	tabs := fetchCityTabs(srv.URL)
	if len(tabs) != 2 {
		t.Fatalf("fetchCityTabs() returned %d tabs, want 2", len(tabs))
	}
	if tabs[0].Name != "bright-lights" || !tabs[0].Running {
		t.Errorf("tabs[0] = %+v, want {bright-lights, true}", tabs[0])
	}
	if tabs[1].Name != "stopped-city" || tabs[1].Running {
		t.Errorf("tabs[1] = %+v, want {stopped-city, false}", tabs[1])
	}
}

func TestAPIFetcherWithScope(t *testing.T) {
	f := NewAPIFetcher("http://example.com", "/tmp/city", "mytown")
	if f.cityScope != "" {
		t.Errorf("new fetcher cityScope = %q, want empty", f.cityScope)
	}

	scoped := f.WithScope("bright-lights")
	if scoped.cityScope != "bright-lights" {
		t.Errorf("scoped cityScope = %q, want %q", scoped.cityScope, "bright-lights")
	}
	// Original unchanged.
	if f.cityScope != "" {
		t.Errorf("original cityScope changed to %q, want empty", f.cityScope)
	}
	// Shared client.
	if scoped.client != f.client {
		t.Error("scoped fetcher should share the HTTP client")
	}
}

func TestResolveSelectedCity(t *testing.T) {
	cities := []CityTab{
		{Name: "alpha", Running: true},
		{Name: "bravo", Running: true},
		{Name: "charlie", Running: false},
	}

	tests := []struct {
		name         string
		requested    string
		defaultCity  string
		cities       []CityTab
		wantSelected string
	}{
		{name: "request wins", requested: "bravo", defaultCity: "alpha", cities: cities, wantSelected: "bravo"},
		{name: "default current city wins over first running", defaultCity: "charlie", cities: cities, wantSelected: "charlie"},
		{name: "fallback first running", cities: cities, wantSelected: "alpha"},
		{name: "fallback first city when none running", cities: []CityTab{{Name: "stopped", Running: false}}, wantSelected: "stopped"},
		{name: "default used without city tabs", defaultCity: "alpha", cities: nil, wantSelected: "alpha"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveSelectedCity(tt.requested, tt.defaultCity, tt.cities); got != tt.wantSelected {
				t.Fatalf("resolveSelectedCity(%q, %q, %+v) = %q, want %q", tt.requested, tt.defaultCity, tt.cities, got, tt.wantSelected)
			}
		})
	}
}

func TestValidateAPI(t *testing.T) {
	t.Run("reachable health endpoint", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/health" {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		if err := ValidateAPI(srv.URL); err != nil {
			t.Fatalf("ValidateAPI(%q): %v", srv.URL, err)
		}
	})

	t.Run("non-200 health endpoint", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/health" {
				http.NotFound(w, r)
				return
			}
			http.Error(w, "broken", http.StatusServiceUnavailable)
		}))
		defer srv.Close()

		if err := ValidateAPI(srv.URL); err == nil {
			t.Fatal("ValidateAPI() succeeded for unhealthy server")
		}
	})
}
