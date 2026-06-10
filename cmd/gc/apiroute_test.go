package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/api"
)

// writeCityTOMLForRoute writes a minimal city.toml into dir and returns dir.
func writeCityTOMLForRoute(t *testing.T, dir, body string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "city.toml"), []byte(body), 0o644); err != nil {
		t.Fatalf("writing city.toml: %v", err)
	}
	return dir
}

// TestStandaloneControllerClient covers the decision that gates apiClient's
// fall-through: a standalone controller endpoint is built only when city.toml
// names a usable [api] port on a loopback bind (or allows mutations). Every
// nil return is a signal for apiClient to try the supervisor-managed client
// instead. (gascity ga-tp7)
func TestStandaloneControllerClient(t *testing.T) {
	cases := []struct {
		name    string
		toml    string
		write   bool
		wantNil bool
	}{
		{name: "no-city-toml", write: false, wantNil: true},
		{name: "no-api-section", toml: "name = \"t\"\n", write: true, wantNil: true},
		{name: "api-port-zero", toml: "name = \"t\"\n[api]\nport = 0\n", write: true, wantNil: true},
		{name: "loopback-port", toml: "name = \"t\"\n[api]\nport = 8080\n", write: true, wantNil: false},
		{name: "explicit-localhost", toml: "name = \"t\"\n[api]\nport = 8080\nbind = \"localhost\"\n", write: true, wantNil: false},
		{name: "non-loopback-no-mutations", toml: "name = \"t\"\n[api]\nport = 8080\nbind = \"0.0.0.0\"\n", write: true, wantNil: true},
		{name: "non-loopback-allow-mutations", toml: "name = \"t\"\n[api]\nport = 8080\nbind = \"0.0.0.0\"\nallow_mutations = true\n", write: true, wantNil: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.write {
				writeCityTOMLForRoute(t, dir, tc.toml)
			}
			got := standaloneControllerClient(dir)
			if tc.wantNil && got != nil {
				t.Fatalf("standaloneControllerClient = non-nil, want nil")
			}
			if !tc.wantNil && got == nil {
				t.Fatalf("standaloneControllerClient = nil, want non-nil")
			}
		})
	}
}

// TestAPIClientRouting covers apiClient's routing: the standalone endpoint
// when the socket is alive and a usable [api] port is configured, the
// supervisor client when the socket is alive but no standalone API is usable
// (the supervisor-managed-city fall-through), the supervisor client when the
// socket is down, nil when nothing is reachable, and nil under the GC_NO_API
// escape hatch.
//
// The general-command fall-through to the supervisor is the gc-1rr12w fork
// behavior: a supervisor-managed city answers the controller socket in-process
// but omits a standalone [api] port, so without the fall-through every `gc`
// read on such a city takes the slow direct-Dolt path. (gascity gc-1rr12w)
func TestAPIClientRouting(t *testing.T) {
	sentinel := api.NewClient("http://supervisor.sentinel:1")

	restore := func(alive func(string) int, sup func(string) *api.Client) {
		apiRouteControllerAliveHook = alive
		apiRouteSupervisorClientHook = sup
	}
	origAlive, origSup := apiRouteControllerAliveHook, apiRouteSupervisorClientHook
	t.Cleanup(func() { restore(origAlive, origSup) })

	t.Run("controller-alive-no-api-port-falls-through-to-supervisor", func(t *testing.T) {
		// Supervisor-managed city: controller socket alive, no standalone [api]
		// port. apiClient must fall through to the supervisor rather than
		// returning nil, so general commands use the API fast path. (gc-1rr12w)
		t.Setenv("GC_NO_API", "")
		restore(func(string) int { return 4242 }, func(string) *api.Client { return sentinel })
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n")
		if got := apiClient(dir); got != sentinel {
			t.Fatalf("apiClient = %p, want supervisor sentinel %p (managed-city fall-through)", got, sentinel)
		}
	})

	t.Run("controller-alive-with-api-port-uses-standalone", func(t *testing.T) {
		t.Setenv("GC_NO_API", "")
		restore(func(string) int { return 4242 }, func(string) *api.Client { return sentinel })
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n[api]\nport = 8080\n")
		got := apiClient(dir)
		if got == nil {
			t.Fatalf("apiClient = nil, want standalone client")
		}
		if got == sentinel {
			t.Fatalf("apiClient returned supervisor sentinel, want standalone client (no regression)")
		}
	})

	t.Run("controller-alive-non-loopback-bind-falls-through", func(t *testing.T) {
		// A non-loopback [api] without allow_mutations is not a usable standalone
		// endpoint, so apiClient falls through to the supervisor. (gc-1rr12w)
		t.Setenv("GC_NO_API", "")
		restore(func(string) int { return 4242 }, func(string) *api.Client { return sentinel })
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n[api]\nport = 8080\nbind = \"0.0.0.0\"\n")
		if got := apiClient(dir); got != sentinel {
			t.Fatalf("apiClient = %p, want supervisor sentinel %p (non-loopback fall-through)", got, sentinel)
		}
	})

	t.Run("controller-down-uses-supervisor", func(t *testing.T) {
		t.Setenv("GC_NO_API", "")
		restore(func(string) int { return 0 }, func(string) *api.Client { return sentinel })
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n[api]\nport = 8080\n")
		if got := apiClient(dir); got != sentinel {
			t.Fatalf("apiClient = %p, want supervisor sentinel %p", got, sentinel)
		}
	})

	t.Run("nothing-reachable-returns-nil", func(t *testing.T) {
		// Controller down and supervisor unreachable: nil, caller uses local fallback.
		t.Setenv("GC_NO_API", "")
		restore(func(string) int { return 0 }, func(string) *api.Client { return nil })
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n")
		if got := apiClient(dir); got != nil {
			t.Fatalf("apiClient = %p, want nil when neither standalone nor supervisor is reachable", got)
		}
	})

	t.Run("escape-hatch-returns-nil", func(t *testing.T) {
		t.Setenv("GC_NO_API", "1")
		restore(func(string) int { return 4242 }, func(string) *api.Client { return sentinel })
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n")
		if got := apiClient(dir); got != nil {
			t.Fatalf("apiClient = %p, want nil under GC_NO_API escape hatch", got)
		}
	})
}

// TestAPIClientFallbackReason covers the reason codes that the gc-1rr12w
// supervisor fall-through introduces. The "standalone-api-disabled" code
// distinguishes a supervisor-managed city (controller socket alive, no [api]
// section, supervisor unreachable) from a genuinely down controller, so
// operators debugging route=fallback don't chase a phantom controller-liveness
// bug. The "escape-hatch" and "controller-down" codes are covered by
// route_log_test.go. (gascity gc-1rr12w)
func TestAPIClientFallbackReason(t *testing.T) {
	origAlive := apiRouteControllerAliveHook
	t.Cleanup(func() { apiRouteControllerAliveHook = origAlive })

	t.Run("standalone-api-disabled", func(t *testing.T) {
		t.Setenv("GC_NO_API", "")
		apiRouteControllerAliveHook = func(string) int { return 4242 }
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n")
		if got := apiClientFallbackReason(dir); got != "standalone-api-disabled" {
			t.Fatalf("apiClientFallbackReason = %q, want %q", got, "standalone-api-disabled")
		}
	})

	t.Run("non-loopback-bind", func(t *testing.T) {
		t.Setenv("GC_NO_API", "")
		apiRouteControllerAliveHook = func(string) int { return 4242 }
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n[api]\nport = 8080\nbind = \"0.0.0.0\"\n")
		if got := apiClientFallbackReason(dir); got != "non-loopback-bind" {
			t.Fatalf("apiClientFallbackReason = %q, want %q", got, "non-loopback-bind")
		}
	})
}

// TestMaintenanceAPIClientRoutesToSupervisor proves maintenanceAPIClient
// resolves the supervisor client for a supervisor-managed city (controller
// socket alive, no standalone [api] port) and honors the GC_NO_API escape
// hatch. (gascity ga-tp7)
func TestMaintenanceAPIClientRoutesToSupervisor(t *testing.T) {
	sentinel := api.NewClient("http://supervisor.sentinel:1")
	origAlive, origSup := apiRouteControllerAliveHook, apiRouteSupervisorClientHook
	t.Cleanup(func() {
		apiRouteControllerAliveHook = origAlive
		apiRouteSupervisorClientHook = origSup
	})

	t.Run("alive-no-api-port-routes-to-supervisor", func(t *testing.T) {
		t.Setenv("GC_NO_API", "")
		apiRouteControllerAliveHook = func(string) int { return 4242 }
		apiRouteSupervisorClientHook = func(string) *api.Client { return sentinel }
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n")
		c, reason := maintenanceAPIClient(dir)
		if c != sentinel {
			t.Fatalf("maintenanceAPIClient client = %p, want supervisor sentinel %p", c, sentinel)
		}
		if reason != "" {
			t.Fatalf("maintenanceAPIClient reason = %q, want empty", reason)
		}
	})

	t.Run("escape-hatch-skips-supervisor", func(t *testing.T) {
		t.Setenv("GC_NO_API", "1")
		apiRouteControllerAliveHook = func(string) int { return 4242 }
		apiRouteSupervisorClientHook = func(string) *api.Client { return sentinel }
		dir := writeCityTOMLForRoute(t, t.TempDir(), "name = \"t\"\n")
		c, reason := maintenanceAPIClient(dir)
		if c != nil {
			t.Fatalf("maintenanceAPIClient client = %p, want nil under GC_NO_API", c)
		}
		if reason != "escape-hatch" {
			t.Fatalf("maintenanceAPIClient reason = %q, want \"escape-hatch\"", reason)
		}
	})
}
