package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/supervisor"
)

// stubApiroutePathHooks replaces the apirouteControllerAliveHook plus the
// supervisor hooks used by supervisorCityAPIClient with the supplied stubs
// and restores them at test cleanup. Tests use this to exercise the
// standalone/supervisor selection inside apiClient and
// apiClientFallbackReason without spinning real controllers.
func stubApiroutePathHooks(
	t *testing.T,
	controllerAlivePID int,
	supervisorPID int,
	supervisorRunning bool,
	supervisorKnown bool,
) {
	t.Helper()
	oldCtrl := apirouteControllerAliveHook
	oldSup := supervisorAliveHook
	oldRunning := supervisorCityRunningHook
	apirouteControllerAliveHook = func(string) int { return controllerAlivePID }
	supervisorAliveHook = func() int { return supervisorPID }
	supervisorCityRunningHook = func(string) (bool, string, bool) {
		return supervisorRunning, "", supervisorKnown
	}
	t.Cleanup(func() {
		apirouteControllerAliveHook = oldCtrl
		supervisorAliveHook = oldSup
		supervisorCityRunningHook = oldRunning
	})
}

// writeSupervisorConfigOnPort writes a supervisor.toml with the given port
// so supervisorAPIBaseURL() succeeds and returns the expected port.
//
//nolint:unparam // accepts port so future tests can exercise non-default ports.
func writeSupervisorConfigOnPort(t *testing.T, port int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(supervisor.ConfigPath()), 0o755); err != nil {
		t.Fatalf("mkdir supervisor config dir: %v", err)
	}
	body := []byte("[supervisor]\nport = " + strconv.Itoa(port) + "\n")
	if err := os.WriteFile(supervisor.ConfigPath(), body, 0o644); err != nil {
		t.Fatalf("write supervisor config: %v", err)
	}
}

// makeCityDir creates cityDir on disk and writes city.toml so config.Load
// has something to find. Caller supplies the city.toml body.
func makeCityDir(t *testing.T, cityDir, cityToml string) {
	t.Helper()
	if err := os.MkdirAll(cityDir, 0o755); err != nil {
		t.Fatalf("mkdir city: %v", err)
	}
	writeCityToml(t, cityDir, cityToml)
}

// TestAPIClientStandaloneAPIConfigured pins the legacy standalone path:
// when the standalone controller is alive and city.toml has a usable [api]
// section, apiClient returns a standalone-style client (no supervisor
// fall-through). The base URL must come from city.toml, not from the
// supervisor config.
func TestAPIClientStandaloneAPIConfigured(t *testing.T) {
	configureIsolatedRuntimeEnv(t)

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n\n[api]\nport = 9123\n")

	// Standalone alive; supervisor also alive but on a different port —
	// if the fall-through fired we would see the supervisor port, not 9123.
	writeSupervisorConfigOnPort(t, 8372)
	reg := supervisor.NewRegistry(supervisor.RegistryPath())
	if err := reg.Register(cityPath, "alpha"); err != nil {
		t.Fatalf("register: %v", err)
	}
	stubApiroutePathHooks(t, 1234, 4242, true, true)

	client := apiClient(cityPath)
	if client == nil {
		t.Fatal("apiClient = nil, want standalone client when API is configured")
	}
	if !strings.Contains(client.BaseURL(), ":9123") {
		t.Fatalf("apiClient base URL = %q, want standalone port :9123", client.BaseURL())
	}
}

// TestAPIClientFallsThroughToSupervisor is the regression test for the
// supervisor-managed-city bug: when the standalone controller socket
// answers ping but city.toml has no [api] section, apiClient must fall
// through to supervisorCityAPIClient rather than returning nil. Before the
// fix, the early-return on cfg.API.Port <= 0 stranded every supervisor-
// managed `gc` invocation on the slow direct-Dolt path.
func TestAPIClientFallsThroughToSupervisor(t *testing.T) {
	configureIsolatedRuntimeEnv(t)

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n") // no [api] section

	writeSupervisorConfigOnPort(t, 8372)
	reg := supervisor.NewRegistry(supervisor.RegistryPath())
	if err := reg.Register(cityPath, "alpha"); err != nil {
		t.Fatalf("register: %v", err)
	}
	stubApiroutePathHooks(t, 1234, 4242, true, true)

	client := apiClient(cityPath)
	if client == nil {
		t.Fatal("apiClient = nil, want supervisor client to be returned via fall-through when standalone API is unconfigured")
	}
	if !strings.Contains(client.BaseURL(), ":8372") {
		t.Fatalf("apiClient base URL = %q, want supervisor port :8372", client.BaseURL())
	}
}

// TestAPIClientFallsThroughOnNonLoopbackBind verifies that even when
// city.toml configures a non-loopback [api] without allow_mutations
// (skipping standalone routing) the supervisor fall-through still fires.
// Before the fix, this case early-returned nil and stranded the caller.
func TestAPIClientFallsThroughOnNonLoopbackBind(t *testing.T) {
	configureIsolatedRuntimeEnv(t)

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n\n[api]\nport = 9123\nbind = \"10.0.0.1\"\n")

	writeSupervisorConfigOnPort(t, 8372)
	reg := supervisor.NewRegistry(supervisor.RegistryPath())
	if err := reg.Register(cityPath, "alpha"); err != nil {
		t.Fatalf("register: %v", err)
	}
	stubApiroutePathHooks(t, 1234, 4242, true, true)

	client := apiClient(cityPath)
	if client == nil {
		t.Fatal("apiClient = nil, want supervisor fall-through when standalone API bind is non-loopback")
	}
	if !strings.Contains(client.BaseURL(), ":8372") {
		t.Fatalf("apiClient base URL = %q, want supervisor port :8372", client.BaseURL())
	}
}

// TestAPIClientControllerDownUsesSupervisor pins the original supervisor
// path: when the standalone controller socket doesn't answer, apiClient
// reaches the supervisor unconditionally. This was already working and
// must not regress.
func TestAPIClientControllerDownUsesSupervisor(t *testing.T) {
	configureIsolatedRuntimeEnv(t)

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n")

	writeSupervisorConfigOnPort(t, 8372)
	reg := supervisor.NewRegistry(supervisor.RegistryPath())
	if err := reg.Register(cityPath, "alpha"); err != nil {
		t.Fatalf("register: %v", err)
	}
	stubApiroutePathHooks(t, 0, 4242, true, true)

	client := apiClient(cityPath)
	if client == nil {
		t.Fatal("apiClient = nil, want supervisor client when standalone controller is down")
	}
	if !strings.Contains(client.BaseURL(), ":8372") {
		t.Fatalf("apiClient base URL = %q, want supervisor port :8372", client.BaseURL())
	}
}

// TestAPIClientReturnsNilWhenNothingReachable asserts the both-down case:
// no standalone controller, no supervisor → apiClient returns nil and
// callers should fall back to direct bd/file mutation.
func TestAPIClientReturnsNilWhenNothingReachable(t *testing.T) {
	configureIsolatedRuntimeEnv(t)

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n")

	// supervisor not alive AND not registered: both fall-through legs fail.
	stubApiroutePathHooks(t, 0, 0, false, false)

	if client := apiClient(cityPath); client != nil {
		t.Fatalf("apiClient = %#v, want nil when neither standalone nor supervisor is reachable", client)
	}
}

// TestAPIClientFallbackReasonStandaloneAPIDisabled covers the new reason
// code: when the standalone controller is alive but its HTTP API isn't
// configured AND the supervisor isn't reachable, the reason must be
// "standalone-api-disabled" rather than the misleading "controller-down".
// Before the fix, this case returned "controller-down" and sent operators
// debugging route=fallback after a phantom controller liveness bug.
func TestAPIClientFallbackReasonStandaloneAPIDisabled(t *testing.T) {
	configureIsolatedRuntimeEnv(t)

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n")

	// Standalone alive, supervisor unregistered → supervisor leg returns nil.
	stubApiroutePathHooks(t, 1234, 0, false, false)

	got := apiClientFallbackReason(cityPath)
	if got != "standalone-api-disabled" {
		t.Fatalf("apiClientFallbackReason = %q, want %q", got, "standalone-api-disabled")
	}
}

// TestAPIClientFallbackReasonNonLoopbackBind preserves the legacy
// non-loopback reason code when standalone API is configured but bound to
// a non-loopback address with mutations disallowed.
func TestAPIClientFallbackReasonNonLoopbackBind(t *testing.T) {
	configureIsolatedRuntimeEnv(t)

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n\n[api]\nport = 9123\nbind = \"10.0.0.1\"\n")

	stubApiroutePathHooks(t, 1234, 0, false, false)

	got := apiClientFallbackReason(cityPath)
	if got != "non-loopback-bind" {
		t.Fatalf("apiClientFallbackReason = %q, want %q", got, "non-loopback-bind")
	}
}

// TestAPIClientFallbackReasonControllerDown preserves the legacy reason
// when no standalone controller answers and no supervisor is reachable.
func TestAPIClientFallbackReasonControllerDown(t *testing.T) {
	configureIsolatedRuntimeEnv(t)

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n")

	stubApiroutePathHooks(t, 0, 0, false, false)

	got := apiClientFallbackReason(cityPath)
	if got != "controller-down" {
		t.Fatalf("apiClientFallbackReason = %q, want %q", got, "controller-down")
	}
}

// TestAPIClientFallbackReasonEscapeHatch preserves the legacy reason when
// the operator sets GC_NO_API.
func TestAPIClientFallbackReasonEscapeHatch(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_NO_API", "1")

	cityPath := filepath.Join(t.TempDir(), "alpha")
	makeCityDir(t, cityPath, "[workspace]\nname = \"alpha\"\n")

	stubApiroutePathHooks(t, 1234, 4242, true, true)

	if client := apiClient(cityPath); client != nil {
		t.Fatalf("apiClient = %#v, want nil when GC_NO_API is set", client)
	}
	got := apiClientFallbackReason(cityPath)
	if got != "escape-hatch" {
		t.Fatalf("apiClientFallbackReason = %q, want %q", got, "escape-hatch")
	}
}
