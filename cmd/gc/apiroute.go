package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gastownhall/gascity/internal/api"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
)

// apirouteControllerAliveHook is a test seam for the standalone-controller
// liveness probe used by apiClient and apiClientFallbackReason. Tests stub
// this so they can exercise the "controller alive but standalone API
// unconfigured" path that supervisor-managed cities hit in production.
var apirouteControllerAliveHook = controllerAlive

// apiClient returns an API client when one is reachable for the city at
// cityPath. It prefers the standalone controller's mutable HTTP API when
// configured; otherwise it falls through to the supervisor's HTTP API
// (which services /v0/city/{name}/... routes for every supervisor-managed
// city). Returns nil only if neither is reachable, or if GC_NO_API is set
// truthy (operator escape hatch), or if the standalone API is bound to a
// non-localhost address without allow_mutations AND no supervisor API is
// available. CLI commands use this to route reads/writes through the API
// when available, falling back to direct bd or file mutation.
//
// Fall-through is load-bearing on supervisor-managed cities: the supervisor
// process services controller.sock for each managed city (so the standalone
// liveness probe returns alive) while typical city.toml has no [api]
// section. Earlier revisions early-returned on cfg.API.Port == 0 inside the
// standalone branch and never reached the supervisor fall-through, leaving
// supervisor-managed cities on the slow direct-Dolt path.
func apiClient(cityPath string) *api.Client {
	// Operator escape hatch: GC_NO_API=1|true|yes → always fall back.
	// Unknown values warn to stderr and fail open (fall through to normal path).
	if disabled, warn := classifyGCNoAPI(os.Getenv("GC_NO_API")); disabled {
		return nil
	} else if warn != "" {
		fmt.Fprintln(os.Stderr, "warning: "+warn) //nolint:errcheck // best-effort stderr
	}
	// If a standalone controller is alive AND its HTTP API is usable, prefer
	// it. Otherwise fall through to the supervisor — its HTTP API may still
	// reach this city even when the standalone API isn't configured.
	if apirouteControllerAliveHook(cityPath) != 0 {
		tomlPath := filepath.Join(cityPath, "city.toml")
		if cfg, err := config.Load(fsys.OSFS{}, tomlPath); err == nil && cfg.API.Port > 0 {
			bind := cfg.API.BindOrDefault()
			nonLoopbackReadOnly := bind != "127.0.0.1" && bind != "localhost" && bind != "::1" && !cfg.API.AllowMutations
			if !nonLoopbackReadOnly {
				baseURL := fmt.Sprintf("http://%s", net.JoinHostPort(bind, strconv.Itoa(cfg.API.Port)))
				// Standalone controller serves /v0/city/{cityName}/... routes via
				// api.NewSupervisorMux, so per-city method calls need a city-scoped
				// client. Derive the city name from config; the controller only
				// serves one city in standalone mode.
				return api.NewCityScopedClient(baseURL, standaloneControllerCityName(cfg, cityPath))
			}
		}
		// Standalone-controller API isn't usable (config absent, API port
		// unset, or non-loopback bind without allow_mutations). Fall through.
	}
	return supervisorCityAPIClient(cityPath)
}

// standaloneControllerCityName resolves the effective city name for a
// standalone controller API client. In standalone mode the controller serves
// exactly one city, so the client must match the runtime identity.
func standaloneControllerCityName(cfg *config.City, cityPath string) string {
	return loadedCityName(cfg, cityPath)
}

// apiClientFallbackReason returns a reason code describing why apiClient
// returned nil for cityPath. Read-path CLI commands call this when the
// client is nil to emit a route=fallback reason=<code> log line.
//
// The closed set: "escape-hatch" (GC_NO_API truthy), "non-loopback-bind"
// (standalone API bound to non-localhost with mutations disallowed and no
// supervisor reachable), "standalone-api-disabled" (standalone controller
// alive but no API port configured and no supervisor reachable),
// "controller-down" (no standalone controller and no supervisor reachable).
//
// The "standalone-api-disabled" code distinguishes the
// supervisor-managed-city case (where the controller socket answers ping
// but city.toml has no [api] section) from a genuinely down controller,
// so operators debugging route=fallback don't chase the wrong thread.
func apiClientFallbackReason(cityPath string) string {
	if disabled, _ := classifyGCNoAPI(os.Getenv("GC_NO_API")); disabled {
		return "escape-hatch"
	}
	if apirouteControllerAliveHook(cityPath) != 0 {
		tomlPath := filepath.Join(cityPath, "city.toml")
		if cfg, err := config.Load(fsys.OSFS{}, tomlPath); err == nil && cfg.API.Port > 0 {
			bind := cfg.API.BindOrDefault()
			if bind != "127.0.0.1" && bind != "localhost" && bind != "::1" && !cfg.API.AllowMutations {
				return "non-loopback-bind"
			}
			// Standalone API is usable; apiClient would have returned a
			// client. Reaching here is unexpected — fall through to the
			// catch-all rather than emit a misleading code.
			return "controller-down"
		}
		// Standalone controller is alive but its HTTP API isn't configured.
		// apiClient fell through to supervisorCityAPIClient and that also
		// returned nil — surface the standalone-side cause so operators
		// don't chase a phantom "controller-down" diagnosis on a perfectly
		// healthy supervisor host.
		return "standalone-api-disabled"
	}
	return "controller-down"
}

// resolveAgentForAPI resolves a bare agent name (e.g., "worker") to its
// qualified form (e.g., "myrig/worker") using the current rig context, so
// the API server can find the agent. If already qualified or resolution
// fails, the original name is returned.
func resolveAgentForAPI(cityPath, name string) string {
	cfg, err := loadCityConfig(cityPath, io.Discard)
	if err != nil {
		cfg, err = loadCityConfigForEditFS(fsys.OSFS{}, filepath.Join(cityPath, "city.toml"))
		if err != nil {
			return name
		}
	}
	resolved, ok := resolveAgentIdentity(cfg, name, currentRigContext(cfg))
	if !ok {
		return name
	}
	return resolved.QualifiedName()
}
