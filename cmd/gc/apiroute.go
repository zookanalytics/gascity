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

// Indirected for tests. apiClient's branch between a standalone controller
// (which binds cfg.API.Port) and a supervisor-managed city (which is reached
// on the supervisor's port via city-scoped routes) depends on live sockets,
// so tests swap these to exercise the fall-through without a running
// controller or supervisor. (gascity ga-tp7)
var (
	apiRouteControllerAliveHook  = controllerAlive
	apiRouteSupervisorClientHook = supervisorCityAPIClient
)

// apiClient returns an API client when one is reachable for the city at
// cityPath, or nil if the caller should use its local bd/file fallback.
// Returns nil if neither a standalone controller nor a supervisor is
// reachable, if no API is configured anywhere, if GC_NO_API is set truthy
// (operator escape hatch), or if the only reachable standalone API is bound to
// a non-localhost address without allow_mutations and no supervisor API is
// available. CLI commands use this to route reads/writes through the API when
// available, falling back to direct bd or file mutation.
//
// A standalone controller (gc controller / gc serve) and a supervisor-managed
// city both answer the per-city controller socket — the supervisor hosts that
// controller in-process. When the socket is alive, apiClient prefers the
// standalone HTTP endpoint if the city configures a usable [api] port;
// otherwise it falls through to the supervisor's HTTP API (which services
// /v0/city/{name}/... routes for every supervisor-managed city). Fall-through
// is load-bearing on supervisor-managed cities: typical city.toml has no [api]
// section, so without it every general `gc` invocation would take the slow
// direct-Dolt path even though the supervisor's API is fully functional.
// (gascity gc-1rr12w)
func apiClient(cityPath string) *api.Client {
	// Remote routing is NOT handled here. A remote target is refused upstream by
	// the capability gate in resolveContext (Phase 1) and, once enabled, will be
	// served by a resolution-aware remote transport keyed on
	// resolvedContext.Remote (Phase 2) — never by sniffing global flags/env in
	// this local loopback ladder. Sniffing here is wrong: a local --city command
	// that merely has a stray GC_CITY_URL in its environment resolves LOCAL (flag
	// beats env), and must still route through its live local controller.
	// GC_NO_API + a resolved remote target is already a loud error at resolution
	// (guardNoAPI), so no remote op can reach the GC_NO_API nil-return below.
	//
	// Operator escape hatch: GC_NO_API=1|true|yes → always fall back.
	// Unknown values warn to stderr and fail open (fall through to normal path).
	if disabled, warn := classifyGCNoAPI(os.Getenv("GC_NO_API")); disabled {
		return nil
	} else if warn != "" {
		fmt.Fprintln(os.Stderr, "warning: "+warn) //nolint:errcheck // best-effort stderr
	}
	if apiRouteControllerAliveHook(cityPath) != 0 {
		// Alive socket: prefer the standalone HTTP endpoint when it's usable.
		if c := standaloneControllerClient(cityPath); c != nil {
			return c
		}
		// Standalone-controller API isn't usable (config absent, [api] port
		// unset, or a non-loopback bind without allow_mutations). A
		// supervisor-managed city hits this on every call — fall through to the
		// supervisor's HTTP API rather than returning nil, so general commands
		// take the fast API path instead of slow direct-Dolt. (gascity gc-1rr12w)
	}
	return apiRouteSupervisorClientHook(cityPath)
}

// standaloneControllerClient builds an API client for a standalone controller
// that binds cfg.API.Port from city.toml. It returns nil — the signal for
// apiClient to fall through to the supervisor-managed client — when no usable
// standalone HTTP endpoint exists: the config is unreadable, [api] port is
// unset, or the listener binds a non-loopback address without allow_mutations.
func standaloneControllerClient(cityPath string) *api.Client {
	tomlPath := filepath.Join(cityPath, "city.toml")
	cfg, err := config.Load(fsys.OSFS{}, tomlPath)
	if err != nil || cfg.API.Port <= 0 {
		return nil
	}
	// Non-localhost bind means API runs read-only — skip API routing
	// (unless allow_mutations is set).
	bind := cfg.API.BindOrDefault()
	if bind != "127.0.0.1" && bind != "localhost" && bind != "::1" && !cfg.API.AllowMutations {
		return nil
	}
	baseURL := fmt.Sprintf("http://%s", net.JoinHostPort(bind, strconv.Itoa(cfg.API.Port)))
	// Standalone controller serves /v0/city/{cityName}/... routes via
	// api.NewSupervisorMux, so per-city method calls need a city-scoped
	// client. Derive the city name from config; the controller only
	// serves one city in standalone mode.
	return api.NewCityScopedClient(baseURL, standaloneControllerCityName(cfg, cityPath))
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
	if apiRouteControllerAliveHook(cityPath) != 0 {
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
