package dashboard

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	gcapi "github.com/gastownhall/gascity/internal/api"
)

// Serve starts the dashboard HTTP server. The dashboard serves static files
// only — all API operations go from the browser directly to the supervisor
// via WebSocket.
func Serve(port int, _, _ string, apiURL, initialCityScope string) error {
	apiURL = strings.TrimRight(apiURL, "/")
	if err := ValidateAPI(apiURL); err != nil {
		return err
	}

	log.Printf("dashboard: using API server at %s", apiURL)
	if initialCityScope != "" {
		log.Printf("dashboard: default city scope %q", initialCityScope)
	}

	isSupervisor := detectSupervisor(apiURL)
	if isSupervisor {
		log.Printf("dashboard: supervisor mode detected")
	}

	mux, err := NewDashboardMux(apiURL, initialCityScope)
	if err != nil {
		return fmt.Errorf("dashboard: failed to create handler: %w", err)
	}

	addr := fmt.Sprintf(":%d", port)
	log.Printf("dashboard: listening on http://localhost%s", addr)
	return http.ListenAndServe(addr, mux)
}

// ValidateAPI checks that the upstream GC API is reachable.
func ValidateAPI(apiURL string) error {
	if strings.TrimSpace(apiURL) == "" {
		return fmt.Errorf("dashboard: API server URL is empty")
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(strings.TrimRight(apiURL, "/") + "/health")
	if err != nil {
		return fmt.Errorf("dashboard: API server %s is not reachable: %w", apiURL, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		body = bytes.TrimSpace(body)
		if len(body) == 0 {
			return fmt.Errorf("dashboard: API server %s returned %s from /health", apiURL, resp.Status)
		}
		return fmt.Errorf("dashboard: API server %s returned %s from /health: %s", apiURL, resp.Status, body)
	}
	return nil
}

// detectSupervisor probes the API server for supervisor mode.
// A successful cities.list response (even with zero cities) means supervisor.
func detectSupervisor(apiURL string) bool {
	client := gcapi.NewClient(apiURL)
	_, err := client.ListCities()
	return err == nil
}
