package api

import (
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestRouteMatrixParityCoversLegacyInventory(t *testing.T) {
	routes := append([]routeExpectation{}, legacyCityRoutes()...)
	routes = append(routes, routeExpectation{
		name:       "cities list",
		method:     http.MethodGet,
		path:       "/v0/cities",
		wantStatus: http.StatusNotFound,
	})

	testNames, err := routeMatrixParityTestNames()
	if err != nil {
		t.Fatalf("read route matrix parity test names: %v", err)
	}

	var missing []string
	for _, route := range routes {
		name := routeMatrixExpectedTestName(route)
		if _, ok := testNames[name]; !ok {
			missing = append(missing, route.method+" "+route.path+" -> "+name)
		}
	}

	if len(missing) > 0 {
		t.Fatalf("route-matrix parity missing %d legacy routes:\n%s", len(missing), strings.Join(missing, "\n"))
	}
}

func routeMatrixParityTestNames() (map[string]struct{}, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return nil, os.ErrNotExist
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(file), "route_matrix_parity_test.go"))
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`(?m)^func (TestRouteMatrixParity_[^(]+)`)
	matches := re.FindAllStringSubmatch(string(data), -1)
	out := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		out[m[1]] = struct{}{}
	}
	return out, nil
}

func routeMatrixExpectedTestName(route routeExpectation) string {
	if route.path == "/v0/city" {
		return "TestRouteMatrixParity_" + route.method + "_v0_city_ViaWS"
	}

	segs := strings.Split(strings.Trim(route.path, "/"), "/")
	parts := []string{route.method, "v0"}
	for i := 1; i < len(segs); {
		seg := segs[i]
		switch seg {
		case "agent":
			parts = append(parts, "agent", "name")
			i += 2
			if i < len(segs) {
				parts = append(parts, routeMatrixNormalizeSegments(segs[i:])...)
			}
			i = len(segs)
		case "rig", "provider", "formula", "formulas", "order", "service":
			parts = append(parts, seg)
			if i+1 < len(segs) {
				if seg == "order" && segs[i+1] == "history" {
					parts = append(parts, "history", "bead_id")
					i += 3
				} else {
					parts = append(parts, "name")
					i += 2
				}
				if i < len(segs) {
					parts = append(parts, routeMatrixNormalizeSegments(segs[i:])...)
				}
				i = len(segs)
				continue
			}
			i++
		case "workflow":
			parts = append(parts, "workflow", "id")
			i += 2
			if i < len(segs) {
				parts = append(parts, routeMatrixNormalizeSegments(segs[i:])...)
			}
			i = len(segs)
		case "convoy":
			parts = append(parts, "convoy", "id")
			i += 2
			if i < len(segs) {
				parts = append(parts, routeMatrixNormalizeSegments(segs[i:])...)
			}
			i = len(segs)
		case "bead":
			parts = append(parts, "bead", "id")
			i += 2
			if i < len(segs) {
				parts = append(parts, routeMatrixNormalizeSegments(segs[i:])...)
			}
			i = len(segs)
		case "beads":
			parts = append(parts, "beads")
			if i+1 < len(segs) && segs[i+1] == "graph" {
				parts = append(parts, "graph", "rootID")
				i = len(segs)
				continue
			}
			i++
			if i < len(segs) {
				parts = append(parts, routeMatrixNormalizeSegments(segs[i:])...)
				i = len(segs)
			}
		case "mail":
			parts = append(parts, "mail")
			if i+1 < len(segs) {
				switch segs[i+1] {
				case "count":
					parts = append(parts, "count")
				case "thread":
					parts = append(parts, "thread", "id")
					if i+2 < len(segs) {
						parts = append(parts, routeMatrixNormalizeSegments(segs[i+3:])...)
					}
				default:
					parts = append(parts, "id")
					if i+2 < len(segs) {
						parts = append(parts, routeMatrixNormalizeSegments(segs[i+2:])...)
					}
				}
			}
			i = len(segs)
		case "session":
			parts = append(parts, "session", "id")
			i += 2
			if i < len(segs) {
				if segs[i] == "agents" {
					parts = append(parts, "agents")
					i++
					if i < len(segs) {
						parts = append(parts, "agent_id")
						i++
					}
				} else {
					parts = append(parts, routeMatrixNormalizeSegments(segs[i:])...)
					i = len(segs)
					continue
				}
			}
		default:
			parts = append(parts, strings.ReplaceAll(seg, "-", "_"))
			i++
		}
	}
	return "TestRouteMatrixParity_" + strings.Join(parts, "_") + "_ViaWS"
}

func routeMatrixNormalizeSegments(segs []string) []string {
	out := make([]string, 0, len(segs))
	for _, seg := range segs {
		out = append(out, strings.ReplaceAll(seg, "-", "_"))
	}
	return out
}
