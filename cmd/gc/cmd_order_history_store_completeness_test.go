package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/orders"
)

// A rig-scoped order is registered once per importing rig, so one bare `name`
// names several order registrations. These tests pin that a bounded read
// (`--limit N`) answers for every one of them, rather than for whichever
// registration happened to be found first (gc-6a6vz).

// orderScopedTestRunCount is how many runs each rig's registration seeds. The
// store-completeness assertions count against it: with two rigs seeded, a total
// short of twice this number means a store was never read.
const orderScopedTestRunCount = 3

// orderRunsStoreForScoped builds a store holding orderScopedTestRunCount
// order-run beads for a single scoped order name, one hour apart going back
// from newest. It mirrors orderHistoryRunsStore but takes the scoped name so
// each rig's registration gets its own rows.
func orderRunsStoreForScoped(t *testing.T, scoped string, newest time.Time) beads.Store {
	t.Helper()
	rows := make([]string, 0, orderScopedTestRunCount)
	for i := 0; i < orderScopedTestRunCount; i++ {
		rows = append(rows, fmt.Sprintf(
			`{"id":%q,"title":"run %d","status":"closed","issue_type":"task","created_at":%q,"labels":["order-run:%s"]}`,
			fmt.Sprintf("%s-%d", strings.ReplaceAll(scoped, ":", "-"), i),
			i,
			newest.Add(-time.Duration(i)*time.Hour).Format(time.RFC3339Nano),
			scoped,
		))
	}
	payload := []byte("[" + strings.Join(rows, ",") + "]")
	return beads.NewBdStore(t.TempDir(), func(_, _ string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "list" {
			return payload, nil
		}
		return []byte("[]"), nil
	})
}

// twoRigOrderRegistrations is the shape at the heart of gc-6a6vz: one order
// name, one registration per importing rig.
func twoRigOrderRegistrations() []orders.Order {
	return []orders.Order{
		{Name: "digest", Rig: "rig-a", Formula: "mol-digest"},
		{Name: "digest", Rig: "rig-b", Formula: "mol-digest"},
	}
}

// TestOrderHistoryBoundedReadIsStoreCompleteAcrossRigs is the bead's stated
// acceptance test: with a limit larger than the total number of retained runs,
// a bare `gc order history <name>` must return rows from EVERY rig, not just
// one. An answer drawn from a single store still renders a RIG column, so a
// partial read is indistinguishable from a complete one at the terminal.
func TestOrderHistoryBoundedReadIsStoreCompleteAcrossRigs(t *testing.T) {
	newest := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	storeA := orderRunsStoreForScoped(t, "digest:rig:rig-a", newest)
	storeB := orderRunsStoreForScoped(t, "digest:rig:rig-b", newest.Add(-30*time.Minute))

	resolver := func(a orders.Order) ([]beads.OrdersStore, error) {
		switch a.Rig {
		case "rig-a":
			return []beads.OrdersStore{{Store: storeA}}, nil
		case "rig-b":
			return []beads.OrdersStore{{Store: storeB}}, nil
		}
		return nil, fmt.Errorf("unexpected rig %q", a.Rig)
	}

	var stdout, stderr bytes.Buffer
	// 50 is far more than the 6 rows that exist, so nothing here is a budget
	// exhaustion: any missing rig is a store the read never opened.
	code := doOrderHistoryBounded("digest", "", twoRigOrderRegistrations(), resolver,
		orderHistoryBounds{Limit: 50}, true, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("doOrderHistoryBounded = %d, want 0; stderr: %s", code, stderr.String())
	}

	payload := orderHistoryEntries(t, &stdout)
	if want := 2 * orderScopedTestRunCount; len(payload.Entries) != want {
		t.Fatalf("entries = %d, want %d (%d per rig across 2 rigs); a short count means a store was not read",
			len(payload.Entries), want, orderScopedTestRunCount)
	}
	seen := map[string]int{}
	for _, e := range payload.Entries {
		seen[e.Rig]++
	}
	if seen["rig-a"] != orderScopedTestRunCount || seen["rig-b"] != orderScopedTestRunCount {
		t.Fatalf("per-rig counts = %v, want %d each; a bounded read must be store-complete", seen, orderScopedTestRunCount)
	}
}

// TestOrderHistoryBoundedReadKeepsNewestAcrossRigs pins the ordering half of
// the contract: the limit is applied AFTER merging every store newest-first, so
// `--limit N` means "the N most recent runs in the city", not "the N most recent
// runs in whichever store was read first".
func TestOrderHistoryBoundedReadKeepsNewestAcrossRigs(t *testing.T) {
	newest := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	// rig-b's newest run is 30 minutes older than rig-a's, so a correct merge
	// interleaves them: a-0, b-0, a-1, b-1, ...
	storeA := orderRunsStoreForScoped(t, "digest:rig:rig-a", newest)
	storeB := orderRunsStoreForScoped(t, "digest:rig:rig-b", newest.Add(-30*time.Minute))

	resolver := func(a orders.Order) ([]beads.OrdersStore, error) {
		switch a.Rig {
		case "rig-a":
			return []beads.OrdersStore{{Store: storeA}}, nil
		case "rig-b":
			return []beads.OrdersStore{{Store: storeB}}, nil
		}
		return nil, fmt.Errorf("unexpected rig %q", a.Rig)
	}

	var stdout, stderr bytes.Buffer
	code := doOrderHistoryBounded("digest", "", twoRigOrderRegistrations(), resolver,
		orderHistoryBounds{Limit: 2}, true, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("doOrderHistoryBounded = %d, want 0; stderr: %s", code, stderr.String())
	}

	payload := orderHistoryEntries(t, &stdout)
	if len(payload.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(payload.Entries))
	}
	// The two newest overall are rig-a's newest and rig-b's newest. Keeping
	// only rig-a's two newest would mean the limit was applied per store.
	gotRigs := []string{payload.Entries[0].Rig, payload.Entries[1].Rig}
	if gotRigs[0] != "rig-a" || gotRigs[1] != "rig-b" {
		t.Fatalf("rigs = %v, want [rig-a rig-b]; the bound must be applied after merging every store", gotRigs)
	}
}

// writeOrderHistoryMultiRigCity builds a city whose site config declares two
// rigs, so a rig-scoped order registration resolves to a real per-rig store.
func writeOrderHistoryMultiRigCity(t *testing.T) string {
	t.Helper()
	cityPath := t.TempDir()
	for _, dir := range []string{".gc", "orders", "formulas", "rig-a", "rig-b"} {
		if err := os.MkdirAll(filepath.Join(cityPath, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(cityPath, "city.toml"), []byte(`[workspace]
name = "test-city"

[[agent]]
name = "mayor"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityPath, ".gc", "site.toml"), []byte(`workspace_name = "test-city"

[[rig]]
name = "rig-a"
path = "./rig-a"

[[rig]]
name = "rig-b"
path = "./rig-b"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityPath, "orders", "digest.toml"), []byte(`[order]
trigger = "manual"
formula = "mol-digest"
scope = "rig"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GC_DOLT", "skip")
	t.Setenv("GC_BEADS", "file")
	t.Setenv("GC_BEADS_SCOPE_ROOT", "")
	return cityPath
}

// orderHistoryNoClientReason is the nilReason handed to routeOrderHistory when
// these tests pass a nil API client. routeRead logs it verbatim on the nil-client
// path, so it doubles as a sentinel: seeing it proves the call reached the API
// route and fell back only for want of a controller, and NOT seeing it proves an
// earlier branch claimed the read.
//
// Passing nil rather than an httptest server is deliberate. The routing decision
// is fully observable from the emitted route= line, which logRoute writes before
// any request is built, so a real listener would prove nothing extra -- and the
// untagged http_test_server census is a "cannot grow" ratchet (TESTING.md, Small
// and Source debt ratchets, ga-80po0c.2.2). Spending two call sites and a file
// against that ceiling to observe what stderr already reports would be a poor
// trade.
const orderHistoryNoClientReason = "no-controller-sentinel"

// TestRouteOrderHistoryBoundedStaysLocalWhenNameSpansRigs is the routing
// regression. The supervisor API request carries a single scoped_name, so it can
// only ever answer for ONE registration; routing a bare, ambiguous name there
// silently drops every other rig's runs while still printing a RIG column. The
// bounded read must therefore stay on the local iterator, which fans out across
// each matching registration's store.
func TestRouteOrderHistoryBoundedStaysLocalWhenNameSpansRigs(t *testing.T) {
	t.Setenv("GC_DEBUG", "1")

	cityPath := writeOrderHistoryMultiRigCity(t)
	cfg, err := loadCityConfig(cityPath, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	var stdout, stderr bytes.Buffer
	// A positive limit is the whole point: this is the path that used to route
	// to the API. --rig is deliberately empty, so "digest" is ambiguous.
	routeOrderHistory(cityPath, cfg, "digest", "", twoRigOrderRegistrations(), nil, orderHistoryNoClientReason,
		orderHistoryBounds{Limit: defaultOrderHistoryLimit}, false, &stdout, &stderr)

	if !strings.Contains(stderr.String(), "route=fallback reason=multi-rig") {
		t.Errorf("stderr missing multi-rig fallback: a bounded read of a name spanning %d rigs reached the API route, which answers for one scoped_name only:\n%s",
			len(twoRigOrderRegistrations()), stderr.String())
	}
	if strings.Contains(stderr.String(), "reason="+orderHistoryNoClientReason) {
		t.Errorf("read reached the API route before the multi-rig guard:\n%s", stderr.String())
	}
}

// TestRouteOrderHistoryBoundedUsesAPIWhenRigQualified is the guard on the other
// side: --rig names exactly one registration, so the API route stays available
// and this fix does not push every bounded read back onto the slower local scan.
func TestRouteOrderHistoryBoundedUsesAPIWhenRigQualified(t *testing.T) {
	t.Setenv("GC_DEBUG", "1")

	cityPath := writeOrderHistoryMultiRigCity(t)
	cfg, err := loadCityConfig(cityPath, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("loadCityConfig: %v", err)
	}

	var stdout, stderr bytes.Buffer
	routeOrderHistory(cityPath, cfg, "digest", "rig-a", twoRigOrderRegistrations(), nil, orderHistoryNoClientReason,
		orderHistoryBounds{Limit: defaultOrderHistoryLimit}, false, &stdout, &stderr)

	if strings.Contains(stderr.String(), "reason=multi-rig") {
		t.Errorf("rig-qualified read names exactly one registration and must keep the API route:\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "reason="+orderHistoryNoClientReason) {
		t.Errorf("rig-qualified read did not reach the API route:\n%s", stderr.String())
	}
}
