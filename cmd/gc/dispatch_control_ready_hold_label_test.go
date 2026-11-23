package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
)

// This file expresses the ga-x9kptu / ga-5736js acceptance criteria at the
// Go-level control-ready evaluation path: route-scoped results
// (filterReadyByRoute, and evaluateControlReady's routed groups) must
// exclude beads carrying an active hold, whatever its value names, while the
// assignee-scoped path (filterReadyByAssignee) stays hold-transparent.

func TestFilterReadyByRouteExcludesActiveHolds(t *testing.T) {
	older := time.Unix(100, 0)
	routed := map[string]string{beadmeta.RunTargetMetadataKey: "core/control-dispatcher"}
	ready := []beads.Bead{
		{ID: "ga-plain", CreatedAt: older, Metadata: routed},
		{ID: "ga-held-mayor", CreatedAt: older, Metadata: routed, Labels: []string{"hold:mayor"}},
		{ID: "ga-held-external", CreatedAt: older, Metadata: routed, Labels: []string{"hold:external"}},
		{ID: "ga-held-both", CreatedAt: older, Metadata: routed, Labels: []string{"hold:mayor", "hold:external"}},
		{ID: "ga-held-po", CreatedAt: older, Metadata: routed, Labels: []string{"hold:po"}},
		{ID: "ga-held-beside-cleared", CreatedAt: older, Metadata: routed, Labels: []string{"hold:cleared", "Hold:Release-Train"}},
		{ID: "ga-cleared", CreatedAt: older, Metadata: routed, Labels: []string{"hold:cleared"}},
	}
	got := filterReadyByRoute(ready, beadmeta.RunTargetMetadataKey, "core/control-dispatcher")
	want := []string{"ga-cleared", "ga-plain"} // created together, so in id order
	if !stringSlicesEqual(beadIDs(got), want) {
		t.Fatalf("filterReadyByRoute ids = %v, want %v (held beads must be excluded whatever the hold names, and a cleared hold is no hold)", beadIDs(got), want)
	}
}

// TestFilterReadyByRouteCutsAfterDroppingHeldBeads pins that held beads take
// no place in the route's scan limit: an unheld bead behind a full scan of held
// ones is still returned.
func TestFilterReadyByRouteCutsAfterDroppingHeldBeads(t *testing.T) {
	routed := map[string]string{beadmeta.RunTargetMetadataKey: "core/control-dispatcher"}
	var ready []beads.Bead
	for i := 0; i < workflowServeScanLimit; i++ {
		ready = append(ready, beads.Bead{ID: fmt.Sprintf("ga-held-%02d", i), CreatedAt: time.Unix(int64(100+i), 0), Metadata: routed, Labels: []string{"hold:po"}})
	}
	ready = append(ready, beads.Bead{ID: "ga-unheld", CreatedAt: time.Unix(1000, 0), Metadata: routed})
	got := filterReadyByRoute(ready, beadmeta.RunTargetMetadataKey, "core/control-dispatcher")
	if want := []string{"ga-unheld"}; !stringSlicesEqual(beadIDs(got), want) {
		t.Fatalf("filterReadyByRoute ids = %v, want %v", beadIDs(got), want)
	}
}

func TestFilterReadyByAssigneeDoesNotExcludeActiveHolds(t *testing.T) {
	ready := []beads.Bead{
		{ID: "ga-held-mayor", Assignee: "cand", Labels: []string{"hold:mayor"}},
	}
	got := filterReadyByAssignee(ready, "cand", workflowServeScanLimit)
	want := []string{"ga-held-mayor"}
	if !stringSlicesEqual(beadIDs(got), want) {
		t.Fatalf("filterReadyByAssignee ids = %v, want %v (assignee-scoped tier must stay hold-transparent)", beadIDs(got), want)
	}
}

// bd's native human label parks a bead for a person. It is not a hold (no
// hold:<value> label), but the route-scoped read must withhold it for the same
// reason: a worker cannot
// advance a bead that is waiting on a person, and serving it again every tick
// burns a turn each time. The assignee-scoped tier stays transparent to it.
func TestFilterReadyByRouteExcludesHumanLabel(t *testing.T) {
	older := time.Unix(100, 0)
	routed := map[string]string{beadmeta.RunTargetMetadataKey: "core/control-dispatcher"}
	ready := []beads.Bead{
		{ID: "ga-plain", CreatedAt: older, Metadata: routed},
		{ID: "ga-human", CreatedAt: older, Metadata: routed, Labels: []string{"human"}},
		{ID: "ga-human-and-held", CreatedAt: older, Metadata: routed, Labels: []string{"human", "hold:mayor"}},
	}
	got := filterReadyByRoute(ready, beadmeta.RunTargetMetadataKey, "core/control-dispatcher")
	want := []string{"ga-plain"}
	if !stringSlicesEqual(beadIDs(got), want) {
		t.Fatalf("filterReadyByRoute ids = %v, want %v (a human-labeled bead must be excluded from the route-scoped tier)", beadIDs(got), want)
	}
}

func TestFilterReadyByAssigneeDoesNotExcludeHumanLabel(t *testing.T) {
	ready := []beads.Bead{
		{ID: "ga-human-assigned", Assignee: "cand", Labels: []string{"human"}},
	}
	got := filterReadyByAssignee(ready, "cand", workflowServeScanLimit)
	want := []string{"ga-human-assigned"}
	if !stringSlicesEqual(beadIDs(got), want) {
		t.Fatalf("filterReadyByAssignee ids = %v, want %v (assignee-scoped tier must stay human-transparent)", beadIDs(got), want)
	}
}

func TestEvaluateControlReadyExcludesActiveHolds(t *testing.T) {
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})
	parsed, ok := parseControlReadyQuery(query)
	if !ok {
		t.Fatalf("parseControlReadyQuery: query not recognized: %q", query)
	}
	envList := []string{
		"GC_SESSION_NAME=gascity--control-dispatcher",
		"GC_ALIAS=gascity/control-dispatcher",
	}
	ready := []beads.Bead{
		{ID: "ga-routed", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "gascity/control-dispatcher"}},
		{ID: "ga-routed-held", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "gascity/control-dispatcher"}, Labels: []string{"hold:mayor"}},
		{ID: "ga-routed-held-both", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "gascity/control-dispatcher"}, Labels: []string{"hold:mayor", "hold:external"}},
		{ID: "ga-routed-held-po", Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "gascity/control-dispatcher"}, Labels: []string{"hold:po"}},
	}
	got := evaluateControlReady(ready, parsed, envList)
	want := []string{"ga-routed"}
	if !stringSlicesEqual(beadIDs(got), want) {
		t.Fatalf("evaluateControlReady ids = %v, want %v (hold-labeled routed bead must be excluded, including a bead carrying both hold labels at once)", beadIDs(got), want)
	}
}

// TestTryControlReadyScanExcludesActiveHoldsOnFileLeg is
// ga-5736js end-to-end on an in-process leg: a routed_to-matching bead carrying
// an active hold must not reach the control dispatcher's
// queue when the answer is served from a file leg's Ready read. The
// filter-level tests above pin the rule; this one pins that the leg read
// actually carries Labels far enough for the rule to fire. (End-to-end coverage
// originates from PR #4787.)
func TestTryControlReadyScanExcludesActiveHoldsOnFileLeg(t *testing.T) {
	cityDir, store := setUpControlReadyFileStoreCity(t)
	noBDOnPathForTest(t)

	target := "gascity/control-dispatcher"
	routed, err := store.Create(beads.Bead{Metadata: map[string]string{beadmeta.RoutedToMetadataKey: target}})
	if err != nil {
		t.Fatalf("create routed bead: %v", err)
	}
	heldMayor, err := store.Create(beads.Bead{
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: target},
		Labels:   []string{"hold:mayor"},
	})
	if err != nil {
		t.Fatalf("create %s routed bead: %v", "hold:mayor", err)
	}
	heldExternal, err := store.Create(beads.Bead{
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: target},
		Labels:   []string{"hold:external"},
	})
	if err != nil {
		t.Fatalf("create %s routed bead: %v", "hold:external", err)
	}
	heldPO, err := store.Create(beads.Bead{
		Metadata: map[string]string{beadmeta.RoutedToMetadataKey: target},
		Labels:   []string{"hold:po"},
	})
	if err != nil {
		t.Fatalf("create %s routed bead: %v", "hold:po", err)
	}

	agentCfg := config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"}
	query := workflowServeControlReadyQuery(agentCfg)

	queue, handled, err := tryControlReadyScan(query, cityDir, nil)
	if err != nil {
		t.Fatalf("tryControlReadyScan: %v", err)
	}
	if !handled {
		t.Fatalf("tryControlReadyScan: handled = false, want true for a control-ready query")
	}

	var gotIDs []string
	for _, b := range queue {
		gotIDs = append(gotIDs, b.ID)
	}
	wantIDs := []string{routed.ID}
	if !stringSlicesEqual(gotIDs, wantIDs) {
		t.Fatalf("queue ids = %#v, want %#v (held beads %s, %s and %s must not be auto-routed to the pool)", gotIDs, wantIDs, heldMayor.ID, heldExternal.ID, heldPO.ID)
	}
}

// TestTryControlReadyScanExcludesActiveHoldsOnBdLeg is ga-5736js
// end-to-end on a bd leg: a routed_to-matching bead carrying an active hold in
// the leg's single `bd ready --json` response must not reach the control
// dispatcher's queue, matching the file leg above. The hold labels are spelled
// as literal JSON in the fake bd script on purpose -- this test pins the wire
// format bd emits.
// (End-to-end coverage originates from PR #4787.)
func TestTryControlReadyScanExcludesActiveHoldsOnBdLeg(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	cityDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace]\nname = \"test-city\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}

	tmp := t.TempDir()
	bdPath := filepath.Join(tmp, "bd")
	target := "gascity/control-dispatcher"
	script := fmt.Sprintf(`#!/bin/sh
set -eu
case "$1" in
  list)
    exit 7
    ;;
esac
printf '[{"id":"ga-fallback-routed","metadata":{"gc.routed_to":"%s"}},{"id":"ga-fallback-held-mayor","metadata":{"gc.routed_to":"%s"},"labels":["hold:mayor"]},{"id":"ga-fallback-held-external","metadata":{"gc.routed_to":"%s"},"labels":["hold:external"]},{"id":"ga-fallback-held-po","metadata":{"gc.routed_to":"%s"},"labels":["hold:po"]}]'
`, target, target, target, target)
	if err := os.WriteFile(bdPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake bd: %v", err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GC_BEADS", "bd")

	agentCfg := config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"}
	query := workflowServeControlReadyQuery(agentCfg)

	queue, handled, err := tryControlReadyScan(query, cityDir, nil)
	if err != nil {
		t.Fatalf("tryControlReadyScan: %v", err)
	}
	if !handled {
		t.Fatalf("handled = false, want true")
	}
	var gotIDs []string
	for _, b := range queue {
		gotIDs = append(gotIDs, b.ID)
	}
	wantIDs := []string{"ga-fallback-routed"}
	if !stringSlicesEqual(gotIDs, wantIDs) {
		t.Fatalf("queue ids = %#v, want %#v (held beads ga-fallback-held-mayor, ga-fallback-held-external and ga-fallback-held-po must not be auto-routed to the pool)", gotIDs, wantIDs)
	}
}
