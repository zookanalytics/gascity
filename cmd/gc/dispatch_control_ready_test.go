package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
)

func TestParseControlReadyQueryRecognizesGeneratedQuery(t *testing.T) {
	// Dir+Name shaped so QualifiedName is a rig-scoped, binding-qualified
	// name ("fixture/core.control-dispatcher"): this is the only shape that
	// produces a non-empty bare-route alias (see TestControlDispatcherBareRoute).
	query := workflowServeControlReadyQuery(config.Agent{Name: "core.control-dispatcher", Dir: "fixture"}, "gascity--control-dispatcher")
	parsed, ok := parseControlReadyQuery(query)
	if !ok {
		t.Fatalf("parseControlReadyQuery: not recognized: %q", query)
	}
	if parsed.target != "fixture/core.control-dispatcher" {
		t.Errorf("target = %q, want %q", parsed.target, "fixture/core.control-dispatcher")
	}
	if parsed.controlSessionName != "gascity--control-dispatcher" {
		t.Errorf("controlSessionName = %q, want %q", parsed.controlSessionName, "gascity--control-dispatcher")
	}
	if parsed.bareTarget != "fixture/control-dispatcher" {
		t.Errorf("bareTarget = %q, want %q", parsed.bareTarget, "fixture/control-dispatcher")
	}
	if parsed.includeEphemeral {
		t.Errorf("includeEphemeral = true, want false (bd-1.0.4 default)")
	}
}

func TestParseControlReadyQueryIncludeEphemeralWhenBD105(t *testing.T) {
	query := workflowServeControlReadyQueryForBeads(
		config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"},
		config.BeadsConfig{BDCompatibility: config.BeadsBDCompatibility105},
	)
	parsed, ok := parseControlReadyQuery(query)
	if !ok {
		t.Fatalf("parseControlReadyQuery: not recognized")
	}
	if !parsed.includeEphemeral {
		t.Errorf("includeEphemeral = false, want true under bd-1.0.5 compatibility")
	}
}

func TestParseControlReadyQueryRejectsNonControlQuery(t *testing.T) {
	for _, q := range []string{
		"",
		"bd ready --json --limit=20",
		"GC_CONTROL_TARGET=core.control-dispatcher sh -c 'bd ready'", // missing the BD_EXPORT_AUTO=false marker prefix
	} {
		if _, ok := parseControlReadyQuery(q); ok {
			t.Errorf("parseControlReadyQuery(%q) = ok, want not recognized", q)
		}
	}
}

func TestControlReadyCandidatesPrecedenceDedupAndLegacyExpansion(t *testing.T) {
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})
	parsed, ok := parseControlReadyQuery(query)
	if !ok {
		t.Fatalf("parseControlReadyQuery: not recognized")
	}
	envList := []string{
		"GC_SESSION_NAME=gascity--control-dispatcher",
		"GC_ALIAS=gascity/control-dispatcher",
	}

	got := controlReadyCandidates(parsed, envList)
	want := []string{
		"gascity--control-dispatcher",
		"gascity--workflow-control",
		"gascity/control-dispatcher",
		"gascity/workflow-control",
	}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("controlReadyCandidates = %#v, want %#v", got, want)
	}
}

func TestControlReadyCandidatesSkipsEmptySlots(t *testing.T) {
	// "control-dispatcher" itself ends in the literal suffix "control-dispatcher",
	// so it also produces the bare "workflow-control" legacy variant.
	parsed := parsedControlReadyQuery{target: "control-dispatcher"}
	got := controlReadyCandidates(parsed, nil)
	want := []string{"control-dispatcher", "workflow-control"}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("controlReadyCandidates = %#v, want %#v", got, want)
	}
}

func TestControlReadyRoutesFiltersEmptyAliases(t *testing.T) {
	parsed := parsedControlReadyQuery{target: "core.control-dispatcher", bareTarget: "control-dispatcher"}
	got := controlReadyRoutes(parsed)
	want := []string{"core.control-dispatcher", "control-dispatcher"}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("controlReadyRoutes = %#v, want %#v", got, want)
	}
}

func TestFilterReadyByAssigneeExcludesEpicAndOtherAssignees(t *testing.T) {
	ready := []beads.Bead{
		{ID: "ga-epic-leak", Assignee: "cand", Type: "epic"},
		{ID: "ga-ready", Assignee: "cand", Type: "task"},
		{ID: "ga-other", Assignee: "someone-else", Type: "task"},
	}
	got := filterReadyByAssignee(ready, "cand", workflowServeScanLimit)
	if len(got) != 1 || got[0].ID != "ga-ready" {
		t.Fatalf("filterReadyByAssignee = %#v, want only ga-ready", got)
	}
}

func TestFilterReadyByAssigneeRespectsLimit(t *testing.T) {
	ready := make([]beads.Bead, 0, 5)
	for i := 0; i < 5; i++ {
		ready = append(ready, beads.Bead{ID: strings.Repeat("z", i+1), Assignee: "cand", Type: "task"})
	}
	got := filterReadyByAssignee(ready, "cand", 2)
	if len(got) != 2 {
		t.Fatalf("filterReadyByAssignee len = %d, want 2", len(got))
	}
}

func TestFilterReadyByRouteRequiresUnassignedAndSortsOldestFirst(t *testing.T) {
	newer := time.Unix(200, 0)
	older := time.Unix(100, 0)
	ready := []beads.Bead{
		{ID: "ga-assigned-routed", CreatedAt: older, Assignee: "someone", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "core/control-dispatcher"}},
		{ID: "ga-newer", CreatedAt: newer, Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "core/control-dispatcher"}},
		{ID: "ga-older", CreatedAt: older, Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "core/control-dispatcher"}},
		{ID: "ga-epic-routed", CreatedAt: older, Type: "epic", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "core/control-dispatcher"}},
		{ID: "ga-other-route", CreatedAt: older, Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "other"}},
	}
	got := filterReadyByRoute(ready, beadmeta.RunTargetMetadataKey, "core/control-dispatcher")
	want := []string{"ga-older", "ga-newer"}
	if !stringSlicesEqual(beadIDs(got), want) {
		t.Fatalf("filterReadyByRoute = %#v, want %#v", beadIDs(got), want)
	}
}

func TestMergeControlReadyGroupsDedupsPreservingFirstOccurrence(t *testing.T) {
	assigned := []beads.Bead{
		{ID: "ga-z-assigned"},
		{ID: "ga-dup", Metadata: map[string]string{"source": "assigned"}},
	}
	runTargetRouted := []beads.Bead{
		{ID: "ga-a-routed"},
		{ID: "ga-route-dup", Metadata: map[string]string{"source": "run-target"}},
	}
	routedToRouted := []beads.Bead{
		{ID: "ga-route-dup", Metadata: map[string]string{"source": "routed-to"}},
	}

	got := mergeControlReadyGroups(assigned, runTargetRouted, routedToRouted)
	wantIDs := []string{"ga-z-assigned", "ga-dup", "ga-a-routed", "ga-route-dup"}
	if !stringSlicesEqual(beadIDs(got), wantIDs) {
		t.Fatalf("mergeControlReadyGroups ids = %#v, want %#v", beadIDs(got), wantIDs)
	}
	for _, b := range got {
		if b.ID == "ga-route-dup" && b.Metadata["source"] != "run-target" {
			t.Fatalf("ga-route-dup source = %q, want first-seen %q", b.Metadata["source"], "run-target")
		}
	}
}

func TestMergeControlReadyGroupsSkipsInstantiatingWithoutMarkingSeen(t *testing.T) {
	assigned := []beads.Bead{
		{ID: "ga-instantiating-assigned", Metadata: map[string]string{beadmeta.InstantiatingMetadataKey: "true"}},
		{ID: "ga-assigned", Metadata: map[string]string{"gc.kind": "retry"}},
	}
	runTargetRouted := []beads.Bead{
		{ID: "ga-instantiating-routed", Metadata: map[string]string{beadmeta.InstantiatingMetadataKey: "true"}},
		{ID: "ga-routed", Metadata: map[string]string{"gc.kind": "scope-check"}},
	}
	// A later group re-surfacing the SAME id without the instantiating tag
	// must still be admitted -- the shell's jq reduce never marks an
	// instantiating occurrence as "seen".
	laterNonInstantiating := []beads.Bead{
		{ID: "ga-instantiating-assigned", Metadata: map[string]string{"gc.kind": "now-real"}},
	}

	got := mergeControlReadyGroups(assigned, runTargetRouted, laterNonInstantiating)
	wantIDs := []string{"ga-assigned", "ga-routed", "ga-instantiating-assigned"}
	if !stringSlicesEqual(beadIDs(got), wantIDs) {
		t.Fatalf("mergeControlReadyGroups ids = %#v, want %#v", beadIDs(got), wantIDs)
	}
}

// TestEvaluateControlReadyMatchesShellQueryPriority ports
// TestWorkflowServeControlReadyQueryPreservesQueryPriorityWhenMerging's
// scenario (cmd_convoy_dispatch_test.go) at the Go level: given the same
// parsed query + env, and a ready set shaped like what the per-leg Ready
// reads or the batched fallback would return, evaluateControlReady must merge candidates
// before routes and drop later ID duplicates exactly like the shell's jq
// reduce does.
func TestEvaluateControlReadyMatchesShellQueryPriority(t *testing.T) {
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})
	parsed, ok := parseControlReadyQuery(query)
	if !ok {
		t.Fatalf("parseControlReadyQuery: not recognized")
	}
	envList := []string{
		"GC_SESSION_NAME=gascity--control-dispatcher",
		"GC_ALIAS=gascity/control-dispatcher",
	}
	ready := []beads.Bead{
		{ID: "ga-z-assigned", Assignee: "gascity--control-dispatcher"},
		{ID: "ga-dup", Assignee: "gascity--control-dispatcher", Metadata: map[string]string{"source": "assigned"}},
		{ID: "ga-a-routed", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "gascity/control-dispatcher"}},
		{ID: "ga-route-dup", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "gascity/control-dispatcher", "source": "run-target"}},
		{ID: "ga-route-dup-2", Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "gascity/control-dispatcher"}},
	}
	// ga-route-dup also appears as a routed_to match with different content;
	// the run_target occurrence (checked first) must win.
	ready = append(ready, beads.Bead{ID: "ga-route-dup", Metadata: map[string]string{beadmeta.RoutedToMetadataKey: "gascity/control-dispatcher", "source": "routed-to"}})

	got := evaluateControlReady(ready, parsed, envList)
	wantIDs := []string{"ga-z-assigned", "ga-dup", "ga-a-routed", "ga-route-dup", "ga-route-dup-2"}
	if !stringSlicesEqual(beadIDs(got), wantIDs) {
		t.Fatalf("evaluateControlReady ids = %#v, want %#v", beadIDs(got), wantIDs)
	}
	for _, b := range got {
		if b.ID == "ga-route-dup" && b.Metadata["source"] != "run-target" {
			t.Fatalf("ga-route-dup source = %q, want first-seen %q", b.Metadata["source"], "run-target")
		}
	}
}

func TestEvaluateControlReadyExcludesEpicAndInstantiating(t *testing.T) {
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})
	parsed, ok := parseControlReadyQuery(query)
	if !ok {
		t.Fatalf("parseControlReadyQuery: not recognized")
	}
	envList := []string{
		"GC_SESSION_NAME=gascity--control-dispatcher",
		"GC_ALIAS=gascity/control-dispatcher",
	}
	ready := []beads.Bead{
		{ID: "ga-epic-leak", Assignee: "gascity--control-dispatcher", Type: "epic"},
		{ID: "ga-ready", Assignee: "gascity--control-dispatcher", Type: "task"},
		{ID: "ga-instantiating-routed", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "gascity/control-dispatcher", beadmeta.InstantiatingMetadataKey: "true"}},
		{ID: "ga-routed", Metadata: map[string]string{beadmeta.RunTargetMetadataKey: "gascity/control-dispatcher", "gc.kind": "scope-check"}},
	}

	got := evaluateControlReady(ready, parsed, envList)
	wantIDs := []string{"ga-ready", "ga-routed"}
	if !stringSlicesEqual(beadIDs(got), wantIDs) {
		t.Fatalf("evaluateControlReady ids = %#v, want %#v", beadIDs(got), wantIDs)
	}
}

func beadIDs(items []beads.Bead) []string {
	out := make([]string, len(items))
	for i, b := range items {
		out[i] = b.ID
	}
	return out
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- End-to-end: nextWorkflowServeBeads wiring (per-leg Ready + fallback) ---

// setUpControlReadyFileStoreCity builds a scope-local FileStore-backed city
// so tryControlReadyScan's leg read answers in-process without any bd/dolt
// process at all, and returns the opened
// store for seeding fixture beads directly.
func setUpControlReadyFileStoreCity(t *testing.T) (cityDir string, store *beads.FileStore) {
	t.Helper()
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir = t.TempDir()
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace]\nname = \"test-city\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(cityDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore: %v", err)
	}
	store, err := openScopeLocalFileStore(cityDir)
	if err != nil {
		t.Fatalf("openScopeLocalFileStore: %v", err)
	}
	return cityDir, store
}

// noBDOnPathForTest ensures no bd (or bd stub) is reachable via PATH, so a
// test can prove a code path made zero subprocess calls: any shell-out would
// fail with "command not found" rather than silently succeeding.
func noBDOnPathForTest(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

func TestTryControlReadyScanAnswersFileLegWithZeroSubprocessCalls(t *testing.T) {
	cityDir, store := setUpControlReadyFileStoreCity(t)
	noBDOnPathForTest(t)

	target := "gascity/control-dispatcher"
	ready, err := store.Create(beads.Bead{Assignee: target, Type: "task"})
	if err != nil {
		t.Fatalf("create ready bead: %v", err)
	}
	epic, err := store.Create(beads.Bead{Assignee: target, Type: "epic"})
	if err != nil {
		t.Fatalf("create epic bead: %v", err)
	}
	routed, err := store.Create(beads.Bead{Metadata: map[string]string{beadmeta.RoutedToMetadataKey: target}})
	if err != nil {
		t.Fatalf("create routed bead: %v", err)
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
	wantIDs := []string{ready.ID, routed.ID}
	if !stringSlicesEqual(gotIDs, wantIDs) {
		t.Fatalf("queue ids = %#v, want %#v (epic bead %s must be excluded)", gotIDs, wantIDs, epic.ID)
	}
}

func TestTryControlReadyScanReturnsUnhandledForNonControlQuery(t *testing.T) {
	cityDir := t.TempDir()
	_, handled, err := tryControlReadyScan("bd ready --json --limit=20", cityDir, nil)
	if handled {
		t.Fatalf("handled = true, want false for a non-control-ready query")
	}
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

// TestTryControlReadyScanMakesOneBDCallPerBdLeg pins the scan's cost on a
// bd-backed scope (ga-vnycm2.19): the whole tick is one `bd ready` call -- not
// the shell script's N per-candidate/route calls, and not the four-list prime
// plus ready projection the snapshot it replaced paid on every scan.
func TestTryControlReadyScanMakesOneBDCallPerBdLeg(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	cityDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace]\nname = \"test-city\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}

	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "bd.log")
	bdPath := filepath.Join(tmp, "bd")
	target := "gascity/control-dispatcher"
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> "%s"
case "$1" in
  ready)
    printf '[{"id":"ga-ready","status":"open","issue_type":"task","assignee":"%s"}]'
    ;;
  *)
    printf '[]'
    ;;
esac
`, logPath, target)
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
	if len(queue) != 1 || queue[0].ID != "ga-ready" {
		t.Fatalf("queue = %#v, want single ga-ready bead", queue)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read bd log: %v", err)
	}
	calls := strings.Split(strings.TrimSpace(string(logData)), "\n")
	if len(calls) != 1 || !strings.HasPrefix(calls[0], "ready ") || !strings.Contains(calls[0], "--include-ephemeral") {
		t.Fatalf("bd calls = %q, want exactly one `bd ready --include-ephemeral` call per scan", calls)
	}
}

// controlReadyBriefBd writes a fake bd that logs every invocation and answers
// bd ready with one row carrying text. When refuseBrief is set it answers any
// invocation carrying --brief the way bd 1.0.4 does: a usage error, exit 1.
func controlReadyBriefBd(t *testing.T, assignee string, refuseBrief bool) (logPath string) {
	t.Helper()
	tmp := t.TempDir()
	logPath = filepath.Join(tmp, "bd.log")
	refuse := ""
	if refuseBrief {
		refuse = `case " $* " in
  *" --brief "*) echo "Error: unknown flag: --brief" >&2; exit 1 ;;
esac
`
	}
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> "%s"
%scase " $* " in
  *" ready "*)
    printf '[{"id":"ga-ready","status":"open","issue_type":"task","assignee":"%s","description":"text no drain reads"}]'
    ;;
  *)
    printf '[]'
    ;;
esac
`, logPath, refuse, assignee)
	if err := os.WriteFile(filepath.Join(tmp, "bd"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake bd: %v", err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GC_BEADS", "bd")
	return logPath
}

// TestTryControlReadyScanAsksBdForBriefRows pins what a drain moves on a bd
// leg: the scan's one bd ready asks for brief rows, because nothing the scan
// hands its rows to reads their text.
func TestTryControlReadyScanAsksBdForBriefRows(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	cityDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace]\nname = \"test-city\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}
	target := "gascity/control-dispatcher"
	logPath := controlReadyBriefBd(t, target, false)
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})

	queue, handled, err := tryControlReadyScan(query, cityDir, nil)
	if err != nil || !handled {
		t.Fatalf("tryControlReadyScan: handled=%v err=%v", handled, err)
	}
	if len(queue) != 1 || queue[0].ID != "ga-ready" {
		t.Fatalf("queue = %#v, want single ga-ready bead", queue)
	}
	calls := readBDCalls(t, logPath)
	if len(calls) != 1 || !strings.HasPrefix(calls[0], "ready ") || !strings.Contains(calls[0], "--brief") {
		t.Fatalf("bd calls = %q, want one `bd ready ... --brief` call per scan", calls)
	}
}

// TestTryControlReadyScanReadsWholeRowsFromABdThatRefusesBrief pins the scan on
// a bd older than 1.2.0: the refused --brief costs one subprocess, once per
// scope, and every scan still answers.
func TestTryControlReadyScanReadsWholeRowsFromABdThatRefusesBrief(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	cityDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace]\nname = \"test-city\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}
	target := "gascity/control-dispatcher"
	logPath := controlReadyBriefBd(t, target, true)
	query := workflowServeControlReadyQuery(config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"})

	for scan := 1; scan <= 2; scan++ {
		queue, handled, err := tryControlReadyScan(query, cityDir, nil)
		if err != nil || !handled {
			t.Fatalf("scan %d: handled=%v err=%v", scan, handled, err)
		}
		if len(queue) != 1 || queue[0].ID != "ga-ready" {
			t.Fatalf("scan %d: queue = %#v, want single ga-ready bead", scan, queue)
		}
	}
	calls := readBDCalls(t, logPath)
	if len(calls) != 3 {
		t.Fatalf("bd calls = %q, want three: one refused --brief, then one whole-row read per scan", calls)
	}
	if !strings.Contains(calls[0], "--brief") || strings.Contains(calls[1], "--brief") || strings.Contains(calls[2], "--brief") {
		t.Fatalf("bd calls = %q, want only the first to carry --brief", calls)
	}
}

// TestControlReadyScopeShellReadyAsksForBriefRows pins the shell arm of the
// fallback: it asks bd for brief rows, and reads whole rows, once refused, from
// a bd that does not know the flag.
func TestControlReadyScopeShellReadyAsksForBriefRows(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		t.Run(fmt.Sprintf("refuseBrief=%v", refuse), func(t *testing.T) {
			configureIsolatedRuntimeEnv(t)
			logPath := controlReadyBriefBd(t, "", refuse)
			dir := t.TempDir()
			for read := 1; read <= 2; read++ {
				result, err := controlReadyFallbackReady(dir, dir, nil, false)
				if err != nil {
					t.Fatalf("read %d: controlReadyFallbackReady: %v", read, err)
				}
				if len(result) != 1 || result[0].ID != "ga-ready" {
					t.Fatalf("read %d: result = %#v, want ga-ready", read, result)
				}
			}
			calls := readBDCalls(t, logPath)
			want := []bool{true, true}
			if refuse {
				want = []bool{true, false, false}
			}
			if len(calls) != len(want) {
				t.Fatalf("bd calls = %q, want %d", calls, len(want))
			}
			for i, brief := range want {
				if strings.Contains(calls[i], "--brief") != brief {
					t.Errorf("bd call %d = %q, want --brief=%v", i, calls[i], brief)
				}
			}
		})
	}
}

// readyPagePayload renders one full page of open task rows routed elsewhere
// (controlReadyFallbackLimit of them), then one row per extra id assigned to
// assignee, as the array bd ready prints.
func readyPagePayload(t *testing.T, assignee string, extra ...string) []byte {
	t.Helper()
	rows := make([]map[string]string, 0, controlReadyFallbackLimit+len(extra))
	for i := 0; i < controlReadyFallbackLimit; i++ {
		rows = append(rows, map[string]string{"id": fmt.Sprintf("ga-other-%d", i), "status": "open", "issue_type": "task", "assignee": "gascity/worker"})
	}
	for _, id := range extra {
		rows = append(rows, map[string]string{"id": id, "status": "open", "issue_type": "task", "assignee": assignee})
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("marshal ready rows: %v", err)
	}
	return payload
}

// installPagedReadyBD puts a fake bd first on PATH. It appends each call's
// arguments to the log it returns, answers a read with no limit (--limit 0 or
// --limit=0) with whole, and answers any other ready read with page.
func installPagedReadyBD(t *testing.T, page, whole []byte) (logPath string) {
	t.Helper()
	tmp := t.TempDir()
	pagePath := filepath.Join(tmp, "page.json")
	wholePath := filepath.Join(tmp, "whole.json")
	for path, payload := range map[string][]byte{pagePath: page, wholePath: whole} {
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}
	logPath = filepath.Join(tmp, "bd.log")
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
case "$*" in
  *"--limit 0"*|*"--limit=0"*) cat %q ;;
  *ready*) cat %q ;;
  *) printf '[]' ;;
esac
`, logPath, wholePath, pagePath)
	if err := os.WriteFile(filepath.Join(tmp, "bd"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake bd: %v", err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GC_BEADS", "bd")
	return logPath
}

func readBDCalls(t *testing.T, logPath string) []string {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read bd log: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

// TestTryControlReadyScanReadsTheWholeReadySetOnABdLeg pins the bd leg's read:
// brief rows, one page first, and the whole ready set when that page comes back
// full. The scan picks out the dispatcher's own beads after the read, so a page
// cut short would hide a control bead that sorts after other agents' work.
func TestTryControlReadyScanReadsTheWholeReadySetOnABdLeg(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	cityDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace]\nname = \"test-city\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}
	target := "gascity/control-dispatcher"
	logPath := installPagedReadyBD(t,
		readyPagePayload(t, target),
		readyPagePayload(t, target, "ga-control"))

	agentCfg := config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"}
	queue, handled, err := tryControlReadyScan(workflowServeControlReadyQuery(agentCfg), cityDir, nil)
	if err != nil || !handled {
		t.Fatalf("tryControlReadyScan = handled %t, err %v; want handled, nil", handled, err)
	}
	if len(queue) != 1 || queue[0].ID != "ga-control" {
		t.Fatalf("queue = %#v, want the dispatcher's ga-control from past the first page", queue)
	}
	want := []string{
		"ready --json --include-ephemeral --limit 5000 --brief",
		"ready --json --include-ephemeral --limit 0 --brief",
	}
	if calls := readBDCalls(t, logPath); strings.Join(calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("bd calls = %q, want a brief page and then the whole set %q", calls, want)
	}
}

// TestControlReadyFallbackReadyReadsTheWholeSetWhenThePageFills pins the shell
// arm's read: a page that comes back full may have stopped short of the
// dispatcher's own beads, so the arm reads the whole ready set again.
func TestControlReadyFallbackReadyReadsTheWholeSetWhenThePageFills(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	logPath := installPagedReadyBD(t,
		readyPagePayload(t, ""),
		readyPagePayload(t, "", "ga-past-the-page"))

	dir := t.TempDir()
	result, err := controlReadyFallbackReady(dir, dir, nil, false)
	if err != nil {
		t.Fatalf("controlReadyFallbackReady: %v", err)
	}
	if len(result) != controlReadyFallbackLimit+1 || !containsID(result, "ga-past-the-page") {
		t.Fatalf("len(result) = %d (row past the page present: %t), want the whole set of %d", len(result), containsID(result, "ga-past-the-page"), controlReadyFallbackLimit+1)
	}
	calls := readBDCalls(t, logPath)
	if len(calls) != 2 || !strings.Contains(calls[0], "--limit=5000") || !strings.Contains(calls[1], "--limit=0") {
		t.Fatalf("bd calls = %q, want the page and then the whole set", calls)
	}
}

// TestControlReadyFallbackReadyReadsOncePastAShortPage is the other half: a
// page below the limit is the whole ready set, read in one call.
func TestControlReadyFallbackReadyReadsOncePastAShortPage(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	logPath := installPagedReadyBD(t, []byte(`[{"id":"ga-fallback-only"}]`), []byte(`[]`))

	dir := t.TempDir()
	result, err := controlReadyFallbackReady(dir, dir, nil, false)
	if err != nil {
		t.Fatalf("controlReadyFallbackReady: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("len(result) = %d, want 1", len(result))
	}
	if calls := readBDCalls(t, logPath); len(calls) != 1 {
		t.Fatalf("bd calls = %q, want one read", calls)
	}
}

func TestNextWorkflowServeBeadsNonControlQueryUsesOriginalShellPath(t *testing.T) {
	tmp := t.TempDir()
	bdPath := filepath.Join(tmp, "bd")
	if err := os.WriteFile(bdPath, []byte(`#!/bin/sh
printf '[{"id":"ga-plain"}]'
`), 0o755); err != nil {
		t.Fatalf("write fake bd: %v", err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))

	got, err := nextWorkflowServeBeads("bd ready --json --limit=20", t.TempDir(), nil)
	if err != nil {
		t.Fatalf("nextWorkflowServeBeads: %v", err)
	}
	if len(got) != 1 || got[0].ID != "ga-plain" {
		t.Fatalf("nextWorkflowServeBeads = %#v, want [{ga-plain}]", got)
	}
}
