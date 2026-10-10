package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/config"
)

// These tests pin the readiness scan of a control dispatcher whose scope ledger
// is a bd workspace: every scan is one `bd ready` call, read fresh, and it never
// primes the open and in-progress sets the way the in-process snapshot does.

const bdScopeControlTarget = "gascity/control-dispatcher"

// fakeBdScope is a stand-in `bd` on PATH for a bd-backed control scope. It
// records every invocation with the Dolt port it was given, reports the
// configured version, and answers `ready` with the rows the test last
// published, limited the way bd limits them: `--limit=N` returns the first N
// rows, `--limit=0` returns every row, and a read with no `--limit` returns
// bd's default of 100. Every other subcommand answers an empty list.
type fakeBdScope struct {
	logPath   string
	envPath   string
	readyPath string
}

// installFakeBdScope puts the fake on PATH. An empty version makes
// `bd version` fail.
func installFakeBdScope(t *testing.T, version string) *fakeBdScope {
	t.Helper()
	tmp := t.TempDir()
	f := &fakeBdScope{
		logPath:   filepath.Join(tmp, "bd.log"),
		envPath:   filepath.Join(tmp, "bd.env"),
		readyPath: filepath.Join(tmp, "ready.jsonl"),
	}
	versionArm := "exit 3"
	if version != "" {
		versionArm = fmt.Sprintf("printf 'bd version %s (fake)\\n'; exit 0", version)
	}
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
printf '%%s GC_DOLT_PORT=%%s BEADS_DOLT_SERVER_PORT=%%s\n' "$1" "${GC_DOLT_PORT:-}" "${BEADS_DOLT_SERVER_PORT:-}" >> %q
case "$1" in
  version) %s ;;
esac
case " $* " in
  *" ready "*)
    limit=100
    prev=
    for arg in "$@"; do
      case "$arg" in
        --limit=*) limit="${arg#--limit=}" ;;
      esac
      [ "$prev" = "--limit" ] && limit="$arg"
      prev="$arg"
    done
    awk -v limit="$limit" 'BEGIN { printf "[" } limit == 0 || NR <= limit { if (n++) printf ","; printf "%%s", $0 } END { printf "]" }' %q
    ;;
  *) printf '[]' ;;
esac
`, f.logPath, f.envPath, versionArm, f.readyPath)
	if err := os.WriteFile(filepath.Join(tmp, "bd"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake bd: %v", err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	f.publish(t)
	return f
}

// publish sets the ledger's ready set to rows, in bd's ready order and in the
// JSON shape bd emits. The fake stores one row per line so that it can return
// the first N.
func (f *fakeBdScope) publish(t *testing.T, rows ...map[string]any) {
	t.Helper()
	var data []byte
	for _, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("marshal ready row: %v", err)
		}
		data = append(append(data, line...), '\n')
	}
	if err := os.WriteFile(f.readyPath, data, 0o644); err != nil {
		t.Fatalf("publish ready rows: %v", err)
	}
}

// calls returns the recorded invocations whose subcommand, the first argument
// that is not a flag, is sub.
func (f *fakeBdScope) calls(t *testing.T, sub string) []string {
	t.Helper()
	data, err := os.ReadFile(f.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read fake bd log: %v", err)
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		for _, field := range strings.Fields(line) {
			if strings.HasPrefix(field, "-") {
				continue
			}
			if field == sub {
				out = append(out, line)
			}
			break
		}
	}
	return out
}

func routedControlRow(id string) map[string]any {
	return map[string]any{
		"id":         id,
		"issue_type": "task",
		"status":     "open",
		"metadata":   map[string]string{beadmeta.RoutedToMetadataKey: bdScopeControlTarget},
	}
}

// setUpControlReadyBdCity builds a city whose scope ledger is a bd workspace.
func setUpControlReadyBdCity(t *testing.T) string {
	t.Helper()
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "bd")
	cityDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace]\nname = \"test-city\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}
	return cityDir
}

func scanBdScopeControlReady(t *testing.T, cityDir string) []string {
	t.Helper()
	agentCfg := config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"}
	queue, handled, err := tryControlReadyFromCacheOrFallback(workflowServeControlReadyQuery(agentCfg), cityDir, nil)
	if err != nil {
		t.Fatalf("control-ready scan: %v", err)
	}
	if !handled {
		t.Fatal("control-ready scan: query not recognized as a control-ready query")
	}
	ids := make([]string, 0, len(queue))
	for _, item := range queue {
		ids = append(ids, item.ID)
	}
	return ids
}

// TestControlReadyScanOfBdScopeIsOneReadyCallWithoutAPrime pins the cost of a
// scan: one `bd ready` page, and none of the `bd list` / `bd query` calls a
// prime of the open and in-progress sets makes. On a ledger of a thousand open
// beads that prime is what made each scan take seconds. The read is bounded
// because bd answers a bounded ready read from an indexed page of ids, which
// costs less than its unbounded read.
func TestControlReadyScanOfBdScopeIsOneReadyCallWithoutAPrime(t *testing.T) {
	cityDir := setUpControlReadyBdCity(t)
	bd := installFakeBdScope(t, "1.3.1")
	bd.publish(t, routedControlRow("ga-ready-1"))

	if got, want := scanBdScopeControlReady(t, cityDir), []string{"ga-ready-1"}; !slices.Equal(got, want) {
		t.Fatalf("queue = %v, want %v", got, want)
	}
	page := fmt.Sprintf("--limit=%d", controlReadyPageLimit)
	if got := bd.calls(t, "ready"); len(got) != 1 || !slices.Contains(strings.Fields(got[0]), page) {
		t.Fatalf("bd ready calls = %q, want exactly one, carrying %s", got, page)
	}
	for _, sub := range []string{"list", "query"} {
		if got := bd.calls(t, sub); len(got) != 0 {
			t.Fatalf("bd %s calls = %q, want none: the scan primed the open set instead of reading ready beads", sub, got)
		}
	}
}

// TestControlReadyScanOfBdScopeReadsTheWholeReadySet pins that the scan sees
// every ready row. It picks the dispatcher's own beads out of the read in Go,
// so a read that stopped at a page of other agents' ready beads would lose
// them, and the dispatcher would report no work while its control bead sat
// ready. A page that comes back full is therefore followed by bd's unbounded
// read.
func TestControlReadyScanOfBdScopeReadsTheWholeReadySet(t *testing.T) {
	cityDir := setUpControlReadyBdCity(t)
	bd := installFakeBdScope(t, "1.3.1")
	rows := make([]map[string]any, 0, controlReadyPageLimit+1)
	for i := range controlReadyPageLimit {
		rows = append(rows, map[string]any{
			"id":         fmt.Sprintf("ga-other-%d", i),
			"issue_type": "task",
			"status":     "open",
			"priority":   0,
			"metadata":   map[string]string{beadmeta.RoutedToMetadataKey: "gascity/worker"},
		})
	}
	rows = append(rows, routedControlRow("ga-control-1"))
	bd.publish(t, rows...)

	if got, want := scanBdScopeControlReady(t, cityDir), []string{"ga-control-1"}; !slices.Equal(got, want) {
		t.Fatalf("queue = %v, want %v: the scan stopped at a full page of %d ready beads routed elsewhere", got, want, controlReadyPageLimit)
	}
	ready := bd.calls(t, "ready")
	if len(ready) != 2 || !slices.Contains(strings.Fields(ready[1]), "--limit=0") {
		t.Fatalf("bd ready calls = %q, want the page and then one unbounded --limit=0 read", ready)
	}
}

// TestControlReadyScanOfBdScopeAsksForBriefRowsWhenBdSupportsIt pins the
// projection: the scan reads no free-form text, so it asks bd to omit it.
func TestControlReadyScanOfBdScopeAsksForBriefRowsWhenBdSupportsIt(t *testing.T) {
	cityDir := setUpControlReadyBdCity(t)
	bd := installFakeBdScope(t, "1.2.1")
	bd.publish(t, routedControlRow("ga-ready-1"))

	scanBdScopeControlReady(t, cityDir)
	ready := bd.calls(t, "ready")
	if len(ready) != 1 || !strings.Contains(ready[0], "--brief") {
		t.Fatalf("bd ready calls = %q, want one carrying --brief", ready)
	}
}

// TestControlReadyScanOfBdScopeOmitsBriefWhenBdCannotTakeIt covers the two bd
// binaries that must not be handed --brief: one older than the release that
// added it, and one whose version cannot be read. Both still answer the scan.
func TestControlReadyScanOfBdScopeOmitsBriefWhenBdCannotTakeIt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version string
	}{
		{name: "bd predates --brief", version: "1.2.0"},
		{name: "bd version unreadable", version: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cityDir := setUpControlReadyBdCity(t)
			bd := installFakeBdScope(t, tc.version)
			bd.publish(t, routedControlRow("ga-ready-1"))

			if got, want := scanBdScopeControlReady(t, cityDir), []string{"ga-ready-1"}; !slices.Equal(got, want) {
				t.Fatalf("queue = %v, want %v", got, want)
			}
			ready := bd.calls(t, "ready")
			if len(ready) != 1 || strings.Contains(ready[0], "--brief") {
				t.Fatalf("bd ready calls = %q, want one without --brief", ready)
			}
		})
	}
}

// TestControlReadyScanOfBdScopeProbesTheBdVersionOnce pins that the --brief
// probe is paid once per bd binary, not once per scan.
func TestControlReadyScanOfBdScopeProbesTheBdVersionOnce(t *testing.T) {
	cityDir := setUpControlReadyBdCity(t)
	bd := installFakeBdScope(t, "1.3.1")

	for range 3 {
		scanBdScopeControlReady(t, cityDir)
	}
	if got := bd.calls(t, "version"); len(got) != 1 {
		t.Fatalf("bd version calls = %q, want exactly one across three scans", got)
	}
	if got := bd.calls(t, "ready"); len(got) != 3 {
		t.Fatalf("bd ready calls = %q, want one per scan", got)
	}
}

// TestControlReadyScanOfBdScopeReadsTheLedgerFreshEveryScan is the freshness
// contract for the bd read: a control the dispatch closed is gone from the next
// scan, and a control a worker's close made ready is in it.
func TestControlReadyScanOfBdScopeReadsTheLedgerFreshEveryScan(t *testing.T) {
	cityDir := setUpControlReadyBdCity(t)
	bd := installFakeBdScope(t, "1.3.1")

	bd.publish(t, routedControlRow("ga-retry-1"), routedControlRow("ga-check-1"))
	if got, want := sortedIDs(scanBdScopeControlReady(t, cityDir)), []string{"ga-check-1", "ga-retry-1"}; !slices.Equal(got, want) {
		t.Fatalf("first scan = %v, want %v", got, want)
	}

	bd.publish(t, routedControlRow("ga-check-1"), routedControlRow("ga-finalize-1"))
	if got, want := sortedIDs(scanBdScopeControlReady(t, cityDir)), []string{"ga-check-1", "ga-finalize-1"}; !slices.Equal(got, want) {
		t.Fatalf("second scan = %v, want %v: the scan answered from an earlier read", got, want)
	}
}

// TestControlReadyScanOfBdScopeRunsWithTheQueryDoltCoordinates pins the guard
// the generated query carries into the read: bd runs with the Dolt port of the
// dispatcher's own environment, even when the runtime env the serve loop built
// at startup names none. mergeRuntimeEnv drops the inherited port, so without
// the guard bd would be pointed at port 0.
func TestControlReadyScanOfBdScopeRunsWithTheQueryDoltCoordinates(t *testing.T) {
	cityDir := setUpControlReadyBdCity(t)
	bd := installFakeBdScope(t, "1.3.1")
	bd.publish(t, routedControlRow("ga-ready-1"))
	t.Setenv("GC_DOLT_HOST", "127.0.0.1")
	t.Setenv("GC_DOLT_PORT", "34567")

	agentCfg := config.Agent{Name: config.ControlDispatcherAgentName, Dir: "gascity"}
	queue, _, err := tryControlReadyFromCacheOrFallback(workflowServeControlReadyQuery(agentCfg), cityDir, map[string]string{})
	if err != nil {
		t.Fatalf("control-ready scan: %v", err)
	}
	if len(queue) != 1 || queue[0].ID != "ga-ready-1" {
		t.Fatalf("queue = %+v, want [ga-ready-1]", queue)
	}
	data, err := os.ReadFile(bd.envPath)
	if err != nil {
		t.Fatalf("read fake bd env log: %v", err)
	}
	var readyEnv []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.HasPrefix(line, "--readonly ") {
			readyEnv = append(readyEnv, line)
		}
	}
	if want := "--readonly GC_DOLT_PORT=34567 BEADS_DOLT_SERVER_PORT=34567"; len(readyEnv) != 1 || readyEnv[0] != want {
		t.Fatalf("bd ready env = %q, want [%q]", readyEnv, want)
	}
}

func sortedIDs(ids []string) []string {
	out := slices.Clone(ids)
	slices.Sort(out)
	return out
}
