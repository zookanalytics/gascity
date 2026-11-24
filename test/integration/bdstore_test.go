//go:build integration && unix

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/beadstest"
	"github.com/gastownhall/gascity/internal/doctor"
	"github.com/gastownhall/gascity/test/dolttest"
	"golang.org/x/mod/semver"
)

const bdInitTimeout = 60 * time.Second

// TestBdStoreConformance runs the beads conformance suite against BdStore
// backed by a real dolt server. This proves the full stack works:
// dolt server → bd CLI → BdStore → beads.Store interface.
//
// Each subtest gets a fresh database directory where bd auto-starts a
// dolt server on a unique port. This avoids port conflicts and lets bd
// manage the server lifecycle.
//
// Requires: Dolt and bd binaries configured via PATH or the integration
// override env vars.
func TestBdStoreConformance(t *testing.T) {
	// Skipped because BdStore fails this suite's
	// ParentIDNamesARowThisStoreDoesNotHave row on every pinned bd: it hands
	// ParentID to bd as --parent, and bd 1.3.x refuses a parent it cannot
	// resolve ("parent issue ... not found"), the divergence ga-6od57 tracks.
	// The bd 1.0.4 reason the skip was added for (ga-e7z613) no longer applies;
	// every pin carries the #3691 empty-DB guard. The suite is in the gating
	// integration-smoke filter, so un-skipping it (ga-x4j5ka) needs that row
	// fixed or opted out, and a green run on both pins.
	t.Skip("BdStore fails ParentIDNamesARowThisStoreDoesNotHave: bd refuses an unresolvable --parent (ga-6od57); un-skip is ga-x4j5ka")
	requireDoltIntegration(t)
	env := newIsolatedToolEnv(t, true)

	rootDir := t.TempDir()
	doltDataDir := filepath.Join(rootDir, "dolt")
	workspacesDir := filepath.Join(rootDir, "workspaces")
	serverPort := startSharedDoltServer(t, env, doltDataDir)
	var dbCounter atomic.Int64

	// Factory: each call creates a fresh workspace bound to the shared Dolt
	// server. This avoids the slow startup/shutdown tail from embedded local
	// server mode and keeps the conformance suite within CI time limits.
	newStore := func() beads.Store {
		n := dbCounter.Add(1)
		prefix := fmt.Sprintf("ct%d", n)

		// Create isolated workspace directory.
		wsDir := filepath.Join(workspacesDir, fmt.Sprintf("ws-%d", n))
		if err := os.MkdirAll(wsDir, 0o755); err != nil {
			t.Fatalf("creating workspace: %v", err)
		}

		// Initialize git repo (bd init requires it).
		gitCmd := exec.Command("git", "init", "--quiet")
		gitCmd.Dir = wsDir
		if out, err := gitCmd.CombinedOutput(); err != nil {
			t.Fatalf("git init: %v: %s", err, out)
		}

		runBDInit(t, env, wsDir, prefix, serverPort)

		configureCustomTypes(t, env, wsDir, doctor.RequiredCustomTypes)

		return beads.NewBdStore(wsDir, pinnedBdStoreCommandRunner())
	}

	// Run conformance suite. We skip RunSequentialIDTests because BdStore
	// uses bd's ID format (prefix-XXXX), not gc-N sequential format.
	beadstest.RunStoreTests(t, newStore)
	beadstest.RunMetadataTests(t, newStore)
	beadstest.RunCloseReasonTests(t, newStore)
}

// startSharedDoltServer starts one explicit Dolt SQL server for the test and
// returns its port. Using a shared server keeps bd commands fast and avoids
// the embedded local-server shutdown delays seen in CI.
func startSharedDoltServer(t *testing.T, env []string, dataDir string) string {
	t.Helper()

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("creating dolt data dir: %v", err)
	}

	srv := dolttest.StartSQLServer(t, dolttest.SQLServerSpec{
		Dolt: doltBinary,
		Args: func(port int) []string {
			return []string{"sql-server", "-H", "127.0.0.1", "-P", strconv.Itoa(port), "--data-dir", dataDir}
		},
		Env:     env,
		LogPath: filepath.Join(dataDir, "sql-server.log"),
	})
	t.Cleanup(func() {
		_ = srv.Process.Kill()
		<-srv.Exited
	})
	return strconv.Itoa(srv.Port)
}

// runBDInit initializes beads against the shared Dolt server with a bounded wait.
func runBDInit(t *testing.T, env []string, dir, prefix, port string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), bdInitTimeout)
	defer cancel()

	bdInit := exec.CommandContext(ctx, bdBinary, "init", "--server", "--server-host", "127.0.0.1", "--server-port", port, "-p", prefix, "--skip-hooks", "--skip-agents")
	bdInit.Dir = dir
	bdInit.Env = isolateBdHomeEnv(env)
	out, err := bdInit.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("bd init timed out after %s: %s", bdInitTimeout, out)
	}
	if err != nil {
		t.Fatalf("bd init: %v: %s", err, out)
	}
}

// TestBdStoreMailWispInsert verifies that creating an ephemeral mail message
// bead via BdStore succeeds through the full bd CLI → Dolt SQL stack.
//
// Regression tripwire for the 2026-06-11 P0 incident: gc mail send broke in
// production with "Field 'id' doesn't have a default value" because the bd CLI
// code omitted id on INSERT INTO wisp_events while a newer schema migration had
// dropped the DEFAULT (UUID()). The e2e mail tests use the file beads provider
// and never touch Dolt SQL; this test closes that gap by exercising the
// BdStore → bd create --ephemeral → Dolt → wisp_events INSERT path directly.
func TestBdStoreMailWispInsert(t *testing.T) {
	requireDoltIntegration(t)
	store := newRealBdStoreWorkspace(t, "mc")

	// Create an ephemeral message bead — exercises bd create --ephemeral →
	// Dolt SQL INSERT INTO wisps + INSERT INTO wisp_events.
	// A NOT NULL / no-DEFAULT failure on wisp_events.id reproduces the incident.
	sent, err := store.Create(beads.Bead{
		Title:     "hello from bdstore mail regression",
		Type:      "message",
		Assignee:  "builder",
		Ephemeral: true,
	})
	if err != nil {
		t.Fatalf("BdStore Create ephemeral message (wisp_events INSERT): %v", err)
	}
	if !sent.Ephemeral {
		t.Fatalf("Ephemeral = false on returned bead %s, want true", sent.ID)
	}
	if sent.ID == "" {
		t.Fatal("returned bead has empty ID")
	}

	// List with TierWisps to confirm the bead is readable after the INSERT.
	results, err := store.List(beads.ListQuery{
		TierMode: beads.TierWisps,
		Assignee: "builder",
	})
	if err != nil {
		t.Fatalf("BdStore List wisp beads: %v", err)
	}
	var found bool
	for _, b := range results {
		if b.ID == sent.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("sent bead %s not in BdStore List(TierWisps); got %d beads total", sent.ID, len(results))
	}
}

// newRealBdStoreWorkspace starts a Dolt server, initializes one bd workspace
// against it with prefix, and returns a BdStore over it running the isolated
// tool environment's bd.
func newRealBdStoreWorkspace(t *testing.T, prefix string) *beads.BdStore {
	t.Helper()
	env := newIsolatedToolEnv(t, true)

	rootDir := t.TempDir()
	doltDataDir := filepath.Join(rootDir, "dolt")
	wsDir := filepath.Join(rootDir, "ws")
	serverPort := startSharedDoltServer(t, env, doltDataDir)

	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("creating workspace: %v", err)
	}
	gitCmd := exec.Command("git", "init", "--quiet")
	gitCmd.Dir = wsDir
	if out, err := gitCmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	runBDInit(t, env, wsDir, prefix, serverPort)
	configureCustomTypes(t, env, wsDir, doctor.RequiredCustomTypes)

	return beads.NewBdStore(wsDir, isolatedBdStoreCommandRunner(env))
}

// TestBdStoreMailWispInsertIsolatesHOMEFromSharedServerConfig mirrors
// TestBdStoreMailWispInsert but deliberately points env's HOME at a
// shared-server config.yaml before running the exact same
// runBDInit/configureCustomTypes/pinnedBdStoreCommandRunnerWithEnv chain.
//
// newIsolatedToolEnv sets env's own HOME explicitly (via isolateGCHomeEnv/
// integrationEnvFor), so t.Setenv("HOME", ...) cannot reach it. This test
// substitutes a controlled, worst-case stand-in for "whatever the
// real invoking user's real home happens to contain" (on a fleet host that
// runs a real shared bd/dolt server out of that real home — this one does —
// that's a real shared-server config, not a hypothetical) so the
// reproduction is deterministic and machine-independent rather than
// depending on whatever this specific test run's real $HOME happens to hold.
func TestBdStoreMailWispInsertIsolatesHOMEFromSharedServerConfig(t *testing.T) {
	requireDoltIntegration(t)
	env := newIsolatedToolEnv(t, true)

	pollutedHome := t.TempDir()
	beadsDir := filepath.Join(pollutedHome, ".beads")
	if err := os.MkdirAll(beadsDir, 0o755); err != nil {
		t.Fatalf("creating polluted HOME .beads dir: %v", err)
	}
	cfg := "no-db: true\ndolt:\n    shared-server: true\n"
	if err := os.WriteFile(filepath.Join(beadsDir, "config.yaml"), []byte(cfg), 0o644); err != nil {
		t.Fatalf("writing polluted HOME config.yaml: %v", err)
	}
	env = replaceEnv(env, "HOME", pollutedHome)
	// Pollute the process HOME too, so the BdStore runner below inherits it
	// unless its own HOME pin wins.
	t.Setenv("HOME", pollutedHome)

	rootDir := t.TempDir()
	doltDataDir := filepath.Join(rootDir, "dolt")
	wsDir := filepath.Join(rootDir, "ws")
	serverPort := startSharedDoltServer(t, env, doltDataDir)

	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("creating workspace: %v", err)
	}
	gitCmd := exec.Command("git", "init", "--quiet")
	gitCmd.Dir = wsDir
	if out, err := gitCmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	runBDInit(t, env, wsDir, "hi", serverPort)
	configureCustomTypes(t, env, wsDir, doctor.RequiredCustomTypes)

	isoHome := parseEnvList(isolateBdHomeEnv(env))["HOME"]
	if isoHome == pollutedHome {
		t.Fatalf("isolateBdHomeEnv left HOME at the polluted %s; env has no GC_HOME to isolate to", pollutedHome)
	}
	store := beads.NewBdStore(wsDir, pinnedBdStoreCommandRunnerWithEnv(map[string]string{"HOME": isoHome}))

	sent, err := store.Create(beads.Bead{
		Title:     "home-isolation probe",
		Type:      "message",
		Assignee:  "builder",
		Ephemeral: true,
	})
	if err != nil {
		t.Fatalf("BdStore Create under a shared-server HOME: %v", err)
	}

	results, err := store.List(beads.ListQuery{
		TierMode: beads.TierWisps,
		Assignee: "builder",
	})
	if err != nil {
		t.Fatalf("BdStore List under a shared-server HOME: %v", err)
	}
	var found bool
	for _, b := range results {
		if b.ID == sent.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("sent bead %s not in BdStore List(TierWisps) under a shared-server HOME; got %d beads total", sent.ID, len(results))
	}
}

// TestBdStoreIDsListReadsByID proves the IDs pushdown against a real bd and
// Dolt. An IDs list reaches bd list as --id, in chunks when it is long, and an
// id --id cannot carry is read alone through a quoted bd query.
func TestBdStoreIDsListReadsByID(t *testing.T) {
	requireDoltIntegration(t)
	env := newIsolatedToolEnv(t, true)

	rootDir := t.TempDir()
	wsDir := filepath.Join(rootDir, "ws")
	serverPort := startSharedDoltServer(t, env, filepath.Join(rootDir, "dolt"))
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("creating workspace: %v", err)
	}
	gitInitWorkspace(t, wsDir)
	runBDInit(t, env, wsDir, "pq", serverPort)

	runner := isolatedBdStoreCommandRunner(env)
	var mu sync.Mutex
	var sent []string
	store := beads.NewBdStore(wsDir, func(dir, name string, args ...string) ([]byte, error) {
		if name == "bd" {
			mu.Lock()
			sent = append(sent, strings.Join(args, " "))
			mu.Unlock()
		}
		return runner(dir, name, args...)
	})
	resetSent := func() {
		mu.Lock()
		sent = nil
		mu.Unlock()
	}
	sentSoFar := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), sent...)
	}

	// Every bead here is a wisp, for the reason
	// TestBdStoreEphemeralMetadataClauseMatchesGoFilter gives: an issues-table
	// bead keeps the oldest supported bd from running this test.
	// TestBdStoreIDReadsSelectIssuesTableRows covers issues-table rows.
	create := func(title, parent string) string {
		t.Helper()
		b, err := store.Create(beads.Bead{Title: title, Type: "task", Ephemeral: true, ParentID: parent})
		if err != nil {
			t.Fatalf("Create(%s): %v", title, err)
		}
		return b.ID
	}
	one := create("parent one", "")
	two := create("parent two", "")
	unlisted := create("unlisted parent", "")
	childOfOne := create("child of one", one)
	childOfTwo := create("child of two", two)
	grandchild := create("grandchild", childOfOne)
	create("child of the unlisted parent", unlisted)
	blocked := create("blocked by parent one", "")
	if err := store.DepAdd(blocked, one, "blocks"); err != nil {
		t.Fatalf("DepAdd(%s blocks on %s): %v", blocked, one, err)
	}
	if err := store.Close(childOfTwo); err != nil {
		t.Fatalf("Close(%s): %v", childOfTwo, err)
	}
	missing := "pq-missing"

	t.Run("ids", func(t *testing.T) {
		resetSent()
		want := []string{childOfOne, grandchild}
		got, err := store.List(beads.ListQuery{IDs: []string{grandchild, missing, childOfOne}, TierMode: beads.TierBoth})
		if err != nil {
			t.Fatalf("List(IDs): %v", err)
		}
		gotIDs := make([]string, 0, len(got))
		for _, b := range got {
			gotIDs = append(gotIDs, b.ID)
		}
		if !sameIDSet(gotIDs, want) {
			t.Fatalf("List(IDs) = %v, want %v", gotIDs, want)
		}
		ids := []string{childOfOne, grandchild, missing}
		slices.Sort(ids)
		wantFlag := "--id=" + strings.Join(ids, ",")
		commands := sentSoFar()
		if !slices.ContainsFunc(commands, func(c string) bool { return strings.HasPrefix(c, "list ") && strings.Contains(c, wantFlag) }) {
			t.Fatalf("bd commands = %q, want a bd list carrying %s", commands, wantFlag)
		}
	})

	t.Run("an id bd list cannot carry", func(t *testing.T) {
		odd, err := store.Create(beads.Bead{ID: "pq-odd,1", Title: "id holding a comma", Type: "task", Ephemeral: true})
		if err != nil {
			t.Fatalf("Create(pq-odd,1): %v", err)
		}
		if odd.ID != "pq-odd,1" {
			t.Fatalf("created id = %q, want pq-odd,1", odd.ID)
		}
		resetSent()
		got, err := store.List(beads.ListQuery{IDs: []string{odd.ID, childOfOne}, IncludeClosed: true, TierMode: beads.TierBoth})
		if err != nil {
			t.Fatalf("List(IDs): %v", err)
		}
		gotIDs := make([]string, 0, len(got))
		for _, b := range got {
			gotIDs = append(gotIDs, b.ID)
		}
		if !sameIDSet(gotIDs, []string{odd.ID, childOfOne}) {
			t.Fatalf("List(IDs) = %q, want [%q %q]", gotIDs, odd.ID, childOfOne)
		}
		commands := sentSoFar()
		quoted := `query --json id="pq-odd,1" --all --limit 0`
		if !slices.Contains(commands, quoted) {
			t.Fatalf("bd commands = %q, want the quoted read %q", commands, quoted)
		}
		for _, c := range commands {
			if strings.HasPrefix(c, "list ") && !strings.Contains(c, " --id="+childOfOne) {
				t.Fatalf("bd list %q, want only the read of --id=%s; all: %q", c, childOfOne, commands)
			}
		}
	})

	t.Run("a long id list", func(t *testing.T) {
		ids := []string{grandchild, childOfOne}
		for i := range 7000 {
			ids = append(ids, fmt.Sprintf("pq-zz%07d", i))
		}
		resetSent()
		got, err := store.List(beads.ListQuery{IDs: ids, IncludeClosed: true, TierMode: beads.TierBoth})
		if err != nil {
			t.Fatalf("List(IDs): %v", err)
		}
		gotIDs := make([]string, 0, len(got))
		for _, b := range got {
			gotIDs = append(gotIDs, b.ID)
		}
		if !sameIDSet(gotIDs, []string{childOfOne, grandchild}) {
			t.Fatalf("List(%d IDs) = %v, want [%s %s]", len(ids), gotIDs, childOfOne, grandchild)
		}
		reads := 0
		for _, c := range sentSoFar() {
			if !strings.HasPrefix(c, "list ") {
				continue
			}
			_, value, ok := strings.Cut(c, " --id=")
			if !ok {
				t.Fatalf("bd list read without an id filter: %.200q", c)
			}
			if len(value) > 64<<10 {
				t.Fatalf("an --id value is %d bytes, over 64 KiB", len(value))
			}
			reads++
		}
		if reads < 2 {
			t.Fatalf("bd list reads = %d, want the %d ids split across at least 2", reads, len(ids))
		}
	})
}

var realBDVersionPattern = regexp.MustCompile(`bd version (\d+\.\d+\.\d+[0-9A-Za-z.+-]*)`)

// requireRealBDAtLeast skips the test when the real bd reports a version below
// minVersion, a semver string such as "v1.3.1-0", which admits that release's
// candidates. feature names what the older bd lacks, for the skip message. An
// older bd is a supported pin, so this is a skip and not a failure.
func requireRealBDAtLeast(t *testing.T, minVersion, feature string) {
	t.Helper()
	cmd := exec.Command(realBDBinary, "version")
	cmd.Dir = t.TempDir()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s version: %v\n%s", realBDBinary, err, out)
	}
	m := realBDVersionPattern.FindStringSubmatch(string(out))
	if m == nil {
		t.Fatalf("cannot parse a bd version from %q", out)
	}
	if semver.Compare("v"+m[1], minVersion) < 0 {
		t.Skipf("bd %s predates %s (needs %s); set %s to a newer bd", m[1], feature, minVersion, integrationRealBDBinaryEnv)
	}
}

// TestBdStoreIDReadsSelectIssuesTableRows proves the id reads against
// issues-table rows on a real bd and Dolt: the --id read alone returns
// issues-table rows, and a wisp row beside them, with no read of every row
// and no wisp read. It needs bd 1.3.1: once a workspace holds an issues-table
// bead, bd 1.0.4 auto-imports issues.jsonl on its next command, and BdStore
// refuses that as a silent fallback.
func TestBdStoreIDReadsSelectIssuesTableRows(t *testing.T) {
	requireDoltIntegration(t)
	requireRealBDAtLeast(t, "v1.3.1-0", "a server-mode workspace that holds issues-table beads without auto-importing issues.jsonl")
	env := newIsolatedToolEnv(t, true)

	rootDir := t.TempDir()
	wsDir := filepath.Join(rootDir, "ws")
	serverPort := startSharedDoltServer(t, env, filepath.Join(rootDir, "dolt"))
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("creating workspace: %v", err)
	}
	gitInitWorkspace(t, wsDir)
	runBDInit(t, env, wsDir, "iq", serverPort)

	runner := isolatedBdStoreCommandRunner(env)
	var mu sync.Mutex
	var sent []string
	store := beads.NewBdStore(wsDir, func(dir, name string, args ...string) ([]byte, error) {
		if name == "bd" {
			mu.Lock()
			sent = append(sent, strings.Join(args, " "))
			mu.Unlock()
		}
		return runner(dir, name, args...)
	})

	create := func(title, parent string, ephemeral bool) string {
		t.Helper()
		b, err := store.Create(beads.Bead{Title: title, Type: "task", ParentID: parent, Ephemeral: ephemeral})
		if err != nil {
			t.Fatalf("Create(%s): %v", title, err)
		}
		return b.ID
	}
	one := create("parent one", "", false)
	two := create("parent two", "", false)
	childOfOne := create("child of one", one, false)
	childOfTwo := create("child of two", two, false)
	wispOfOne := create("wisp child of one", one, true)
	create("unrelated", "", false)
	if err := store.Close(childOfTwo); err != nil {
		t.Fatalf("Close(%s): %v", childOfTwo, err)
	}

	cases := []struct {
		name  string
		query beads.ListQuery
		want  []string
	}{
		{name: "ids, both tiers", query: beads.ListQuery{IDs: []string{wispOfOne, childOfTwo, childOfOne}, IncludeClosed: true, TierMode: beads.TierBoth}, want: []string{childOfOne, childOfTwo, wispOfOne}},
		{name: "ids", query: beads.ListQuery{IDs: []string{childOfTwo, childOfOne}, IncludeClosed: true}, want: []string{childOfOne, childOfTwo}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			sent = nil
			mu.Unlock()
			got, err := store.List(tc.query)
			if err != nil {
				t.Fatalf("List(%s): %v", tc.name, err)
			}
			gotIDs := make([]string, 0, len(got))
			for _, b := range got {
				gotIDs = append(gotIDs, b.ID)
			}
			if !sameIDSet(gotIDs, tc.want) {
				t.Fatalf("List(%s) = %v, want %v", tc.name, gotIDs, tc.want)
			}
			mu.Lock()
			commands := append([]string(nil), sent...)
			mu.Unlock()
			idReads := 0
			for _, c := range commands {
				switch {
				case strings.HasPrefix(c, "query "):
					t.Fatalf("bd query %q ran; the --id read returned every row; all: %q", c, commands)
				case strings.HasPrefix(c, "list "):
					if !strings.Contains(c, " --id=") {
						t.Fatalf("bd list %q read without an id filter; all: %q", c, commands)
					}
					idReads++
				}
			}
			if idReads != 1 {
				t.Fatalf("bd list reads = %d, want the one --id read; all: %q", idReads, commands)
			}
		})
	}
}

// sameIDSet reports whether got and want hold the same ids, in any order.
func sameIDSet(got, want []string) bool {
	got = slices.Sorted(slices.Values(got))
	want = slices.Sorted(slices.Values(want))
	return slices.Equal(got, want)
}

func configureCustomTypes(t *testing.T, env []string, wsDir string, customTypes []string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), bdInitTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, bdBinary, "config", "set", "types.custom", strings.Join(customTypes, ","))
	cmd.Dir = wsDir
	cmd.Env = isolateBdHomeEnv(env)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("bd config set types.custom timed out after %s: %s", bdInitTimeout, out)
	}
	if err != nil {
		t.Fatalf("bd config set types.custom: %v: %s", err, out)
	}
}
