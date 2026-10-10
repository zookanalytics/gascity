//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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
)

const (
	bdInitTimeout          = 60 * time.Second
	doltServerStartupLimit = 10 * time.Second
	// doltServerReadyTimeout budgets startDoltServerOnAllInterfaces's
	// TCP-dial readiness wait (dolt_config_test.go). Kept separate from
	// doltServerStartupLimit — same shape of wait, but a distinct call
	// site — so a future tuning of one doesn't silently retune the other;
	// that kind of implicit coupling is what let runBDInitCompat's timeout
	// drift out of sync with bdInitTimeout in the first place (ga-gajll3).
	doltServerReadyTimeout = 60 * time.Second
)

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
	// Skipped while gascity is pinned to bd 1.0.4 (ga-e7z613). bd 1.0.4 avoids
	// bd 1.0.5's data-corruption bug but lacks the #3691 empty-DB-guard fix (it
	// was tagged ~6.5h before #3691 merged), so it silently auto-imports into an
	// empty on-disk DB, which trips gascity's ErrBDSilentFallback guard (#2080).
	// This is NOT a regression: gascity 1.2.1 shipped on bd 1.0.4 with identical
	// bd behavior; only the loud-fallback detection added after 1.2.1
	// (#1930/#2080/#2079) is new. The user explicitly authorized applying this
	// skip on main on 2026-06-21 as part of merging release/v1.3.0 into main.
	// Remove once gascity moves to a clean bd (#3691 + the corruption fix).
	t.Skip("bd 1.0.4 trips the silent-fallback guard (pre-existing, non-regression; pinned to avoid 1.0.5 corruption) — ga-e7z613")
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

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocating dolt port: %v", err)
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatalf("closing dolt port probe: %v", err)
	}

	logPath := filepath.Join(dataDir, "sql-server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("creating dolt log file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, doltBinary, "sql-server", "-H", "127.0.0.1", "-P", port, "--data-dir", dataDir)
	cmd.Env = env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("starting dolt sql-server: %v", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	deadline := time.Now().Add(doltServerStartupLimit)
	addr := net.JoinHostPort("127.0.0.1", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Cleanup(func() {
				cancel()
				<-waitCh
				_ = logFile.Close()
			})
			return port
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancel()
	<-waitCh
	_ = logFile.Close()
	logBytes, _ := os.ReadFile(logPath)
	t.Fatalf("dolt sql-server did not become ready on %s within %s:\n%s", addr, doltServerStartupLimit, logBytes)
	return ""
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
	runBDInit(t, env, wsDir, "mc", serverPort)
	configureCustomTypes(t, env, wsDir, doctor.RequiredCustomTypes)

	store := beads.NewBdStore(wsDir, isolatedBdStoreCommandRunner(env))

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

// TestBdStoreEphemeralMetadataClauseMatchesGoFilter proves the ephemeral
// leg's metadata pushdown against a real bd and Dolt. For each value shape
// BdStore sends as a metadata.<key> clause, bd's own answer to the clause holds
// exactly the wisps the Go-side filter keeps, and a TierBoth list returns them.
func TestBdStoreEphemeralMetadataClauseMatchesGoFilter(t *testing.T) {
	requireDoltIntegration(t)
	env := newIsolatedToolEnv(t, true)

	rootDir := t.TempDir()
	wsDir := filepath.Join(rootDir, "ws")
	serverPort := startSharedDoltServer(t, env, filepath.Join(rootDir, "dolt"))
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("creating workspace: %v", err)
	}
	gitInitWorkspace(t, wsDir)
	runBDInit(t, env, wsDir, "mq", serverPort)

	runner := isolatedBdStoreCommandRunner(env)
	var mu sync.Mutex
	var sent []string
	store := beads.NewBdStore(wsDir, func(dir, name string, args ...string) ([]byte, error) {
		if name == "bd" && len(args) > 2 && args[0] == "query" {
			mu.Lock()
			sent = append(sent, args[2])
			mu.Unlock()
		}
		return runner(dir, name, args...)
	})

	// Every bead here is a wisp. Once a workspace holds an issues-table bead,
	// bd 1.0.4 auto-imports issues.jsonl on its next command and BdStore
	// refuses that as a silent fallback, so a non-ephemeral bead would keep
	// this row from running against the oldest supported bd.
	create := func(title string, metadata map[string]string) string {
		t.Helper()
		b, err := store.Create(beads.Bead{Title: title, Type: "task", Ephemeral: true, Metadata: metadata})
		if err != nil {
			t.Fatalf("Create(%s): %v", title, err)
		}
		return b.ID
	}
	routed := create("routed", map[string]string{
		"gc.routed_to": "gascity/gc-toolkit.polecat",
		"note":         `say "hi" \ bye`,
		"body":         "line one\n\tline two",
	})
	other := create("other route", map[string]string{"gc.routed_to": "gascity/other"})
	create("upper-case route", map[string]string{"gc.routed_to": "Gascity/GC-toolkit.polecat"})
	create("trailing-space route", map[string]string{"gc.routed_to": "gascity/gc-toolkit.polecat "})
	readString := create("read as a string", map[string]string{"mail.read": "true"})
	// bd update --set-metadata stores true and false as JSON booleans, where
	// Create's --metadata stores the string "true".
	for id, read := range map[string]string{routed: "true", other: "false"} {
		if err := store.SetMetadata(id, "mail.read", read); err != nil {
			t.Fatalf("SetMetadata(%s, mail.read=%s): %v", id, read, err)
		}
	}

	cases := []struct {
		name     string
		metadata map[string]string
		clause   string
		want     []string
	}{
		{
			name:     "value with a slash",
			metadata: map[string]string{"gc.routed_to": "gascity/gc-toolkit.polecat"},
			clause:   `metadata.gc.routed_to="gascity/gc-toolkit.polecat"`,
			want:     []string{routed},
		},
		{
			name:     "quote and backslash",
			metadata: map[string]string{"note": `say "hi" \ bye`},
			clause:   `metadata.note="say \"hi\" \\ bye"`,
			want:     []string{routed},
		},
		{
			name:     "newline and tab",
			metadata: map[string]string{"body": "line one\n\tline two"},
			clause:   "metadata.body=\"line one\n\tline two\"",
			want:     []string{routed},
		},
		{
			name:     "true stored as a boolean and as a string",
			metadata: map[string]string{"mail.read": "true"},
			clause:   `metadata.mail.read="true"`,
			want:     []string{routed, readString},
		},
		{
			name:     "false stored as a boolean",
			metadata: map[string]string{"mail.read": "false"},
			clause:   `metadata.mail.read="false"`,
			want:     []string{other},
		},
		{
			name:     "no wisp matches",
			metadata: map[string]string{"gc.routed_to": "gascity/none"},
			clause:   `metadata.gc.routed_to="gascity/none"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			sent = nil
			mu.Unlock()
			got, err := store.List(beads.ListQuery{Metadata: tc.metadata, TierMode: beads.TierBoth})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			gotIDs := make([]string, 0, len(got))
			for _, b := range got {
				gotIDs = append(gotIDs, b.ID)
			}
			if !sameIDSet(gotIDs, tc.want) {
				t.Fatalf("List(TierBoth, %v) = %v, want %v", tc.metadata, gotIDs, tc.want)
			}

			mu.Lock()
			queries := append([]string(nil), sent...)
			mu.Unlock()
			wantExpr := "ephemeral=true AND " + tc.clause
			if len(queries) != 1 || queries[0] != wantExpr {
				t.Fatalf("bd query expressions = %q, want [%q]", queries, wantExpr)
			}
			out, err := runner(wsDir, "bd", "query", "--json", wantExpr, "--limit", "0")
			if err != nil {
				t.Fatalf("bd query %q: %v\n%s", wantExpr, err, out)
			}
			start := bytes.IndexByte(out, '[')
			if start < 0 {
				t.Fatalf("bd query %q printed no JSON array:\n%s", wantExpr, out)
			}
			var rows []struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(out[start:], &rows); err != nil {
				t.Fatalf("parsing bd query %q output: %v\n%s", wantExpr, err, out)
			}
			bdIDs := make([]string, 0, len(rows))
			for _, row := range rows {
				bdIDs = append(bdIDs, row.ID)
			}
			if !sameIDSet(bdIDs, tc.want) {
				t.Fatalf("bd query %q = %v, want exactly %v", wantExpr, bdIDs, tc.want)
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
