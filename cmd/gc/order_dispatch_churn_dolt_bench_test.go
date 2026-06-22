//go:build dolt_integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	_ "github.com/go-sql-driver/mysql"
)

// Tier 2 of the order-dispatch store-open churn harness (epic gc-k8r4y):
// real-Dolt connections-per-open, measured against a dedicated sql-server owned
// by the test (ZERO background noise — the isolation the live measurement
// gc-k8r4y.1 could not get on the shared production server).
//
// EMPIRICAL FINDING (this is the answer, not a measurement bug): with the
// native Dolt store, per-tick reopen produces conn/tick ≈ 0. The upstream beads
// library keeps a PROCESS-GLOBAL connection pool keyed by server DSN that
// SURVIVES store close — so reopening a native store every tick reuses the
// existing pooled connection rather than dialing a new one. A direct probe of
// 20 open+close cycles against a fresh server moved Connections and
// Threads_connected by 0. The cost the cached-store-reuse fix (gc-t5rev / PR
// #68) removes is therefore the per-tick OPEN LATENCY (pool checkout +
// native-store preflight + the issue_prefix config round-trip + object setup),
// which the ns/op metric captures and which scales with the active scope count
// — NOT raw server connections. Corollary for the epic: the controller's
// observed connection saturation is not the native reopen path; it is the
// non-pooled fallback (each bd-CLI/exec subprocess dials its own connection
// with no shared pool). conn/tick is reported regardless so the pooled-to-zero
// result is captured durably, and Aborted_connects is surfaced so a
// connection-refusing server is never mistaken for a clean zero.
//
// Gated behind //go:build dolt_integration so default `go test` / CI needs no
// Dolt. Go build tags apply per-file, so this lives in its own file, separate
// from the hermetic Tier 0/1 in order_dispatch_churn_bench_test.go; together
// the two files are the single churn-harness deliverable.
//
// Invocation (requires the `dolt` binary in PATH):
//
//	go test ./cmd/gc/ -tags dolt_integration -run '^$' \
//	    -bench BenchmarkOrderDispatchDoltConnections -benchtime=200x
//
// The benchmark starts its OWN dedicated dolt sql-server, so it is safe to run
// alongside a city: it never touches the production server on 3307.

const (
	churnDoltDatabase = "beads"
	churnDoltUser     = "root"
	churnDoltHost     = "127.0.0.1"
)

// runChurnDolt runs a one-shot `dolt` subcommand in dir and fails the test on
// error. Used for repo init before the server starts.
func runChurnDolt(tb testing.TB, doltPath, dir string, args ...string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, doltPath, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "DOLT_CLI_PASSWORD=")
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("dolt %v in %s: %v\n%s", args, dir, err, out)
	}
}

// startChurnDoltServer starts a dedicated dolt sql-server on an ephemeral port
// serving the databases under dataDir, and returns the port. The server is
// stopped via t.Cleanup.
func startChurnDoltServer(tb testing.TB, doltPath, dataDir string) int {
	tb.Helper()
	ln, err := net.Listen("tcp", churnDoltHost+":0")
	if err != nil {
		tb.Fatalf("allocate dolt port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		tb.Fatalf("release dolt port probe: %v", err)
	}

	logFile, err := os.Create(filepath.Join(dataDir, "sql-server.log"))
	if err != nil {
		tb.Fatalf("create dolt server log: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, doltPath, "sql-server",
		"-H", churnDoltHost,
		"-P", strconv.Itoa(port),
		"--data-dir", dataDir,
		"--loglevel", "warning",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		tb.Fatalf("start dolt sql-server: %v", err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	tb.Cleanup(func() {
		cancel()
		select {
		case <-waitCh:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-waitCh
		}
		_ = logFile.Close()
	})
	return port
}

// openChurnMeasureDB opens a single-connection measurement handle to the test
// server. SetMaxOpenConns(1) keeps the status reads on one connection — opened
// once, before the first counter read — so consecutive SHOW GLOBAL STATUS calls
// add no new connections of their own and the before/after delta is pure
// dispatch-driven churn.
func openChurnMeasureDB(tb testing.TB, port int) *sql.DB {
	tb.Helper()
	dsn := fmt.Sprintf("%s@tcp(%s:%d)/?timeout=10s&allowNativePasswords=true&tls=false",
		churnDoltUser, churnDoltHost, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		tb.Fatalf("open measurement db: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	tb.Cleanup(func() { _ = db.Close() })
	return db
}

// waitChurnDoltReady blocks until the measurement handle can query the server.
func waitChurnDoltReady(tb testing.TB, db *sql.DB) {
	tb.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, lastErr = db.ExecContext(ctx, "SELECT 1")
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	tb.Fatalf("dolt sql-server never became query-ready: %v", lastErr)
}

// churnDoltConnections reads the cumulative server-wide Connections counter.
// Aborted_connects is read too and surfaced on failure so a connection-refusing
// server (max_connections exhaustion) is diagnosable rather than silent.
func churnDoltConnections(tb testing.TB, db *sql.DB) int64 {
	tb.Helper()
	var name string
	var val int64
	if err := db.QueryRow("SHOW GLOBAL STATUS LIKE 'Connections'").Scan(&name, &val); err != nil {
		tb.Fatalf("read Connections status: %v", err)
	}
	return val
}

func churnDoltAbortedConnects(tb testing.TB, db *sql.DB) int64 {
	tb.Helper()
	var name string
	var val int64
	if err := db.QueryRow("SHOW GLOBAL STATUS LIKE 'Aborted_connects'").Scan(&name, &val); err != nil {
		return -1 // not all builds expose it; -1 signals "unavailable"
	}
	return val
}

// churnDoltServerEnv returns the BEADS_* server-mode env that points
// beads.OpenNativeDoltStoreAt at the dedicated test server. These are the same
// keys mirrorBeadsDoltEnv projects in production (bd_env.go) — server mode is
// implied by HOST+PORT+USER presence.
func churnDoltServerEnv(port int) map[string]string {
	return map[string]string{
		"BEADS_DOLT_SERVER_HOST":     churnDoltHost,
		"BEADS_DOLT_SERVER_PORT":     strconv.Itoa(port),
		"BEADS_DOLT_SERVER_USER":     churnDoltUser,
		"BEADS_DOLT_SERVER_DATABASE": churnDoltDatabase,
		"BEADS_DOLT_AUTO_START":      "0",
	}
}

// openChurnNativeStore opens a fresh native Dolt store against the test server.
// This is the exact native opener openStoreAtForCity delegates to (main.go:
// OpenNativeStore -> beads.OpenNativeDoltStoreAt), wired here as the
// dispatcher's per-tick storeFn so the benchmark drives the REAL open path.
func openChurnNativeStore(scopeRoot string, port int) (beads.Store, error) {
	return beads.OpenNativeDoltStoreAt(context.Background(), scopeRoot, churnDoltServerEnv(port))
}

// newDoltChurnHarness builds the same dispatcher as the hermetic harness (scopes
// distinct target-scopes, no per-tick cap, all orders seeded not-due) but wires
// storeFn to the REAL native Dolt opener against the test server. Returns the
// dispatcher and the atomic open counter.
func newDoltChurnHarness(tb testing.TB, scopes, port int) (*memoryOrderDispatcher, string, time.Time, *int64) {
	tb.Helper()
	rec := &memRecorder{}
	// store is only the storeFn return value's fallback identity; the real opens
	// go through openChurnNativeStore below.
	ad := buildOrderDispatcherFromListExec(buildChurnOrders(scopes), beads.NewMemStore(), nil, nil, rec)
	if ad == nil {
		tb.Fatalf("buildOrderDispatcherFromListExec returned nil for scopes=%d", scopes)
	}
	mad := ad.(*memoryOrderDispatcher)

	cityPath := tb.TempDir()
	var opens int64
	mad.storeFn = func(execStoreTarget) (beads.Store, error) {
		atomic.AddInt64(&opens, 1)
		return openChurnNativeStore(cityPath, port)
	}
	mad.maxDispatchesPerTick = 0

	now := time.Now()
	for _, a := range mad.aa {
		target, err := resolveOrderStoreTarget(cityPath, mad.cfg, a)
		if err != nil {
			tb.Fatalf("resolving target for %s: %v", a.ScopedName(), err)
		}
		keys := []string{orderStoreTargetKey(target)}
		if legacyOrderCityFallbackNeeded(cityPath, target) {
			keys = append(keys, orderStoreTargetKey(legacyOrderCityTarget(cityPath, mad.cfg)))
		}
		mad.rememberLastRun(a.ScopedName(), keys, now)
	}
	return mad, cityPath, now, &opens
}

// BenchmarkOrderDispatchDoltConnections measures real Dolt connections per
// dispatch tick, before (perTickOpen: storeFn opens a fresh native store per
// target every tick) and after (cachedReuse: one pre-opened native store reused
// every tick), across target-scope counts. conn/tick is the headline metric:
// the per-tick-open arm pays connections proportional to the active scope count
// every tick; the cached arm pays ~zero.
func BenchmarkOrderDispatchDoltConnections(b *testing.B) {
	doltPath, err := exec.LookPath("dolt")
	if err != nil {
		b.Skipf("dolt not found in PATH: %v", err)
	}

	serverCity := b.TempDir()
	dataDir := filepath.Join(serverCity, ".beads", "dolt")
	dbDir := filepath.Join(dataDir, churnDoltDatabase)
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		b.Fatalf("mkdir dolt db dir: %v", err)
	}
	runChurnDolt(b, doltPath, dbDir, "init", "--name", "Gas City", "--email", "churn@example.com")

	port := startChurnDoltServer(b, doltPath, dataDir)
	db := openChurnMeasureDB(b, port)
	waitChurnDoltReady(b, db)

	// Initialize the beads schema on the server once: the first native open runs
	// the upstream beads migrations, after which dispatch's gate reads succeed.
	initStore, err := openChurnNativeStore(serverCity, port)
	if err != nil {
		b.Fatalf("initialize beads schema via native open: %v", err)
	}
	if err := closeBeadStoreHandle(initStore); err != nil {
		b.Fatalf("close schema-init store: %v", err)
	}

	for _, scopes := range churnTargetScopeCounts {
		scopes := scopes
		b.Run(fmt.Sprintf("scopes=%d/perTickOpen", scopes), func(b *testing.B) {
			mad, cityPath, now, opens := newDoltChurnHarness(b, scopes, port)
			ctx := context.Background()
			atomic.StoreInt64(opens, 0)
			c0 := churnDoltConnections(b, db)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mad.dispatch(ctx, cityPath, now)
				drainCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				if !mad.drain(drainCtx) {
					cancel()
					b.Fatal("drain timed out")
				}
				cancel()
			}
			b.StopTimer()
			// Let the detached per-tick store closers settle so a slow close does
			// not bleed connections into the next sub-benchmark's baseline.
			time.Sleep(200 * time.Millisecond)
			c1 := churnDoltConnections(b, db)
			if aborted := churnDoltAbortedConnects(b, db); aborted > 0 {
				b.Logf("Aborted_connects=%d (server may be nearing max_connections)", aborted)
			}
			b.ReportMetric(float64(c1-c0)/float64(b.N), "conn/tick")
			b.ReportMetric(float64(atomic.LoadInt64(opens))/float64(b.N), "opens/tick")
		})

		b.Run(fmt.Sprintf("scopes=%d/cachedReuse", scopes), func(b *testing.B) {
			mad, cityPath, now, opens := newDoltChurnHarness(b, scopes, port)
			cached, err := openChurnNativeStore(cityPath, port)
			if err != nil {
				b.Fatalf("open cached store: %v", err)
			}
			b.Cleanup(func() { _ = closeBeadStoreHandle(cached) })
			mad.cachedStoreFn = func(execStoreTarget) beads.Store { return cached }
			ctx := context.Background()
			atomic.StoreInt64(opens, 0)
			c0 := churnDoltConnections(b, db)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mad.dispatch(ctx, cityPath, now)
			}
			b.StopTimer()
			c1 := churnDoltConnections(b, db)
			b.ReportMetric(float64(c1-c0)/float64(b.N), "conn/tick")
			b.ReportMetric(float64(atomic.LoadInt64(opens))/float64(b.N), "opens/tick")
		})
	}
}
