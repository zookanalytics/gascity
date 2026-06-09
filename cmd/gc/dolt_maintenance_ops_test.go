package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDoltMaintenanceClient is an injectable doltMaintenanceClient that records
// calls so tests can assert on iteration order and error propagation without a
// live Dolt server.
type fakeDoltMaintenanceClient struct {
	databases  []string
	listErr    error
	gcErrs     map[string]error // per-database GCDatabase errors
	countByDB  map[string]int   // per-database CountIssues results
	countErrs  map[string]error // per-database CountIssues errors
	gcCalls    []string         // databases passed to GCDatabase, in order
	countCalls []string         // databases passed to CountIssues, in order
	closed     bool
	closeErr   error
}

func (f *fakeDoltMaintenanceClient) ListUserDatabases(_ context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.databases, nil
}

func (f *fakeDoltMaintenanceClient) GCDatabase(_ context.Context, name string) error {
	f.gcCalls = append(f.gcCalls, name)
	if f.gcErrs != nil {
		return f.gcErrs[name]
	}
	return nil
}

func (f *fakeDoltMaintenanceClient) CountIssues(_ context.Context, name string) (int, error) {
	f.countCalls = append(f.countCalls, name)
	if f.countErrs != nil {
		if err := f.countErrs[name]; err != nil {
			return 0, err
		}
	}
	if f.countByDB != nil {
		return f.countByDB[name], nil
	}
	return 0, nil
}

func (f *fakeDoltMaintenanceClient) Close() error {
	f.closed = true
	return f.closeErr
}

func TestManagedDoltMaintenanceOpsExecGCRunsEveryDatabaseInOrder(t *testing.T) {
	fake := &fakeDoltMaintenanceClient{databases: []string{"gc", "tk", "sl", "lx", "su"}}
	ops := &managedDoltMaintenanceOps{client: fake}

	if err := ops.ExecGC(context.Background()); err != nil {
		t.Fatalf("ExecGC: unexpected error: %v", err)
	}
	want := []string{"gc", "tk", "sl", "lx", "su"}
	if strings.Join(fake.gcCalls, ",") != strings.Join(want, ",") {
		t.Fatalf("GCDatabase calls = %v, want %v (every managed database, in order)", fake.gcCalls, want)
	}
}

func TestManagedDoltMaintenanceOpsExecGCFailsFastNamingDatabase(t *testing.T) {
	boom := errors.New("dolt gc exploded")
	fake := &fakeDoltMaintenanceClient{
		databases: []string{"gc", "tk", "sl"},
		gcErrs:    map[string]error{"tk": boom},
	}
	ops := &managedDoltMaintenanceOps{client: fake}

	err := ops.ExecGC(context.Background())
	if err == nil {
		t.Fatal("ExecGC: expected error when a database fails to gc")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("ExecGC error = %v, want wrapped %v", err, boom)
	}
	if !strings.Contains(err.Error(), "tk") {
		t.Fatalf("ExecGC error = %q, want it to name the failing database %q", err.Error(), "tk")
	}
	// Fail-fast: gc on the database after the failure must not run; gc on the
	// database before it must (its reclaimed space is preserved).
	if got := strings.Join(fake.gcCalls, ","); got != "gc,tk" {
		t.Fatalf("GCDatabase calls = %q, want %q (stop at the failure)", got, "gc,tk")
	}
}

func TestManagedDoltMaintenanceOpsExecGCErrorsWhenNoUserDatabases(t *testing.T) {
	fake := &fakeDoltMaintenanceClient{databases: nil}
	ops := &managedDoltMaintenanceOps{client: fake}

	if err := ops.ExecGC(context.Background()); err == nil {
		t.Fatal("ExecGC: expected error when the server has no managed user databases")
	}
	if len(fake.gcCalls) != 0 {
		t.Fatalf("GCDatabase called %v with no databases; expected none", fake.gcCalls)
	}
}

func TestManagedDoltMaintenanceOpsExecGCPropagatesListError(t *testing.T) {
	boom := errors.New("show databases failed")
	fake := &fakeDoltMaintenanceClient{listErr: boom}
	ops := &managedDoltMaintenanceOps{client: fake}

	err := ops.ExecGC(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("ExecGC error = %v, want wrapped %v", err, boom)
	}
}

func TestManagedDoltMaintenanceOpsSmokeCountSumsAcrossDatabases(t *testing.T) {
	fake := &fakeDoltMaintenanceClient{
		databases: []string{"gc", "tk", "sl"},
		countByDB: map[string]int{"gc": 12, "tk": 0, "sl": 7},
	}
	ops := &managedDoltMaintenanceOps{client: fake}

	got, err := ops.SmokeCount(context.Background())
	if err != nil {
		t.Fatalf("SmokeCount: unexpected error: %v", err)
	}
	if got != 19 {
		t.Fatalf("SmokeCount = %d, want 19 (sum across databases; an empty database is tolerated)", got)
	}
	want := []string{"gc", "tk", "sl"}
	if strings.Join(fake.countCalls, ",") != strings.Join(want, ",") {
		t.Fatalf("CountIssues calls = %v, want %v", fake.countCalls, want)
	}
}

func TestManagedDoltMaintenanceOpsSmokeCountPropagatesErrorNamingDatabase(t *testing.T) {
	boom := errors.New("issues table missing")
	fake := &fakeDoltMaintenanceClient{
		databases: []string{"gc", "tk"},
		countErrs: map[string]error{"tk": boom},
	}
	ops := &managedDoltMaintenanceOps{client: fake}

	_, err := ops.SmokeCount(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("SmokeCount error = %v, want wrapped %v", err, boom)
	}
	if !strings.Contains(err.Error(), "tk") {
		t.Fatalf("SmokeCount error = %q, want it to name the failing database %q", err.Error(), "tk")
	}
}

func TestManagedDoltMaintenanceOpsSmokeCountPropagatesListError(t *testing.T) {
	boom := errors.New("show databases failed")
	fake := &fakeDoltMaintenanceClient{listErr: boom}
	ops := &managedDoltMaintenanceOps{client: fake}

	if _, err := ops.SmokeCount(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("SmokeCount error = %v, want wrapped %v", err, boom)
	}
}

func TestManagedDoltMaintenanceOpsCloseClosesClient(t *testing.T) {
	fake := &fakeDoltMaintenanceClient{}
	ops := &managedDoltMaintenanceOps{client: fake}

	if err := ops.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
	if !fake.closed {
		t.Fatal("Close did not close the underlying client")
	}
}

func TestManagedDoltBaseConfigOmitsReadDeadlineForLongOperations(t *testing.T) {
	cfg, err := managedDoltBaseConfig("", "3307", "")
	if err != nil {
		t.Fatalf("managedDoltBaseConfig: unexpected error: %v", err)
	}
	// The shared base config must NOT carry a per-read/write socket deadline:
	// CALL DOLT_GC() can run for minutes and is bounded by the caller's
	// context (gc_timeout) instead. managedDoltOpenDB layers the 5 s probe
	// deadlines on top for quick health checks; the maintenance opener relies
	// on the base as-is, so a short read deadline here would silently kill a
	// long gc after 5 s.
	if cfg.ReadTimeout != 0 {
		t.Errorf("ReadTimeout = %v, want 0 (no short read deadline on the shared base)", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0", cfg.WriteTimeout)
	}
	if cfg.Timeout != 5*time.Second {
		t.Errorf("dial Timeout = %v, want 5s (fast-fail on an unreachable server)", cfg.Timeout)
	}
	if cfg.User != "root" {
		t.Errorf("User = %q, want defaulted to root", cfg.User)
	}
}

func TestManagedDoltBaseConfigRequiresPort(t *testing.T) {
	if _, err := managedDoltBaseConfig("host", "", "root"); err == nil {
		t.Fatal("managedDoltBaseConfig: expected error for empty port")
	}
}

func TestOpenManagedDoltMaintenanceDBRequiresPort(t *testing.T) {
	if _, err := openManagedDoltMaintenanceDB("host", "", "root"); err == nil {
		t.Fatal("openManagedDoltMaintenanceDB: expected error for empty port")
	}
}

// TestManagedDoltMaintenanceOpsReconnectsAcrossOnlineGC is the regression for
// the online-GC connection-invalidation path (Error 1105). The managed Dolt
// server invalidates every connection that was open across an online
// CALL DOLT_GC(): the next statement on such a connection fails with
// "...this connection can no longer be used. please reconnect." database/sql's
// Conn.Close() returns a connection to the pool for reuse, so without pool
// tuning a GC-poisoned connection is handed straight back out — the next
// database's USE aborts the whole multi-database run (observed in production as
// lx failing at 0.3s). tuneManagedDoltMaintenancePool must make the pool retain
// no idle connection so every operation gets a fresh physical connection (a
// reconnect) after each GC.
//
// The doltGCPoisonServer fake models that invalidation at the driver level: a
// connection opened in an earlier GC generation fails its next statement, while
// a freshly-opened connection adopts the current generation and succeeds.
func TestManagedDoltMaintenanceOpsReconnectsAcrossOnlineGC(t *testing.T) {
	cases := []struct {
		name    string
		tune    bool
		wantErr bool
	}{
		// Documents the bug: the default pool reuses the poisoned connection.
		{name: "untuned_pool_reuses_poisoned_connection", tune: false, wantErr: true},
		// Verifies the fix: the tuned pool reconnects after every GC.
		{name: "tuned_pool_reconnects_after_each_gc", tune: true, wantErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := &doltGCPoisonServer{databases: []string{"gc", "lx"}}
			dsn := registerDoltGCPoisonServer(t, server)

			db, err := sql.Open(doltGCPoisonDriverName, dsn)
			if err != nil {
				t.Fatalf("sql.Open: %v", err)
			}
			defer db.Close() //nolint:errcheck
			if tc.tune {
				tuneManagedDoltMaintenancePool(db)
			}

			ops := &managedDoltMaintenanceOps{client: &sqlDoltMaintenanceClient{db: db}}
			err = ops.ExecGC(context.Background())

			if tc.wantErr {
				if err == nil {
					t.Fatal("ExecGC: want a reconnect error from the reused poisoned connection, got nil")
				}
				if !strings.Contains(err.Error(), "lx") || !strings.Contains(strings.ToLower(err.Error()), "reconnect") {
					t.Fatalf("ExecGC error = %v, want it to name the second database (lx) and mention reconnect", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("ExecGC: unexpected error after pool tuning: %v", err)
			}
			if got := server.gcRuns(); got != 2 {
				t.Fatalf("CALL DOLT_GC ran %d times, want 2 (once per managed database — proves the second db was reached after a reconnect)", got)
			}
			// The post-gc smoke count runs on connections opened after the final
			// GC; a fresh-connection pool keeps it readable too.
			total, err := ops.SmokeCount(context.Background())
			if err != nil {
				t.Fatalf("SmokeCount after GC: unexpected error: %v", err)
			}
			if total == 0 {
				t.Fatal("SmokeCount = 0 after GC, want the summed issue counts")
			}
		})
	}
}

// --- doltGCPoisonServer: a fake database/sql driver that models Dolt's
// online-GC connection invalidation, for the regression test above. ---

const doltGCPoisonDriverName = "gc_dolt_gc_poison"

// errDoltGCReconnect is the driver-level analog of Dolt's Error 1105 ("this
// connection was established when this server performed an online garbage
// collection ... please reconnect."). The code under test does not inspect the
// error code, only propagates it, so a plain error faithfully reproduces the
// failure that reusing a poisoned connection triggers. Kept lowercase and
// unpunctuated per Go error-string convention; the test matches on "reconnect".
var errDoltGCReconnect = errors.New("dolt error 1105: connection established across an online garbage collection can no longer be used; please reconnect")

// doltGCPoisonServers maps a per-test DSN to its server so subtests stay
// isolated while sharing one registered driver (sql.Register panics on a
// duplicate name).
var doltGCPoisonServers = struct {
	sync.Mutex
	byDSN map[string]*doltGCPoisonServer
}{byDSN: map[string]*doltGCPoisonServer{}}

func init() {
	sql.Register(doltGCPoisonDriverName, doltGCPoisonDriver{})
}

// registerDoltGCPoisonServer associates server with a unique DSN derived from
// the test name and returns that DSN for sql.Open.
func registerDoltGCPoisonServer(t *testing.T, server *doltGCPoisonServer) string {
	t.Helper()
	dsn := t.Name()
	doltGCPoisonServers.Lock()
	doltGCPoisonServers.byDSN[dsn] = server
	doltGCPoisonServers.Unlock()
	t.Cleanup(func() {
		doltGCPoisonServers.Lock()
		delete(doltGCPoisonServers.byDSN, dsn)
		doltGCPoisonServers.Unlock()
	})
	return dsn
}

// doltGCPoisonServer is the shared state behind every connection a test opens.
// generation advances each time a connection runs CALL DOLT_GC(); a connection
// opened in an earlier generation is "poisoned" and fails its next statement.
type doltGCPoisonServer struct {
	mu         sync.Mutex
	databases  []string
	generation int
	gcCount    int
}

func (s *doltGCPoisonServer) open() *doltGCPoisonConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &doltGCPoisonConn{server: s, generation: s.generation}
}

func (s *doltGCPoisonServer) gcRuns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gcCount
}

type doltGCPoisonDriver struct{}

func (doltGCPoisonDriver) Open(dsn string) (driver.Conn, error) {
	doltGCPoisonServers.Lock()
	server := doltGCPoisonServers.byDSN[dsn]
	doltGCPoisonServers.Unlock()
	if server == nil {
		return nil, fmt.Errorf("no doltGCPoisonServer registered for dsn %q", dsn)
	}
	return server.open(), nil
}

type doltGCPoisonConn struct {
	server     *doltGCPoisonServer
	generation int
}

func (c *doltGCPoisonConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *doltGCPoisonConn) Close() error              { return nil }
func (c *doltGCPoisonConn) Begin() (driver.Tx, error) { return nil, errors.New("tx unsupported") }

// staleLocked reports whether this connection predates the latest online GC.
// Callers hold server.mu.
func (c *doltGCPoisonConn) staleLocked() bool {
	return c.generation < c.server.generation
}

func (c *doltGCPoisonConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.server.mu.Lock()
	defer c.server.mu.Unlock()
	if c.staleLocked() {
		return nil, errDoltGCReconnect
	}
	if strings.HasPrefix(query, "CALL DOLT_GC()") {
		c.server.gcCount++
		// Online GC invalidates every currently-open connection, including
		// this one — its next statement would now be stale.
		c.server.generation++
	}
	return driver.RowsAffected(0), nil
}

func (c *doltGCPoisonConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.server.mu.Lock()
	defer c.server.mu.Unlock()
	if c.staleLocked() {
		return nil, errDoltGCReconnect
	}
	switch {
	case strings.HasPrefix(query, "SHOW DATABASES"):
		rows := make([][]driver.Value, 0, len(c.server.databases))
		for _, name := range c.server.databases {
			rows = append(rows, []driver.Value{name})
		}
		return &doltGCPoisonRows{cols: []string{"Database"}, data: rows}, nil
	case strings.Contains(query, "COUNT(*)"):
		return &doltGCPoisonRows{cols: []string{"count"}, data: [][]driver.Value{{int64(7)}}}, nil
	}
	return nil, fmt.Errorf("unexpected query %q", query)
}

type doltGCPoisonRows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (r *doltGCPoisonRows) Columns() []string { return r.cols }
func (r *doltGCPoisonRows) Close() error      { return nil }
func (r *doltGCPoisonRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}
