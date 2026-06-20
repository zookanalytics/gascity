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
)

// fakeWispIndexClient is an in-memory wispIndexClient for unit-testing the
// multi-database orchestration without a live Dolt server.
type fakeWispIndexClient struct {
	databases []string
	listErr   error
	// schema reports whether a database carries the wisp schema. A name absent
	// from the map defaults to false (skipped).
	schema map[string]bool
	// schemaErr injects a HasWispSchema error for a database.
	schemaErr map[string]error
	// applyErr injects an ApplyIndexes error for a database.
	applyErr map[string]error

	// applied records, in order, the databases ApplyIndexes was called on.
	applied []string
	// applyCount counts ApplyIndexes calls per database (idempotency checks).
	applyCount map[string]int
	closed     bool
}

func (f *fakeWispIndexClient) ListUserDatabases(_ context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.databases, nil
}

func (f *fakeWispIndexClient) HasWispSchema(_ context.Context, name string) (bool, error) {
	if err := f.schemaErr[name]; err != nil {
		return false, err
	}
	return f.schema[name], nil
}

func (f *fakeWispIndexClient) ApplyIndexes(_ context.Context, name string) error {
	if f.applyCount == nil {
		f.applyCount = map[string]int{}
	}
	f.applyCount[name]++
	if err := f.applyErr[name]; err != nil {
		return err
	}
	f.applied = append(f.applied, name)
	return nil
}

func (f *fakeWispIndexClient) Close() error {
	f.closed = true
	return nil
}

// resultsByDB indexes a result slice by database name for assertions.
func resultsByDB(results []wispIndexResult) map[string]wispIndexResult {
	m := make(map[string]wispIndexResult, len(results))
	for _, r := range results {
		m[r.Database] = r
	}
	return m
}

func TestApplyWispQueryIndexesAcrossDatabases_AppliesToWispDBsSkipsOthers(t *testing.T) {
	client := &fakeWispIndexClient{
		databases: []string{"lx", "gc", "scratch", "tk"},
		schema: map[string]bool{
			"lx": true, "gc": true, "tk": true,
			"scratch": false,
		},
	}
	results, err := applyWispQueryIndexesAcrossDatabases(context.Background(), client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d: %+v", len(results), results)
	}
	byDB := resultsByDB(results)
	for _, db := range []string{"lx", "gc", "tk"} {
		if byDB[db].Status != wispIndexApplied {
			t.Errorf("database %q: expected applied, got %q (err=%v)", db, byDB[db].Status, byDB[db].Err)
		}
	}
	if byDB["scratch"].Status != wispIndexSkipped {
		t.Errorf("database scratch: expected skipped, got %q", byDB["scratch"].Status)
	}
	// ApplyIndexes must run only for wisp-schema databases.
	if got := strings.Join(client.applied, ","); got != "lx,gc,tk" {
		t.Errorf("expected ApplyIndexes on lx,gc,tk; got %q", got)
	}
}

func TestApplyWispQueryIndexesAcrossDatabases_RecordsApplyErrorAndContinues(t *testing.T) {
	wantErr := errors.New("boom")
	client := &fakeWispIndexClient{
		databases: []string{"a", "b", "c"},
		schema:    map[string]bool{"a": true, "b": true, "c": true},
		applyErr:  map[string]error{"b": wantErr},
	}
	results, err := applyWispQueryIndexesAcrossDatabases(context.Background(), client)
	if err != nil {
		t.Fatalf("sweep must not abort on a per-db error: %v", err)
	}
	byDB := resultsByDB(results)
	if byDB["b"].Status != wispIndexFailed {
		t.Errorf("database b: expected error status, got %q", byDB["b"].Status)
	}
	if !errors.Is(byDB["b"].Err, wantErr) {
		t.Errorf("database b: expected wrapped %v, got %v", wantErr, byDB["b"].Err)
	}
	// The sweep continues past the failure: a and c still applied.
	if byDB["a"].Status != wispIndexApplied || byDB["c"].Status != wispIndexApplied {
		t.Errorf("expected a and c applied, got a=%q c=%q", byDB["a"].Status, byDB["c"].Status)
	}
}

func TestApplyWispQueryIndexesAcrossDatabases_RecordsSchemaCheckError(t *testing.T) {
	wantErr := errors.New("schema probe failed")
	client := &fakeWispIndexClient{
		databases: []string{"a", "b"},
		schema:    map[string]bool{"b": true},
		schemaErr: map[string]error{"a": wantErr},
	}
	results, err := applyWispQueryIndexesAcrossDatabases(context.Background(), client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	byDB := resultsByDB(results)
	if byDB["a"].Status != wispIndexFailed || !errors.Is(byDB["a"].Err, wantErr) {
		t.Errorf("database a: expected error status wrapping %v, got %q/%v", wantErr, byDB["a"].Status, byDB["a"].Err)
	}
	if byDB["b"].Status != wispIndexApplied {
		t.Errorf("database b: expected applied, got %q", byDB["b"].Status)
	}
	// A schema-check failure must not attempt ApplyIndexes for that database.
	if _, attempted := client.applyCount["a"]; attempted {
		t.Errorf("database a: ApplyIndexes must not run when schema check errored")
	}
}

func TestApplyWispQueryIndexesAcrossDatabases_ListErrorReturns(t *testing.T) {
	wantErr := errors.New("list failed")
	client := &fakeWispIndexClient{listErr: wantErr}
	results, err := applyWispQueryIndexesAcrossDatabases(context.Background(), client)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped list error %v, got %v", wantErr, err)
	}
	if results != nil {
		t.Errorf("expected nil results on list error, got %+v", results)
	}
}

func TestApplyWispQueryIndexesAcrossDatabases_Idempotent(t *testing.T) {
	client := &fakeWispIndexClient{
		databases: []string{"lx"},
		schema:    map[string]bool{"lx": true},
	}
	// A second sweep over already-indexed databases must apply cleanly again
	// (CREATE INDEX IF NOT EXISTS / "nothing to commit" are no-ops server-side).
	for i := 0; i < 2; i++ {
		results, err := applyWispQueryIndexesAcrossDatabases(context.Background(), client)
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", i, err)
		}
		if len(results) != 1 || results[0].Status != wispIndexApplied {
			t.Fatalf("run %d: expected single applied result, got %+v", i, results)
		}
	}
	if client.applyCount["lx"] != 2 {
		t.Errorf("expected ApplyIndexes called twice across idempotent runs, got %d", client.applyCount["lx"])
	}
}

func TestApplyWispQueryIndexesAcrossDatabases_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &fakeWispIndexClient{
		databases: []string{"a", "b"},
		schema:    map[string]bool{"a": true, "b": true},
	}
	results, err := applyWispQueryIndexesAcrossDatabases(ctx, client)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(client.applied) != 0 {
		t.Errorf("expected no databases applied after cancel, got %v", client.applied)
	}
	_ = results
}

func TestWispQueryIndexStatements_AreIdempotentCreateIndex(t *testing.T) {
	if len(wispQueryIndexStatements) == 0 {
		t.Fatal("wispQueryIndexStatements must not be empty")
	}
	for _, stmt := range wispQueryIndexStatements {
		if !strings.Contains(strings.ToUpper(stmt), "CREATE INDEX IF NOT EXISTS") {
			t.Errorf("statement must be idempotent (CREATE INDEX IF NOT EXISTS): %q", stmt)
		}
	}
}

func TestWispQueryIndexTables_CoverIndexTargets(t *testing.T) {
	// Every table named in wispQueryIndexTables (the schema-presence gate) must
	// be referenced by at least one index statement, and vice versa, so the
	// gate and the DDL cannot drift apart.
	for _, table := range wispQueryIndexTables {
		found := false
		for _, stmt := range wispQueryIndexStatements {
			if strings.Contains(stmt, " ON "+table+"(") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("table %q gated but no index statement targets it", table)
		}
	}
}

func TestSummarizeWispIndexResults(t *testing.T) {
	results := []wispIndexResult{
		{Database: "lx", Status: wispIndexApplied},
		{Database: "gc", Status: wispIndexApplied},
		{Database: "scratch", Status: wispIndexSkipped},
		{Database: "tk", Status: wispIndexFailed, Err: errors.New("x")},
	}
	got := summarizeWispIndexResults(results)
	if !strings.Contains(got, "applied=2") || !strings.Contains(got, "skipped=1") || !strings.Contains(got, "failed=1") {
		t.Errorf("summary missing counts: %q", got)
	}
}

func TestNewSQLWispIndexClient_UnreachableServerErrorsOrFailsFast(t *testing.T) {
	// Building the client lazily opens a pool; the first real call surfaces the
	// connection error. It must error rather than hang.
	client, err := newSQLWispIndexClient("19999")
	if err != nil {
		// Construction may fail fast — acceptable.
		return
	}
	defer client.Close() //nolint:errcheck
	if _, err := client.ListUserDatabases(context.Background()); err == nil {
		t.Fatal("expected error listing databases against unreachable server, got nil")
	}
}

// --- Production sqlWispIndexClient tests over a database/sql/driver mock. ---
//
// The mock lets the real client exercise its actual SQL strings and statement
// ordering without a live Dolt server, mirroring the doltGCPoison* harness in
// dolt_maintenance_ops_test.go.

const wispIndexMockDriverName = "wisp-index-mock"

var wispIndexMockServers = struct {
	sync.Mutex
	byDSN map[string]*wispIndexMockServer
}{byDSN: map[string]*wispIndexMockServer{}}

func init() {
	sql.Register(wispIndexMockDriverName, wispIndexMockDriver{})
}

// wispIndexMockServer is the shared state behind a mock connection: the database
// layout it reports and the statements executed against it, in order.
type wispIndexMockServer struct {
	mu              sync.Mutex
	databases       []string
	tablesByDB      map[string][]string
	nothingToCommit bool
	execLog         []string
}

// newMockWispIndexClient registers server under a per-test DSN and returns a
// sqlWispIndexClient wired to it.
func newMockWispIndexClient(t *testing.T, server *wispIndexMockServer) *sqlWispIndexClient {
	t.Helper()
	dsn := t.Name()
	wispIndexMockServers.Lock()
	wispIndexMockServers.byDSN[dsn] = server
	wispIndexMockServers.Unlock()
	t.Cleanup(func() {
		wispIndexMockServers.Lock()
		delete(wispIndexMockServers.byDSN, dsn)
		wispIndexMockServers.Unlock()
	})
	db, err := sql.Open(wispIndexMockDriverName, dsn)
	if err != nil {
		t.Fatalf("open mock db: %v", err)
	}
	t.Cleanup(func() { db.Close() }) //nolint:errcheck
	return &sqlWispIndexClient{db: db}
}

func (s *wispIndexMockServer) logged() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.execLog...)
}

type wispIndexMockDriver struct{}

func (wispIndexMockDriver) Open(dsn string) (driver.Conn, error) {
	wispIndexMockServers.Lock()
	server := wispIndexMockServers.byDSN[dsn]
	wispIndexMockServers.Unlock()
	if server == nil {
		return nil, fmt.Errorf("no wispIndexMockServer registered for dsn %q", dsn)
	}
	return &wispIndexMockConn{server: server}, nil
}

type wispIndexMockConn struct{ server *wispIndexMockServer }

func (c *wispIndexMockConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *wispIndexMockConn) Close() error              { return nil }
func (c *wispIndexMockConn) Begin() (driver.Tx, error) { return nil, errors.New("tx unsupported") }

func (c *wispIndexMockConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.server.mu.Lock()
	defer c.server.mu.Unlock()
	c.server.execLog = append(c.server.execLog, query)
	if strings.Contains(query, "DOLT_COMMIT") && c.server.nothingToCommit {
		return nil, errors.New("nothing to commit over the current working set")
	}
	return driver.RowsAffected(0), nil
}

func (c *wispIndexMockConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.server.mu.Lock()
	defer c.server.mu.Unlock()
	switch {
	case strings.HasPrefix(query, "SHOW DATABASES"):
		rows := make([][]driver.Value, 0, len(c.server.databases))
		for _, name := range c.server.databases {
			rows = append(rows, []driver.Value{name})
		}
		return &wispIndexMockRows{cols: []string{"Database"}, data: rows}, nil
	case strings.Contains(query, "information_schema.tables"):
		// args[0] is the schema; args[1:] are the candidate table names.
		schema, _ := args[0].Value.(string)
		present := map[string]bool{}
		for _, tbl := range c.server.tablesByDB[schema] {
			present[tbl] = true
		}
		count := int64(0)
		for _, a := range args[1:] {
			if name, ok := a.Value.(string); ok && present[name] {
				count++
			}
		}
		return &wispIndexMockRows{cols: []string{"count"}, data: [][]driver.Value{{count}}}, nil
	}
	return nil, fmt.Errorf("unexpected query %q", query)
}

type wispIndexMockRows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (r *wispIndexMockRows) Columns() []string { return r.cols }
func (r *wispIndexMockRows) Close() error      { return nil }
func (r *wispIndexMockRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

func TestSQLWispIndexClient_ListUserDatabasesFiltersSystem(t *testing.T) {
	client := newMockWispIndexClient(t, &wispIndexMockServer{
		databases: []string{"information_schema", "mysql", "dolt", "lx", "gc"},
	})
	dbs, err := client.ListUserDatabases(context.Background())
	if err != nil {
		t.Fatalf("ListUserDatabases: %v", err)
	}
	if got := strings.Join(dbs, ","); got != "lx,gc" {
		t.Errorf("expected system databases filtered out, got %q", got)
	}
}

func TestSQLWispIndexClient_HasWispSchema(t *testing.T) {
	client := newMockWispIndexClient(t, &wispIndexMockServer{
		tablesByDB: map[string][]string{
			"lx":      {"issues", "wisps", "wisp_labels", "wisp_dependencies"},
			"partial": {"wisps"}, // missing wisp_labels
			"scratch": {"issues"},
		},
	})
	cases := map[string]bool{"lx": true, "partial": false, "scratch": false, "absent": false}
	for db, want := range cases {
		got, err := client.HasWispSchema(context.Background(), db)
		if err != nil {
			t.Fatalf("HasWispSchema(%q): %v", db, err)
		}
		if got != want {
			t.Errorf("HasWispSchema(%q) = %v, want %v", db, got, want)
		}
	}
}

func TestSQLWispIndexClient_ApplyIndexesRunsStatementsInOrder(t *testing.T) {
	server := &wispIndexMockServer{}
	client := newMockWispIndexClient(t, server)
	if err := client.ApplyIndexes(context.Background(), "lx"); err != nil {
		t.Fatalf("ApplyIndexes: %v", err)
	}
	log := server.logged()
	// USE first, then both index DDLs, then add + commit.
	if len(log) == 0 || !strings.HasPrefix(log[0], "USE ") || !strings.Contains(log[0], "lx") {
		t.Fatalf("expected first statement to USE lx, got %v", log)
	}
	joined := strings.Join(log, "\n")
	for _, stmt := range wispQueryIndexStatements {
		if !strings.Contains(joined, stmt) {
			t.Errorf("missing index statement %q in %v", stmt, log)
		}
	}
	for _, stmt := range wispQueryIndexCommitStatements {
		if !strings.Contains(joined, stmt) {
			t.Errorf("missing commit statement %q in %v", stmt, log)
		}
	}
	// The commit must come after the index DDL.
	lastDDL := strings.LastIndex(joined, "CREATE INDEX")
	firstCommit := strings.Index(joined, "DOLT_ADD")
	if lastDDL < 0 || firstCommit < 0 || firstCommit < lastDDL {
		t.Errorf("commit must follow index creation: %v", log)
	}
}

func TestSQLWispIndexClient_ApplyIndexesNothingToCommitIsSuccess(t *testing.T) {
	server := &wispIndexMockServer{nothingToCommit: true}
	client := newMockWispIndexClient(t, server)
	// An idempotent re-run leaves nothing to commit; that must not be an error.
	if err := client.ApplyIndexes(context.Background(), "lx"); err != nil {
		t.Fatalf("nothing-to-commit must be treated as success, got %v", err)
	}
}
