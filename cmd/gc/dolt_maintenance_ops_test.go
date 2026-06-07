package main

import (
	"context"
	"errors"
	"strings"
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
