package queryindex

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fakeClock advances only when slept on, so a test controls every wait.
type fakeClock struct {
	now    time.Time
	slept  []time.Duration
	onWake func()
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 10, 10, 2, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.slept = append(c.slept, d)
	c.now = c.now.Add(d)
	if c.onWake != nil {
		c.onWake()
	}
	return ctx.Err()
}

func TestWaitQuietReturnsOnceHashHoldsForQuietFor(t *testing.T) {
	clock := newFakeClock()
	hashes := []string{"a", "b", "b", "b", "b", "b"}
	reads := 0
	hash := func(context.Context) (string, error) {
		h := hashes[reads]
		reads++
		return h, nil
	}
	opts := QuietOptions{QuietFor: 15 * time.Second, Poll: 5 * time.Second, MaxWait: time.Minute}
	start := clock.Now()
	quiet, err := WaitQuiet(context.Background(), clock, hash, opts)
	if err != nil || !quiet {
		t.Fatalf("WaitQuiet = %v, %v; want true, nil", quiet, err)
	}
	// "b" first appears at +5s and must hold until +20s.
	if got := clock.Now().Sub(start); got != 20*time.Second {
		t.Fatalf("WaitQuiet returned after %v, want 20s", got)
	}
}

func TestWaitQuietGivesUpAtMaxWait(t *testing.T) {
	clock := newFakeClock()
	n := 0
	hash := func(context.Context) (string, error) {
		n++
		return fmt.Sprint(n), nil
	}
	opts := QuietOptions{QuietFor: 15 * time.Second, Poll: 5 * time.Second, MaxWait: time.Minute}
	start := clock.Now()
	quiet, err := WaitQuiet(context.Background(), clock, hash, opts)
	if err != nil || quiet {
		t.Fatalf("WaitQuiet = %v, %v; want false, nil", quiet, err)
	}
	if got := clock.Now().Sub(start); got != time.Minute {
		t.Fatalf("WaitQuiet gave up after %v, want 1m", got)
	}
}

func TestWaitQuietPropagatesHashError(t *testing.T) {
	boom := errors.New("dolt_hashof_table unsupported")
	_, err := WaitQuiet(context.Background(), newFakeClock(), func(context.Context) (string, error) { return "", boom }, QuietOptions{QuietFor: time.Second, Poll: time.Second, MaxWait: time.Minute})
	if !errors.Is(err, boom) {
		t.Fatalf("WaitQuiet error = %v, want %v", err, boom)
	}
}

// fakeStore is a bead store whose catalog, table hashes and builds a test scripts.
type fakeStore struct {
	label      string
	present    map[string]bool // index names present
	inspectErr error
	hash       func(table string) string
	buildErr   map[string]error
	noop       map[string]bool // build reports success but creates nothing
	built      []string
	// version is the Dolt version the store reports; empty reports "test".
	version string
	// unsafe makes the self-test fail with this reason.
	unsafe string
	// selfTestErr makes the self-test fail to run.
	selfTestErr error
	selfTests   int
}

func (f *fakeStore) store() Store {
	return Store{
		Label: f.label,
		Inspect: func(_ context.Context, want []Index) (Catalog, error) {
			if f.inspectErr != nil {
				return Catalog{}, f.inspectErr
			}
			var rows []statisticsRow
			tables := map[string]bool{}
			for _, ix := range want {
				tables[ix.Table] = true
				switch {
				case !f.present[ix.Name]:
				case ix.MetadataKey != "":
					rows = append(rows, statisticsRow{table: ix.Table, index: ix.Name, seq: 1, expression: nullString(ix.expression())})
				default:
					for i, column := range ix.Columns {
						rows = append(rows, statisticsRow{table: ix.Table, index: ix.Name, seq: i + 1, column: nullString(column)})
					}
				}
			}
			var names []string
			for table := range tables {
				names = append(names, table)
			}
			return catalogFromRows(names, rows), nil
		},
		TableHash: func(_ context.Context, table string) (string, error) {
			if f.hash != nil {
				return f.hash(table), nil
			}
			return "steady", nil
		},
		Build: func(_ context.Context, ix Index) error {
			f.built = append(f.built, ix.Name)
			if err := f.buildErr[ix.Name]; err != nil {
				return err
			}
			if !f.noop[ix.Name] {
				f.present[ix.Name] = true
			}
			return nil
		},
		DoltVersion: func(context.Context) (string, error) {
			if f.version == "" {
				return "test", nil
			}
			return f.version, nil
		},
		SelfTest: func(context.Context) (SelfTestResult, error) {
			f.selfTests++
			if f.selfTestErr != nil {
				return SelfTestResult{}, f.selfTestErr
			}
			if f.unsafe != "" {
				return SelfTestResult{Failure: f.unsafe}, nil
			}
			return SelfTestResult{OK: true}, nil
		},
	}
}

func newTestMaintainer(clock *fakeClock, log *[]string) *Maintainer {
	return &Maintainer{
		Quiet:       QuietOptions{QuietFor: 10 * time.Second, Poll: 5 * time.Second, MaxWait: time.Minute},
		Spacing:     time.Minute,
		BaseBackoff: time.Hour,
		MaxBackoff:  4 * time.Hour,
		Clock:       clock,
		Logf: func(format string, args ...any) {
			*log = append(*log, fmt.Sprintf(format, args...))
		},
	}
}

func TestMaintainerBuildsOnlyMissingIndexesOneAtATime(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	anchor := MetadataIndex("issues", "anchor_bead")
	status := ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	city := &fakeStore{label: "city", present: map[string]bool{status.Name: true}}
	rig := &fakeStore{label: "rig/gascity", present: map[string]bool{status.Name: true, root.Name: true}}
	clock := newFakeClock()
	var log []string
	m := newTestMaintainer(clock, &log)

	if err := m.Pass(context.Background(), []Store{city.store(), rig.store()}, []Index{status, root, anchor}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if want := []string{root.Name, anchor.Name}; !reflect.DeepEqual(city.built, want) {
		t.Fatalf("city built %v, want %v", city.built, want)
	}
	if want := []string{anchor.Name}; !reflect.DeepEqual(rig.built, want) {
		t.Fatalf("rig built %v, want %v", rig.built, want)
	}
	spacings := 0
	for _, d := range clock.slept {
		if d == time.Minute {
			spacings++
		}
	}
	if spacings != 3 {
		t.Fatalf("slept the build spacing %d times, want once per build (3); sleeps %v", spacings, clock.slept)
	}
	if !containsLine(log, "city: built issues(metadata gc.root_bead_id)") {
		t.Fatalf("log lacks the city build line:\n%s", strings.Join(log, "\n"))
	}
}

func TestMaintainerDefersBusyTableWithoutBackoff(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	n := 0
	busy := &fakeStore{label: "rig/tk", present: map[string]bool{}, hash: func(string) string {
		n++
		return fmt.Sprint(n)
	}}
	clock := newFakeClock()
	var log []string
	m := newTestMaintainer(clock, &log)

	if err := m.Pass(context.Background(), []Store{busy.store()}, []Index{root}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if len(busy.built) != 0 {
		t.Fatalf("built %v on a table that never went quiet", busy.built)
	}
	if !containsLine(log, "rig/tk: issues(metadata gc.root_bead_id) waits for a quiet table") {
		t.Fatalf("log lacks the deferral line:\n%s", strings.Join(log, "\n"))
	}
	// A deferral is not a failure: once the table settles, the next pass builds.
	busy.hash = nil
	if err := m.Pass(context.Background(), []Store{busy.store()}, []Index{root}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if want := []string{root.Name}; !reflect.DeepEqual(busy.built, want) {
		t.Fatalf("second pass built %v, want %v", busy.built, want)
	}
}

func TestMaintainerBacksOffFailedBuildAndDoublesTheDelay(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	store := &fakeStore{label: "city", present: map[string]bool{}, buildErr: map[string]error{root.Name: errors.New("context canceled")}}
	clock := newFakeClock()
	var log []string
	m := newTestMaintainer(clock, &log)
	pass := func() {
		t.Helper()
		if err := m.Pass(context.Background(), []Store{store.store()}, []Index{root}); err != nil {
			t.Fatalf("Pass: %v", err)
		}
	}

	pass()
	if len(store.built) != 1 {
		t.Fatalf("first pass attempted %d builds, want 1", len(store.built))
	}
	if !containsLine(log, "city: building issues(metadata gc.root_bead_id) failed; next attempt in 1h0m0s: context canceled") {
		t.Fatalf("log lacks the failure line:\n%s", strings.Join(log, "\n"))
	}
	pass()
	if len(store.built) != 1 {
		t.Fatalf("a pass inside the backoff retried the build (%d attempts)", len(store.built))
	}
	clock.now = clock.now.Add(time.Hour)
	pass()
	if len(store.built) != 2 {
		t.Fatalf("a pass after the backoff did not retry (%d attempts)", len(store.built))
	}
	if !containsLine(log, "next attempt in 2h0m0s") {
		t.Fatalf("second failure did not double the backoff:\n%s", strings.Join(log, "\n"))
	}
	delete(store.buildErr, root.Name)
	clock.now = clock.now.Add(2 * time.Hour)
	pass()
	if !store.present[root.Name] {
		t.Fatal("index was not built once the failure cleared")
	}
}

func TestMaintainerTreatsBuildThatLeavesIndexMissingAsFailure(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	store := &fakeStore{label: "city", present: map[string]bool{}, noop: map[string]bool{root.Name: true}}
	var log []string
	m := newTestMaintainer(newFakeClock(), &log)
	if err := m.Pass(context.Background(), []Store{store.store()}, []Index{root}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if !containsLine(log, "still missing after the build") {
		t.Fatalf("a build that created nothing was logged as success:\n%s", strings.Join(log, "\n"))
	}
	if err := m.Pass(context.Background(), []Store{store.store()}, []Index{root}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if len(store.built) != 1 {
		t.Fatalf("a build that created nothing was retried without backoff (%d attempts)", len(store.built))
	}
}

func TestMaintainerContinuesPastAStoreItCannotRead(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	broken := &fakeStore{label: "rig/down", inspectErr: errors.New("connection refused")}
	healthy := &fakeStore{label: "city", present: map[string]bool{}}
	var log []string
	m := newTestMaintainer(newFakeClock(), &log)
	if err := m.Pass(context.Background(), []Store{broken.store(), healthy.store()}, []Index{root}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if len(healthy.built) != 1 {
		t.Fatalf("healthy store built %v after an unreadable one, want one build", healthy.built)
	}
	if !containsLine(log, "rig/down: reading indexes: connection refused") {
		t.Fatalf("log lacks the read failure:\n%s", strings.Join(log, "\n"))
	}
}

func TestMaintainerStopsWhenContextEnds(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	anchor := MetadataIndex("issues", "anchor_bead")
	store := &fakeStore{label: "city", present: map[string]bool{}}
	clock := newFakeClock()
	ctx, cancel := context.WithCancel(context.Background())
	clock.onWake = cancel
	var log []string
	m := newTestMaintainer(clock, &log)
	err := m.Pass(ctx, []Store{store.store()}, []Index{root, anchor})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Pass error = %v, want context.Canceled", err)
	}
	if len(store.built) != 0 {
		t.Fatalf("built %v after the context ended", store.built)
	}
}

func TestMaintainerHoldsMetadataIndexesOnADoltThatFailsTheSelfTest(t *testing.T) {
	status := ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	root := MetadataIndex("issues", "gc.root_bead_id")
	city := &fakeStore{label: "city", present: map[string]bool{}, version: "2.4.2", unsafe: "an aliased UPDATE after narrowing a column failed: table not found: probe"}
	rig := &fakeStore{label: "rig/gascity", present: map[string]bool{}, version: "2.4.2", unsafe: "unused: the verdict is per version"}
	var log []string
	m := newTestMaintainer(newFakeClock(), &log)

	for pass := 0; pass < 2; pass++ {
		if err := m.Pass(context.Background(), []Store{city.store(), rig.store()}, []Index{status, root}); err != nil {
			t.Fatalf("Pass: %v", err)
		}
	}
	for _, s := range []*fakeStore{city, rig} {
		if s.present[root.Name] {
			t.Errorf("%s: built a metadata index on a Dolt that failed the self-test", s.label)
		}
		if !s.present[status.Name] {
			t.Errorf("%s: the column index was held along with the metadata index", s.label)
		}
	}
	if got := city.selfTests + rig.selfTests; got != 1 {
		t.Errorf("ran the self-test %d times for one Dolt version, want 1", got)
	}
	held := 0
	for _, line := range log {
		if strings.Contains(line, "metadata indexes held while the Dolt server runs 2.4.2: an aliased UPDATE after narrowing a column failed") {
			held++
		}
	}
	if held != 1 {
		t.Errorf("logged the hold %d times, want once per Dolt version:\n%s", held, strings.Join(log, "\n"))
	}

	// A server upgraded to a release that passes gets its indexes.
	city.version, city.unsafe = "2.5.0", ""
	if err := m.Pass(context.Background(), []Store{city.store()}, []Index{status, root}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if !city.present[root.Name] {
		t.Fatal("the metadata index was not built after the server passed the self-test")
	}
}

func TestMaintainerRetriesASelfTestThatCouldNotRun(t *testing.T) {
	root := MetadataIndex("issues", "gc.root_bead_id")
	store := &fakeStore{label: "city", present: map[string]bool{}, selfTestErr: errors.New("registering the self-test tables in dolt_ignore: access denied")}
	var log []string
	m := newTestMaintainer(newFakeClock(), &log)
	if err := m.Pass(context.Background(), []Store{store.store()}, []Index{root}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if store.present[root.Name] {
		t.Fatal("built a metadata index without a self-test verdict")
	}
	if !containsLine(log, "city: metadata indexes held: the Dolt test index self-test did not run: registering the self-test tables in dolt_ignore: access denied") {
		t.Fatalf("log lacks the self-test failure:\n%s", strings.Join(log, "\n"))
	}
	store.selfTestErr = nil
	if err := m.Pass(context.Background(), []Store{store.store()}, []Index{root}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if store.selfTests != 2 || !store.present[root.Name] {
		t.Fatalf("the self-test was not retried on the next pass (runs %d, built %v)", store.selfTests, store.present[root.Name])
	}
}

func TestMaintainerSkipsTheSelfTestWhenNoMetadataIndexIsMissing(t *testing.T) {
	status := ColumnIndex("wisps", "idx_wisps_status_type", "status", "issue_type")
	store := &fakeStore{label: "city", present: map[string]bool{}}
	var log []string
	m := newTestMaintainer(newFakeClock(), &log)
	if err := m.Pass(context.Background(), []Store{store.store()}, []Index{status}); err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if store.selfTests != 0 {
		t.Fatalf("ran the self-test %d times with no metadata index to build", store.selfTests)
	}
}

func containsLine(lines []string, substr string) bool {
	for _, line := range lines {
		if strings.Contains(line, substr) {
			return true
		}
	}
	return false
}
