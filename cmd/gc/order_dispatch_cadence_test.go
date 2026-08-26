package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
)

// countingOrderDispatcher records the wall-clock time of every dispatch call so
// a test can assert on dispatch CADENCE rather than just dispatch count.
type countingOrderDispatcher struct {
	mu    sync.Mutex
	times []time.Time
}

func (c *countingOrderDispatcher) dispatch(_ context.Context, _ string, _ time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.times = append(c.times, time.Now())
}

func (c *countingOrderDispatcher) drain(context.Context) bool { return true }

// countSince returns how many dispatches landed at or after mark.
func (c *countingOrderDispatcher) countSince(mark time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, ts := range c.times {
		if !ts.Before(mark) {
			n++
		}
	}
	return n
}

// An order declares its own cadence (`every = "60s"`), and dispatch has to hold
// that cadence independently of reconcile latency. The reconcile phases are
// unbounded, and time.Ticker coalesces every patrol tick that elapses while the
// loop is busy, so any coupling between the two degrades every declared cadence
// city-wide and without a signal. This pins what keeps them independent: while
// a reconcile is blocked, order dispatch keeps running at the patrol cadence.
func TestOrderDispatchCadenceSurvivesABlockedReconcile(t *testing.T) {
	cityPath := t.TempDir()
	tomlPath := cityPath + "/city.toml"
	writeCityRuntimeConfig(t, tomlPath, "fake")

	cfg, err := config.Load(osFS{}, tomlPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.Daemon.PatrolInterval = "20ms"

	sp := runtime.NewFake()
	var stdout, stderr bytes.Buffer

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// blockReconcile is held shut for the duration of the measurement window so
	// the first desired-state build — and with it the whole reconciler
	// goroutine — is stuck exactly the way a slow reconcile sticks it.
	blockReconcile := make(chan struct{})
	reconcileEntered := make(chan struct{})
	var enterOnce sync.Once

	cr := newTestCityRuntime(t, CityRuntimeParams{
		CityPath: cityPath,
		CityName: "test-city",
		TomlPath: tomlPath,
		Cfg:      cfg,
		SP:       sp,
		BuildFn: func(*config.City, runtime.Provider, beads.Store) DesiredStateResult {
			enterOnce.Do(func() {
				close(reconcileEntered)
				<-blockReconcile
			})
			return DesiredStateResult{State: map[string]TemplateParams{}}
		},
		Dops:   newDrainOps(sp),
		Rec:    events.Discard,
		Stdout: &stdout,
		Stderr: &stderr,
	})

	cs := newControllerState(context.Background(), cfg, sp, events.NewFake(), "test-city", cityPath)
	cs.cityBeadStore = beads.NewMemStore()
	cr.setControllerState(cs)

	counter := &countingOrderDispatcher{}
	cr.od = counter

	done := make(chan struct{})
	go func() {
		cr.run(ctx)
		close(done)
	}()

	// Wait until the reconciler is provably wedged inside the desired-state
	// build, then measure only the dispatches that land after that point.
	select {
	case <-reconcileEntered:
	case <-time.After(hangBudget):
		close(blockReconcile)
		cancel()
		<-done
		t.Fatal("reconcile never started; cannot measure dispatch cadence under a blocked reconcile")
	}

	// Wait for repeated dispatches to land AFTER the reconcile wedged. One would
	// prove only that a pass had started; several prove the loop keeps its own
	// clock while the reconciler is stuck. Polling rather than sleeping a fixed
	// window keeps the assertion on the property instead of on wall-clock, so a
	// loaded box makes this slower, never flaky.
	mark := time.Now()
	const wantAtLeast = 5
	awaitCond(t, func() bool { return counter.countSince(mark) >= wantAtLeast },
		fmt.Sprintf("%d order dispatches while a reconcile is blocked", wantAtLeast))

	close(blockReconcile)
	cancel()
	awaitClose(t, done, "run returning after cancel")
}
