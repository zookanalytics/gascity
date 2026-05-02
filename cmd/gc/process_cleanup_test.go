package main

import (
	"os"
	"sync"
	"testing"
)

// processCleanupRegistry holds functions to run during test-process
// shutdown, before testscript.Main calls os.Exit (which would otherwise
// skip any deferred functions in TestMain).
//
// Use this for resources allocated in TestMain, package init(), or
// sync.Once blocks that outlive any single test and therefore cannot
// use t.Cleanup. The drain runs in LIFO order so that resources are
// torn down in the reverse of registration order.
type processCleanupRegistry struct {
	mu       sync.Mutex
	handlers []func()
}

func (r *processCleanupRegistry) register(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = append(r.handlers, fn)
}

func (r *processCleanupRegistry) drain() {
	r.mu.Lock()
	handlers := r.handlers
	r.handlers = nil
	r.mu.Unlock()
	for i := len(handlers) - 1; i >= 0; i-- {
		handlers[i]()
	}
}

var globalProcessCleanups = &processCleanupRegistry{}

// registerProcessCleanup arranges for fn to run once the test process
// is finishing, before testscript.Main calls os.Exit. Use this for
// resources whose lifetime spans the entire test process.
func registerProcessCleanup(fn func()) {
	globalProcessCleanups.register(fn)
}

// processCleanupRunner is the subset of *testing.M that processCleanupM
// needs. It also matches testscript.TestingM, allowing us to wrap any
// runner without depending on the concrete *testing.M type.
type processCleanupRunner interface {
	Run() int
}

// processCleanupM wraps a Run() int target so that the global cleanup
// registry is drained between Run() returning and testscript.Main
// calling os.Exit. Without this shim, every /tmp directory created at
// process startup leaks across test runs.
type processCleanupM struct {
	M processCleanupRunner
}

func (p *processCleanupM) Run() int {
	code := p.M.Run()
	globalProcessCleanups.drain()
	return code
}

// runTestscriptSubcommand wraps a testscript subcommand handler so the
// global cleanup registry is drained before os.Exit. Without this, every
// re-exec of the test binary as a subcommand (e.g. exec.Command(gcBin, ...)
// in lifecycle tests) leaks the temporary directories its TestMain creates,
// because os.Exit bypasses any deferred cleanup.
func runTestscriptSubcommand(fn func() int) func() {
	return func() {
		code := fn()
		globalProcessCleanups.drain()
		os.Exit(code)
	}
}

// helperProcessTestEnvActive reports whether this binary is running as
// a Go-test helper subprocess. The exec.Cmd-based helper-process pattern
// re-execs the test binary with GO_WANT_HELPER_PROCESS=1 and a
// -test.run pointing at a single TestHelperProcessXxx that calls
// os.Exit directly. Any package init() or TestMain setup that allocates
// temp directories will leak on that os.Exit, so heavy setup should be
// skipped when this returns true.
func helperProcessTestEnvActive() bool {
	return os.Getenv("GO_WANT_HELPER_PROCESS") == "1"
}

func TestProcessCleanupRegistryRunsLIFO(t *testing.T) {
	var got []int
	reg := &processCleanupRegistry{}
	reg.register(func() { got = append(got, 1) })
	reg.register(func() { got = append(got, 2) })
	reg.register(func() { got = append(got, 3) })

	reg.drain()

	want := []int{3, 2, 1}
	if len(got) != len(want) {
		t.Fatalf("drain order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("drain order = %v, want %v (LIFO)", got, want)
		}
	}
}

func TestProcessCleanupRegistryDrainResetsHandlers(t *testing.T) {
	var calls int
	reg := &processCleanupRegistry{}
	reg.register(func() { calls++ })

	reg.drain()
	reg.drain()

	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1 (drain should clear the registry)", calls)
	}
}

func TestProcessCleanupRegistryConcurrentRegister(t *testing.T) {
	reg := &processCleanupRegistry{}
	var wg sync.WaitGroup
	const n = 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reg.register(func() {})
		}()
	}
	wg.Wait()

	reg.mu.Lock()
	got := len(reg.handlers)
	reg.mu.Unlock()
	if got != n {
		t.Fatalf("registered %d handlers, want %d", got, n)
	}
}

func TestProcessCleanupRegistryEmptyDrainNoPanic(_ *testing.T) {
	reg := &processCleanupRegistry{}
	reg.drain() // no handlers; must not panic
}

type stubProcessCleanupInnerM struct {
	code   int
	called int
}

func (f *stubProcessCleanupInnerM) Run() int {
	f.called++
	return f.code
}

func TestProcessCleanupMRunsInnerThenDrainsGlobalRegistry(t *testing.T) {
	prev := globalProcessCleanups
	globalProcessCleanups = &processCleanupRegistry{}
	t.Cleanup(func() { globalProcessCleanups = prev })

	var order []string
	registerProcessCleanup(func() { order = append(order, "first") })
	registerProcessCleanup(func() { order = append(order, "second") })

	inner := &stubProcessCleanupInnerM{code: 7}
	wrap := &processCleanupM{M: &recordingRunner{inner: inner, on: func() {
		order = append(order, "run")
	}}}

	if code := wrap.Run(); code != 7 {
		t.Fatalf("Run() exit code = %d, want 7", code)
	}
	if inner.called != 1 {
		t.Fatalf("inner Run called %d times, want 1", inner.called)
	}
	want := []string{"run", "second", "first"}
	if len(order) != len(want) {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("execution order = %v, want %v (run before LIFO drain)", order, want)
		}
	}

	// A second drain is a no-op; the wrapper must clear the registry.
	globalProcessCleanups.drain()
	if len(order) != 3 {
		t.Fatalf("registry not cleared after wrap.Run; order = %v", order)
	}
}

type recordingRunner struct {
	inner processCleanupRunner
	on    func()
}

func (r *recordingRunner) Run() int {
	r.on()
	return r.inner.Run()
}
