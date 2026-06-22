package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/orders"
)

func TestOrderDispatchInstrumentEnvGate(t *testing.T) {
	t.Setenv(orderDispatchInstrumentEnvVar, "")
	if orderDispatchInstrumentEnabled() {
		t.Fatal("instrumentation must be OFF when the env switch is unset")
	}
	for _, v := range []string{"1", "true", "TRUE", "yes", "on"} {
		t.Setenv(orderDispatchInstrumentEnvVar, v)
		if !orderDispatchInstrumentEnabled() {
			t.Fatalf("instrumentation must be ON for env value %q", v)
		}
	}
	for _, v := range []string{"0", "false", "no", "off", "bogus"} {
		t.Setenv(orderDispatchInstrumentEnvVar, v)
		if orderDispatchInstrumentEnabled() {
			t.Fatalf("instrumentation must be OFF for env value %q", v)
		}
	}
}

func TestBoundRigNameSetSkipsUnboundRigs(t *testing.T) {
	cfg := &config.City{Rigs: []config.Rig{
		{Name: "bound-a", Path: "/city/rigs/a"},
		{Name: "unbound", Path: ""},
		{Name: "bound-b", Path: "/city/rigs/b"},
	}}
	set := boundRigNameSet(cfg)
	if _, ok := set["bound-a"]; !ok {
		t.Error("bound-a (non-empty path) must be cached")
	}
	if _, ok := set["bound-b"]; !ok {
		t.Error("bound-b (non-empty path) must be cached")
	}
	if _, ok := set["unbound"]; ok {
		t.Error("unbound rig (empty path) must NOT be cached — controller builds no handle for it")
	}
	if got := len(set); got != 2 {
		t.Fatalf("set size = %d, want 2", got)
	}
}

func TestRecordStoreOpenClassifiesEliminable(t *testing.T) {
	var buf bytes.Buffer
	m := &memoryOrderDispatcher{
		instrumentOpens: true,
		boundRigNames:   map[string]struct{}{"gc-toolkit": {}},
		stderr:          &buf,
	}
	// City scope: controller always holds cityBeadStore → eliminable.
	m.recordStoreOpen(execStoreTarget{ScopeKind: "city", ScopeRoot: "/city"}, nil)
	// Bound rig: controller holds beadStores[gc-toolkit] → eliminable.
	m.recordStoreOpen(execStoreTarget{ScopeKind: "rig", ScopeRoot: "/city/rigs/gc-toolkit", RigName: "gc-toolkit"}, nil)
	// Unknown/unbound rig: no controller handle → NOT eliminable.
	m.recordStoreOpen(execStoreTarget{ScopeKind: "rig", ScopeRoot: "/elsewhere", RigName: "stranger"}, nil)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 instrumentation lines, got %d: %q", len(lines), buf.String())
	}
	if !strings.Contains(lines[0], "scope=city") || !strings.Contains(lines[0], "cached=true") {
		t.Errorf("city line wrong: %q", lines[0])
	}
	if !strings.Contains(lines[1], "rig=gc-toolkit") || !strings.Contains(lines[1], "cached=true") {
		t.Errorf("bound-rig line wrong: %q", lines[1])
	}
	if !strings.Contains(lines[2], "rig=stranger") || !strings.Contains(lines[2], "cached=false") {
		t.Errorf("unbound-rig line wrong: %q", lines[2])
	}
	// Every line must carry the grep prefix and an err flag.
	for i, ln := range lines {
		if !strings.HasPrefix(ln, orderStoreOpenLogPrefix) {
			t.Errorf("line %d missing prefix: %q", i, ln)
		}
		if !strings.Contains(ln, "err=false") {
			t.Errorf("line %d missing err flag: %q", i, ln)
		}
	}
}

func TestRecordStoreOpenNoopWhenDisabled(t *testing.T) {
	var buf bytes.Buffer
	m := &memoryOrderDispatcher{instrumentOpens: false, stderr: &buf}
	m.recordStoreOpen(execStoreTarget{ScopeKind: "city", ScopeRoot: "/city"}, nil)
	if buf.Len() != 0 {
		t.Fatalf("disabled instrumentation must emit nothing, got %q", buf.String())
	}
}

// TestDispatchEmitsStoreOpenInstrumentation exercises the real dispatch-loop
// call site: a single city-scoped order opens the city store exactly once per
// tick, and the instrumentation classifies it as eliminable (city → cached).
func TestDispatchEmitsStoreOpenInstrumentation(t *testing.T) {
	store := beads.NewMemStore()
	var buf bytes.Buffer
	m := &memoryOrderDispatcher{
		aa: []orders.Order{{
			Name:     "beads-health",
			Trigger:  "cooldown",
			Interval: "30s",
			Exec:     "true",
		}},
		storeFn:         func(_ execStoreTarget) (beads.Store, error) { return store, nil },
		execRun:         shellExecRunner,
		rec:             events.Discard,
		stderr:          lockedStderr(&buf),
		cfg:             &config.City{},
		instrumentOpens: true,
	}

	m.dispatch(context.Background(), t.TempDir(), time.Now())
	m.drain(context.Background())

	out := buf.String()
	if !strings.Contains(out, orderStoreOpenLogPrefix) {
		t.Fatalf("expected a store-open instrumentation line, got: %q", out)
	}
	if !strings.Contains(out, "scope=city") || !strings.Contains(out, "cached=true") {
		t.Fatalf("expected city/eliminable classification, got: %q", out)
	}
	// One distinct target ⇒ exactly one open per tick, even across many orders.
	if n := strings.Count(out, orderStoreOpenLogPrefix); n != 1 {
		t.Fatalf("expected exactly 1 store open this tick, got %d: %q", n, out)
	}
}

// TestDispatchStoreOpensPerTickOverWindow demonstrates the core churn claim:
// the city store is re-opened once per tick regardless of whether any order is
// due. A single city-scoped cooldown order is due only on the first tick, yet
// every subsequent tick still opens the store to evaluate its gate — so opens
// scale with ticks, not with dispatch activity. ticks opens == ticks ⇒ on a 30s
// patrol that is 2 opens/min, all city-scoped and therefore eliminable.
func TestDispatchStoreOpensPerTickOverWindow(t *testing.T) {
	store := beads.NewMemStore()
	var buf bytes.Buffer
	m := &memoryOrderDispatcher{
		aa: []orders.Order{{
			Name:     "beads-health",
			Trigger:  "cooldown",
			Interval: "1h", // due once, then dormant for the rest of the window
			Exec:     "true",
		}},
		storeFn:         func(_ execStoreTarget) (beads.Store, error) { return store, nil },
		execRun:         shellExecRunner,
		rec:             events.Discard,
		stderr:          lockedStderr(&buf),
		cfg:             &config.City{},
		instrumentOpens: true,
	}

	const ticks = 10
	base := time.Now()
	dir := t.TempDir()
	for k := 0; k < ticks; k++ {
		m.dispatch(context.Background(), dir, base.Add(time.Duration(k)*30*time.Second))
		m.drain(context.Background())
	}

	opens := strings.Count(buf.String(), orderStoreOpenLogPrefix)
	if opens != ticks {
		t.Fatalf("expected exactly %d store opens (1/tick), got %d:\n%s", ticks, opens, buf.String())
	}
	if got := strings.Count(buf.String(), "cached=true"); got != ticks {
		t.Fatalf("expected all %d opens classified eliminable (cached=true), got %d", ticks, got)
	}
}
