//go:build integration

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/citylayout"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/orders"
	"github.com/gastownhall/gascity/internal/runtime"
)

func TestControllerDiscoversAddedCronOrderWithoutRestart(t *testing.T) {
	clearInheritedBeadsEnv(t)
	t.Setenv("GC_BEADS", "")

	oldRescanInterval := orderRescanInterval
	orderRescanInterval = 20 * time.Millisecond
	t.Cleanup(func() { orderRescanInterval = oldRescanInterval })

	sp := runtime.NewFake()
	var reconcileCount atomic.Int32
	buildFn := func(c *config.City, _ runtime.Provider, _ beads.Store) DesiredStateResult {
		reconcileCount.Add(1)
		ds := make(map[string]TemplateParams)
		for _, a := range c.Agents {
			if a.Implicit {
				continue
			}
			ds[a.Name] = TemplateParams{SessionName: a.Name, TemplateName: a.Name, Command: "echo hello"}
		}
		return DesiredStateResult{State: ds}
	}

	dir := shortSocketTempDir(t, "gc-order-dynamic-")
	disableManagedDoltRecoveryForTest(t)
	cleanupManagedDoltTestCity(t, dir)
	if err := os.MkdirAll(filepath.Join(dir, ".gc"), 0o755); err != nil {
		t.Fatal(err)
	}
	tomlPath := writeCityTOML(t, dir, "test", "mayor")
	cfg, prov, err := loadCityConfigWithBuiltinPacks(dir)
	if err != nil {
		t.Fatal(err)
	}
	applyFeatureFlags(cfg)
	cfg.Daemon.PatrolInterval = "50ms"

	var stderr bytes.Buffer
	allOrders, err := scanAllOrders(dir, cfg, &stderr, "integration")
	if err != nil {
		t.Fatal(err)
	}
	for _, order := range allOrders {
		cfg.Orders.Skip = append(cfg.Orders.Skip, order.Name)
	}
	configRev := config.Revision(fsys.OSFS{}, prov, cfg, dir)

	var stdout bytes.Buffer
	done := make(chan struct{})
	go func() {
		runController(dir, tomlPath, cfg, configRev, buildFn, nil, sp, nil, nil, nil, nil, events.Discard, nil, &stdout, &stderr)
		close(done)
	}()
	t.Cleanup(func() {
		tryStopController(dir, &bytes.Buffer{})
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})
	waitForController(t, dir)
	waitForCondition(t, 5*time.Second, func() bool {
		return reconcileCount.Load() > 0
	}, "initial reconcile")

	if err := os.MkdirAll(filepath.Join(dir, "orders"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orders", "test-tick.toml"), []byte(`
[order]
exec = "true"
trigger = "cron"
schedule = "*/1 * * * *"
`), 0o644); err != nil {
		t.Fatal(err)
	}

	// gc-7hf34: cooldown/cron orders record each fire by advancing the durable
	// last-run file cursor instead of minting an order-run tracking bead, so the
	// fire signal is the cursor for "test-tick" advancing off the zero value —
	// not an order-run bead appearing.
	runtimeDir := citylayout.RuntimeDataDir(dir)
	waitForCondition(t, 10*time.Second, func() bool {
		lastRun, err := orders.ReadLastRun(runtimeDir, "test-tick")
		return err == nil && !lastRun.IsZero()
	}, "dynamic cron order fire")
}

func waitForCondition(t *testing.T, timeout time.Duration, ok func() bool, name string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", name)
}
