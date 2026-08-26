package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/orders"
)

// A dispatch pass resolves order stores on the order-dispatch goroutine, from
// the config the reconciler owns and swaps on reload. A resolver that writes a
// rig's absolute path back into cfg.Rigs makes that pass a second writer to
// shared config, which no lock on the pointer read can make safe.
func TestOrderStoreResolversLeaveRigPathsUnmutated(t *testing.T) {
	const relative = "rigs/alpha"

	tests := []struct {
		name    string
		resolve func(cityPath string, cfg *config.City) (string, error)
	}{
		{
			name: "resolveOrderStoreTarget",
			resolve: func(cityPath string, cfg *config.City) (string, error) {
				target, err := resolveOrderStoreTarget(cityPath, cfg, orders.Order{Name: "sweep", Rig: "alpha"})
				return target.ScopeRoot, err
			},
		},
		{
			name: "orderTrackingSweepTargetsForConfig",
			resolve: func(cityPath string, cfg *config.City) (string, error) {
				for _, target := range orderTrackingSweepTargetsForConfig(cityPath, cfg) {
					if target.target.RigName == "alpha" {
						return target.target.ScopeRoot, nil
					}
				}
				return "", fmt.Errorf("no sweep target for rig %q", "alpha")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cityPath := t.TempDir()
			cfg := &config.City{Rigs: []config.Rig{{Name: "alpha", Path: relative}}}

			got, err := tt.resolve(cityPath, cfg)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if want := filepath.Join(cityPath, relative); got != want {
				t.Fatalf("resolved scope root = %q, want %q", got, want)
			}
			if cfg.Rigs[0].Path != relative {
				t.Fatalf("resolver rewrote cfg.Rigs[0].Path to %q, want %q left as authored", cfg.Rigs[0].Path, relative)
			}
		})
	}
}

// A dispatch pass runs on the order-dispatch goroutine while the reconciler
// swaps cr.cfg under serviceStateMu. Every config read inside a pass must come
// from the pass's own snapshot; a helper that reaches for cr.cfg instead races
// that swap.
//
// Run this with -race for it to mean anything.
func TestDispatchOrdersReadsConfigOnlyThroughItsSnapshot(t *testing.T) {
	cityPath := t.TempDir()
	cr := &CityRuntime{
		cityName:            "test-city",
		cityPath:            cityPath,
		cfg:                 &config.City{Workspace: config.Workspace{Name: "test-city"}},
		standaloneCityStore: beads.NewMemStore(),
		stdout:              io.Discard,
		stderr:              io.Discard,
		logPrefix:           "gc test",
	}

	// The swapper runs for as long as the passes do. Bounding it by an
	// iteration count instead lets it finish before the first pass starts, and
	// the reads then order after every write with no race left to report.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			next := &config.City{Workspace: config.Workspace{Name: "test-city"}}
			cr.serviceStateMu.Lock()
			cr.cfg = next
			cr.serviceStateMu.Unlock()
		}
	}()

	for i := 0; i < 50; i++ {
		// Every watchdog has to run on every pass: their interval stamps would
		// otherwise skip the store resolution this test is here to exercise.
		cr.orderSweepWatchdogLast = time.Time{}
		cr.orderTrackingRetentionWatchdogLast = time.Time{}
		cr.nudgeMailSweepWatchdogLast = time.Time{}
		cr.dispatchOrders(context.Background(), cityPath)
	}
	close(stop)
	wg.Wait()
}
