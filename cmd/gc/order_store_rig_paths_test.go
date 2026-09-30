package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/orders"
)

// TestOrderStoreResolversLeaveRigPathsUnmutated pins that the order-store
// resolvers resolve a rig's relative path into a local rather than writing the
// absolute form back through cfg.Rigs: the orders lane holds a config the
// reconciler owns, so an in-place rewrite from that goroutine races every other
// reader of the shared slice (gc-4jard).
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
