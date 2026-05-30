package main

import (
	"reflect"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// TestEffectivePackDirsForRig pins the per-agent pack-dir union that lets a
// rig-imported sub-pack's PACK-level template-fragments/ register at render
// time (gc-a7qb1c). City dirs always come first so city-level precedence is
// unchanged; the rig's dirs are appended and deduped.
func TestEffectivePackDirsForRig(t *testing.T) {
	cfg := &config.City{
		PackDirs: []string{"/city-a", "/city-b"},
		RigPackDirs: map[string][]string{
			"rig1": {"/rig1-x", "/city-a"}, // /city-a duplicates a city dir
			"rig2": {"/rig2-y"},
		},
	}

	tests := []struct {
		name    string
		rigName string
		want    []string
	}{
		{
			name:    "city or HQ agent gets city dirs unchanged",
			rigName: "",
			want:    []string{"/city-a", "/city-b"},
		},
		{
			name:    "unknown rig falls back to city dirs unchanged",
			rigName: "does-not-exist",
			want:    []string{"/city-a", "/city-b"},
		},
		{
			name:    "rig agent unions city dirs then rig dirs, deduped",
			rigName: "rig1",
			want:    []string{"/city-a", "/city-b", "/rig1-x"},
		},
		{
			name:    "second rig sees only its own dirs unioned",
			rigName: "rig2",
			want:    []string{"/city-a", "/city-b", "/rig2-y"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := effectivePackDirsForRig(cfg, tc.rigName)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("effectivePackDirsForRig(cfg, %q) = %v, want %v", tc.rigName, got, tc.want)
			}
		})
	}
}

// TestEffectivePackDirsForRigNilConfig guards the nil-config path so a render
// site missing its city config cannot panic.
func TestEffectivePackDirsForRigNilConfig(t *testing.T) {
	if got := effectivePackDirsForRig(nil, "rig1"); got != nil {
		t.Errorf("effectivePackDirsForRig(nil, ...) = %v, want nil", got)
	}
}

// TestEffectivePackDirsForRigNoRigDirs confirms a rig with no imported pack
// dirs returns the city dirs unchanged (no needless allocation/divergence).
func TestEffectivePackDirsForRigNoRigDirs(t *testing.T) {
	cfg := &config.City{PackDirs: []string{"/city-a"}}
	got := effectivePackDirsForRig(cfg, "rig-with-no-imports")
	if !reflect.DeepEqual(got, []string{"/city-a"}) {
		t.Errorf("effectivePackDirsForRig(cfg, rig-with-no-imports) = %v, want [/city-a]", got)
	}
}
