package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// TestOrderTrackingSweepTargetsSkipMissingRigPaths covers gc-q40pm Problem B:
// the order-tracking sweep must not open stores for configured rigs whose
// directory no longer exists on disk. Every bd subprocess against a dead
// scope root can burn the full 2m command timeout (worse when bd's silent
// on-disk fallback auto-imports a stale issues.jsonl), and the controller
// watchdog re-opens sweep stores every 30s — so a single stale rig entry
// repeatedly starves the reconciler tick. Missing directories are skipped
// loudly instead.
func TestOrderTrackingSweepTargetsSkipMissingRigPaths(t *testing.T) {
	cityDir := t.TempDir()
	liveRig := filepath.Join(cityDir, "live-rig")
	if err := os.MkdirAll(liveRig, 0o755); err != nil {
		t.Fatalf("MkdirAll(liveRig): %v", err)
	}
	goneRig := filepath.Join(cityDir, "gone-rig") // never created

	cfg := &config.City{
		Workspace: config.Workspace{Name: "demo"},
		Rigs: []config.Rig{
			{Name: "live", Path: liveRig},
			{Name: "gone", Path: goneRig},
		},
	}

	var stderr bytes.Buffer
	targets := orderTrackingSweepTargetsForConfig(cityDir, cfg, &stderr)

	var labels []string
	for _, target := range targets {
		labels = append(labels, target.label)
	}
	if len(targets) != 2 {
		t.Fatalf("targets = %v, want city + live rig only", labels)
	}
	if targets[0].label != "city" {
		t.Errorf("targets[0].label = %q, want city", targets[0].label)
	}
	if targets[1].target.RigName != "live" {
		t.Errorf("targets[1].RigName = %q, want live", targets[1].target.RigName)
	}
	if !strings.Contains(stderr.String(), `"gone"`) || !strings.Contains(stderr.String(), goneRig) {
		t.Errorf("stderr = %q, want skip log naming rig \"gone\" and path %q", stderr.String(), goneRig)
	}
}

// TestOrderTrackingSweepTargetsNonDirRigPathSkipped treats a rig path that
// exists but is not a directory like a missing one: there is no store to
// sweep underneath it, only a bd subprocess timeout to pay.
func TestOrderTrackingSweepTargetsNonDirRigPathSkipped(t *testing.T) {
	cityDir := t.TempDir()
	filePath := filepath.Join(cityDir, "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg := &config.City{
		Workspace: config.Workspace{Name: "demo"},
		Rigs: []config.Rig{
			{Name: "flat", Path: filePath},
		},
	}

	targets := orderTrackingSweepTargetsForConfig(cityDir, cfg, nil) // nil stderr must be safe

	if len(targets) != 1 || targets[0].label != "city" {
		var labels []string
		for _, target := range targets {
			labels = append(labels, target.label)
		}
		t.Fatalf("targets = %v, want city only", labels)
	}
}
