package doctor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

func writeBeadsConfig(t *testing.T, scope, body string) {
	t.Helper()
	dir := filepath.Join(scope, ".beads")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir .beads: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0o644); err != nil {
		t.Fatalf("write config.yaml: %v", err)
	}
}

func TestBdConfigParseCheck_Missing(t *testing.T) {
	scope := t.TempDir()
	c := NewBdConfigParseCheck(scope)
	r := c.Run(&CheckContext{})
	if r.Status != StatusOK {
		t.Fatalf("status = %d, want OK; msg = %s", r.Status, r.Message)
	}
}

func TestBdConfigParseCheck_Valid(t *testing.T) {
	scope := t.TempDir()
	writeBeadsConfig(t, scope, "issue_prefix: lx\nbackup.enabled: false\n")
	c := NewBdConfigParseCheck(scope)
	r := c.Run(&CheckContext{})
	if r.Status != StatusOK {
		t.Fatalf("status = %d, want OK; msg = %s", r.Status, r.Message)
	}
}

func TestBdConfigParseCheck_MalformedConcatenatedKeys(t *testing.T) {
	// Reproduces the gc-0kuep regression: two separate keys collapsed onto
	// a single line so the value of `backup.enabled` becomes
	// `falsetypes.custom: molecule,...`. yaml.v3 reports
	// "mapping values are not allowed in this context".
	scope := t.TempDir()
	body := strings.Join([]string{
		"issue_prefix: gc",
		"dolt.auto-start: false",
		"sync.remote: \"git+ssh://git@github.com/example/example.git\"",
		"",
		"backup.enabled: falsetypes.custom: molecule,convoy,message",
		"types.custom: molecule,convoy,message",
		"",
	}, "\n")
	writeBeadsConfig(t, scope, body)

	c := NewBdConfigParseCheck(scope)
	r := c.Run(&CheckContext{})
	if r.Status != StatusWarning {
		t.Fatalf("status = %d, want Warning; msg = %s", r.Status, r.Message)
	}
	if !strings.Contains(r.Message, "config.yaml") {
		t.Errorf("message %q should mention config.yaml", r.Message)
	}
	if r.FixHint == "" {
		t.Error("expected FixHint explaining bd fallback behavior")
	}
}

func TestBdConfigParseCheck_NonMappingRoot(t *testing.T) {
	// A scalar at the root is valid YAML but not a usable bd config.
	scope := t.TempDir()
	writeBeadsConfig(t, scope, "just-a-string\n")
	c := NewBdConfigParseCheck(scope)
	r := c.Run(&CheckContext{})
	if r.Status != StatusWarning {
		t.Fatalf("status = %d, want Warning; msg = %s", r.Status, r.Message)
	}
}

func TestBdConfigParseCheck_EmptyFile(t *testing.T) {
	// An empty config is degenerate but not malformed; bd treats it as defaults
	// without surfacing a parse warning.
	scope := t.TempDir()
	writeBeadsConfig(t, scope, "")
	c := NewBdConfigParseCheck(scope)
	r := c.Run(&CheckContext{})
	if r.Status != StatusOK {
		t.Fatalf("status = %d, want OK for empty file; msg = %s", r.Status, r.Message)
	}
}

func TestRigBdConfigParseCheck_NameAndScope(t *testing.T) {
	scope := t.TempDir()
	writeBeadsConfig(t, scope, "issue_prefix: gc\n")
	rig := config.Rig{Name: "gascity", Path: scope}
	c := NewRigBdConfigParseCheck(rig)
	if c.Name() != "rig:gascity:bd-config-parse" {
		t.Errorf("name = %q, want rig:gascity:bd-config-parse", c.Name())
	}
	r := c.Run(&CheckContext{})
	if r.Status != StatusOK {
		t.Fatalf("status = %d, want OK; msg = %s", r.Status, r.Message)
	}
}

func TestRigBdConfigParseCheck_FlagsRigMalformedYAML(t *testing.T) {
	scope := t.TempDir()
	writeBeadsConfig(t, scope, "backup.enabled: falsetypes.custom: molecule\ntypes.custom: molecule\n")
	rig := config.Rig{Name: "gascity", Path: scope}
	c := NewRigBdConfigParseCheck(rig)
	r := c.Run(&CheckContext{})
	if r.Status != StatusWarning {
		t.Fatalf("status = %d, want Warning; msg = %s", r.Status, r.Message)
	}
}
