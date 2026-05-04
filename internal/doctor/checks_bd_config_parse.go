package doctor

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/gastownhall/gascity/internal/config"
)

// BdConfigParseCheck verifies that .beads/config.yaml at the given scope is
// valid YAML with a mapping root. bd silently falls back to defaults when its
// config fails to parse, which can re-enable auto-backup after a git remote
// is detected and drive a CALL DOLT_BACKUP('add'/'rm'/'sync', 'backup_export')
// hot loop that fills the disk with archive chunks. See gc-0kuep.
type BdConfigParseCheck struct {
	name      string
	scopePath string
}

// NewBdConfigParseCheck creates a city-level bd config parse check.
func NewBdConfigParseCheck(scopePath string) *BdConfigParseCheck {
	return &BdConfigParseCheck{name: "bd-config-parse", scopePath: scopePath}
}

// NewRigBdConfigParseCheck creates a rig-level bd config parse check.
func NewRigBdConfigParseCheck(rig config.Rig) *BdConfigParseCheck {
	return &BdConfigParseCheck{name: "rig:" + rig.Name + ":bd-config-parse", scopePath: rig.Path}
}

// Name returns the check identifier.
func (c *BdConfigParseCheck) Name() string { return c.name }

// Run reads .beads/config.yaml at the configured scope and reports a Warning
// when the file fails to parse as a YAML mapping. Missing or empty files are
// treated as OK because bd handles those without falling back to defaults
// for unrelated keys.
func (c *BdConfigParseCheck) Run(_ *CheckContext) *CheckResult {
	r := &CheckResult{Name: c.Name()}
	cfgPath := filepath.Join(c.scopePath, ".beads", "config.yaml")
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		if os.IsNotExist(err) {
			r.Status = StatusOK
			r.Message = ".beads/config.yaml not present"
			return r
		}
		r.Status = StatusWarning
		r.Message = fmt.Sprintf("read .beads/config.yaml: %v", err)
		return r
	}
	if len(bytes.TrimSpace(data)) == 0 {
		r.Status = StatusOK
		r.Message = ".beads/config.yaml is empty (bd uses defaults)"
		return r
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		r.Status = StatusWarning
		r.Message = fmt.Sprintf(".beads/config.yaml has invalid YAML: %v", err)
		r.FixHint = "rewrite .beads/config.yaml as valid YAML — bd silently falls back to defaults on parse error, which can re-enable auto-backup against a git remote and trigger a dolt_backup('backup_export') hot loop (gc-0kuep)"
		return r
	}
	if len(doc.Content) > 0 && doc.Content[0].Kind != yaml.MappingNode {
		r.Status = StatusWarning
		r.Message = ".beads/config.yaml root is not a mapping"
		r.FixHint = "the file must be a YAML mapping (key: value pairs); bd ignores non-mapping documents and falls back to defaults"
		return r
	}
	r.Status = StatusOK
	r.Message = ".beads/config.yaml parses cleanly"
	return r
}

// CanFix returns false — fixing requires inspecting the corrupted content.
func (c *BdConfigParseCheck) CanFix() bool { return false }

// Fix is a no-op.
func (c *BdConfigParseCheck) Fix(_ *CheckContext) error { return nil }
