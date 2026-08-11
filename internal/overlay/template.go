// Package overlay — text/template rendering for staged overlay files.
package overlay

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

// TemplateMarker is the filename segment that marks an overlay file as
// templated. A pack file named "<name>.template.<ext>" is rendered through
// text/template during staging and lands at "<name>.<ext>".
//
// The convention is deliberately the one the MCP catalog loader already uses
// for "<name>.template.toml", so a pack author learns a single rule that
// covers every templated pack file class. It exists because some values a
// pack wants to ship — the city root above all — are known only at install
// time, and a staged file that carries them unbound is wrong the moment it
// lands rather than merely incomplete.
//
// The marker alone does not make a file this package's to render. It is a
// shared naming convention, not a private one: a pack's
// agents/<name>/prompt.template.md carries the same marker and belongs to the
// prompt renderer, which expands it later against its own data map and
// funcmap. Rendering here is therefore opt-in per copy operation — see
// WithTemplateData — so a copy that carries no install context leaves every
// templated file untouched, name included.
const TemplateMarker = ".template"

// TemplateTargetName returns the staged filename for a templated overlay file
// name, with the marker segment removed. It reports false for a name that
// does not carry the marker directly before its extension, and for a name
// whose entire stem is the marker (".template.json"), which has no target
// name left to stage to.
func TemplateTargetName(name string) (string, bool) {
	ext := filepath.Ext(name)
	if ext == "" {
		return "", false
	}
	stem := strings.TrimSuffix(name, ext)
	base := strings.TrimSuffix(stem, TemplateMarker)
	if base == stem || base == "" {
		return "", false
	}
	return base + ext, true
}

// CopyOption configures an overlay copy operation.
type CopyOption func(*copyConfig)

// copyConfig carries per-operation staging settings down the directory walk.
type copyConfig struct {
	renderTemplates bool
	templateData    map[string]string
}

// WithTemplateData supplies the data map used to render templated overlay
// files (see TemplateTargetName). Its keys are the same expansion surface MCP
// catalog templates use — CityRoot, RigRoot, WorkDir, AgentName, and the
// agent env — so one pack-authoring vocabulary covers both file classes.
//
// Passing this option is what opts a copy into rendering at all. A copy
// without it treats a templated file as any other file: byte-for-byte, marker
// left in the name. That boundary is load-bearing rather than a convenience —
// the same .template.<ext> convention names pack files owned by other
// renderers (agent prompts above all), and directory copies that carry no
// install context walk right through them.
//
// Opting in with a nil map is still opting in. Rendering uses
// missingkey=error, so a caller that reaches this seam with nothing to bind
// fails loudly, naming the template, instead of staging a half-bound file.
func WithTemplateData(data map[string]string) CopyOption {
	return func(c *copyConfig) {
		c.renderTemplates = true
		c.templateData = data
	}
}

// newCopyConfig folds opts into a copyConfig.
func newCopyConfig(opts []CopyOption) copyConfig {
	var cfg copyConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// stagedRelPath returns the destination-relative path relPath stages to under
// cfg, resolving the template marker when this copy renders templates.
// Staging rules that key on the destination — JSON merge, hook wrapping,
// provider preserve-existing — must consult this path rather than the source
// path, so a templated settings file behaves exactly like its non-templated
// twin. A copy that does not render templates stages every file at its own
// name, so the path is returned unchanged.
func (c copyConfig) stagedRelPath(relPath string) string {
	if !c.renderTemplates {
		return relPath
	}
	target, ok := TemplateTargetName(filepath.Base(relPath))
	if !ok {
		return relPath
	}
	return filepath.Join(filepath.Dir(relPath), target)
}

// renderTemplateFile expands src through text/template with data and returns
// the rendered bytes together with src's permission bits.
func renderTemplateFile(src string, data map[string]string) ([]byte, os.FileMode, error) {
	raw, err := os.ReadFile(src)
	if err != nil {
		return nil, 0, fmt.Errorf("overlay: reading template %q: %w", src, err)
	}
	info, err := os.Stat(src)
	if err != nil {
		return nil, 0, fmt.Errorf("overlay: stat template %q: %w", src, err)
	}
	tmpl, err := template.New(filepath.Base(src)).Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return nil, 0, fmt.Errorf("overlay: parsing template %q: %w", src, err)
	}
	if data == nil {
		data = map[string]string{}
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return nil, 0, fmt.Errorf("overlay: rendering template %q: %w", src, err)
	}
	return out.Bytes(), info.Mode().Perm(), nil
}
