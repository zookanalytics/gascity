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
	templateData map[string]string
}

// WithTemplateData supplies the data map used to render templated overlay
// files (see TemplateTargetName). Its keys are the same expansion surface MCP
// catalog templates use — CityRoot, RigRoot, WorkDir, AgentName, and the
// agent env — so one pack-authoring vocabulary covers both file classes.
//
// Rendering uses missingkey=error: a token with no entry in data fails the
// copy loudly instead of staging a half-bound file. A caller that stages a
// templated pack without supplying data therefore cannot silently ship an
// unbound file; it gets an error naming the template.
func WithTemplateData(data map[string]string) CopyOption {
	return func(c *copyConfig) { c.templateData = data }
}

// newCopyConfig folds opts into a copyConfig.
func newCopyConfig(opts []CopyOption) copyConfig {
	var cfg copyConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// stagedRelPath returns the destination-relative path relPath stages to,
// resolving the template marker. Staging rules that key on the destination —
// JSON merge, hook wrapping, provider preserve-existing — must consult this
// path rather than the source path, so a templated settings file behaves
// exactly like its non-templated twin.
func stagedRelPath(relPath string) string {
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
