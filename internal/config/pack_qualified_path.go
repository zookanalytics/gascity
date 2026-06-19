package config

import (
	"fmt"
	"path/filepath"
	"strings"
)

// splitPackQualifiedPath splits a pack-qualified asset path of the form
// "<pack>//<subpath>" (e.g. "gastown//agents/mayor/prompt.template.md")
// into its pack name and subpath. The form lets a native agent reference a
// file inside an imported pack without depending on that pack's
// content-addressed cache location. It returns ok=false for city-root paths
// ("//x"), absolute paths, URLs, and anything lacking a bare pack name
// before the first "//".
func splitPackQualifiedPath(p string) (pack, sub string, ok bool) {
	if p == "" || strings.HasPrefix(p, "//") || filepath.IsAbs(p) {
		return "", "", false
	}
	idx := strings.Index(p, "//")
	if idx <= 0 {
		return "", "", false
	}
	pack = p[:idx]
	sub = p[idx+2:]
	// The pack name is a bare binding key: reject anything carrying a path
	// separator or scheme colon (so "https://host//x" is not mistaken for a
	// pack reference).
	if sub == "" || strings.ContainsAny(pack, "/:") {
		return "", "", false
	}
	return pack, sub, true
}

// ambiguousPackDir marks a pack name that resolves to more than one distinct
// imported directory (e.g. the same pack imported at different pins across
// rigs). Resolution treats it as an error rather than silently picking one.
const ambiguousPackDir = ""

// packDirsByName maps imported pack names to their resolved directory across
// the city- and rig-level pack closures. A name present with the
// ambiguousPackDir sentinel resolves to multiple distinct directories.
func packDirsByName(cfg *City) map[string]string {
	byName := make(map[string]string)
	add := func(dir string) {
		name := readPackNameFromDir(dir)
		if name == "" {
			return
		}
		existing, seen := byName[name]
		switch {
		case !seen:
			byName[name] = dir
		case existing == ambiguousPackDir:
			// already ambiguous
		case existing != dir:
			byName[name] = ambiguousPackDir
		}
	}
	for _, dir := range cfg.PackDirs {
		add(dir)
	}
	for _, dirs := range cfg.RigPackDirs {
		for _, dir := range dirs {
			add(dir)
		}
	}
	return byName
}

// resolvePackQualifiedAgentPaths rewrites every agent path field written in
// "<pack>//<subpath>" form to the absolute path inside the imported pack's
// resolved directory. It runs after composition has populated the pack-dir
// closure. An unknown or ambiguous pack name is a hard error so a stale
// reference fails config load instead of rendering an empty prompt.
func resolvePackQualifiedAgentPaths(cfg *City) error {
	if cfg == nil {
		return nil
	}
	var byName map[string]string
	resolve := func(field, val string) (string, error) {
		pack, sub, ok := splitPackQualifiedPath(val)
		if !ok {
			return val, nil
		}
		if byName == nil {
			byName = packDirsByName(cfg)
		}
		dir, found := byName[pack]
		if !found {
			return "", fmt.Errorf("%s %q references unknown imported pack %q", field, val, pack)
		}
		if dir == ambiguousPackDir {
			return "", fmt.Errorf("%s %q references pack %q, which resolves to multiple imported packs", field, val, pack)
		}
		return filepath.Join(dir, filepath.FromSlash(sub)), nil
	}

	for i := range cfg.Agents {
		a := &cfg.Agents[i]
		for _, f := range []struct {
			field string
			ptr   *string
		}{
			{"prompt_template", &a.PromptTemplate},
			{"overlay_dir", &a.OverlayDir},
			{"namepool", &a.Namepool},
		} {
			resolved, err := resolve(f.field, *f.ptr)
			if err != nil {
				return fmt.Errorf("agent %q: %w", a.Name, err)
			}
			*f.ptr = resolved
		}
	}
	return nil
}
