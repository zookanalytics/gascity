package config

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gastownhall/gascity/internal/fsys"
)

// importClosureDirs returns the imported-pack directories across the city- and
// rig-level closures in a deterministic order (city packs first, then rig
// packs by sorted rig name). This is the same closure packDirsByName walks; it
// is enumerated here without the name index because closure fallthrough keys
// off the asset subpath, not the pack name.
func importClosureDirs(cfg *City) []string {
	dirs := make([]string, 0, len(cfg.PackDirs))
	dirs = append(dirs, cfg.PackDirs...)
	rigNames := make([]string, 0, len(cfg.RigPackDirs))
	for name := range cfg.RigPackDirs {
		rigNames = append(rigNames, name)
	}
	sort.Strings(rigNames)
	for _, name := range rigNames {
		dirs = append(dirs, cfg.RigPackDirs[name]...)
	}
	return dirs
}

// resolveAgentImportClosurePaths rewrites each agent's relative path field
// (prompt_template, overlay_dir, namepool) that does not resolve
// city-root-relative to its absolute location inside an imported pack, when
// exactly one pack in the import closure contains that subpath. This lets a
// native agent reference an imported pack's asset with a plain relative path
// (e.g. "agents/polecat/prompt.template.md") — no "<pack>//" token required.
//
// Resolution is deterministic:
//   - empty or absolute paths are left unchanged (absolute paths are already
//     resolved, e.g. by resolvePackQualifiedAgentPaths or supplied absolute);
//   - a path that exists city-root-relative wins and is left unchanged (the
//     city takes precedence over the import closure);
//   - a path found in exactly one imported pack is rewritten to the absolute
//     path there;
//   - a path found in more than one imported pack is a hard config-load error
//     (ambiguous) rather than an arbitrary silent pick;
//   - a path found nowhere is left unchanged, preserving the prior graceful
//     behavior where an unreachable asset renders empty. The change is purely
//     additive.
//
// It runs after resolvePackQualifiedAgentPaths, so any "<pack>//<subpath>"
// references are already absolute and skip the closure search here.
func resolveAgentImportClosurePaths(fs fsys.FS, cfg *City, cityRoot string) error {
	if fs == nil || cfg == nil {
		return nil
	}

	var dirs []string
	dirsInit := false

	resolve := func(field, val string) (string, error) {
		if val == "" || filepath.IsAbs(val) {
			return val, nil
		}
		rel := filepath.FromSlash(val)
		// City-root precedence: an existing city-root-relative asset wins.
		if pathExists(fs, filepath.Join(cityRoot, rel)) {
			return val, nil
		}
		if !dirsInit {
			dirs = importClosureDirs(cfg)
			dirsInit = true
		}
		var matches []string
		for _, dir := range dirs {
			if dir == "" {
				continue
			}
			cand := filepath.Join(dir, rel)
			if pathExists(fs, cand) {
				matches = appendUniqueString(matches, cand)
			}
		}
		switch len(matches) {
		case 0:
			return val, nil
		case 1:
			return matches[0], nil
		default:
			return "", fmt.Errorf("%s %q resolves to multiple imported packs: %s",
				field, val, strings.Join(matches, ", "))
		}
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

// pathExists reports whether p names an existing filesystem entry (file or
// directory). It is the existence predicate behind import-closure fallthrough.
func pathExists(fs fsys.FS, p string) bool {
	if p == "" {
		return false
	}
	_, err := fs.Stat(p)
	return err == nil
}

// appendUniqueString appends v to s only if it is not already present. Closure
// dirs that differ only by a trailing separator resolve to the same cleaned
// candidate path, so deduping by the final path keeps such cases from reading
// as ambiguous.
func appendUniqueString(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}
