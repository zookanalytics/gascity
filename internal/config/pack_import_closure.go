package config

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/gastownhall/gascity/internal/fsys"
)

// resolveAgentImportClosurePaths rewrites each agent's relative path field
// (prompt_template, overlay_dir, namepool) that does not resolve
// city-root-relative to its absolute location inside an imported pack, when
// exactly one pack in the agent's effective import closure contains that
// subpath. This lets a native agent reference an imported pack's asset with a
// plain relative path (e.g. "agents/polecat/prompt.template.md") — no
// "<pack>//" token required.
//
// The closure is scoped to the agent's effective rig via PackDirsForRig(a.Dir),
// mirroring runtime prompt rendering (which renders against
// cfg.PackDirsForRig(rigName) precisely so one rig's fragments cannot override
// another rig's same-named fragments). A city agent (empty Dir) sees only
// city-level packs; a rig agent (Dir == its rig name, stamped at pack load)
// sees city packs plus its own rig's packs — never another rig's. Without this
// scoping, a convention subpath shared by two rigs' packs (e.g. a common
// agents/polecat/prompt.template.md) would read as ambiguous for either rig's
// native agent, or silently bind another rig's asset.
//
// Resolution is deterministic:
//   - empty or absolute paths are left unchanged (absolute paths are already
//     resolved, e.g. by resolvePackQualifiedAgentPaths or supplied absolute);
//   - a path that exists city-root-relative wins and is left unchanged (the
//     city takes precedence over the import closure);
//   - a path found in exactly one in-scope imported pack is rewritten to the
//     absolute path there;
//   - a path found in more than one in-scope imported pack is a hard
//     config-load error (ambiguous) rather than an arbitrary silent pick;
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

	resolve := func(dirs []string, field, val string) (string, error) {
		if val == "" || filepath.IsAbs(val) {
			return val, nil
		}
		rel := filepath.FromSlash(val)
		// City-root precedence: an existing city-root-relative asset wins.
		if pathExists(fs, filepath.Join(cityRoot, rel)) {
			return val, nil
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
		// Scope the closure to the agent's effective rig (empty Dir → city
		// packs only), matching the runtime PackDirsForRig render path.
		dirs := cfg.PackDirsForRig(a.Dir)
		for _, f := range []struct {
			field string
			ptr   *string
		}{
			{"prompt_template", &a.PromptTemplate},
			{"overlay_dir", &a.OverlayDir},
			{"namepool", &a.Namepool},
		} {
			resolved, err := resolve(dirs, f.field, *f.ptr)
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
