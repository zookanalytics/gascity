// Package packlint — is_known_agent behavior test for orphan-sweep.
//
// TestOrphanSweepIsKnownAgent guards the regression fixed in gc-3xw: the
// orphan-sweep reset logic must recognize PackV2 binding-qualified assignee
// names (e.g. "gastown.deacon", "gascity/refinery", "signal-loom/polecat-3")
// as belonging to their bare template, otherwise every PackV2 agent that
// self-assigns gets reset to open/unassigned on the next 5-minute cycle.
package packlint

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// orphanSweepScriptPath is the single canonical source of the sweep logic;
// all other copies under rigs/*/scripts/ and .gc/system/... are refreshed
// from this one.
const orphanSweepScriptPath = "examples/gastown/packs/maintenance/assets/scripts/orphan-sweep.sh"

// TestOrphanSweepIsKnownAgent extracts the is_known_agent() shell function
// from the shipped script and exercises it against known-agent / orphan
// cases. If the function is removed or renamed, the test fails loudly
// rather than silently passing.
func TestOrphanSweepIsKnownAgent(t *testing.T) {
	scriptPath := filepath.Join(repoRoot(), orphanSweepScriptPath)
	body, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("reading %s: %v", orphanSweepScriptPath, err)
	}
	src := string(body)
	if !strings.Contains(src, "is_known_agent()") {
		t.Fatalf("is_known_agent() not found in %s — function renamed or removed", orphanSweepScriptPath)
	}

	// Harness: populate KNOWN_AGENTS with bare template names (as extracted
	// by the script from `gc config show`), source the real script's
	// function definition, and invoke it. Exit 0 means "known", non-zero
	// means "orphan".
	harness := fmt.Sprintf(`
set -euo pipefail
declare -A KNOWN_AGENTS
for a in deacon mayor mechanik polecat refinery dog; do KNOWN_AGENTS["$a"]=1; done

# Import the is_known_agent function body from the real script.
%s

if is_known_agent "$1"; then echo known; else echo orphan; fi
`, extractFuncBlock(src, "is_known_agent"))

	cases := []struct {
		name string
		in   string
		want string
	}{
		// Case 1: direct match on bare template.
		{"bare deacon", "deacon", "known"},
		{"bare mayor", "mayor", "known"},

		// Case 2: pool instance with -N suffix.
		{"pool polecat-3", "polecat-3", "known"},
		{"pool polecat-12", "polecat-12", "known"},

		// Case 3: PackV2 binding-qualified (the regression).
		{"dot qualified deacon", "gastown.deacon", "known"},
		{"dot qualified mayor", "gastown.mayor", "known"},
		{"slash qualified refinery", "gascity/refinery", "known"},
		{"dot qualified mechanik", "gc-toolkit.mechanik", "known"},

		// Case 4: PackV2 qualified combined with pool suffix.
		{"rig slash pool polecat-3", "signal-loom/polecat-3", "known"},
		{"rig slash pool polecat-7", "gascity/polecat-7", "known"},

		// Case 5: true orphans still detected (fix must not weaken the check).
		{"unknown template", "stranger", "orphan"},
		{"qualified unknown", "gastown.stranger", "orphan"},
		{"unknown slash qualified", "foo/bar", "orphan"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("bash", "-c", harness, "_", tc.in)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("bash harness failed for %q: %v\n%s", tc.in, err, out)
			}
			got := strings.TrimSpace(string(out))
			if got != tc.want {
				t.Fatalf("is_known_agent(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// extractFuncBlock returns the text of a bash function definition (from
// `name()` through the matching closing brace) so the real source can be
// sourced by a test harness without pulling in the rest of the script.
func extractFuncBlock(src, name string) string {
	marker := name + "()"
	start := strings.Index(src, marker)
	if start == -1 {
		return ""
	}
	openBrace := strings.Index(src[start:], "{")
	if openBrace == -1 {
		return ""
	}
	depth := 0
	for i := start + openBrace; i < len(src); i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[start : i+1]
			}
		}
	}
	return ""
}
