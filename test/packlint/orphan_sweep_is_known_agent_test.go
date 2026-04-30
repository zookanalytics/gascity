// Package packlint — orphan-sweep canonical-form rewrite guards.
//
// The rewrite (gc-dwd) replaced the strip-and-match scheme with rig-scoped
// invocation + canonical-form match. KNOWN_AGENTS is populated directly
// from `gc status --json` qualified names plus `gc session list --json`
// agent and session identities, with a V1 alternate composed from
// (rig, bare-name) for rig V2 agents so production assignees like
// "gascity/refinery" continue to match without modifying the assignee.
//
// These tests guard:
//  1. is_known_agent stays a direct hash lookup (no string-stripping
//     patterns from the previous implementation).
//  2. Realistic canonical, V1-alternate, session-name, and pool-member
//     assignees resolve correctly via direct lookup, while truly
//     orphaned assignees do not.
//  3. The full script orphan-resets only the right beads when run with
//     mocked gc/bd commands.
package packlint

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const orphanSweepScriptPath = "examples/gastown/packs/maintenance/assets/scripts/orphan-sweep.sh"

// TestOrphanSweepNoAssigneeStripping asserts the canonical-form rewrite
// does not regress to strip-and-match. The old implementation mutated
// the assignee with `${name##*[./]}` and `${tmpl%-[0-9]*}` to bridge
// formats; the new implementation enumerates canonical names directly.
func TestOrphanSweepNoAssigneeStripping(t *testing.T) {
	src := readOrphanSweepScript(t)
	forbidden := []struct {
		pattern, why string
	}{
		{"${name##*[./]}", "strips assignee binding/rig prefix to bridge formats"},
		{"${tmpl%-[0-9]*}", "strips assignee pool-instance -N suffix to bridge formats"},
	}
	for _, f := range forbidden {
		if strings.Contains(src, f.pattern) {
			t.Errorf("forbidden assignee-stripping pattern %q present:\n  reason: %s",
				f.pattern, f.why)
		}
	}
}

// TestOrphanSweepIsKnownAgentLookup verifies is_known_agent is a pure
// direct hash lookup. The harness pre-populates KNOWN_AGENTS the way the
// rewritten build phase does — canonical V2, V1 alternates, namepool
// members, session names — sources only the is_known_agent function from
// the live script, and confirms each assignee form resolves correctly.
func TestOrphanSweepIsKnownAgentLookup(t *testing.T) {
	src := readOrphanSweepScript(t)
	if !strings.Contains(src, "is_known_agent()") {
		t.Fatalf("is_known_agent() not found in %s — function renamed or removed",
			orphanSweepScriptPath)
	}

	harness := fmt.Sprintf(`
set -euo pipefail
declare -A KNOWN_AGENTS
# HQ canonical names: bare pool members and binding-qualified V2 agents.
for a in dog-1 dog-2 dog-3 control-dispatcher gastown.boot gastown.deacon gastown.mayor; do
    KNOWN_AGENTS["$a"]=1
done
# Rig (gascity) canonical: V2 qualified, V1 alternates, namepool members.
for a in \
    gascity/gastown.refinery gascity/refinery \
    gascity/gastown.witness  gascity/witness \
    gascity/control-dispatcher \
    gascity/furiosa gascity/nux gascity/slit; do
    KNOWN_AGENTS["$a"]=1
done
# Live session identities (running sessions for HQ and rig agents).
for a in \
    gastown__deacon gastown__mayor-lx-abcd \
    gastown__polecat-lx-wwyml gascity--gastown__witness; do
    KNOWN_AGENTS["$a"]=1
done

%s

if is_known_agent "$1"; then echo known; else echo orphan; fi
`, extractFuncBlock(src, "is_known_agent"))

	cases := []struct {
		name, in, want string
	}{
		// V2 binding-qualified canonical (HQ).
		{"hq dot-qualified deacon", "gastown.deacon", "known"},
		{"hq dot-qualified mayor", "gastown.mayor", "known"},

		// HQ pool members enumerated as canonical (no suffix stripping).
		{"hq pool dog-1", "dog-1", "known"},
		{"hq pool dog-3", "dog-3", "known"},

		// V2 binding-qualified canonical (rig).
		{"rig V2 refinery", "gascity/gastown.refinery", "known"},
		{"rig V2 witness", "gascity/gastown.witness", "known"},

		// V1 alternates of rig V2 agents — pre-populated by the build
		// phase, looked up directly without modifying the assignee.
		{"rig V1-alt refinery", "gascity/refinery", "known"},
		{"rig V1-alt witness", "gascity/witness", "known"},

		// Rig namepool members (no binding).
		{"rig namepool furiosa", "gascity/furiosa", "known"},
		{"rig namepool nux", "gascity/nux", "known"},

		// Live session identities — direct match without parsing.
		{"hq session deacon", "gastown__deacon", "known"},
		{"rig session polecat", "gastown__polecat-lx-wwyml", "known"},
		{"rig session witness", "gascity--gastown__witness", "known"},

		// True orphans — never appear in KNOWN_AGENTS.
		{"hq unknown bare", "stranger", "orphan"},
		{"hq unknown qualified", "gastown.stranger", "orphan"},
		{"unknown rig V1", "foo/refinery", "orphan"},
		{"unknown rig V2", "foo/gastown.refinery", "orphan"},

		// Pool instance beyond the enumerated max — orphan under
		// canonical-form match (dog-12 was never enumerated).
		{"hq pool out-of-range dog-12", "dog-12", "orphan"},

		// No fallback strip — bare HQ name doesn't match V2 entry.
		{"hq bare deacon (no fallback)", "deacon", "orphan"},
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

// TestOrphanSweepEndToEnd runs the full script with mocked gc and bd
// commands. Asserts the canonical V2 entry, the V1 alternate, the
// namepool member, the live session-name assignee, and the empty
// assignee are all kept; truly orphaned assignees are reset.
func TestOrphanSweepEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq required for orphan-sweep")
	}

	tmpDir := t.TempDir()

	statusJSON := `{
  "agents": [
    {"name":"refinery","qualified_name":"gascity/gastown.refinery","scope":"rig","running":true,"suspended":false},
    {"name":"witness","qualified_name":"gascity/gastown.witness","scope":"rig","running":true,"suspended":false},
    {"name":"furiosa","qualified_name":"gascity/furiosa","scope":"rig","running":false,"suspended":false},
    {"name":"control-dispatcher","qualified_name":"gascity/control-dispatcher","scope":"rig","running":false,"suspended":false},
    {"name":"deacon","qualified_name":"gastown.deacon","scope":"city","running":true,"suspended":false}
  ]
}`

	sessionsJSON := `[
  {"AgentName":"gascity/gastown.furiosa","SessionName":"gastown__polecat-lx-wwyml"},
  {"AgentName":"gascity/gastown.witness","SessionName":"gascity--gastown__witness"},
  {"AgentName":"gastown.deacon","SessionName":"gastown__deacon"}
]`

	inProgressJSON := `[
  {"id":"bead-canonical","assignee":"gascity/gastown.refinery"},
  {"id":"bead-v1alt","assignee":"gascity/refinery"},
  {"id":"bead-namepool","assignee":"gascity/furiosa"},
  {"id":"bead-session","assignee":"gastown__polecat-lx-wwyml"},
  {"id":"bead-no-assignee","assignee":""},
  {"id":"bead-orphan-stranger","assignee":"gastown.stranger"},
  {"id":"bead-orphan-rig","assignee":"foo/refinery"}
]`

	bdLog := filepath.Join(tmpDir, "bd-update.log")

	gcMock := fmt.Sprintf(`#!/usr/bin/env bash
case "$1 $2" in
    "status --json")
        cat <<'GC_STATUS_EOF'
%s
GC_STATUS_EOF
        ;;
    "session list")
        # gc session list [--json]
        if [ "$3" = "--json" ]; then
            cat <<'GC_SESSIONS_EOF'
%s
GC_SESSIONS_EOF
        else
            exit 1
        fi
        ;;
    *) exit 1 ;;
esac
`, statusJSON, sessionsJSON)

	bdMock := fmt.Sprintf(`#!/usr/bin/env bash
case "$1" in
    list)
        cat <<'BD_LIST_EOF'
%s
BD_LIST_EOF
        ;;
    update)
        echo "$@" >> %q
        ;;
    *) exit 1 ;;
esac
`, inProgressJSON, bdLog)

	writeMockBin(t, filepath.Join(tmpDir, "gc"), gcMock)
	writeMockBin(t, filepath.Join(tmpDir, "bd"), bdMock)

	scriptPath := filepath.Join(repoRoot(), orphanSweepScriptPath)
	cmd := exec.Command("bash", scriptPath)
	cmd.Env = append(os.Environ(),
		"PATH="+tmpDir+":"+os.Getenv("PATH"),
		"GC_RIG=gascity",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("orphan-sweep failed: %v\n%s", err, out)
	}

	logBytes, _ := os.ReadFile(bdLog)
	logStr := string(logBytes)

	keep := []string{
		"bead-canonical",
		"bead-v1alt",
		"bead-namepool",
		"bead-session",
		"bead-no-assignee",
	}
	for _, b := range keep {
		if strings.Contains(logStr, b) {
			t.Errorf("bead %q should be kept but was reset.\nbd update log:\n%s\nscript output:\n%s",
				b, logStr, out)
		}
	}

	reset := []string{
		"bead-orphan-stranger",
		"bead-orphan-rig",
	}
	for _, b := range reset {
		if !strings.Contains(logStr, b) {
			t.Errorf("bead %q should be reset but wasn't.\nbd update log:\n%s\nscript output:\n%s",
				b, logStr, out)
		}
	}
}

// readOrphanSweepScript reads the canonical orphan-sweep.sh body.
func readOrphanSweepScript(t *testing.T) string {
	t.Helper()
	p := filepath.Join(repoRoot(), orphanSweepScriptPath)
	body, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("reading %s: %v", orphanSweepScriptPath, err)
	}
	return string(body)
}

// writeMockBin writes an executable shell script for use as a PATH
// override in TestOrphanSweepEndToEnd.
func writeMockBin(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("writing mock %s: %v", path, err)
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
