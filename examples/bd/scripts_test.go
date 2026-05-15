// Package bd_test exercises shell-script helpers shipped in the bd pack.
package bd_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	gcBeadsBdScript    = "assets/scripts/gc-beads-bd.sh"
	fixtureCustomTypes = "molecule,convoy"
)

// callEnsureTypesCustomInYaml extracts ensure_types_custom_in_yaml from
// gc-beads-bd.sh and invokes it against the given config directory with
// the fixture custom-types string. The function is sourced in isolation
// so we can exercise its behavior without running the script's main
// dispatch (which requires GC_CITY_PATH and a fully wired dolt environment).
func callEnsureTypesCustomInYaml(t *testing.T, dir string) {
	t.Helper()
	scriptPath, err := filepath.Abs(gcBeadsBdScript)
	if err != nil {
		t.Fatalf("abs script path: %v", err)
	}
	wrapper := `#!/usr/bin/env bash
set -euo pipefail
script="$1"; shift
fn=$(awk '/^ensure_types_custom_in_yaml\(\) \{/,/^\}$/' "$script")
if [ -z "$fn" ]; then
	echo "could not extract ensure_types_custom_in_yaml from $script" >&2
	exit 2
fi
eval "$fn"
ensure_types_custom_in_yaml "$@"
`
	cmd := exec.Command("bash", "-c", wrapper, "_", scriptPath, dir, fixtureCustomTypes)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("ensure_types_custom_in_yaml: %v", err)
	}
}

func writeConfig(t *testing.T, dir, body string) string {
	t.Helper()
	beadsDir := filepath.Join(dir, ".beads")
	if err := os.MkdirAll(beadsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	configPath := filepath.Join(beadsDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return configPath
}

func readConfig(t *testing.T, path string) string {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return string(got)
}

func countTypesCustomLines(s string) int {
	n := 0
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "types.custom:") {
			n++
		}
	}
	return n
}

// TestEnsureTypesCustomInYamlAppendsCleanlyOnHappyPath verifies that the
// function adds the types.custom line on its own line when the input
// config already ends with a newline.
func TestEnsureTypesCustomInYamlAppendsCleanlyOnHappyPath(t *testing.T) {
	dir := t.TempDir()
	original := "issue_prefix: gc\nbackup.enabled: false\n"
	configPath := writeConfig(t, dir, original)

	callEnsureTypesCustomInYaml(t, dir)

	want := original + "types.custom: " + fixtureCustomTypes + "\n"
	if got := readConfig(t, configPath); got != want {
		t.Fatalf("config mismatch:\n got: %q\nwant: %q", got, want)
	}
}

// TestEnsureTypesCustomInYamlPreservesNewlineWhenInputLacksTrailingNewline
// pins the regression: when the existing config does not end with a
// newline, the appended types.custom line must NOT be concatenated to the
// final value. Concatenation produced YAML like
// "backup.enabled: falsetypes.custom: ..." which broke bd's config parser,
// silently re-enabled bd's auto-backup, and drove the 2026-04-28..2026-05-01
// dolt_backup hot loop (gc-0kuep).
func TestEnsureTypesCustomInYamlPreservesNewlineWhenInputLacksTrailingNewline(t *testing.T) {
	dir := t.TempDir()
	// No trailing newline — this is the input shape that triggered the bug.
	original := "issue_prefix: gc\nbackup.enabled: false"
	configPath := writeConfig(t, dir, original)

	callEnsureTypesCustomInYaml(t, dir)

	got := readConfig(t, configPath)
	if strings.Contains(got, "falsetypes.custom") {
		t.Fatalf("types.custom was concatenated to previous line: %q", got)
	}
	if !strings.Contains(got, "backup.enabled: false\n") {
		t.Fatalf("backup.enabled line not preserved with newline: %q", got)
	}
	if !strings.Contains(got, "\ntypes.custom: "+fixtureCustomTypes+"\n") {
		t.Fatalf("types.custom line not appended on its own line: %q", got)
	}
}

// TestEnsureTypesCustomInYamlSelfHealsCorruptedLineWithoutDuplicate
// verifies that an existing concatenated line (left over from a pre-fix
// run that hit the missing-newline path) is split into two clean lines.
// Without self-heal, the function would early-return because no line
// begins with "types.custom:", but the file would remain corrupted.
func TestEnsureTypesCustomInYamlSelfHealsCorruptedLineWithoutDuplicate(t *testing.T) {
	dir := t.TempDir()
	original := "issue_prefix: gc\nbackup.enabled: falsetypes.custom: " + fixtureCustomTypes + "\n"
	configPath := writeConfig(t, dir, original)

	callEnsureTypesCustomInYaml(t, dir)

	got := readConfig(t, configPath)
	if strings.Contains(got, "falsetypes.custom") {
		t.Fatalf("concatenated line not split: %q", got)
	}
	if !strings.Contains(got, "backup.enabled: false\n") {
		t.Fatalf("backup.enabled line not preserved: %q", got)
	}
	if !strings.Contains(got, "\ntypes.custom: "+fixtureCustomTypes+"\n") {
		t.Fatalf("types.custom not on its own line: %q", got)
	}
	if count := countTypesCustomLines(got); count != 1 {
		t.Fatalf("expected exactly one ^types.custom: line, got %d:\n%s", count, got)
	}
}

// TestEnsureTypesCustomInYamlSelfHealsCorruptedLineWithDuplicate verifies
// that when self-healing produces a duplicate types.custom line (because
// a later attempt to add the entry succeeded after the corruption), the
// duplicate is removed and exactly one types.custom line remains.
func TestEnsureTypesCustomInYamlSelfHealsCorruptedLineWithDuplicate(t *testing.T) {
	dir := t.TempDir()
	original := "issue_prefix: gc\n" +
		"backup.enabled: falsetypes.custom: " + fixtureCustomTypes + "\n" +
		"types.custom: " + fixtureCustomTypes + "\n"
	configPath := writeConfig(t, dir, original)

	callEnsureTypesCustomInYaml(t, dir)

	got := readConfig(t, configPath)
	if strings.Contains(got, "falsetypes.custom") {
		t.Fatalf("concatenated line not split: %q", got)
	}
	if !strings.Contains(got, "backup.enabled: false\n") {
		t.Fatalf("backup.enabled line not preserved: %q", got)
	}
	if count := countTypesCustomLines(got); count != 1 {
		t.Fatalf("expected exactly one ^types.custom: line after dedup, got %d:\n%s", count, got)
	}
}

// TestEnsureTypesCustomInYamlIsIdempotent verifies that re-running the
// function does not duplicate the types.custom line.
func TestEnsureTypesCustomInYamlIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConfig(t, dir, "issue_prefix: gc\n")

	for i := 0; i < 3; i++ {
		callEnsureTypesCustomInYaml(t, dir)
	}

	got := readConfig(t, configPath)
	if count := countTypesCustomLines(got); count != 1 {
		t.Fatalf("types.custom should appear exactly once after idempotent re-runs, got %d:\n%s", count, got)
	}
}
