package main

import (
	"os"
	"testing"
)

// TestMainScrubsInheritedGCEnvVars verifies that TestMain clears GC_*/BEADS_*
// environment variables that pollute hermetic test setups. Polecat sessions
// (and other gc-managed shells) inject vars like GC_BEADS=bd, GC_CITY_PATH,
// and GC_BEADS_SCOPE_ROOT that take precedence over per-test city.toml
// settings — without scrubbing, a test that writes [beads] provider = "file"
// in city.toml ends up running the bd lifecycle and leaking dolt sql-server
// processes when its t.TempDir() is cleaned up.
//
// A short allowlist of test-mode toggles (e.g. GC_FAST_UNIT) is exempt.
func TestMainScrubsInheritedGCEnvVars(t *testing.T) {
	mustBeUnset := []string{
		"GC_BEADS",
		"GC_DOLT",
		"GC_SESSION",
		"GC_CITY",
		"GC_CITY_PATH",
		"GC_CITY_RUNTIME_DIR",
		"GC_BEADS_SCOPE_ROOT",
		"GC_RIG",
		"GC_RIG_ROOT",
		"GC_AGENT",
		"GC_ALIAS",
		"GC_TEMPLATE",
		"GC_PROVIDER",
		"GC_SESSION_ID",
		"GC_SESSION_NAME",
		"GC_SESSION_ORIGIN",
		"GC_PACK_STATE_DIR",
		"GC_DOLT_DATA_DIR",
		"GC_DOLT_LOG_FILE",
		"GC_DOLT_STATE_FILE",
		"GC_DOLT_PID_FILE",
		"GC_DOLT_LOCK_FILE",
		"GC_DOLT_CONFIG_FILE",
		"GC_DOLT_HOST",
		"GC_DOLT_PORT",
		"GC_RUNTIME_EPOCH",
		"GC_CONTINUATION_EPOCH",
		"BEADS_DOLT_SERVER_HOST",
		"BEADS_DOLT_SERVER_PORT",
		"BEADS_DOLT_AUTO_START",
		"BEADS_DIR",
		"BEADS_ACTOR",
	}
	for _, name := range mustBeUnset {
		if got := os.Getenv(name); got != "" {
			t.Errorf("os.Getenv(%q) = %q after TestMain, want empty (TestMain must scrub inherited GC_*/BEADS_* env vars to keep tests hermetic)", name, got)
		}
	}

	// Allowlist sanity: GC_HOME (set by TestMain) and PATH (extended by
	// TestMain) MUST remain set. Tests rely on them.
	if os.Getenv("GC_HOME") == "" {
		t.Error("GC_HOME is empty; TestMain should set a temporary GC_HOME")
	}
	if os.Getenv("PATH") == "" {
		t.Error("PATH is empty; TestMain should preserve PATH")
	}
}

// TestEnvScrubAllowlistKeepsTestModeToggles verifies that the scrub does not
// accidentally clear test-mode toggle vars like GC_FAST_UNIT (gates slow
// tests) or GC_DOLT_REAL_BINARY (overrides dolt path). These come from
// `make test-cmd-gc-process` or the developer's shell and tests rely on
// them being preserved.
func TestEnvScrubAllowlistKeepsTestModeToggles(t *testing.T) {
	for _, name := range []string{"GC_FAST_UNIT", "GC_DOLT_REAL_BINARY", "GC_LIVE_CITY", "GC_SESSION_CHAOS_SEED"} {
		if !shouldKeepInheritedEnvForTests(name) {
			t.Errorf("shouldKeepInheritedEnvForTests(%q) = false, want true (test-mode toggle must be preserved)", name)
		}
	}
	for _, name := range []string{"GC_BEADS", "GC_CITY_PATH", "GC_BEADS_SCOPE_ROOT", "BEADS_DOLT_AUTO_START"} {
		if shouldKeepInheritedEnvForTests(name) {
			t.Errorf("shouldKeepInheritedEnvForTests(%q) = true, want false (polecat-injected var must be scrubbed)", name)
		}
	}
}
