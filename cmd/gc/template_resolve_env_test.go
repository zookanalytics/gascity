package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
)

func TestResolveTemplatePrependsGCBinDirToPATH(t *testing.T) {
	cityPath := t.TempDir()
	writeTemplateResolveCityConfig(t, cityPath, "file")
	sep := string(os.PathListSeparator)
	t.Setenv("PATH", "/opt/homebrew/bin"+sep+"/usr/bin")

	params := &agentBuildParams{
		cityName:   "city",
		cityPath:   cityPath,
		workspace:  &config.Workspace{Provider: "test"},
		providers:  map[string]config.ProviderSpec{"test": {Command: "echo", PromptMode: "none"}},
		lookPath:   func(string) (string, error) { return "/bin/echo", nil },
		fs:         fsys.OSFS{},
		beaconTime: time.Unix(0, 0),
		beadNames:  make(map[string]string),
		stderr:     io.Discard,
	}

	agent := &config.Agent{Name: "runner"}
	tp, err := resolveTemplate(params, agent, agent.QualifiedName(), nil)
	if err != nil {
		t.Fatalf("resolveTemplate: %v", err)
	}

	gcBin := tp.Env["GC_BIN"]
	if gcBin == "" {
		t.Fatal("GC_BIN is empty")
	}
	wantDir := filepath.Dir(gcBin)
	parts := strings.Split(tp.Env["PATH"], sep)
	if len(parts) == 0 || parts[0] != wantDir {
		t.Fatalf("PATH first entry = %q, want gc bin dir %q (PATH=%q)", parts[0], wantDir, tp.Env["PATH"])
	}
	count := 0
	for _, part := range parts {
		if part == wantDir {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("gc bin dir %q should appear exactly once, found %d in PATH=%q", wantDir, count, tp.Env["PATH"])
	}
}

func TestResolveTemplatePrependsGCBinDirToConfiguredAgentPATH(t *testing.T) {
	cityPath := t.TempDir()
	writeTemplateResolveCityConfig(t, cityPath, "file")
	sep := string(os.PathListSeparator)
	t.Setenv("PATH", "/opt/homebrew/bin"+sep+"/usr/bin")

	params := &agentBuildParams{
		cityName:   "city",
		cityPath:   cityPath,
		workspace:  &config.Workspace{Provider: "test"},
		providers:  map[string]config.ProviderSpec{"test": {Command: "echo", PromptMode: "none"}},
		lookPath:   func(string) (string, error) { return "/bin/echo", nil },
		fs:         fsys.OSFS{},
		beaconTime: time.Unix(0, 0),
		beadNames:  make(map[string]string),
		stderr:     io.Discard,
	}

	configuredPATH := "/custom/tools" + sep + "/usr/local/bin"
	agent := &config.Agent{
		Name: "runner",
		Env:  map[string]string{"PATH": configuredPATH},
	}
	tp, err := resolveTemplate(params, agent, agent.QualifiedName(), nil)
	if err != nil {
		t.Fatalf("resolveTemplate: %v", err)
	}

	gcBin := tp.Env["GC_BIN"]
	if gcBin == "" {
		t.Fatal("GC_BIN is empty")
	}
	wantDir := filepath.Dir(gcBin)
	parts := strings.Split(tp.Env["PATH"], sep)
	wantPrefix := []string{wantDir, "/custom/tools", "/usr/local/bin"}
	if len(parts) < len(wantPrefix) {
		t.Fatalf("PATH=%q has fewer entries than expected prefix %v", tp.Env["PATH"], wantPrefix)
	}
	for i, want := range wantPrefix {
		if parts[i] != want {
			t.Fatalf("PATH entry %d = %q, want %q (PATH=%q)", i, parts[i], want, tp.Env["PATH"])
		}
	}
}

func TestResolveTemplateUsesTrustedRuntimeRootForControlTraceDefault(t *testing.T) {
	cityPath := t.TempDir()
	writeTemplateResolveCityConfig(t, cityPath, "file")
	customRuntimeDir := filepath.Join(t.TempDir(), "runtime-root")
	t.Setenv("GC_CITY_PATH", cityPath)
	t.Setenv("GC_CITY_RUNTIME_DIR", customRuntimeDir)

	params := &agentBuildParams{
		cityName:   "city",
		cityPath:   cityPath,
		workspace:  &config.Workspace{Provider: "test"},
		providers:  map[string]config.ProviderSpec{"test": {Command: "echo", PromptMode: "none"}},
		lookPath:   func(string) (string, error) { return "/bin/echo", nil },
		fs:         fsys.OSFS{},
		beaconTime: time.Unix(0, 0),
		beadNames:  make(map[string]string),
		stderr:     io.Discard,
	}

	agent := &config.Agent{Name: "runner"}
	tp, err := resolveTemplate(params, agent, agent.QualifiedName(), nil)
	if err != nil {
		t.Fatalf("resolveTemplate: %v", err)
	}

	if got := tp.Env["GC_CITY_RUNTIME_DIR"]; got != customRuntimeDir {
		t.Fatalf("GC_CITY_RUNTIME_DIR = %q, want %q", got, customRuntimeDir)
	}
	wantTraceDefault := filepath.Join(customRuntimeDir, "control-dispatcher-trace.log")
	if got := tp.Env["GC_CONTROL_DISPATCHER_TRACE_DEFAULT"]; got != wantTraceDefault {
		t.Fatalf("GC_CONTROL_DISPATCHER_TRACE_DEFAULT = %q, want %q", got, wantTraceDefault)
	}
}

func TestResolveTemplateUsesTrustedRuntimeRootForControlDispatcherTraceDefault(t *testing.T) {
	cityPath := t.TempDir()
	writeTemplateResolveCityConfig(t, cityPath, "file")
	customRuntimeDir := filepath.Join(t.TempDir(), "runtime-root")
	t.Setenv("GC_CITY_PATH", cityPath)
	t.Setenv("GC_CITY_RUNTIME_DIR", customRuntimeDir)

	params := &agentBuildParams{
		cityName:   "city",
		cityPath:   cityPath,
		workspace:  &config.Workspace{Provider: "test"},
		providers:  map[string]config.ProviderSpec{"test": {Command: "echo", PromptMode: "none"}},
		lookPath:   func(string) (string, error) { return "/bin/echo", nil },
		fs:         fsys.OSFS{},
		beaconTime: time.Unix(0, 0),
		beadNames:  make(map[string]string),
		stderr:     io.Discard,
	}

	qualifiedName := "app/" + config.ControlDispatcherAgentName
	agent := &config.Agent{Name: config.ControlDispatcherAgentName, Dir: "app"}
	tp, err := resolveTemplate(params, agent, qualifiedName, nil)
	if err != nil {
		t.Fatalf("resolveTemplate: %v", err)
	}

	if got := tp.Env["GC_CITY_RUNTIME_DIR"]; got != customRuntimeDir {
		t.Fatalf("GC_CITY_RUNTIME_DIR = %q, want %q", got, customRuntimeDir)
	}
	wantTraceDefault := filepath.Join(customRuntimeDir, "app--control-dispatcher-trace.log")
	if got := tp.Env["GC_CONTROL_DISPATCHER_TRACE_DEFAULT"]; got != wantTraceDefault {
		t.Fatalf("GC_CONTROL_DISPATCHER_TRACE_DEFAULT = %q, want %q", got, wantTraceDefault)
	}
}

// TestResolveTemplateInjectsPerDispatcherTraceDefault asserts that
// resolveTemplate produces a per-dispatcher GC_CONTROL_DISPATCHER_TRACE_DEFAULT
// in agentEnv for control-dispatcher agents (closes #1650). The override
// goes in agentEnv (last in mergeEnv) so it deterministically wins over
// the uniform city-level default seeded by cityRuntimeEnvMapForCity.
func TestResolveTemplateInjectsPerDispatcherTraceDefault(t *testing.T) {
	cases := []struct {
		name          string
		dir           string
		qualifiedName string
		wantFilename  string
	}{
		{
			name:          "city dispatcher",
			dir:           "",
			qualifiedName: config.ControlDispatcherAgentName,
			wantFilename:  "control-dispatcher-trace.log",
		},
		{
			name:          "rig dispatcher uses double-dash filename",
			dir:           "app",
			qualifiedName: "app/control-dispatcher",
			wantFilename:  "app--control-dispatcher-trace.log",
		},
		{
			name:          "non-dispatcher agent untouched",
			dir:           "",
			qualifiedName: "polecat",
			wantFilename:  "control-dispatcher-trace.log", // city-uniform default preserved
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cityPath := t.TempDir()
			writeTemplateResolveCityConfig(t, cityPath, "file")
			t.Setenv("GC_CITY_PATH", cityPath)
			t.Setenv("GC_CITY_RUNTIME_DIR", "")

			params := &agentBuildParams{
				cityName:   "city",
				cityPath:   cityPath,
				workspace:  &config.Workspace{Provider: "test"},
				providers:  map[string]config.ProviderSpec{"test": {Command: "echo", PromptMode: "none"}},
				lookPath:   func(string) (string, error) { return "/bin/echo", nil },
				fs:         fsys.OSFS{},
				beaconTime: time.Unix(0, 0),
				beadNames:  make(map[string]string),
				stderr:     io.Discard,
			}

			agentName := config.ControlDispatcherAgentName
			if tc.qualifiedName == "polecat" {
				agentName = "polecat"
			}
			agent := &config.Agent{Name: agentName, Dir: tc.dir}
			tp, err := resolveTemplate(params, agent, tc.qualifiedName, nil)
			if err != nil {
				t.Fatalf("resolveTemplate: %v", err)
			}

			wantPath := filepath.Join(cityPath, ".gc", "runtime", tc.wantFilename)
			if got := tp.Env["GC_CONTROL_DISPATCHER_TRACE_DEFAULT"]; got != wantPath {
				t.Fatalf("GC_CONTROL_DISPATCHER_TRACE_DEFAULT = %q, want %q", got, wantPath)
			}
		})
	}
}

// TestResolveTemplateExpandsAgentEnvVarsInConfiguredEnv verifies that
// agent.toml [env] values can reference city/rig-scoped vars
// (GT_ROOT, GC_ALIAS, GC_RIG, GC_RIG_ROOT, GC_DIR) using ${VAR} syntax
// even when those vars are not present in the supervisor's own process
// environment. This is the gc-rch40w bug that broke PR #32 (BASH_ENV
// expanded to "/rigs/.../init.sh" with a stray leading slash because
// ${GT_ROOT} silently expanded to "") and PR #34 (PATH lost the
// rig-scoped bin dir for the same reason).
//
// The fix expands cfgAgent.Env (and resolved.Env) against an environment
// that includes the in-flight agentEnv, not just os.Environ() of the
// supervisor. ${PATH} must still resolve from the supervisor passthrough.
func TestResolveTemplateExpandsAgentEnvVarsInConfiguredEnv(t *testing.T) {
	cityPath := t.TempDir()
	writeTemplateResolveCityConfig(t, cityPath, "file")
	rigRoot := filepath.Join(cityPath, "demo")
	if err := os.MkdirAll(rigRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	sep := string(os.PathListSeparator)
	t.Setenv("PATH", "/usr/bin")
	// Match the production supervisor: these vars are NOT in the
	// process env. Set them to empty defensively in case the test
	// harness inherited them from a parent shell — empty is
	// indistinguishable from unset under os.Getenv.
	t.Setenv("GT_ROOT", "")
	t.Setenv("GC_ALIAS", "")
	t.Setenv("GC_RIG", "")
	t.Setenv("GC_RIG_ROOT", "")
	t.Setenv("GC_DIR", "")

	params := &agentBuildParams{
		cityName:   "city",
		cityPath:   cityPath,
		workspace:  &config.Workspace{Provider: "test"},
		providers:  map[string]config.ProviderSpec{"test": {Command: "echo", PromptMode: "none"}},
		lookPath:   func(string) (string, error) { return "/bin/echo", nil },
		fs:         fsys.OSFS{},
		rigs:       []config.Rig{{Name: "demo", Path: rigRoot}},
		beaconTime: time.Unix(0, 0),
		beadNames:  make(map[string]string),
		stderr:     io.Discard,
	}

	agent := &config.Agent{
		Name: "runner",
		Dir:  "demo",
		Env: map[string]string{
			"PATH":     "${GT_ROOT}/rigs/x/bin" + sep + "${PATH}",
			"BASH_ENV": "${GC_RIG_ROOT}/init.sh",
			"ALIAS_AT": "${GC_ALIAS}",
			"RIG_AT":   "${GC_RIG}",
			"DIR_AT":   "${GC_DIR}",
			"BEADS_AT": "${BEADS_DIR}",
		},
	}
	qualifiedName := agent.QualifiedName()
	tp, err := resolveTemplate(params, agent, qualifiedName, nil)
	if err != nil {
		t.Fatalf("resolveTemplate: %v", err)
	}

	// PATH must contain the GT_ROOT-prefixed segment, not the broken
	// form "/rigs/x/bin" produced when ${GT_ROOT} silently expanded
	// to empty. The supervisor's ${PATH} must also remain resolvable.
	wantSegment := cityPath + "/rigs/x/bin"
	pathSegments := strings.Split(tp.Env["PATH"], sep)
	foundExpanded := false
	for _, seg := range pathSegments {
		if seg == "/rigs/x/bin" {
			t.Fatalf("PATH = %q contains broken segment %q — ${GT_ROOT} expanded to empty", tp.Env["PATH"], seg)
		}
		if seg == wantSegment {
			foundExpanded = true
		}
	}
	if !foundExpanded {
		t.Fatalf("PATH = %q, want a segment %q (expanded ${GT_ROOT})", tp.Env["PATH"], wantSegment)
	}
	foundUsrBin := false
	for _, seg := range pathSegments {
		if seg == "/usr/bin" {
			foundUsrBin = true
			break
		}
	}
	if !foundUsrBin {
		t.Fatalf("PATH = %q, want a segment /usr/bin (expanded ${PATH} from passthroughEnv)", tp.Env["PATH"])
	}

	wantBashEnv := rigRoot + "/init.sh"
	if got := tp.Env["BASH_ENV"]; got != wantBashEnv {
		t.Fatalf("BASH_ENV = %q, want %q (expanded ${GC_RIG_ROOT})", got, wantBashEnv)
	}
	if got := tp.Env["ALIAS_AT"]; got != qualifiedName {
		t.Fatalf("ALIAS_AT = %q, want %q (expanded ${GC_ALIAS})", got, qualifiedName)
	}
	if got := tp.Env["RIG_AT"]; got != "demo" {
		t.Fatalf("RIG_AT = %q, want %q (expanded ${GC_RIG})", got, "demo")
	}
	if got := tp.Env["DIR_AT"]; got != tp.WorkDir {
		t.Fatalf("DIR_AT = %q, want %q (expanded ${GC_DIR} = tp.WorkDir)", got, tp.WorkDir)
	}
	wantBeadsDir := filepath.Join(rigRoot, ".beads")
	if got := tp.Env["BEADS_AT"]; got != wantBeadsDir {
		t.Fatalf("BEADS_AT = %q, want %q (expanded ${BEADS_DIR})", got, wantBeadsDir)
	}
}

// TestResolveTemplateExpandsAgentEnvVarsInProviderEnv verifies that
// provider [env] entries (resolved.Env) also see agentEnv vars during
// expansion. Symmetry with cfgAgent.Env matters because anything a
// provider preset wants to compute from the city/rig layout (e.g. a
// trace-file path under ${GT_ROOT}) would have failed for the same
// gc-rch40w reason. Supervisor passthrough must also remain visible.
func TestResolveTemplateExpandsAgentEnvVarsInProviderEnv(t *testing.T) {
	cityPath := t.TempDir()
	writeTemplateResolveCityConfig(t, cityPath, "file")
	t.Setenv("PATH", "/usr/bin")
	t.Setenv("GT_ROOT", "")

	params := &agentBuildParams{
		cityName:  "city",
		cityPath:  cityPath,
		workspace: &config.Workspace{Provider: "test"},
		providers: map[string]config.ProviderSpec{
			"test": {
				Command:    "echo",
				PromptMode: "none",
				Env: map[string]string{
					"PROVIDER_ROOT": "${GT_ROOT}/provider-stuff",
					"PROVIDER_PATH": "${PATH}",
				},
			},
		},
		lookPath:   func(string) (string, error) { return "/bin/echo", nil },
		fs:         fsys.OSFS{},
		beaconTime: time.Unix(0, 0),
		beadNames:  make(map[string]string),
		stderr:     io.Discard,
	}

	agent := &config.Agent{Name: "runner"}
	tp, err := resolveTemplate(params, agent, agent.QualifiedName(), nil)
	if err != nil {
		t.Fatalf("resolveTemplate: %v", err)
	}

	wantProviderRoot := cityPath + "/provider-stuff"
	if got := tp.Env["PROVIDER_ROOT"]; got != wantProviderRoot {
		t.Fatalf("PROVIDER_ROOT = %q, want %q (resolved.Env must see agentEnv GT_ROOT)", got, wantProviderRoot)
	}
	if got := tp.Env["PROVIDER_PATH"]; got != "/usr/bin" {
		t.Fatalf("PROVIDER_PATH = %q, want %q (resolved.Env must see passthrough PATH)", got, "/usr/bin")
	}
}
