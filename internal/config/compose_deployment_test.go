package config

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

// writeDeploymentFixture writes files under root, creating parent
// directories, and returns root.
func writeDeploymentFixture(t *testing.T, root string, files map[string]string) string {
	t.Helper()
	for rel, data := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", path, err)
		}
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", path, err)
		}
	}
	return root
}

// TestLoadDeploymentMatchesFullCompositionOnDeploymentFields pins the contract
// LoadDeployment's callers rely on: on a city whose full composition succeeds,
// every deployment fact it returns is the one LoadWithIncludes returns. The
// fixture carries each layer that can move one (an include fragment that adds
// a rig and half of a split [storage], rig patches from both files, the site
// binding's rig paths and workspace identity) alongside each pack layer that
// must not be read (the city pack.toml, a city import, a rig import, and an
// agent patch that only the composed pack agents satisfy).
func TestLoadDeploymentMatchesFullCompositionOnDeploymentFields(t *testing.T) {
	cityDir := filepath.Join(t.TempDir(), "city")
	rigsDir := filepath.Join(t.TempDir(), "rigs")
	writeDeploymentFixture(t, cityDir, map[string]string{
		"city.toml": `
include = ["fragments/more.toml"]

[beads]
provider = "bd"

[storage.classes]
work = "work"
graph = "infra"
sessions = "infra"
messaging = "infra"
orders = "infra"
nudges = "infra"

[workspace.env]
BD_BIN = "/opt/bd/bin/bd"
PATH = "/opt/bd/bin:/usr/bin"

[[rigs]]
name = "frontend"
prefix = "fe"
dolt_host = "db.internal"
dolt_port = "3307"

[[rigs]]
name = "backend"

[rigs.imports.helpers]
source = "./packs/helpers"

[[patches.rigs]]
name = "frontend"
suspended = true

[[patches.agent]]
name = "worker"
suspended = true
`,
		"fragments/more.toml": `
[storage.bindings.infra]
provider = "sqlite-beads"
path = ".gc/store"

[[rigs]]
name = "docs"
prefix = "dx"

[[patches.rigs]]
name = "backend"
prefix = "bk"
`,
		"pack.toml": `
[pack]
name = "demo"
schema = 2

[imports.tools]
source = "./packs/tools"
`,
		"packs/tools/pack.toml":                 "[pack]\nname = \"tools\"\nschema = 2\n",
		"packs/tools/agents/worker/prompt.md":   "You work.\n",
		"packs/helpers/pack.toml":               "[pack]\nname = \"helpers\"\nschema = 2\n",
		"packs/helpers/agents/helper/prompt.md": "You help.\n",
		".gc/site.toml": "workspace_name = \"demo\"\nworkspace_prefix = \"dm\"\n" +
			"\n[[rig]]\nname = \"frontend\"\npath = " + strconv.Quote(filepath.Join(rigsDir, "frontend")) +
			"\n\n[[rig]]\nname = \"backend\"\npath = " + strconv.Quote(filepath.Join(rigsDir, "backend")) +
			"\n\n[[rig]]\nname = \"docs\"\npath = " + strconv.Quote(filepath.Join(rigsDir, "docs")) + "\n",
	})
	cityTOML := filepath.Join(cityDir, "city.toml")

	full, _, err := LoadWithIncludes(fsys.OSFS{}, cityTOML)
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}
	dep, prov, err := LoadDeployment(fsys.OSFS{}, cityTOML)
	if err != nil {
		t.Fatalf("LoadDeployment: %v", err)
	}

	// The fixture really composes packs: the full load carries their agents,
	// the deployment load none of them.
	fullAgents := map[string]bool{}
	for _, a := range full.Agents {
		fullAgents[a.QualifiedName()] = true
	}
	if !fullAgents["tools.worker"] {
		t.Fatalf("full composition agents = %v, want the imported tools.worker", fullAgents)
	}
	if len(dep.Agents) != 0 {
		t.Fatalf("LoadDeployment agents = %d, want none (no pack is read)", len(dep.Agents))
	}

	if !reflect.DeepEqual(dep.Rigs, full.Rigs) {
		t.Errorf("Rigs differ:\n deployment %+v\n full       %+v", dep.Rigs, full.Rigs)
	}
	for _, f := range []struct {
		name      string
		dep, full any
	}{
		{"EffectiveHQPrefix", EffectiveHQPrefix(dep), EffectiveHQPrefix(full)},
		{"EffectiveCityName", dep.EffectiveCityName(), full.EffectiveCityName()},
		{"ResolvedWorkspaceName", dep.ResolvedWorkspaceName, full.ResolvedWorkspaceName},
		{"ResolvedWorkspacePrefix", dep.ResolvedWorkspacePrefix, full.ResolvedWorkspacePrefix},
		{"Workspace.Name", dep.Workspace.Name, full.Workspace.Name},
		{"Workspace.Prefix", dep.Workspace.Prefix, full.Workspace.Prefix},
		{"Workspace.Env", dep.Workspace.Env, full.Workspace.Env},
		{"Beads", dep.Beads, full.Beads},
		{"Storage", dep.Storage, full.Storage},
		{"EffectiveStorage", dep.EffectiveStorage(), full.EffectiveStorage()},
		{"Dolt", dep.Dolt, full.Dolt},
		{"Daemon", dep.Daemon, full.Daemon},
	} {
		if !reflect.DeepEqual(f.dep, f.full) {
			t.Errorf("%s: deployment %#v, full %#v", f.name, f.dep, f.full)
		}
	}

	// The fixture moves each fact it compares, so a load that skipped a layer
	// cannot match by accident.
	byName := map[string]Rig{}
	for _, r := range dep.Rigs {
		byName[r.Name] = r
	}
	if r := byName["frontend"]; !r.Suspended || r.Path != filepath.Join(rigsDir, "frontend") || r.DoltPort != "3307" {
		t.Errorf("frontend = %+v, want the city.toml rig patch, its site path and its Dolt port", r)
	}
	if r := byName["backend"]; r.EffectivePrefix() != "bk" {
		t.Errorf("backend prefix = %q, want the fragment's rig patch", r.EffectivePrefix())
	}
	if _, ok := byName["docs"]; !ok {
		t.Errorf("rigs = %v, want the fragment's docs rig", dep.Rigs)
	}
	if got := EffectiveHQPrefix(dep); got != "dm" {
		t.Errorf("EffectiveHQPrefix = %q, want the site binding's", got)
	}
	if got := dep.EffectiveStorage().Bindings["infra"].Provider; got != "sqlite-beads" {
		t.Errorf("infra binding provider = %q, want the fragment's", got)
	}

	wantSources := []string{cityTOML, filepath.Join(cityDir, "fragments", "more.toml")}
	if !reflect.DeepEqual(prov.Sources, wantSources) {
		t.Errorf("Sources = %v, want %v", prov.Sources, wantSources)
	}
}

// TestLoadDeploymentReadsNoPack pins that LoadDeployment never opens a pack:
// an import whose source does not exist fails the full composition and leaves
// the deployment load untouched.
func TestLoadDeploymentReadsNoPack(t *testing.T) {
	cityDir := writeDeploymentFixture(t, t.TempDir(), map[string]string{
		"city.toml": "[[rigs]]\nname = \"frontend\"\nprefix = \"fe\"\n",
		"pack.toml": "[pack]\nname = \"demo\"\nschema = 2\n\n[imports.tools]\nsource = \"./packs/missing\"\n",
	})
	cityTOML := filepath.Join(cityDir, "city.toml")

	if _, _, err := LoadWithIncludes(fsys.OSFS{}, cityTOML); err == nil {
		t.Fatal("LoadWithIncludes succeeded with a missing import; the fixture no longer proves anything")
	}
	cfg, _, err := LoadDeployment(fsys.OSFS{}, cityTOML)
	if err != nil {
		t.Fatalf("LoadDeployment: %v", err)
	}
	if len(cfg.Rigs) != 1 || cfg.Rigs[0].EffectivePrefix() != "fe" {
		t.Fatalf("Rigs = %+v, want the frontend rig", cfg.Rigs)
	}
}

// TestLoadDeploymentChecksWhatTheFullLoadChecks pins that the deployment layer
// is checked exactly as the full composition checks it. Each config is invalid
// only in a field the deployment layer composes: both loaders refuse it with
// the same error, or both accept it and report the same warning. Every warning
// the deployment load reports, the full load reports too.
func TestLoadDeploymentChecksWhatTheFullLoadChecks(t *testing.T) {
	t.Setenv("GC_HOME", "")
	const schema2Pack = "[pack]\nname = \"demo\"\nschema = 2\n"
	const monitor = "[[rigs]]\nname = \"frontend\"\n\n[[github.pr_monitor]]\nname = \"main\"\nowner = \"acme\"\nrepo = \"app\"\n" +
		"base_branches = [\"main\"]\nrig = \"ghost\"\nrepair_route = \"frontend/fixer\"\n"
	for _, tc := range []struct {
		name        string
		files       map[string]string
		wantErr     string // both loads fail with this in the error
		wantWarning string // both loads succeed with this in a warning
	}{
		{
			name:    "rig patch for an undeclared rig",
			files:   map[string]string{"city.toml": "[[rigs]]\nname = \"frontend\"\n\n[[patches.rigs]]\nname = \"ghost\"\nsuspended = true\n"},
			wantErr: "patches.rigs[0]",
		},
		{
			name:    "reserved storage binding",
			files:   map[string]string{"city.toml": "[storage.bindings.work]\nprovider = \"sqlite-beads\"\n"},
			wantErr: "storage.bindings.work is reserved",
		},
		{
			name: "storage class naming no binding",
			files: map[string]string{"city.toml": "[storage.classes]\nwork = \"work\"\ngraph = \"infra\"\nsessions = \"work\"\n" +
				"messaging = \"work\"\norders = \"work\"\nnudges = \"work\"\n"},
			wantErr: "storage.classes.graph references undefined binding \"infra\"",
		},
		{
			name:    "negative dolt limit",
			files:   map[string]string{"city.toml": "[dolt]\nmax_connections = -1\n"},
			wantErr: "max_connections must not be negative",
		},
		{
			name:    "negative dolt stop timeout",
			files:   map[string]string{"city.toml": "[daemon]\ndolt_stop_timeout = \"-5s\"\n"},
			wantErr: "dolt_stop_timeout must not be negative",
		},
		{
			name:    "bead policy storage the bd compatibility mode cannot keep",
			files:   map[string]string{"city.toml": "[beads.policies.workflow]\nstorage = \"no_history\"\n"},
			wantErr: "[beads.policies.workflow] storage = \"no_history\" is not bd-1.0.4-compatible",
		},
		{
			name:    "bead retention that is not positive",
			files:   map[string]string{"city.toml": "[beads.policies.session]\ndelete_after_close = \"0s\"\n"},
			wantErr: "delete_after_close must be a positive duration",
		},
		{
			name:    "beads mode a fragment sets out of its enum",
			files:   map[string]string{"city.toml": "include = [\"frag.toml\"]\n", "frag.toml": "[beads]\nnative_transport = \"bogus\"\n"},
			wantErr: "beads.native_transport: invalid mode \"bogus\"",
		},
		{
			name:    "GitHub PR monitor on an undeclared rig",
			files:   map[string]string{"city.toml": monitor},
			wantErr: "rig \"ghost\" is not declared",
		},
		{
			name:    "schema-2 city with a rig path in city.toml",
			files:   map[string]string{"pack.toml": schema2Pack, "city.toml": "[[rigs]]\nname = \"frontend\"\npath = \"/srv/frontend\"\n"},
			wantErr: "unsupported pre-1.0 rig.path for rig \"frontend\"",
		},
		{
			name:    "schema-2 city with PackV1 workspace includes",
			files:   map[string]string{"pack.toml": schema2Pack, "city.toml": "[workspace]\nincludes = [\"packs/tools\"]\n"},
			wantErr: "unsupported PackV1 workspace.includes",
		},
		{
			name: "schema-2 fragment with PackV1 workspace includes",
			files: map[string]string{
				"pack.toml": schema2Pack,
				"city.toml": "include = [\"frag.toml\"]\n",
				"frag.toml": "[workspace]\nincludes = [\"packs/tools\"]\n",
			},
			wantErr: "fragment \"frag.toml\": PackV1 config surfaces are no longer supported",
		},
		{
			name:    "city pack.toml that does not parse",
			files:   map[string]string{"pack.toml": "[pack\n", "city.toml": "[[rigs]]\nname = \"frontend\"\n"},
			wantErr: "parsing city pack.toml",
		},
		{
			name:        "duration that does not parse",
			files:       map[string]string{"city.toml": "[daemon]\npatrol_interval = \"5mins\"\n"},
			wantWarning: "[daemon] patrol_interval = \"5mins\" is not a valid duration",
		},
		{
			name:        "rig session sleep that does not parse",
			files:       map[string]string{"city.toml": "[[rigs]]\nname = \"frontend\"\n\n[rigs.session_sleep]\nnoninteractive = \"later\"\n"},
			wantWarning: "rig \"frontend\" [session_sleep] noninteractive = \"later\"",
		},
		{
			name:        "events rotation that deletes recent archives",
			files:       map[string]string{"city.toml": "[events.rotation]\narchive_retain_age = \"1h\"\n"},
			wantWarning: "archive_retain_age=1h may delete recent archives",
		},
		{
			name:        "proxied idle timeout that does not parse",
			files:       map[string]string{"city.toml": "[beads]\nproxied_idle_timeout = \"soon\"\n"},
			wantWarning: "[beads] proxied_idle_timeout",
		},
		{
			name:        "deprecated workspace field",
			files:       map[string]string{"city.toml": "[workspace]\nstart_command = \"run\"\n"},
			wantWarning: "workspace.start_command is deprecated",
		},
		{
			name:        "schema-2 city with workspace identity in city.toml",
			files:       map[string]string{"pack.toml": schema2Pack, "city.toml": "[workspace]\nname = \"demo\"\n"},
			wantWarning: legacyWorkspaceIdentityWarningFragment,
		},
		{
			name:  "GitHub PR monitor a patch moves onto a declared rig",
			files: map[string]string{"city.toml": monitor + "\n[[patches.github_pr_monitor]]\nname = \"main\"\nrig = \"frontend\"\n"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cityTOML := filepath.Join(writeDeploymentFixture(t, t.TempDir(), tc.files), "city.toml")
			_, fullProv, fullErr := LoadWithIncludes(fsys.OSFS{}, cityTOML)
			_, depProv, depErr := LoadDeployment(fsys.OSFS{}, cityTOML)
			if tc.wantErr != "" {
				if fullErr == nil || depErr == nil {
					t.Fatalf("full load error = %v, deployment load error = %v; want both to fail with %q", fullErr, depErr, tc.wantErr)
				}
				if fullErr.Error() != depErr.Error() {
					t.Fatalf("the loaders refuse it differently:\n full       %v\n deployment %v", fullErr, depErr)
				}
				if !strings.Contains(depErr.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one containing %q", depErr, tc.wantErr)
				}
				return
			}
			if fullErr != nil || depErr != nil {
				t.Fatalf("full load error = %v, deployment load error = %v; want both to succeed", fullErr, depErr)
			}
			for _, w := range depProv.Warnings {
				if !slices.Contains(fullProv.Warnings, w) {
					t.Errorf("the deployment load warns %q, which the full load does not; full warnings %q", w, fullProv.Warnings)
				}
			}
			if tc.wantWarning == "" {
				return
			}
			for name, warnings := range map[string][]string{"full": fullProv.Warnings, "deployment": depProv.Warnings} {
				if !slices.ContainsFunc(warnings, func(w string) bool { return strings.Contains(w, tc.wantWarning) }) {
					t.Errorf("%s load warnings %q, want one containing %q", name, warnings, tc.wantWarning)
				}
			}
		})
	}
}
