package config

import (
	"os"
	"path/filepath"
	"reflect"
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

// TestLoadDeploymentRefusesInvalidDeploymentConfig pins that skipping packs
// skips no check on the layers LoadDeployment does compose.
func TestLoadDeploymentRefusesInvalidDeploymentConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		city    string
		wantErr string
	}{
		{
			name:    "rig patch for an undeclared rig",
			city:    "[[rigs]]\nname = \"frontend\"\n\n[[patches.rigs]]\nname = \"ghost\"\nsuspended = true\n",
			wantErr: "patches.rigs[0]",
		},
		{
			name:    "reserved storage binding",
			city:    "[storage.bindings.work]\nprovider = \"sqlite-beads\"\n",
			wantErr: "storage.bindings.work is reserved",
		},
		{
			name:    "negative dolt limit",
			city:    "[dolt]\nmax_connections = -1\n",
			wantErr: "max_connections must not be negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := fsys.NewFake()
			fs.Files["/city/city.toml"] = []byte(tc.city)
			_, _, err := LoadDeployment(fs, "/city/city.toml")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("LoadDeployment error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}
