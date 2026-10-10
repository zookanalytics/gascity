package config

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

func TestExpandCityPacks_PackMetadataIndexesComposed(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/alpha/pack.toml", `
[pack]
name = "alpha"
schema = 1

[beads]
metadata_indexes = ["anchor_bead", "branch", "anchor_bead"]
`)

	cfg := &City{Workspace: Workspace{Includes: []string{"packs/alpha"}}}
	if _, _, _, err := ExpandCityPacks(cfg, fsys.OSFS{}, dir); err != nil {
		t.Fatalf("ExpandCityPacks: %v", err)
	}
	if got, want := cfg.MetadataIndexKeys(), []string{"anchor_bead", "branch"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("MetadataIndexKeys() = %v, want %v", got, want)
	}
	first := cfg.PackMetadataIndexes[0]
	if first.PackName != "alpha" || first.PackDir != filepath.Join(dir, "packs/alpha") {
		t.Errorf("provenance = %q in %q, want alpha in %q", first.PackName, first.PackDir, filepath.Join(dir, "packs/alpha"))
	}
}

func TestExpandCityPacks_PackMetadataIndexesFromIncludesAndImports(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/inner/pack.toml", `
[pack]
name = "inner"
schema = 1

[beads]
metadata_indexes = ["inner.key"]
`)
	writeFile(t, dir, "packs/outer/pack.toml", `
[pack]
name = "outer"
schema = 1
includes = ["../inner"]

[beads]
metadata_indexes = ["outer_key"]
`)
	writeFile(t, dir, "packs/imported/pack.toml", `
[pack]
name = "imported"
schema = 1

[beads]
metadata_indexes = ["imported/key", "outer_key"]
`)

	cfg := &City{
		Workspace: Workspace{Includes: []string{"packs/outer"}},
		Imports:   map[string]Import{"im": {Source: "packs/imported"}},
	}
	if _, _, _, err := ExpandCityPacks(cfg, fsys.OSFS{}, dir); err != nil {
		t.Fatalf("ExpandCityPacks: %v", err)
	}
	got := map[string]string{}
	for _, ix := range cfg.PackMetadataIndexes {
		got[ix.Key] = ix.PackName
	}
	want := map[string]string{"inner.key": "inner", "outer_key": "outer", "imported/key": "imported"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("composed keys by declaring pack = %v, want %v (a key two packs declare keeps its first declaration)", got, want)
	}
}

func TestExpandCityPacks_NonTransitiveImportFiltersNestedMetadataIndexes(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/dep/pack.toml", `
[pack]
name = "dep"
schema = 1

[beads]
metadata_indexes = ["hidden_key"]
`)
	writeFile(t, dir, "packs/direct/pack.toml", `
[pack]
name = "direct"
schema = 1

[imports.dep]
source = "../dep"

[beads]
metadata_indexes = ["visible_key"]
`)

	transitive := false
	cfg := &City{Imports: map[string]Import{
		"d": {Source: "packs/direct", Transitive: &transitive},
	}}
	if _, _, _, err := ExpandCityPacks(cfg, fsys.OSFS{}, dir); err != nil {
		t.Fatalf("ExpandCityPacks: %v", err)
	}
	if got, want := cfg.MetadataIndexKeys(), []string{"visible_key"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("MetadataIndexKeys() = %v, want %v (a non-transitive import hides its nested packs)", got, want)
	}
}

func TestExpandPacks_RigImportMetadataIndexesRegisterCityWide(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/rigpack/pack.toml", `
[pack]
name = "rigpack"
schema = 1

[beads]
metadata_indexes = ["merge_result"]
`)

	cfg := &City{
		Rigs: []Rig{{
			Name:    "r1",
			Imports: map[string]Import{"rp": {Source: "packs/rigpack"}},
		}},
	}
	if err := ExpandPacks(cfg, fsys.OSFS{}, dir, map[string][]string{}); err != nil {
		t.Fatalf("ExpandPacks: %v", err)
	}
	if got, want := cfg.MetadataIndexKeys(), []string{"merge_result"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("MetadataIndexKeys() = %v, want %v", got, want)
	}
}

func TestExpandCityPacks_InvalidMetadataIndexKeyFailsLoad(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/bad/pack.toml", `
[pack]
name = "bad"
schema = 1

[beads]
metadata_indexes = ["anchor_bead", "x') OR 1=1 -- "]
`)

	cfg := &City{Workspace: Workspace{Includes: []string{"packs/bad"}}}
	_, _, _, err := ExpandCityPacks(cfg, fsys.OSFS{}, dir)
	if err == nil {
		t.Fatal("ExpandCityPacks accepted a metadata key that would break out of the index expression")
	}
	for _, want := range []string{`pack "bad"`, "[beads].metadata_indexes", "invalid metadata key"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

func TestLoadWithIncludes_RootPackMetadataIndexes(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "city.toml", `
[workspace]
name = "test"
`)
	writeFile(t, dir, "pack.toml", `
[pack]
name = "test"
schema = 2

[beads]
metadata_indexes = ["city_key"]

[imports.gs]
source = "./packs/gastown"
`)
	writeFile(t, dir, "packs/gastown/pack.toml", `
[pack]
name = "gastown"
schema = 2

[beads]
metadata_indexes = ["pack_key"]
`)

	cfg, _, err := LoadWithIncludes(fsys.OSFS{}, filepath.Join(dir, "city.toml"))
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}
	got := map[string]bool{}
	for _, key := range cfg.MetadataIndexKeys() {
		got[key] = true
	}
	for _, key := range []string{"city_key", "pack_key"} {
		if !got[key] {
			t.Errorf("MetadataIndexKeys() = %v lacks %q", cfg.MetadataIndexKeys(), key)
		}
	}
}
