package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/materialize"
	"github.com/gastownhall/gascity/internal/runtime"
)

// TestConfigDrift_StoreSideAndCheckSideMatch is the acceptance regression for
// gc-e9g. The store-side path (cmd/gc/session_lifecycle_parallel.go
// buildPreparedStart) and the check-side path (cmd/gc/session_reconciler.go
// config-drift block) must produce byte-identical CoreFingerprints and
// per-field breakdowns for the same TemplateParams. When they diverge, live
// sessions drain every reconcile tick with "config drift detected" even
// though no configuration has changed.
//
// Both paths call templateParamsToConfig(tp) -> runtime.CoreFingerprint(cfg)
// with the same tp instance from desiredState; this test pins that invariant
// so a future divergence (different template_override handling, asymmetric
// agentCfg.Command mutation, etc.) is caught at compile/test time rather
// than in production drain storms.
func TestConfigDrift_StoreSideAndCheckSideMatch(t *testing.T) {
	cityPath := t.TempDir()
	writeTemplateResolveCityConfig(t, cityPath, "file")
	if err := os.WriteFile(filepath.Join(cityPath, "pack.toml"),
		[]byte("[pack]\nname = \"drift-test\"\nversion = \"0.1.0\"\nschema = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	skillDir := filepath.Join(cityPath, "skills", "plan")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"),
		[]byte("---\nname: plan\ndescription: test\n---\nbody\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sharedCat, err := materialize.LoadCityCatalog(filepath.Join(cityPath, "skills"))
	if err != nil {
		t.Fatal(err)
	}

	makeParams := func() *agentBuildParams {
		return &agentBuildParams{
			cityName:  "city",
			cityPath:  cityPath,
			workspace: &config.Workspace{Provider: "claude"},
			providers: map[string]config.ProviderSpec{
				"claude": {Command: "echo", PromptMode: "none", SupportsACP: boolPtr(true)},
			},
			lookPath:        func(string) (string, error) { return "/bin/echo", nil },
			fs:              fsys.OSFS{},
			rigs:            []config.Rig{},
			beaconTime:      time.Unix(0, 0),
			beadNames:       make(map[string]string),
			stderr:          io.Discard,
			skillCatalog:    &sharedCat,
			sessionProvider: "tmux",
		}
	}

	agent := &config.Agent{
		Name:              "mayor",
		Scope:             "city",
		Provider:          "claude",
		MaxActiveSessions: intPtr(1),
		WakeMode:          "fresh",
		WorkDir:           ".gc/agents/mayor",
	}
	tp, err := resolveTemplate(makeParams(), agent, agent.QualifiedName(), buildFingerprintExtra(agent))
	if err != nil {
		t.Fatalf("resolveTemplate: %v", err)
	}

	// Store-side mimics cmd/gc/session_lifecycle_parallel.go buildPreparedStart
	// (lines ~286-321): templateParamsToConfig → CoreFingerprint.
	storeCfg := templateParamsToConfig(tp)
	storeHash := runtime.CoreFingerprint(storeCfg)
	storeBreakdown := runtime.CoreFingerprintBreakdown(storeCfg)

	// Check-side mimics cmd/gc/session_reconciler.go config-drift detection
	// (lines ~624-650): templateParamsToConfig → CoreFingerprint, against the
	// same tp from desiredState.
	checkCfg := templateParamsToConfig(tp)
	checkHash := runtime.CoreFingerprint(checkCfg)
	checkBreakdown := runtime.CoreFingerprintBreakdown(checkCfg)

	if storeHash != checkHash {
		t.Fatalf("store-side hash %q != check-side hash %q for identical TemplateParams; breakdown store=%v check=%v",
			storeHash, checkHash, storeBreakdown, checkBreakdown)
	}
	for field, sh := range storeBreakdown {
		ch, ok := checkBreakdown[field]
		if !ok {
			t.Errorf("check-side breakdown missing field %q (store=%q)", field, sh)
			continue
		}
		if sh != ch {
			t.Errorf("per-field hash drift for %q: store=%q check=%q\n  store.PreStart=%v check.PreStart=%v\n  store.FPExtra=%v check.FPExtra=%v",
				field, sh, ch, storeCfg.PreStart, checkCfg.PreStart, storeCfg.FingerprintExtra, checkCfg.FingerprintExtra)
		}
	}
}

// TestConfigDrift_ResolveTemplateIsDeterministicAcrossTicks pins that two
// successive resolveTemplate calls with the same inputs produce byte-
// identical TemplateParams (and therefore byte-identical CoreFingerprint /
// per-field breakdowns). The bug signature in gc-e9g was a persistent drift
// where the stored hash captured at session-create time did not match the
// hash recomputed at the next reconciler tick — binary restarts did not
// clear it because each restart re-stored a hash that flipped again on
// the next check.
func TestConfigDrift_ResolveTemplateIsDeterministicAcrossTicks(t *testing.T) {
	cityPath := t.TempDir()
	writeTemplateResolveCityConfig(t, cityPath, "file")
	if err := os.WriteFile(filepath.Join(cityPath, "pack.toml"),
		[]byte("[pack]\nname = \"drift-test\"\nversion = \"0.1.0\"\nschema = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	skillDir := filepath.Join(cityPath, "skills", "plan")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"),
		[]byte("---\nname: plan\ndescription: test\n---\nbody\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sharedCat, err := materialize.LoadCityCatalog(filepath.Join(cityPath, "skills"))
	if err != nil {
		t.Fatal(err)
	}

	makeParams := func() *agentBuildParams {
		return &agentBuildParams{
			cityName:  "city",
			cityPath:  cityPath,
			workspace: &config.Workspace{Provider: "claude"},
			providers: map[string]config.ProviderSpec{
				"claude": {Command: "echo", PromptMode: "none", SupportsACP: boolPtr(true)},
			},
			lookPath:        func(string) (string, error) { return "/bin/echo", nil },
			fs:              fsys.OSFS{},
			rigs:            []config.Rig{},
			beaconTime:      time.Unix(0, 0),
			beadNames:       make(map[string]string),
			stderr:          io.Discard,
			skillCatalog:    &sharedCat,
			sessionProvider: "tmux",
		}
	}

	cases := []struct {
		name  string
		agent *config.Agent
	}{
		{
			name: "pool_agent_with_skills",
			agent: &config.Agent{
				Name:              "mayor",
				Scope:             "city",
				Provider:          "claude",
				MaxActiveSessions: intPtr(1),
				WakeMode:          "fresh",
				WorkDir:           ".gc/agents/mayor",
			},
		},
		{
			name: "named_agent_no_pool",
			agent: &config.Agent{
				Name:     "mechanik",
				Scope:    "city",
				Provider: "claude",
				WakeMode: "fresh",
				WorkDir:  ".gc/agents/mechanik",
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			tpTickN, err := resolveTemplate(makeParams(), c.agent, c.agent.QualifiedName(), buildFingerprintExtra(c.agent))
			if err != nil {
				t.Fatalf("resolveTemplate tick N: %v", err)
			}
			tpTickN1, err := resolveTemplate(makeParams(), c.agent, c.agent.QualifiedName(), buildFingerprintExtra(c.agent))
			if err != nil {
				t.Fatalf("resolveTemplate tick N+1: %v", err)
			}

			cfgN := templateParamsToConfig(tpTickN)
			cfgN1 := templateParamsToConfig(tpTickN1)
			hashN := runtime.CoreFingerprint(cfgN)
			hashN1 := runtime.CoreFingerprint(cfgN1)
			if hashN != hashN1 {
				breakdownN := runtime.CoreFingerprintBreakdown(cfgN)
				breakdownN1 := runtime.CoreFingerprintBreakdown(cfgN1)
				t.Fatalf("CoreFingerprint drifts across ticks for %q: tickN=%q tickN+1=%q\n  breakdown N=%v\n  breakdown N+1=%v\n  PreStart N=%v\n  PreStart N+1=%v\n  FPExtra N=%v\n  FPExtra N+1=%v",
					c.agent.QualifiedName(), hashN, hashN1, breakdownN, breakdownN1,
					cfgN.PreStart, cfgN1.PreStart, cfgN.FingerprintExtra, cfgN1.FingerprintExtra)
			}
		})
	}
}
