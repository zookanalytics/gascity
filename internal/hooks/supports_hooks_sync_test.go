package hooks_test

import (
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/hooks"
)

// TestSupportsHooksSyncWithProviderSpec verifies that the hooks supported list
// stays in sync with ProviderSpec.SupportsHooks across all builtin providers.
// It lives in the external test package because internal/config imports
// internal/hooks.
func TestSupportsHooksSyncWithProviderSpec(t *testing.T) {
	sup := make(map[string]bool, len(hooks.SupportedProviders()))
	for _, p := range hooks.SupportedProviders() {
		sup[p] = true
	}

	providers := config.BuiltinProviders()
	for name, spec := range providers {
		supports := spec.SupportsHooks != nil && *spec.SupportsHooks
		if supports && !sup[name] {
			t.Errorf("provider %q has SupportsHooks=true but is not in hooks.SupportedProviders()", name)
		}
		if !supports && sup[name] {
			t.Errorf("provider %q is in hooks.SupportedProviders() but has SupportsHooks=false", name)
		}
	}
	// Reverse check: every supported provider must be a known builtin.
	for _, p := range hooks.SupportedProviders() {
		if _, ok := providers[p]; !ok {
			t.Errorf("hooks.SupportedProviders() contains %q which is not a builtin provider", p)
		}
	}
}
