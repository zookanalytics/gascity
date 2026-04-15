package api

import "testing"

func TestActionTableCapabilitiesAreExecutableByRole(t *testing.T) {
	for name, entry := range actionTable {
		if entry.supportsRole(actionServerRoleCity) && entry.Handler == nil {
			t.Errorf("city action %q is advertised without a city handler", name)
		}

		if !entry.supportsRole(actionServerRoleSupervisor) {
			continue
		}
		if entry.SupervisorHandler != nil {
			continue
		}
		if entry.RequiresCityScope && entry.Handler != nil {
			continue
		}
		t.Errorf("supervisor action %q is advertised without a supervisor dispatch path", name)
	}
}
