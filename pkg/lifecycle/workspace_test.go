package lifecycle

import (
	"errors"
	"testing"
)

func TestWorkspaceValidTransitions(t *testing.T) {
	valid := []struct {
		from WorkspaceState
		to   WorkspaceState
	}{
		{WorkspacePendingPlacement, WorkspaceCreating},
		{WorkspacePendingPlacement, WorkspaceFailed},
		{WorkspaceCreating, WorkspaceActive},
		{WorkspaceCreating, WorkspaceFailed},
		{WorkspaceActive, WorkspaceSuspending},
		{WorkspaceActive, WorkspaceDeleting},
		{WorkspaceActive, WorkspaceFailed},
		{WorkspaceSuspending, WorkspaceSuspended},
		{WorkspaceSuspending, WorkspaceFailed},
		{WorkspaceSuspended, WorkspaceResuming},
		{WorkspaceSuspended, WorkspaceDeleting},
		{WorkspaceResuming, WorkspaceActive},
		{WorkspaceResuming, WorkspaceFailed},
		{WorkspaceFailed, WorkspaceDeleting},
		{WorkspaceFailed, WorkspaceResuming},
		{WorkspaceDeleting, WorkspaceDeleted},
		{WorkspaceDeleting, WorkspaceFailed},
	}
	for _, tc := range valid {
		if !CanTransitionWorkspace(tc.from, tc.to) {
			t.Errorf("CanTransitionWorkspace(%s, %s) = false, want true", tc.from, tc.to)
		}
		got, err := TransitionWorkspace(tc.from, tc.to)
		if err != nil {
			t.Errorf("TransitionWorkspace(%s, %s) error: %v", tc.from, tc.to, err)
		}
		if got != tc.to {
			t.Errorf("TransitionWorkspace(%s, %s) = %s, want %s", tc.from, tc.to, got, tc.to)
		}
	}
}

func TestWorkspaceInvalidTransitions(t *testing.T) {
	invalid := []struct {
		from WorkspaceState
		to   WorkspaceState
	}{
		{WorkspaceActive, WorkspaceCreating},
		{WorkspaceDeleted, WorkspaceActive},
		{WorkspaceDeleted, WorkspaceDeleting},
		{WorkspaceSuspended, WorkspaceActive}, // must go through resuming
		{WorkspacePendingPlacement, WorkspaceActive},
		{WorkspaceCreating, WorkspaceSuspended},
	}
	for _, tc := range invalid {
		if CanTransitionWorkspace(tc.from, tc.to) {
			t.Errorf("CanTransitionWorkspace(%s, %s) = true, want false", tc.from, tc.to)
		}
		got, err := TransitionWorkspace(tc.from, tc.to)
		if err == nil {
			t.Errorf("TransitionWorkspace(%s, %s) should return error", tc.from, tc.to)
		}
		var ite *InvalidTransitionError
		if !errors.As(err, &ite) {
			t.Errorf("TransitionWorkspace(%s, %s) error type = %T, want *InvalidTransitionError", tc.from, tc.to, err)
		}
		if got != tc.from {
			t.Errorf("TransitionWorkspace(%s, %s) on error returned %s, want %s (original)", tc.from, tc.to, got, tc.from)
		}
	}
}

func TestWorkspaceTerminalStates(t *testing.T) {
	terminal := map[WorkspaceState]bool{
		WorkspaceDeleted: true,
	}
	all := []WorkspaceState{
		WorkspacePendingPlacement, WorkspaceCreating, WorkspaceActive,
		WorkspaceSuspending, WorkspaceSuspended, WorkspaceResuming,
		WorkspaceFailed, WorkspaceDeleting, WorkspaceDeleted,
	}
	for _, s := range all {
		want := terminal[s]
		if got := IsTerminalWorkspace(s); got != want {
			t.Errorf("IsTerminalWorkspace(%s) = %v, want %v", s, got, want)
		}
	}
}

func TestWorkspaceStateCategories(t *testing.T) {
	cases := []struct {
		state WorkspaceState
		want  StateCategory
	}{
		{WorkspaceActive, CategoryActive},
		{WorkspaceSuspended, CategoryActive},
		{WorkspacePendingPlacement, CategoryTransitioning},
		{WorkspaceCreating, CategoryTransitioning},
		{WorkspaceSuspending, CategoryTransitioning},
		{WorkspaceResuming, CategoryTransitioning},
		{WorkspaceDeleting, CategoryTransitioning},
		{WorkspaceDeleted, CategoryTerminal},
		{WorkspaceFailed, CategoryError},
		{WorkspaceState("unknown"), CategoryError},
	}
	for _, tc := range cases {
		if got := WorkspaceStateCategory(tc.state); got != tc.want {
			t.Errorf("WorkspaceStateCategory(%s) = %s, want %s", tc.state, got, tc.want)
		}
	}
}

func TestWorkspaceNonTerminalHasOutbound(t *testing.T) {
	all := []WorkspaceState{
		WorkspacePendingPlacement, WorkspaceCreating, WorkspaceActive,
		WorkspaceSuspending, WorkspaceSuspended, WorkspaceResuming,
		WorkspaceFailed, WorkspaceDeleting,
	}
	transitions := ValidWorkspaceTransitions()
	for _, s := range all {
		if IsTerminalWorkspace(s) {
			continue
		}
		hasOutbound := false
		for _, tr := range transitions {
			if tr.From == s {
				hasOutbound = true
				break
			}
		}
		if !hasOutbound {
			t.Errorf("non-terminal state %s has no outbound transitions", s)
		}
	}
}

func TestValidWorkspaceTransitionsReturnsCopy(t *testing.T) {
	a := ValidWorkspaceTransitions()
	b := ValidWorkspaceTransitions()
	a[0].Trigger = "mutated"
	if b[0].Trigger == "mutated" {
		t.Error("ValidWorkspaceTransitions returned a shared slice")
	}
}
