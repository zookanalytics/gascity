package lifecycle

import (
	"errors"
	"testing"
)

func TestRuntimeValidTransitions(t *testing.T) {
	valid := []struct {
		from RuntimeState
		to   RuntimeState
	}{
		{RuntimeStarting, RuntimeActive},
		{RuntimeStarting, RuntimeFailed},
		{RuntimeActive, RuntimePausing},
		{RuntimeActive, RuntimeCompleting},
		{RuntimeActive, RuntimeFailed},
		{RuntimeActive, RuntimeExpired},
		{RuntimePausing, RuntimePaused},
		{RuntimePausing, RuntimeFailed},
		{RuntimePaused, RuntimeResuming},
		{RuntimePaused, RuntimeExpired},
		{RuntimeResuming, RuntimeActive},
		{RuntimeResuming, RuntimeFailed},
		{RuntimeCompleting, RuntimeCompleted},
		{RuntimeCompleting, RuntimeFailed},
	}
	for _, tc := range valid {
		if !CanTransitionRuntime(tc.from, tc.to) {
			t.Errorf("CanTransitionRuntime(%s, %s) = false, want true", tc.from, tc.to)
		}
		got, err := TransitionRuntime(tc.from, tc.to)
		if err != nil {
			t.Errorf("TransitionRuntime(%s, %s) error: %v", tc.from, tc.to, err)
		}
		if got != tc.to {
			t.Errorf("TransitionRuntime(%s, %s) = %s, want %s", tc.from, tc.to, got, tc.to)
		}
	}
}

func TestRuntimeInvalidTransitions(t *testing.T) {
	invalid := []struct {
		from RuntimeState
		to   RuntimeState
	}{
		{RuntimeCompleted, RuntimeActive},
		{RuntimeFailed, RuntimeActive},
		{RuntimeExpired, RuntimeActive},
		{RuntimePaused, RuntimeActive}, // must go through resuming
		{RuntimeStarting, RuntimeCompleted},
		{RuntimeActive, RuntimeStarting},
	}
	for _, tc := range invalid {
		if CanTransitionRuntime(tc.from, tc.to) {
			t.Errorf("CanTransitionRuntime(%s, %s) = true, want false", tc.from, tc.to)
		}
		got, err := TransitionRuntime(tc.from, tc.to)
		if err == nil {
			t.Errorf("TransitionRuntime(%s, %s) should return error", tc.from, tc.to)
		}
		var ite *InvalidTransitionError
		if !errors.As(err, &ite) {
			t.Errorf("TransitionRuntime(%s, %s) error type = %T, want *InvalidTransitionError", tc.from, tc.to, err)
		}
		if got != tc.from {
			t.Errorf("TransitionRuntime(%s, %s) on error returned %s, want %s (original)", tc.from, tc.to, got, tc.from)
		}
	}
}

func TestRuntimeTerminalStates(t *testing.T) {
	terminal := map[RuntimeState]bool{
		RuntimeCompleted: true,
		RuntimeFailed:    true,
		RuntimeExpired:   true,
	}
	all := []RuntimeState{
		RuntimeStarting, RuntimeActive, RuntimePausing, RuntimePaused,
		RuntimeResuming, RuntimeCompleting, RuntimeCompleted,
		RuntimeFailed, RuntimeExpired,
	}
	for _, s := range all {
		want := terminal[s]
		if got := IsTerminalRuntime(s); got != want {
			t.Errorf("IsTerminalRuntime(%s) = %v, want %v", s, got, want)
		}
	}
}

func TestRuntimeStateCategories(t *testing.T) {
	cases := []struct {
		state RuntimeState
		want  StateCategory
	}{
		{RuntimeActive, CategoryActive},
		{RuntimePaused, CategoryActive},
		{RuntimeStarting, CategoryTransitioning},
		{RuntimePausing, CategoryTransitioning},
		{RuntimeResuming, CategoryTransitioning},
		{RuntimeCompleting, CategoryTransitioning},
		{RuntimeCompleted, CategoryTerminal},
		{RuntimeExpired, CategoryTerminal},
		{RuntimeFailed, CategoryError},
		{RuntimeState("unknown"), CategoryError},
	}
	for _, tc := range cases {
		if got := RuntimeStateCategory(tc.state); got != tc.want {
			t.Errorf("RuntimeStateCategory(%s) = %s, want %s", tc.state, got, tc.want)
		}
	}
}

func TestRuntimeNonTerminalHasOutbound(t *testing.T) {
	all := []RuntimeState{
		RuntimeStarting, RuntimeActive, RuntimePausing, RuntimePaused,
		RuntimeResuming, RuntimeCompleting,
	}
	transitions := ValidRuntimeTransitions()
	for _, s := range all {
		if IsTerminalRuntime(s) {
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

func TestValidRuntimeTransitionsReturnsCopy(t *testing.T) {
	a := ValidRuntimeTransitions()
	b := ValidRuntimeTransitions()
	a[0].Trigger = "mutated"
	if b[0].Trigger == "mutated" {
		t.Error("ValidRuntimeTransitions returned a shared slice")
	}
}
