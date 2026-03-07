package operation

import (
	"testing"
	"time"
)

func TestOperationTransitionValid(t *testing.T) {
	cases := []struct {
		from Phase
		to   Phase
	}{
		{Pending, Running},
		{Pending, Canceled},
		{Pending, Failed},
		{Running, Succeeded},
		{Running, Failed},
		{Running, Canceled},
	}
	for _, tc := range cases {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			op := Operation{Phase: tc.from, CreatedAt: time.Now()}
			if err := op.Transition(tc.to); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if op.Phase != tc.to {
				t.Errorf("Phase = %s, want %s", op.Phase, tc.to)
			}
		})
	}
}

func TestOperationTransitionInvalid(t *testing.T) {
	cases := []struct {
		from Phase
		to   Phase
	}{
		{Succeeded, Running},
		{Failed, Running},
		{Canceled, Running},
		{Running, Pending},
		{Succeeded, Failed},
	}
	for _, tc := range cases {
		t.Run(string(tc.from)+"->"+string(tc.to), func(t *testing.T) {
			op := Operation{Phase: tc.from, CreatedAt: time.Now()}
			if err := op.Transition(tc.to); err == nil {
				t.Error("expected error for invalid transition")
			}
			if op.Phase != tc.from {
				t.Errorf("Phase changed to %s on error, should stay %s", op.Phase, tc.from)
			}
		})
	}
}

func TestOperationIsTerminal(t *testing.T) {
	terminal := map[Phase]bool{
		Succeeded: true,
		Failed:    true,
		Canceled:  true,
	}
	for _, p := range []Phase{Pending, Running, Succeeded, Failed, Canceled} {
		op := Operation{Phase: p}
		want := terminal[p]
		if got := op.IsTerminal(); got != want {
			t.Errorf("IsTerminal(%s) = %v, want %v", p, got, want)
		}
	}
}

func TestOperationTransitionSetsCompletedAt(t *testing.T) {
	op := Operation{Phase: Running, CreatedAt: time.Now()}
	if err := op.Transition(Succeeded); err != nil {
		t.Fatal(err)
	}
	if op.CompletedAt == nil {
		t.Error("CompletedAt should be set on terminal transition")
	}
	if op.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should be set on transition")
	}
}

func TestOperationTransitionUpdatesTimestamp(t *testing.T) {
	op := Operation{Phase: Pending, CreatedAt: time.Now()}
	before := time.Now()
	if err := op.Transition(Running); err != nil {
		t.Fatal(err)
	}
	if op.UpdatedAt.Before(before.Add(-time.Second)) {
		t.Error("UpdatedAt should be recent")
	}
	if op.CompletedAt != nil {
		t.Error("CompletedAt should not be set for non-terminal transition")
	}
}
