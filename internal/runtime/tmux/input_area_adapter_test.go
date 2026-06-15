package tmux

import (
	"context"
	"strings"
	"testing"
)

func TestProviderInputAreaIssuesANSICapture(t *testing.T) {
	// Empty pane: provider detection falls through to unknown, but the call
	// must still issue an ANSI-preserving capture-pane and return a state
	// (delegation to Tmux.InputArea). Parser correctness is covered by the
	// parser tests; this guards the adapter delegation only.
	fe := &fakeExecutor{out: ""}
	p := NewProviderWithConfig(Config{SocketName: "x"})
	p.tm.exec = fe

	state, err := p.InputArea(context.Background(), "runner")
	if err != nil {
		t.Fatalf("InputArea: %v", err)
	}
	if state == nil {
		t.Fatal("InputArea returned nil state")
	}
	ansiCapture := false
	for _, call := range fe.calls {
		joined := strings.Join(call, " ")
		if strings.Contains(joined, "capture-pane") && strings.Contains(joined, "-e") {
			ansiCapture = true
			break
		}
	}
	if !ansiCapture {
		t.Fatalf("InputArea did not issue an ANSI capture-pane call: %v", fe.calls)
	}
}

// Provider must satisfy the input-area capability consumed by the CLI.
var _ InputAreaCapturer = (*Provider)(nil)
