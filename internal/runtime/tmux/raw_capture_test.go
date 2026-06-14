package tmux

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// callEndsWith reports whether the recorded tmux call ends with the given
// args. The executor prepends socket flags (-u -L <socket>) to every call, so
// tests assert on the meaningful suffix rather than the full slice.
func callEndsWith(call, suffix []string) bool {
	if len(call) < len(suffix) {
		return false
	}
	return reflect.DeepEqual(call[len(call)-len(suffix):], suffix)
}

func TestProviderPeekRawPreservesANSIAndFlags(t *testing.T) {
	fe := &fakeExecutor{out: "❯ \x1b[2mghost\x1b[0m\n"}
	p := NewProviderWithConfig(Config{SocketName: "x"})
	p.tm.exec = fe

	out, err := p.PeekRaw("runner", 5)
	if err != nil {
		t.Fatalf("PeekRaw: %v", err)
	}
	if out != "❯ \x1b[2mghost\x1b[0m\n" {
		t.Fatalf("PeekRaw output = %q, want ANSI preserved", out)
	}
	if len(fe.calls) == 0 {
		t.Fatal("PeekRaw issued no tmux call")
	}
	got := fe.calls[len(fe.calls)-1]
	want := []string{"capture-pane", "-p", "-e", "-t", "runner", "-S", "-5"}
	if !callEndsWith(got, want) {
		t.Fatalf("PeekRaw call = %v, want suffix %v", got, want)
	}
}

func TestProviderPeekRawAllScrollback(t *testing.T) {
	fe := &fakeExecutor{out: "x"}
	p := NewProviderWithConfig(Config{SocketName: "x"})
	p.tm.exec = fe

	if _, err := p.PeekRaw("runner", 0); err != nil {
		t.Fatalf("PeekRaw(0): %v", err)
	}
	got := fe.calls[len(fe.calls)-1]
	want := []string{"capture-pane", "-p", "-e", "-t", "runner", "-S", "-"}
	if !callEndsWith(got, want) {
		t.Fatalf("PeekRaw(0) call = %v, want all-scrollback suffix %v", got, want)
	}
}

func TestProviderInputAreaIssuesANSICapture(t *testing.T) {
	// Empty pane: provider detection falls through to unknown, but the call
	// must still issue an ANSI-preserving capture-pane and return a state
	// (delegation to Tmux.InputArea). Parser correctness is covered by the
	// Stage A parser tests; this guards the adapter delegation only.
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

// Provider must satisfy both capability interfaces consumed by the CLI.
var (
	_ RawPeeker         = (*Provider)(nil)
	_ InputAreaCapturer = (*Provider)(nil)
)
