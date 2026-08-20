package shellpipeline

import "testing"

func TestFindPipefailGrepQPipelines(t *testing.T) {
	const pipefail = "set -euo pipefail\n"
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "piped grep -q", body: pipefail + `echo "$x" | grep -q foo`, want: 1},
		{name: "piped grep -qF", body: pipefail + `printf '%s' "$x" | grep -qF "$n"`, want: 1},
		{name: "piped grep -Fxq", body: pipefail + `printf '%s\n' "$x" | grep -Fxq -- "$n"`, want: 1},
		{name: "piped grep -qiE", body: pipefail + `echo "$x" | grep -qiE 'a|b'`, want: 1},
		{name: "piped grep -Eq", body: pipefail + `printf '%s' "$x" | grep -Eq '\[.\]$'`, want: 1},
		{name: "git diff into grep -q", body: pipefail + `git diff --name-only a b -- '*.go' 2>/dev/null | grep -q .`, want: 1},
		{name: "here-string grep -q is safe", body: pipefail + `grep -q foo <<<"$x"`},
		{name: "here-string grep -Fxq is safe", body: pipefail + `grep -Fxq -- "$n" <<<"$x"`},
		{name: "list_contains_line is safe", body: pipefail + `list_contains_line "$x" "$n"`},
		{name: "capture then test is safe", body: pipefail + "out=$(git diff --name-only a b)\nif [ -n \"$out\" ]; then :; fi"},
		{name: "not gated without pipefail", body: `echo "$x" | grep -q foo`},
		{name: "full-line comment is ignored", body: pipefail + `  # bad: echo "$x" | grep -q foo`},
		{name: "grep -v without q is not flagged", body: pipefail + `echo "$x" | grep -v foo`},
		{name: "grep pattern q is not a flag", body: pipefail + `echo "$x" | grep 'q'`},
		{name: "grep -e q pattern is not a flag", body: pipefail + `echo "$x" | grep -e q`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := len(FindPipefailGrepQPipelines([]byte(tt.body))); got != tt.want {
				t.Fatalf("FindPipefailGrepQPipelines() = %d, want %d for %q", got, tt.want, tt.body)
			}
		})
	}
}

func TestDescribeLine(t *testing.T) {
	data := []byte("set -euo pipefail\nfoo\n  echo \"$x\" | grep -q bar\n")
	offsets := FindPipefailGrepQPipelines(data)
	if len(offsets) != 1 {
		t.Fatalf("FindPipefailGrepQPipelines() = %d offsets, want 1", len(offsets))
	}
	line, text := DescribeLine(data, offsets[0])
	if line != 3 {
		t.Errorf("DescribeLine() line = %d, want 3", line)
	}
	if want := `echo "$x" | grep -q bar`; text != want {
		t.Errorf("DescribeLine() text = %q, want %q", text, want)
	}
}
