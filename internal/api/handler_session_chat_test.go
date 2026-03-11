package api

import "testing"

func TestShellJoinArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"empty slice", nil, ""},
		{"single arg no metachar", []string{"--model"}, "--model"},
		{"two clean args", []string{"--model", "opus"}, "--model opus"},
		{"arg with space", []string{"hello world"}, "'hello world'"},
		{"arg with single quote", []string{"it's"}, "'it'\"'\"'s'"},
		{"empty string arg", []string{""}, ""},
		{"mixed clean and dirty", []string{"--flag", "value with space", "--other"}, "--flag 'value with space' --other"},
		{"arg with special chars", []string{"$(whoami)"}, "'$(whoami)'"},
		{"arg with semicolon", []string{"foo;bar"}, "'foo;bar'"},
		{"multiple special", []string{"a b", "c'd", "e|f"}, "'a b' 'c'\"'\"'d' 'e|f'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shellJoinArgs(tt.args)
			if got != tt.want {
				t.Errorf("shellJoinArgs(%q) = %q, want %q", tt.args, got, tt.want)
			}
		})
	}
}
