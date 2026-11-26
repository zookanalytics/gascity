package beadstest

import "testing"

// TestEmbeddedDoltUnsupported pins the classifier that decides whether a
// failed `bd` invocation is the ambient embedded-Dolt/CGO capability gap
// (skip) or a real defect (fail). Both directions matter: too loose and a
// genuine bd regression is silently skipped into green; too strict and the
// CGO_ENABLED=0 hosts are back to a hard-failing push gate.
func TestEmbeddedDoltUnsupported(t *testing.T) {
	cases := []struct {
		name   string
		output string
		want   bool
	}{
		{
			name: "verbatim bd CGO_ENABLED=0 init failure",
			output: "  ✓ Initialized git repository\n" +
				"Error: failed to open Dolt store: embedded Dolt requires a CGO build, " +
				"but this bd binary was built with CGO_ENABLED=0.\n\n" +
				"Three options:\n\n" +
				"  1. Use the proxied dolt sql-server (no external server, no reinstall):\n" +
				"       bd init --proxied-server\n",
			want: true,
		},
		{
			name: "verbatim bd CGO_ENABLED=0 migrate schema failure",
			output: "warning: no beads configuration found in /tmp/probe/db; using default database name \"beads\"\n" +
				"Error: failed to open database: embedded Dolt requires a CGO build, " +
				"but this bd binary was built with CGO_ENABLED=0.\n",
			want: true,
		},
		{
			name:   "reworded phrasing of the same capability gap",
			output: "Error: embedded dolt requires a cgo-enabled build",
			want:   true,
		},
		{
			name:   "empty output is not a capability gap",
			output: "",
			want:   false,
		},
		{
			name:   "unrelated store failure still fails the test",
			output: "Error: failed to open Dolt store: database is locked",
			want:   false,
		},
		{
			name:   "embedded Dolt named without a CGO cause still fails",
			output: "Error: embedded Dolt store is corrupt; run bd doctor",
			want:   false,
		},
		{
			name:   "CGO named without embedded Dolt still fails",
			output: "Error: cgo linker failure while loading extension",
			want:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EmbeddedDoltUnsupported(tc.output); got != tc.want {
				t.Errorf("EmbeddedDoltUnsupported(%q) = %v, want %v", tc.output, got, tc.want)
			}
		})
	}
}
