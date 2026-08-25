package contract

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

func TestLocalVersionWitnessPath(t *testing.T) {
	got := LocalVersionWitnessPath("/city")
	want := filepath.Join("/city", ".beads", ".local_version")
	if got != want {
		t.Fatalf("LocalVersionWitnessPath() = %q, want %q", got, want)
	}
}

func TestReadLocalVersionWitness(t *testing.T) {
	tests := []struct {
		name    string
		write   bool
		content string
		want    string
		wantOK  bool
	}{
		{name: "absent", write: false, wantOK: false},
		{name: "empty", write: true, content: "", wantOK: false},
		{name: "whitespace only", write: true, content: "  \n\t ", wantOK: false},
		{name: "version", write: true, content: "1.2.1\n", want: "1.2.1", wantOK: true},
		{name: "legacy version", write: true, content: "0.55.0\n", want: "0.55.0", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scope := t.TempDir()
			if tc.write {
				if err := os.MkdirAll(filepath.Join(scope, ".beads"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(LocalVersionWitnessPath(scope), []byte(tc.content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, ok, err := ReadLocalVersionWitness(fsys.OSFS{}, scope)
			if err != nil {
				t.Fatalf("ReadLocalVersionWitness() error = %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("ReadLocalVersionWitness() ok = %v, want %v", ok, tc.wantOK)
			}
			if got != tc.want {
				t.Fatalf("ReadLocalVersionWitness() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEnsureLocalVersionWitnessWritesWhenAbsent(t *testing.T) {
	scope := t.TempDir()
	wrote, err := EnsureLocalVersionWitness(fsys.OSFS{}, scope, "1.2.1")
	if err != nil {
		t.Fatalf("EnsureLocalVersionWitness() error = %v", err)
	}
	if !wrote {
		t.Fatal("EnsureLocalVersionWitness() wrote = false, want true")
	}
	data, err := os.ReadFile(LocalVersionWitnessPath(scope))
	if err != nil {
		t.Fatalf("read witness: %v", err)
	}
	if string(data) != "1.2.1\n" {
		t.Fatalf("witness contents = %q, want %q", string(data), "1.2.1\n")
	}
}

func TestEnsureLocalVersionWitnessPreservesExisting(t *testing.T) {
	// bd owns this file once it has run. An existing witness — including a
	// pre-1.0 one that must keep triggering bd's migration refusal — is never
	// overwritten.
	for _, existing := range []string{"0.55.0\n", "1.1.0\n"} {
		t.Run(existing, func(t *testing.T) {
			scope := t.TempDir()
			if err := os.MkdirAll(filepath.Join(scope, ".beads"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(LocalVersionWitnessPath(scope), []byte(existing), 0o600); err != nil {
				t.Fatal(err)
			}
			wrote, err := EnsureLocalVersionWitness(fsys.OSFS{}, scope, "1.2.1")
			if err != nil {
				t.Fatalf("EnsureLocalVersionWitness() error = %v", err)
			}
			if wrote {
				t.Fatal("EnsureLocalVersionWitness() wrote = true over an existing witness, want false")
			}
			data, err := os.ReadFile(LocalVersionWitnessPath(scope))
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != existing {
				t.Fatalf("witness contents = %q, want preserved %q", string(data), existing)
			}
		})
	}
}

func TestEnsureLocalVersionWitnessRejectsUnusableVersions(t *testing.T) {
	// Only a version bd itself would accept as a post-1.0 witness may be
	// written. Anything else either leaves the workspace refused anyway or
	// records a claim gc cannot make.
	for _, version := range []string{"", "   ", "1.2", "1.2.1.4", "0.62.0", "v0.9.9", "abc", "1.x.1", "1.-2.1"} {
		t.Run(version, func(t *testing.T) {
			scope := t.TempDir()
			wrote, err := EnsureLocalVersionWitness(fsys.OSFS{}, scope, version)
			if err == nil {
				t.Fatalf("EnsureLocalVersionWitness(%q) error = nil, want rejection", version)
			}
			if wrote {
				t.Fatalf("EnsureLocalVersionWitness(%q) wrote = true, want false", version)
			}
			if _, statErr := os.Stat(LocalVersionWitnessPath(scope)); !os.IsNotExist(statErr) {
				t.Fatalf("rejected version %q still created a witness file", version)
			}
		})
	}
}

func TestEnsureLocalVersionWitnessAcceptsVPrefixedVersion(t *testing.T) {
	scope := t.TempDir()
	wrote, err := EnsureLocalVersionWitness(fsys.OSFS{}, scope, "v1.2.1")
	if err != nil {
		t.Fatalf("EnsureLocalVersionWitness() error = %v", err)
	}
	if !wrote {
		t.Fatal("EnsureLocalVersionWitness() wrote = false, want true")
	}
	data, err := os.ReadFile(LocalVersionWitnessPath(scope))
	if err != nil {
		t.Fatal(err)
	}
	// bd parses the witness with strings.TrimPrefix(version, "v"), so either
	// form is readable; normalize to the bare form bd itself writes.
	if string(data) != "1.2.1\n" {
		t.Fatalf("witness contents = %q, want %q", string(data), "1.2.1\n")
	}
}

func TestEnsureLocalVersionWitnessIsIdempotent(t *testing.T) {
	scope := t.TempDir()
	if _, err := EnsureLocalVersionWitness(fsys.OSFS{}, scope, "1.2.1"); err != nil {
		t.Fatalf("first EnsureLocalVersionWitness() error = %v", err)
	}
	wrote, err := EnsureLocalVersionWitness(fsys.OSFS{}, scope, "1.2.1")
	if err != nil {
		t.Fatalf("second EnsureLocalVersionWitness() error = %v", err)
	}
	if wrote {
		t.Fatal("second EnsureLocalVersionWitness() wrote = true, want false")
	}
}
