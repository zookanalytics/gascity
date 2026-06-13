package runtime

import (
	"testing"
	"testing/fstest"
)

func TestHashFSContentFile(t *testing.T) {
	fsys := fstest.MapFS{
		"SKILL.md": {Data: []byte("hello")},
	}
	h1 := HashFSContent(fsys, "SKILL.md")
	if h1 == "" {
		t.Fatal("expected non-empty hash for embedded file")
	}
	if h2 := HashFSContent(fsys, "SKILL.md"); h1 != h2 {
		t.Errorf("same embedded file produced different hashes: %s vs %s", h1, h2)
	}

	changed := fstest.MapFS{"SKILL.md": {Data: []byte("world")}}
	if HashFSContent(changed, "SKILL.md") == h1 {
		t.Error("different embedded file content should produce different hash")
	}
}

func TestHashFSContentDirectory(t *testing.T) {
	fsys := fstest.MapFS{
		"skills/gc-dispatch/SKILL.md":     {Data: []byte("dispatch body")},
		"skills/gc-dispatch/reference.md": {Data: []byte("reference body")},
		"skills/gc-work/SKILL.md":         {Data: []byte("work body")},
		"agents/mayor/prompt.template.md": {Data: []byte("prompt")},
	}
	h1 := HashFSContent(fsys, "skills/gc-dispatch")
	if h1 == "" {
		t.Fatal("expected non-empty hash for embedded directory")
	}
	if h2 := HashFSContent(fsys, "skills/gc-dispatch"); h1 != h2 {
		t.Error("same embedded directory produced different hashes")
	}

	// A sibling skill's bytes must not affect this skill's hash.
	withSiblingChange := fstest.MapFS{
		"skills/gc-dispatch/SKILL.md":     {Data: []byte("dispatch body")},
		"skills/gc-dispatch/reference.md": {Data: []byte("reference body")},
		"skills/gc-work/SKILL.md":         {Data: []byte("work body CHANGED")},
	}
	if got := HashFSContent(withSiblingChange, "skills/gc-dispatch"); got != h1 {
		t.Errorf("sibling-skill change leaked into hash: %s != %s", got, h1)
	}

	// Changing a file within the subtree changes the hash.
	withChange := fstest.MapFS{
		"skills/gc-dispatch/SKILL.md":     {Data: []byte("dispatch body CHANGED")},
		"skills/gc-dispatch/reference.md": {Data: []byte("reference body")},
	}
	if HashFSContent(withChange, "skills/gc-dispatch") == h1 {
		t.Error("subtree content change should produce different hash")
	}
}

// TestHashFSContentIndependentOfSourceFS is the core determinism guarantee
// behind the builtin-pack fingerprint fix: two filesystems with byte-identical
// content at root hash equal, regardless of where that content came from. The
// supervisor's embedded bytes are constant for its process lifetime, so the
// fingerprint cannot flap when a foreign process restages an on-disk copy.
func TestHashFSContentIndependentOfSourceFS(t *testing.T) {
	a := fstest.MapFS{"skills/x/SKILL.md": {Data: []byte("same")}}
	b := fstest.MapFS{"skills/x/SKILL.md": {Data: []byte("same")}}
	if HashFSContent(a, "skills/x") != HashFSContent(b, "skills/x") {
		t.Error("identical content in distinct filesystems must hash equal")
	}
}

func TestHashFSContentWholeFS(t *testing.T) {
	fsys := fstest.MapFS{
		"SKILL.md":  {Data: []byte("root skill")},
		"extra.txt": {Data: []byte("extra")},
	}
	if HashFSContent(fsys, ".") == "" {
		t.Fatal(`expected non-empty hash for "." root`)
	}
}

func TestHashFSContentMissingAndEmpty(t *testing.T) {
	fsys := fstest.MapFS{"SKILL.md": {Data: []byte("x")}}
	if got := HashFSContent(fsys, "does/not/exist"); got != "" {
		t.Errorf("missing path: got %q, want empty", got)
	}
	if got := HashFSContent(fsys, ""); got != "" {
		t.Errorf("empty root: got %q, want empty", got)
	}
	if got := HashFSContent(nil, "SKILL.md"); got != "" {
		t.Errorf("nil fsys: got %q, want empty", got)
	}
}

func TestHashFSContentSkipsRuntimeArtifacts(t *testing.T) {
	base := fstest.MapFS{
		"skills/x/SKILL.md": {Data: []byte("body")},
	}
	withArtifacts := fstest.MapFS{
		"skills/x/SKILL.md":              {Data: []byte("body")},
		"skills/x/__pycache__/m.cpython": {Data: []byte("cache")},
		"skills/x/module.pyc":            {Data: []byte("pyc")},
		"skills/x/.SKILL.md.swp":         {Data: []byte("swap")},
		"skills/x/backup~":               {Data: []byte("backup")},
	}
	if HashFSContent(base, "skills/x") != HashFSContent(withArtifacts, "skills/x") {
		t.Error("runtime-generated artifacts must not affect the content hash")
	}
}
