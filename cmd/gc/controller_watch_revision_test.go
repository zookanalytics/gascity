package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
)

// writeWatchTestFile writes body to rel (slash-separated) under root, creating
// parent directories, and returns the file's path.
func writeWatchTestFile(t *testing.T, root, rel, body string) string {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
	return path
}

// newTestConfigWatchRegistrar returns a registrar over a live fsnotify watcher
// with targets marked the way watchConfigTargets marks them. Nothing is
// registered with the watcher until the test calls addPath.
func newTestConfigWatchRegistrar(t *testing.T, targets ...config.WatchTarget) (*configWatchRegistrar, *fsnotify.Watcher) {
	t.Helper()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatalf("fsnotify.NewWatcher: %v", err)
	}
	t.Cleanup(func() { _ = watcher.Close() })
	registrar := newConfigWatchRegistrar(watcher, io.Discard)
	for _, target := range targets {
		registrar.markTarget(target)
	}
	return registrar, watcher
}

// watchedRelPaths returns the watcher's registered paths relative to root,
// slash-separated and sorted. The recursive walk registers nested directories
// under root's symlink-resolved spelling, so both spellings are accepted.
func watchedRelPaths(t *testing.T, watcher *fsnotify.Watcher, root string) []string {
	t.Helper()
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%s): %v", root, err)
	}
	var rels []string
	for _, path := range watcher.WatchList() {
		rel, ok := "", false
		for _, base := range []string{root, resolvedRoot} {
			if r, err := filepath.Rel(base, path); err == nil && r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator)) {
				rel, ok = r, true
				break
			}
		}
		if !ok {
			t.Fatalf("watched path %s lies outside root %s", path, root)
		}
		rels = append(rels, filepath.ToSlash(rel))
	}
	sort.Strings(rels)
	return rels
}

// The recursive walk registers no directory the config revision cannot see.
// A pack root is often a live git checkout: watching its .git means one
// watch per object directory, and one descriptor per file under kqueue.
func TestConfigWatchRegistrarWalkSkipsWhatTheRevisionSkips(t *testing.T) {
	root := t.TempDir()
	writeWatchTestFile(t, root, "pack.toml", "[pack]\nname = \"sample\"\n")
	for _, rel := range []string{
		".git/objects/ab",
		".git/refs/heads",
		".git/worktrees/wt",
		".cache/tool",
		"tmp/run",
		"state/runs",
		"node_modules/dep/lib",
		"formulas/__pycache__",
		"skills/s/node_modules/dep",
		"orders",
		"prompts/state",
		"vendored/.git/objects",
		"vendored/tmp",
	} {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(rel)), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", rel, err)
		}
	}

	registrar, watcher := newTestConfigWatchRegistrar(t, config.WatchTarget{Path: root, Recursive: true})
	done := make(chan struct{})
	if !registrar.addPath(root, true, done) {
		t.Fatalf("addPath(%s) failed", root)
	}

	got := watchedRelPaths(t, watcher, root)
	// Only the leading segment of .git, .cache, tmp and state is skipped;
	// node_modules and __pycache__ are skipped at any depth. vendored/.git
	// and vendored/tmp are hashed, so they stay watched.
	want := []string{
		".",
		"formulas",
		"orders",
		"prompts",
		"prompts/state",
		"skills",
		"skills/s",
		"vendored",
		"vendored/.git",
		"vendored/.git/objects",
		"vendored/tmp",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("watched directories under the pack root:\n  got  %q\n  want %q", got, want)
	}

	// A directory created later is walked from itself, the way the event
	// loop enqueues it, and the enclosing root's rule still decides: the root
	// hashes late/tmp, and skips late/node_modules.
	for _, rel := range []string{"late/tmp/run", "late/node_modules/dep"} {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(rel)), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", rel, err)
		}
	}
	if !registrar.addPath(filepath.Join(root, "late"), true, done) {
		t.Fatalf("addPath(%s) failed", filepath.Join(root, "late"))
	}
	got = watchedRelPaths(t, watcher, root)
	want = append(want, "late", "late/tmp", "late/tmp/run")
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("watched directories after walking late/:\n  got  %q\n  want %q", got, want)
	}
}

// The watcher drops a write exactly when the config revision cannot see it.
// Each case writes one file and asks the registrar about that write's event,
// with the recursive roots' content hashes as the oracle. The roots nest the
// way they do for a rig that imports a pack and one of its sub-packs
// (packs/sub), and one root sits inside a directory the outer root skips
// (state/pack).
func TestConfigWatchRegistrarDropsExactlyTheWritesTheRevisionSkips(t *testing.T) {
	root := t.TempDir()
	writeWatchTestFile(t, root, "pack.toml", "[pack]\nname = \"outer\"\n")
	writeWatchTestFile(t, root, "packs/sub/pack.toml", "[pack]\nname = \"sub\"\n")
	writeWatchTestFile(t, root, "state/pack/pack.toml", "[pack]\nname = \"inner\"\n")
	roots := []string{root, filepath.Join(root, "packs", "sub"), filepath.Join(root, "state", "pack")}
	targets := make([]config.WatchTarget, 0, len(roots))
	for _, r := range roots {
		targets = append(targets, config.WatchTarget{Path: r, Recursive: true})
	}
	registrar, _ := newTestConfigWatchRegistrar(t, targets...)
	revision := func() string {
		var b strings.Builder
		for _, r := range roots {
			b.WriteString(config.PackContentHashRecursive(fsys.OSFS{}, r))
			b.WriteByte(0)
		}
		return b.String()
	}
	// Events name nested paths under the symlink-resolved root, so both
	// spellings must classify the same way.
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%s): %v", root, err)
	}

	for _, tc := range []struct {
		rel         string
		wantDropped bool
	}{
		{".git/objects/ab/cdef0123456789", true},
		{".git/worktrees/wt/HEAD.lock", true},
		{".git/prep-checkout/orders/copied.toml", true},
		{".cache/tool/result.json", true},
		{"tmp/scratch.txt", true},
		{"state/run.json", true},
		{"node_modules/dep/index.js", true},
		{"skills/s/node_modules/dep/index.js", true},
		{"formulas/__pycache__/helper.pyc", true},
		{"orders/new.toml", false},
		{"pack.toml", false},
		{"prompts/state/example.md", false},
		{"vendored/.git/HEAD", false},
		{"vendored/tmp/data.txt", false},
		// packs/sub skips its own tmp, but the outer root hashes it.
		{"packs/sub/tmp/run.log", false},
		{"packs/sub/node_modules/dep/index.js", true},
		{"packs/sub/orders/new.toml", false},
		// The outer root skips state/, but state/pack hashes its own orders.
		{"state/pack/orders/new.toml", false},
		{"state/pack/tmp/run.log", true},
	} {
		before := revision()
		writeWatchTestFile(t, root, tc.rel, "write "+tc.rel+"\n")
		unchanged := revision() == before
		if unchanged != tc.wantDropped {
			t.Fatalf("%s: revision unchanged = %v, want %v; the case table no longer matches the content hash", tc.rel, unchanged, tc.wantDropped)
		}
		for _, base := range []string{root, resolvedRoot} {
			event := filepath.Join(base, filepath.FromSlash(tc.rel))
			if got := registrar.ignoresEvent(event); got != unchanged {
				t.Errorf("ignoresEvent(%s) = %v, want %v: the write left the revision unchanged = %v", event, got, unchanged, unchanged)
			}
		}
	}
}

// A config source file can sit in a directory that a recursive root skips,
// such as a fragment under the pack root's state/. That directory is a
// shallow target, whose watch delivers events for itself and its direct
// entries.
func TestConfigWatchRegistrarDeliversShallowTargetEntriesARootSkips(t *testing.T) {
	root := t.TempDir()
	fragDir := filepath.Join(root, "state", "conf")
	frag := writeWatchTestFile(t, fragDir, "frag.toml", "[workspace]\n")
	registrar, _ := newTestConfigWatchRegistrar(t,
		config.WatchTarget{Path: root, Recursive: true},
		config.WatchTarget{Path: fragDir},
	)
	for _, path := range []string{fragDir, frag} {
		if registrar.ignoresEvent(path) {
			t.Errorf("ignoresEvent(%s) = true, want false: a shallow target delivers itself and its direct entries", path)
		}
	}
	nested := filepath.Join(fragDir, "deeper", "run.log")
	if !registrar.ignoresEvent(nested) {
		t.Errorf("ignoresEvent(%s) = false, want true: below a shallow target's direct entries the root's skip rule applies", nested)
	}
}

// End to end through the real watcher: writes the revision cannot see, into
// existing directories and into new subtrees, set nothing dirty, while a
// write it hashes still does.
func TestWatchConfigTargets_PackRootRuntimeWritesDoNotPoke(t *testing.T) {
	root := t.TempDir()
	writeWatchTestFile(t, root, "pack.toml", "[pack]\nname = \"sample\"\n")
	gitObject := writeWatchTestFile(t, root, ".git/objects/ab/cdef0123456789", "original\n")
	fetchHead := writeWatchTestFile(t, root, ".git/FETCH_HEAD", "original\n")
	scratch := writeWatchTestFile(t, root, "tmp/scratch.txt", "original\n")
	dep := writeWatchTestFile(t, root, "skills/s/node_modules/dep/index.js", "original\n")
	order := writeWatchTestFile(t, root, "orders/sample.toml", "original\n")

	var dirty atomic.Bool
	pokeCh := make(chan struct{}, 1)
	var stderr bytes.Buffer
	cleanup := watchConfigTargets([]config.WatchTarget{{Path: root, Recursive: true}}, testConfigDebounce, &dirty, newLegacyWake(pokeCh, nil), &stderr)
	defer cleanup()

	select {
	case <-pokeCh:
	default:
	}
	dirty.Store(false)

	for _, path := range []string{gitObject, fetchHead, scratch, dep} {
		if err := os.WriteFile(path, []byte("rewritten\n"), 0o644); err != nil {
			t.Fatalf("rewrite %s: %v", path, err)
		}
	}
	// New subtrees: a checkout nested under .git and a run dir under tmp.
	writeWatchTestFile(t, root, ".git/prep-checkout/orders/copied.toml", "copied\n")
	writeWatchTestFile(t, root, "tmp/run/output.log", "line\n")

	// Negative-assertion window: asserts no watcher poke arrives.
	select {
	case <-pokeCh:
		t.Fatalf("watcher poked for writes the config revision cannot see (dirty=%v); stderr=%q", dirty.Load(), stderr.String())
	case <-time.After(250 * time.Millisecond):
	}
	if dirty.Load() {
		t.Fatalf("dirty flag set by writes the config revision cannot see; stderr=%q", stderr.String())
	}

	// Control: the same watcher still reports a write the revision hashes.
	if err := os.WriteFile(order, []byte("edited\n"), 0o644); err != nil {
		t.Fatalf("rewrite %s: %v", order, err)
	}
	awaitClose(t, pokeCh, "watcher poke after orders/ edit")
	if !dirty.Load() {
		t.Fatalf("dirty flag not set after orders/ edit; stderr=%q", stderr.String())
	}
}
