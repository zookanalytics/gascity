package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"

	"github.com/gastownhall/gascity/internal/fsys"
)

// Pack-tree cache. The composer parses ~85 TOML files on every cold gc
// invocation; this cache memoizes the composed *City + *Provenance to disk
// keyed by the running binary's identity plus the mtime/size of every
// filesystem path the prior load touched. A fresh stat sweep is enough to
// confirm the cache is valid; on any mismatch the loader falls through to
// the normal parse and rewrites the cache atomically.
//
// The cache holds only correctness-relevant fields. Provenance unexported
// fields (sourceContents, revisionSnapshot) are not persisted — they are
// rebuilt lazily by Revision() and friends when needed (those callers are
// long-running services, not the CLI hot path).

const (
	packCacheEnvVar  = "GC_PACK_CACHE"
	packCacheOff     = "off"
	packCacheFile    = "pack-tree.gob"
	packCacheVersion = 1
)

// packCachePayload is the on-disk cache representation.
type packCachePayload struct {
	Version int
	BuildID string
	Extras  []string
	Sources []packCacheSource
	City    *City
	Prov    packCacheProvenance
}

// packCacheSource records one filesystem path that influenced a prior load.
// Existed=false means the path was probed but missing — if it later appears,
// the cache is invalidated.
type packCacheSource struct {
	Path    string
	Existed bool
	IsDir   bool
	ModNs   int64
	Size    int64
}

// packCacheProvenance carries the exported subset of Provenance.
type packCacheProvenance struct {
	Root      string
	Sources   []string
	Imports   map[string]string
	Agents    map[string]string
	Rigs      map[string]string
	Workspace map[string]string
	Warnings  []string
}

// packCacheDisabled returns true when GC_PACK_CACHE=off.
func packCacheDisabled() bool {
	return os.Getenv(packCacheEnvVar) == packCacheOff
}

// packCachePath returns the absolute cache file path for a city.
func packCachePath(cityRoot string) string {
	return filepath.Join(cityRoot, ".gc", "runtime", "cache", packCacheFile)
}

var (
	packCacheBuildIDOnce sync.Once
	packCacheBuildIDStr  string
)

// packCacheBuildID derives a build identity for the running gc binary.
// Combines the executable mtime+size with debug.BuildInfo VCS metadata so
// `make install` (which rewrites the binary) and `go build` (which changes
// vcs.modified) both bust the cache. Returns "" if neither input is
// available, which disables caching for that invocation.
func packCacheBuildID() string {
	packCacheBuildIDOnce.Do(func() {
		h := sha256.New()
		var contributed bool
		if exe, err := os.Executable(); err == nil {
			if info, err := os.Stat(exe); err == nil {
				_, _ = fmt.Fprintf(h, "exe=%s\x00size=%d\x00mod=%d\x00", exe, info.Size(), info.ModTime().UnixNano())
				contributed = true
			}
		}
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision", "vcs.modified", "vcs.time":
					_, _ = fmt.Fprintf(h, "%s=%s\x00", s.Key, s.Value)
				}
			}
			_, _ = fmt.Fprintf(h, "go=%s\x00", bi.GoVersion)
			contributed = true
		}
		if !contributed {
			packCacheBuildIDStr = ""
			return
		}
		packCacheBuildIDStr = fmt.Sprintf("%x", h.Sum(nil))
	})
	return packCacheBuildIDStr
}

// loadPackCache attempts to deserialize the cache file and validate it
// against the current filesystem state. Returns (city, prov, true) on a
// verified cache hit; (nil, nil, false) on any miss.
func loadPackCache(cachePath, buildID string, extras []string, fs fsys.FS) (*City, *Provenance, bool) {
	if buildID == "" {
		return nil, nil, false
	}
	f, err := os.Open(cachePath)
	if err != nil {
		return nil, nil, false
	}
	defer func() { _ = f.Close() }()

	var p packCachePayload
	if err := decodePackCache(f, &p); err != nil {
		return nil, nil, false
	}
	if p.Version != packCacheVersion || p.BuildID != buildID {
		return nil, nil, false
	}
	if !equalStringSlice(p.Extras, extras) {
		return nil, nil, false
	}
	if !packCacheSourcesValid(p.Sources, fs) {
		return nil, nil, false
	}
	if p.City == nil {
		return nil, nil, false
	}

	prov := &Provenance{
		Root:      p.Prov.Root,
		Sources:   append([]string(nil), p.Prov.Sources...),
		Imports:   cloneStringMap(p.Prov.Imports),
		Agents:    cloneStringMap(p.Prov.Agents),
		Rigs:      cloneStringMap(p.Prov.Rigs),
		Workspace: cloneStringMap(p.Prov.Workspace),
		Warnings:  append([]string(nil), p.Prov.Warnings...),
	}
	return p.City, prov, true
}

// savePackCache serializes the loader output and writes it atomically.
// All errors are silent: caching is opportunistic, never load-blocking.
//
// Skips writing if the city's .gc/ runtime root does not already exist.
// Pre-init cities have city.toml but no .gc/, and creating the cache
// directory would silently transform a "not bootstrapped" city into one
// that passes the HasRuntimeRoot check. Once `gc init` runs (creating
// .gc/), subsequent loads cache normally.
func savePackCache(cachePath, buildID string, extras []string, sources []packCacheSource, city *City, prov *Provenance) {
	if buildID == "" || city == nil || prov == nil {
		return
	}
	if !runtimeRootExistsForCache(cachePath) {
		return
	}
	payload := &packCachePayload{
		Version: packCacheVersion,
		BuildID: buildID,
		Extras:  append([]string(nil), extras...),
		Sources: sources,
		City:    city,
		Prov: packCacheProvenance{
			Root:      prov.Root,
			Sources:   append([]string(nil), prov.Sources...),
			Imports:   cloneStringMap(prov.Imports),
			Agents:    cloneStringMap(prov.Agents),
			Rigs:      cloneStringMap(prov.Rigs),
			Workspace: cloneStringMap(prov.Workspace),
			Warnings:  append([]string(nil), prov.Warnings...),
		},
	}
	if err := writePackCachePayload(cachePath, payload); err != nil {
		// Best-effort: don't surface caching errors to the loader.
		_ = err
	}
}

// runtimeRootExistsForCache reports whether the .gc/ ancestor of a cache
// path already exists as a directory. Inlined here to avoid pulling in
// citylayout (config sits below citylayout in the layering, not above).
func runtimeRootExistsForCache(cachePath string) bool {
	// cachePath = <cityRoot>/.gc/runtime/cache/<file>
	// runtimeRoot = <cityRoot>/.gc
	runtimeRoot := filepath.Dir(filepath.Dir(filepath.Dir(cachePath)))
	if runtimeRoot == "" || runtimeRoot == "." {
		return false
	}
	fi, err := os.Stat(runtimeRoot)
	return err == nil && fi.IsDir()
}

// writePackCachePayload encodes payload and writes it atomically to path.
func writePackCachePayload(path string, payload *packCachePayload) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir cache dir: %w", err)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
		return fmt.Errorf("encode pack cache: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp cache: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write temp cache: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close temp cache: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename temp cache: %w", err)
	}
	return nil
}

// decodePackCache decodes a gob payload from r into out.
func decodePackCache(r io.Reader, out *packCachePayload) error {
	return gob.NewDecoder(r).Decode(out)
}

// packCacheSourcesValid stats every recorded source and returns true only if
// every entry's existence/size/mtime matches what we observed on the prior
// load. A directory's mtime changes when entries are added or removed inside
// it, which is how convention-discovery (agents/, commands/, doctor/, …)
// invalidates after a new file appears.
func packCacheSourcesValid(sources []packCacheSource, fs fsys.FS) bool {
	for _, src := range sources {
		info, err := fs.Stat(src.Path)
		if err != nil {
			if os.IsNotExist(err) {
				if src.Existed {
					return false
				}
				continue
			}
			return false
		}
		if !src.Existed {
			return false
		}
		if info.IsDir() != src.IsDir {
			return false
		}
		if info.Size() != src.Size {
			return false
		}
		if info.ModTime().UnixNano() != src.ModNs {
			return false
		}
	}
	return true
}

// recordingFS wraps an fsys.FS and records every read it observes so the
// cache key reflects exactly what the loader actually touched. Writes are
// passed through but not recorded — only inputs determine cache validity.
type recordingFS struct {
	inner   fsys.FS
	mu      sync.Mutex
	sources []packCacheSource
	seen    map[string]int
}

// newRecordingFS wraps inner.
func newRecordingFS(inner fsys.FS) *recordingFS {
	return &recordingFS{inner: inner, seen: make(map[string]int)}
}

func (r *recordingFS) record(path string, info os.FileInfo, err error) {
	src := packCacheSource{Path: path}
	if err == nil && info != nil {
		src.Existed = true
		src.IsDir = info.IsDir()
		src.ModNs = info.ModTime().UnixNano()
		src.Size = info.Size()
	}
	// On any error (ENOENT or otherwise) Existed stays false. Validation
	// re-stats the path; a path that errored before but now exists, or
	// existed before but now errors, fails the equality check and busts
	// the cache.
	r.mu.Lock()
	defer r.mu.Unlock()
	if idx, ok := r.seen[path]; ok {
		r.sources[idx] = src
		return
	}
	r.seen[path] = len(r.sources)
	r.sources = append(r.sources, src)
}

// snapshot returns a copy of the recorded sources, safe to encode.
func (r *recordingFS) snapshot() []packCacheSource {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]packCacheSource, len(r.sources))
	copy(out, r.sources)
	return out
}

// addSource records a path the caller observed outside this fs (e.g.
// implicit-import.toml read via os.ReadFile). Allows the cache to invalidate
// when those files change.
func (r *recordingFS) addSource(path string) {
	if path == "" {
		return
	}
	info, err := os.Stat(path)
	r.record(path, info, err)
}

// MkdirAll passes through unchanged.
func (r *recordingFS) MkdirAll(path string, perm os.FileMode) error {
	return r.inner.MkdirAll(path, perm)
}

// WriteFile passes through unchanged.
func (r *recordingFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	return r.inner.WriteFile(name, data, perm)
}

// ReadFile records a stat-equivalent observation, then reads.
func (r *recordingFS) ReadFile(name string) ([]byte, error) {
	info, statErr := r.inner.Stat(name)
	r.record(name, info, statErr)
	return r.inner.ReadFile(name)
}

// Stat records the observation and returns the result.
func (r *recordingFS) Stat(name string) (os.FileInfo, error) {
	info, err := r.inner.Stat(name)
	r.record(name, info, err)
	return info, err
}

// Lstat records the observation and returns the result.
func (r *recordingFS) Lstat(name string) (os.FileInfo, error) {
	info, err := r.inner.Lstat(name)
	r.record(name, info, err)
	return info, err
}

// ReadDir records a directory observation, then reads entries.
func (r *recordingFS) ReadDir(name string) ([]os.DirEntry, error) {
	info, statErr := r.inner.Stat(name)
	r.record(name, info, statErr)
	return r.inner.ReadDir(name)
}

// Rename passes through unchanged.
func (r *recordingFS) Rename(oldpath, newpath string) error {
	return r.inner.Rename(oldpath, newpath)
}

// Remove passes through unchanged.
func (r *recordingFS) Remove(name string) error {
	return r.inner.Remove(name)
}

// Chmod passes through unchanged.
func (r *recordingFS) Chmod(name string, mode os.FileMode) error {
	return r.inner.Chmod(name, mode)
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
