package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
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
	packCacheVersion = 2
)

// packCachePayload is the on-disk cache representation.
type packCachePayload struct {
	Version int
	BuildID string
	Extras  []string
	Sources []packCacheSource
	City    *City
	Agents  []packCacheAgentState
	Prov    packCacheProvenance
	// ZeroPtrs records the paths of pointer fields in City whose pointee
	// is the zero value for its type. gob flattens pointers and omits
	// zero-valued struct fields, so ptr-to-zero (min_active_sessions = 0,
	// inject_assigned_skills = false, ...) would otherwise round-trip as
	// nil — silently turning "explicitly zero/false" into "unset" on warm
	// loads. Collected by collectZeroPointers at save, re-materialized by
	// restoreZeroPointers at load. See zeroPtrSegment for path encoding.
	ZeroPtrs [][]string
}

// packCacheAgentState carries the unexported Agent fields that gob cannot
// serialize directly. Parallel to City.Agents by index; reapplied after decode.
type packCacheAgentState struct {
	Source uint8
	Layout uint8
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
	if len(p.Agents) == len(p.City.Agents) {
		for i := range p.City.Agents {
			p.City.Agents[i].source = agentSource(p.Agents[i].Source)
			p.City.Agents[i].layout = agentLayout(p.Agents[i].Layout)
		}
	}
	restoreZeroPointers(reflect.ValueOf(p.City), p.ZeroPtrs)

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
	agents := make([]packCacheAgentState, len(city.Agents))
	for i := range city.Agents {
		agents[i] = packCacheAgentState{
			Source: uint8(city.Agents[i].source),
			Layout: uint8(city.Agents[i].layout),
		}
	}
	payload := &packCachePayload{
		Version:  packCacheVersion,
		BuildID:  buildID,
		Extras:   append([]string(nil), extras...),
		Sources:  sources,
		City:     city,
		Agents:   agents,
		ZeroPtrs: collectZeroPointers(reflect.ValueOf(city)),
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

// Zero-pointer fidelity. gob's wire format omits zero-valued struct fields
// and flattens pointers, so a pointer field whose pointee is the zero value
// (*int → 0, *bool → false, *string → "") decodes as nil. nil-vs-zero is
// semantically load-bearing across the config tree (tri-state knobs like
// Agent.MinActiveSessions and Agent.InjectAssignedSkills distinguish
// "explicitly zero/false" from "unset/inherit"), so the cache records the
// paths of all such pointers at save time and re-materializes them after
// decode. The walk is generic over the City tree: new pointer fields are
// covered automatically with no per-field maintenance.
//
// Path encoding (zeroPtrSegment): each path is a []string of typed
// segments — "f:<FieldName>" (struct field), "i:<index>" (slice/array
// element), "k:<key>" (string-keyed map entry). Navigation auto-derefs
// non-nil pointers between segments on restore, mirroring the collect
// walk's descent through pointers.

const (
	zeroPtrFieldSeg = "f:"
	zeroPtrIndexSeg = "i:"
	zeroPtrMapSeg   = "k:"
)

// collectZeroPointers walks v and returns the paths of every reachable
// pointer whose pointee is the zero value of its type. Nil pointers are
// NOT recorded — they must stay nil after a cache round-trip. Unexported
// fields are skipped (gob does not serialize them; the packCacheAgentState
// sidecar carries the two that matter). Interface values are skipped (the
// config tree has none; defensive).
func collectZeroPointers(v reflect.Value) [][]string {
	var out [][]string
	var walk func(v reflect.Value, path []string)
	walk = func(v reflect.Value, path []string) {
		if !v.IsValid() {
			return
		}
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				return
			}
			if v.Elem().IsZero() {
				out = append(out, append([]string(nil), path...))
				return
			}
			walk(v.Elem(), path)
		case reflect.Struct:
			t := v.Type()
			for i := 0; i < t.NumField(); i++ {
				if t.Field(i).PkgPath != "" {
					continue // unexported: not in the gob payload either
				}
				walk(v.Field(i), append(path, zeroPtrFieldSeg+t.Field(i).Name))
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i), append(path, zeroPtrIndexSeg+strconv.Itoa(i)))
			}
		case reflect.Map:
			if v.Type().Key().Kind() != reflect.String {
				return // config maps are string-keyed; skip anything else
			}
			for _, k := range v.MapKeys() {
				walk(v.MapIndex(k), append(path, zeroPtrMapSeg+k.String()))
			}
		}
	}
	walk(v, nil)
	return out
}

// restoreZeroPointers re-materializes pointer-to-zero values at the
// recorded paths after a gob decode left them nil. Paths that no longer
// navigate (renamed field, shorter slice) are skipped silently: the cache
// is keyed by binary identity, so structural drift means a stale payload
// that the BuildID check rejects anyway.
func restoreZeroPointers(root reflect.Value, paths [][]string) {
	for _, path := range paths {
		applyZeroPointer(root, path)
	}
}

// applyZeroPointer navigates one collected path from root and sets the
// terminal nil pointer to a fresh pointer-to-zero. Returns true when the
// value it visited (or a descendant) was modified, which map handling uses
// to know when to write a mutated copy back via SetMapIndex.
func applyZeroPointer(v reflect.Value, path []string) bool {
	if !v.IsValid() {
		return false
	}
	if len(path) == 0 {
		if v.Kind() != reflect.Pointer || !v.IsNil() || !v.CanSet() {
			return false
		}
		v.Set(reflect.New(v.Type().Elem()))
		return true
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return false
		}
		return applyZeroPointer(v.Elem(), path)
	}
	seg := path[0]
	switch {
	case strings.HasPrefix(seg, zeroPtrFieldSeg):
		if v.Kind() != reflect.Struct {
			return false
		}
		fld := v.FieldByName(strings.TrimPrefix(seg, zeroPtrFieldSeg))
		if !fld.IsValid() {
			return false
		}
		return applyZeroPointer(fld, path[1:])
	case strings.HasPrefix(seg, zeroPtrIndexSeg):
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return false
		}
		idx, err := strconv.Atoi(strings.TrimPrefix(seg, zeroPtrIndexSeg))
		if err != nil || idx < 0 || idx >= v.Len() {
			return false
		}
		return applyZeroPointer(v.Index(idx), path[1:])
	case strings.HasPrefix(seg, zeroPtrMapSeg):
		if v.Kind() != reflect.Map || v.Type().Key().Kind() != reflect.String {
			return false
		}
		key := reflect.ValueOf(strings.TrimPrefix(seg, zeroPtrMapSeg)).Convert(v.Type().Key())
		val := v.MapIndex(key)
		if !val.IsValid() {
			return false
		}
		// Map values are not addressable: mutate a copy, store it back.
		cp := reflect.New(val.Type()).Elem()
		cp.Set(val)
		if !applyZeroPointer(cp, path[1:]) {
			return false
		}
		v.SetMapIndex(key, cp)
		return true
	}
	return false
}
