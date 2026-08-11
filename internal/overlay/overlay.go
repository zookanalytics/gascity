// Package overlay copies directory trees into agent working directories.
package overlay

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// PreserveExistingWarningPrefix prefixes nonfatal warnings for provider overlay
// files that intentionally preserve an existing destination file.
const PreserveExistingWarningPrefix = "overlay: preserving existing "

// IsPreserveExistingWarning reports whether line is a nonfatal preservation
// warning emitted by provider-aware overlay staging.
func IsPreserveExistingWarning(line string) bool {
	return strings.HasPrefix(strings.TrimSpace(line), PreserveExistingWarningPrefix)
}

// CopyFileOrDir copies src into dst. If src is a directory, it recursively
// copies all files into dst (like CopyDir). If src is a single file, it
// copies the file to dst, creating parent directories as needed. When dst
// already exists as a directory, the source basename is preserved under dst.
func CopyFileOrDir(src, dst string, stderr io.Writer) error {
	info, err := os.Stat(src)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("overlay: stat %q: %w", src, err)
	}
	if info.IsDir() {
		return CopyDir(src, dst, stderr)
	}
	if dstInfo, err := os.Stat(dst); err == nil && dstInfo.IsDir() {
		dst = filepath.Join(dst, filepath.Base(src))
	}
	return copyFile(src, dst)
}

// CopyDir recursively copies all files from srcDir into dstDir.
// Directory structure is preserved. File permissions are preserved.
// If srcDir does not exist, returns nil (no-op).
// Individual file copy failures are logged to stderr but don't abort.
func CopyDir(srcDir, dstDir string, stderr io.Writer) error {
	return copyDir(srcDir, dstDir, stderr, nil, copyConfig{})
}

type preserveExistingFunc func(relPath string) bool

// skipRuntimeMirror reports whether relPath is the runtime `.gc` mirror (the
// entry itself or anything beneath it) at the root of a copy operation, so it is
// never staged into an overlay destination. It is intentionally placed in the
// shared copyDirRecursive walk, so it applies to every copyDir caller —
// CopyDir, StageDir, stageDirStrict, CopyFileOrDir, and the provider-aware
// CopyDirForProvider(s) — not only the provider-specific staging paths. That is
// correct for today's callers, which are all overlay-to-workdir staging paths
// where a top-level `.gc/` mirror must never be copied; a future caller that
// legitimately needs to copy a tree containing a top-level `.gc/` would need a
// variant that does not carry this guard. Names merely prefixed with ".gc"
// (e.g. ".gcignore") are not matched.
func skipRuntimeMirror(relPath string) bool {
	clean := filepath.Clean(relPath)
	return clean == ".gc" || strings.HasPrefix(clean, ".gc"+string(filepath.Separator))
}

func copyDir(srcDir, dstDir string, stderr io.Writer, preserveExisting preserveExistingFunc, cfg copyConfig) error {
	info, err := os.Stat(srcDir)
	if os.IsNotExist(err) {
		return nil // Missing source dir is a no-op (like Gas Town).
	}
	if err != nil {
		return fmt.Errorf("overlay: stat %q: %w", srcDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("overlay: %q is not a directory", srcDir)
	}
	return copyDirRecursive(srcDir, dstDir, "", stderr, preserveExisting, cfg)
}

// copyDirRecursive walks srcBase/rel and copies files into dstBase/rel.
func copyDirRecursive(srcBase, dstBase, rel string, stderr io.Writer, preserveExisting preserveExistingFunc, cfg copyConfig) error {
	srcPath := srcBase
	if rel != "" {
		srcPath = filepath.Join(srcBase, rel)
	}

	entries, err := os.ReadDir(srcPath)
	if err != nil {
		return fmt.Errorf("overlay: reading %q: %w", srcPath, err)
	}

	for _, entry := range entries {
		entryRel := entry.Name()
		if rel != "" {
			entryRel = filepath.Join(rel, entry.Name())
		}

		if skipRuntimeMirror(entryRel) {
			continue
		}

		if entry.IsDir() {
			// Create destination subdirectory and recurse.
			dstSubDir := filepath.Join(dstBase, entryRel)
			if err := os.MkdirAll(dstSubDir, 0o755); err != nil {
				fmt.Fprintf(stderr, "overlay: mkdir %q: %v\n", dstSubDir, err) //nolint:errcheck
				continue
			}
			if err := copyDirRecursive(srcBase, dstBase, entryRel, stderr, preserveExisting, cfg); err != nil {
				fmt.Fprintf(stderr, "overlay: %v\n", err) //nolint:errcheck
			}
			continue
		}

		// Copy file (render if templated, merge if applicable). Preservation
		// keys on the staged destination, so a templated file is preserved
		// under the same rule as its non-templated twin.
		src := filepath.Join(srcBase, entryRel)
		stagedRel := stagedRelPath(entryRel)
		if preserveExisting != nil && preserveExisting(stagedRel) {
			dst := filepath.Join(dstBase, stagedRel)
			if _, err := os.Stat(dst); err == nil {
				fmt.Fprintf(stderr, "%s%q; skipped %q\n", PreserveExistingWarningPrefix, dst, src) //nolint:errcheck
				continue
			} else if !os.IsNotExist(err) {
				fmt.Fprintf(stderr, "overlay: stat %q: %v\n", dst, err) //nolint:errcheck
				continue
			}
		}
		if err := stageEntry(srcBase, dstBase, entryRel, cfg); err != nil {
			fmt.Fprintf(stderr, "overlay: %v\n", err) //nolint:errcheck
		}
	}
	return nil
}

// SkipFunc reports whether a file or directory should be skipped during copy.
// relPath is relative to the source root. isDir indicates whether it's a directory.
type SkipFunc func(relPath string, isDir bool) bool

// CopyDirWithSkip recursively copies srcDir into dstDir, skipping entries
// where skip returns true. If skip is nil, copies everything.
// Unlike CopyDir, this function does not silently ignore errors on individual
// files — it returns on the first error encountered.
func CopyDirWithSkip(srcDir, dstDir string, skip SkipFunc, _ io.Writer) error {
	return copyDirWithSkip(srcDir, dstDir, skip, copyConfig{})
}

func copyDirWithSkip(srcDir, dstDir string, skip SkipFunc, cfg copyConfig) error {
	info, err := os.Stat(srcDir)
	if os.IsNotExist(err) {
		return nil // Missing source dir is a no-op (consistent with CopyDir).
	}
	if err != nil {
		return fmt.Errorf("overlay: stat %q: %w", srcDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("overlay: %q is not a directory", srcDir)
	}
	return copyDirWithSkipRecursive(srcDir, dstDir, "", skip, cfg)
}

// copyDirWithSkipRecursive walks srcBase/rel and copies files into dstBase/rel,
// consulting skip for each entry.
func copyDirWithSkipRecursive(srcBase, dstBase, rel string, skip SkipFunc, cfg copyConfig) error {
	srcPath := srcBase
	if rel != "" {
		srcPath = filepath.Join(srcBase, rel)
	}

	entries, err := os.ReadDir(srcPath)
	if err != nil {
		return fmt.Errorf("overlay: reading %q: %w", srcPath, err)
	}

	for _, entry := range entries {
		entryRel := entry.Name()
		if rel != "" {
			entryRel = filepath.Join(rel, entry.Name())
		}

		if skip != nil && skip(entryRel, entry.IsDir()) {
			continue
		}

		if entry.IsDir() {
			dstSubDir := filepath.Join(dstBase, entryRel)
			if err := os.MkdirAll(dstSubDir, 0o755); err != nil {
				return fmt.Errorf("overlay: mkdir %q: %w", dstSubDir, err)
			}
			if err := copyDirWithSkipRecursive(srcBase, dstBase, entryRel, skip, cfg); err != nil {
				return err
			}
			continue
		}

		if err := stageEntry(srcBase, dstBase, entryRel, cfg); err != nil {
			return err
		}
	}
	return nil
}

// PerProviderDir is the conventional subdirectory name for provider-specific
// overlay files. Files in overlay/per-provider/<provider>/ are copied to the
// agent's working directory only when the agent's resolved provider matches.
const PerProviderDir = "per-provider"

// HasProviderDir reports whether srcDir contains a per-provider overlay
// directory for providerName (per-provider/<providerName>/). Staging uses it to
// decide whether a concrete provider overlay exists before falling back to the
// launch family overlay (gc-6bw8o).
func HasProviderDir(srcDir, providerName string) bool {
	if srcDir == "" || providerName == "" {
		return false
	}
	info, err := os.Stat(filepath.Join(srcDir, PerProviderDir, providerName))
	return err == nil && info.IsDir()
}

// CopyDirForProvider copies overlay files with provider awareness:
//  1. Copies everything EXCEPT the per-provider/ subtree (universal files).
//  2. If per-provider/<providerName>/ exists, copies its contents into dst
//     (flattened — the per-provider/<provider>/ prefix is stripped).
//
// Templated files (<name>.template.<ext>) are rendered with the data map from
// WithTemplateData; see CopyDirForProviders.
//
// This implements the V2 overlay layering described in doc-agent-v2.md.
func CopyDirForProvider(srcDir, dstDir, providerName string, stderr io.Writer, opts ...CopyOption) error {
	cfg := newCopyConfig(opts)
	info, err := os.Stat(srcDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("overlay: stat %q: %w", srcDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("overlay: %q is not a directory", srcDir)
	}

	// Step 1: copy universal files (skip per-provider/).
	skip := func(relPath string, _ bool) bool {
		if skipRuntimeMirror(relPath) {
			return true
		}
		// Skip the per-provider directory itself and all its contents.
		return relPath == PerProviderDir || filepath.Dir(relPath) == PerProviderDir ||
			len(relPath) > len(PerProviderDir)+1 && relPath[:len(PerProviderDir)+1] == PerProviderDir+string(filepath.Separator)
	}
	if err := copyDirWithSkip(srcDir, dstDir, skip, cfg); err != nil {
		return err
	}

	// Step 2: copy provider-specific files (flattened into dst).
	if providerName != "" {
		providerDir := filepath.Join(srcDir, PerProviderDir, providerName)
		if err := copyDir(providerDir, dstDir, stderr, providerPreserveExisting(providerName), cfg); err != nil {
			return err
		}
	}

	return nil
}

// CopyDirForProviders copies overlay files for multiple provider slots.
// Universal (non per-provider/) files are copied once, then per-provider/<p>/
// content is copied for each name in providers. Used when an agent has
// install_agent_hooks declaring additional provider hook slots beyond its
// resolved provider — e.g. an agent running Claude that wants the Gemini
// hook staged too.
//
// Duplicate provider names in the list are de-duped; empty strings are
// skipped. The order in providers determines which per-provider copy
// wins when two providers ship the same rel path (last-writer-wins via
// overwrite or JSON merge).
func CopyDirForProviders(srcDir, dstDir string, providers []string, stderr io.Writer, opts ...CopyOption) error {
	cfg := newCopyConfig(opts)
	info, err := os.Stat(srcDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("overlay: stat %q: %w", srcDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("overlay: %q is not a directory", srcDir)
	}

	// Step 1: copy universal files (skip per-provider/).
	skip := func(relPath string, _ bool) bool {
		if skipRuntimeMirror(relPath) {
			return true
		}
		return relPath == PerProviderDir || filepath.Dir(relPath) == PerProviderDir ||
			len(relPath) > len(PerProviderDir)+1 && relPath[:len(PerProviderDir)+1] == PerProviderDir+string(filepath.Separator)
	}
	if err := copyDirWithSkip(srcDir, dstDir, skip, cfg); err != nil {
		return err
	}

	// Step 2: copy per-provider slots in order, deduped.
	seen := make(map[string]bool, len(providers))
	for _, p := range providers {
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		providerDir := filepath.Join(srcDir, PerProviderDir, p)
		if err := copyDir(providerDir, dstDir, stderr, providerPreserveExisting(p), cfg); err != nil {
			return err
		}
	}
	return nil
}

func providerPreserveExisting(providerName string) preserveExistingFunc {
	if providerName != "kiro" {
		return nil
	}
	return func(relPath string) bool {
		// Kiro's AGENTS.md is a workspace-root instruction fallback. Once any
		// workspace, pack, or earlier overlay has provided it, later Kiro
		// overlays preserve that file instead of replacing instructions.
		return filepath.Clean(relPath) == "AGENTS.md"
	}
}

// stageEntry stages one overlay file from srcBase/relPath.
//
// A templated source (see TemplateTargetName) is rendered through
// text/template with cfg's data map and staged at its target name; every
// destination-keyed rule — JSON merge, hook wrapping — then follows that
// target name, so ".codex/hooks.template.json" merges exactly as
// ".codex/hooks.json" would. Any other file keeps the historical byte-copy /
// merge path untouched.
func stageEntry(srcBase, dstBase, relPath string, cfg copyConfig) error {
	src := filepath.Join(srcBase, relPath)
	stagedRel := stagedRelPath(relPath)
	dst := filepath.Join(dstBase, stagedRel)
	if stagedRel == relPath {
		return copyOrMergeFile(src, dst, IsMergeablePath(relPath), WrapsBareHooks(relPath))
	}
	data, mode, err := renderTemplateFile(src, cfg.templateData)
	if err != nil {
		return err
	}
	return stageFileData(data, mode, dst, IsMergeablePath(stagedRel), WrapsBareHooks(stagedRel))
}

// copyOrMergeFile copies src to dst, optionally merging JSON if merge is true
// and dst already exists. When wrapBareHooks is true (Claude settings), bare
// hook entries in the result are normalized into wrapped form, both when
// merging and when creating the file fresh. Falls back to plain copy on any
// merge error.
func copyOrMergeFile(src, dst string, merge, wrapBareHooks bool) error {
	if !merge {
		return copyFile(src, dst)
	}
	srcData, err := os.ReadFile(src)
	if err != nil {
		return copyFile(src, dst)
	}
	info, err := os.Stat(src)
	if err != nil {
		return copyFile(src, dst)
	}
	return stageFileData(srcData, info.Mode().Perm(), dst, true, wrapBareHooks)
}

// stageFileData writes srcData to dst. When merge is true and dst already
// holds a readable document, the two are JSON-merged (identity-keyed for hook
// entries) instead of overwritten; otherwise dst is created from srcData's
// canonicalized JSON. mode is the permission set used when dst is created
// fresh — an existing dst keeps its own permissions. Falls back to writing
// srcData verbatim on any merge error.
//
// This is the content-level core both staging paths share: copyOrMergeFile
// reads an overlay file into it, and templated files feed it rendered bytes
// that must never be replaced by a re-read of the unrendered source.
func stageFileData(srcData []byte, mode os.FileMode, dst string, merge, wrapBareHooks bool) error {
	if !merge {
		return writeStagedFile(dst, srcData, mode)
	}
	// Only merge if destination already exists and is readable.
	dstInfo, err := os.Stat(dst)
	if err != nil {
		return writeCanonicalSettingsData(srcData, mode, dst, wrapBareHooks)
	}
	dstData, err := os.ReadFile(dst)
	if err != nil {
		return writeCanonicalSettingsData(srcData, mode, dst, wrapBareHooks)
	}
	var opts []MergeOption
	if wrapBareHooks {
		opts = append(opts, WithWrapBareHooks())
	}
	merged, err := MergeSettingsJSON(dstData, srcData, opts...)
	if err != nil {
		// Merge failed — fall back to overwrite.
		return writeStagedFile(dst, srcData, mode)
	}
	// Preserve the destination file's permissions.
	return writeStagedFile(dst, merged, dstInfo.Mode().Perm())
}

// writeCanonicalSettingsData creates dst from srcData's canonicalized JSON.
// For wrap-style files (wrapBareHooks) it also normalizes bare hook entries
// into wrapped form by merging the source over an empty object. Falls back to
// srcData verbatim when it isn't a JSON object.
func writeCanonicalSettingsData(srcData []byte, mode os.FileMode, dst string, wrapBareHooks bool) error {
	if wrapBareHooks {
		if out, err := MergeSettingsJSON([]byte("{}"), srcData, WithWrapBareHooks()); err == nil {
			return writeStagedFile(dst, out, mode)
		}
	}
	canonical, err := CanonicalJSON(srcData)
	if err != nil {
		return writeStagedFile(dst, srcData, mode)
	}
	return writeStagedFile(dst, canonical, mode)
}

// writeStagedFile writes data to dst, creating parent directories. mode
// applies only when dst is created; an existing file keeps its permissions.
func writeStagedFile(dst string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("creating parent for %q: %w", dst, err)
	}
	if err := os.WriteFile(dst, data, mode); err != nil {
		return fmt.Errorf("writing %q: %w", dst, err)
	}
	return nil
}

// copyFile copies a single file preserving permissions.
func copyFile(src, dst string) error {
	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("creating parent for %q: %w", dst, err)
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("opening %q: %w", src, err)
	}
	defer srcFile.Close() //nolint:errcheck // read-only file

	info, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("stat %q: %w", src, err)
	}

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
	if err != nil {
		return fmt.Errorf("creating %q: %w", dst, err)
	}

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		closeErr := dstFile.Close()
		_ = closeErr
		return fmt.Errorf("copying %q → %q: %w", src, dst, err)
	}
	return dstFile.Close()
}
