package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// CLI invocation tracing. When enabled, the root command wraps Execute()
// and appends one JSON line per invocation to
// <city>/.gc/runtime/gc-invocations.jsonl. Enabled either via city.toml
// ([instrumentation] cli_trace_enabled=true) or the GC_CLI_TRACE
// environment variable. Defaults to off.
//
// The hook is fail-safe: any error during instrumentation logs a single
// warn line to stderr and never affects the command's exit code.

const (
	// cliTraceFileBaseName is the active JSONL file under .gc/runtime/.
	cliTraceFileBaseName = "gc-invocations.jsonl"
	// cliTraceMaxArgsBytes caps the args_truncated string length.
	cliTraceMaxArgsBytes = 200
	// cliTraceMaxArgValueBytes is the per-arg cap; values above this are
	// replaced with the <len:N> sentinel.
	cliTraceMaxArgValueBytes = 64
	// cliTraceMaxSizeBytes triggers rotation of the active log.
	cliTraceMaxSizeBytes int64 = 100 * 1024 * 1024
	// cliTraceMaxArchives is the number of compressed archives retained.
	cliTraceMaxArchives = 10
)

// cliInvocationRecord is the schema written to gc-invocations.jsonl.
// JSON field names are stable contract — downstream analyzers depend on them.
type cliInvocationRecord struct {
	TS              string `json:"ts"`
	Cmd             string `json:"cmd"`
	ArgsTruncated   string `json:"args_truncated"`
	DurationMS      int64  `json:"duration_ms"`
	ExitCode        int    `json:"exit_code"`
	PID             int    `json:"pid"`
	PPID            int    `json:"ppid"`
	GCSessionID     string `json:"gc_session_id,omitempty"`
	GCAlias         string `json:"gc_alias,omitempty"`
	GCSessionOrigin string `json:"gc_session_origin,omitempty"`
}

// cliTraceResolved captures the decision of whether to record and where.
// Resolved at most once per invocation; the disabled path stops at Enabled.
type cliTraceResolved struct {
	Enabled bool
	Path    string
}

// recordCLIInvocation is the entry point invoked after root.Execute returns.
// It performs the enable check and, if enabled, builds and appends a record.
// All errors are swallowed (with a single stderr warn) — instrumentation
// must never affect the host command's exit code.
func recordCLIInvocation(root *cobra.Command, args []string, startTime time.Time, exitCode int, stderr io.Writer) {
	defer func() {
		if r := recover(); r != nil {
			warnCLITrace(stderr, "panic: %v", r)
		}
	}()

	resolved := resolveCLITrace()
	if !resolved.Enabled {
		return
	}

	rec := buildCLIInvocationRecord(root, args, startTime, exitCode)
	appendCLIInvocation(resolved.Path, rec, stderr)
}

// resolveCLITrace decides whether tracing is on and where to write.
// Order: GC_CLI_TRACE env override (1/0/true/false) → city.toml
// [instrumentation] cli_trace_enabled → default false. Returns Enabled=false
// when no writable city path can be resolved.
func resolveCLITrace() cliTraceResolved {
	envOverride, envSet := parseCLITraceEnv()

	if envSet && !envOverride {
		return cliTraceResolved{}
	}

	cityPath, err := resolveCity()
	if err != nil || cityPath == "" {
		return cliTraceResolved{}
	}

	cfgEnabled := false
	if cfg, err := loadCityConfig(cityPath, io.Discard); err == nil {
		cfgEnabled = cfg.Instrumentation.CLITraceEnabled
	}

	enabled := cfgEnabled
	if envSet {
		enabled = envOverride
	}
	if !enabled {
		return cliTraceResolved{}
	}

	return cliTraceResolved{
		Enabled: true,
		Path:    filepath.Join(cityPath, ".gc", "runtime", cliTraceFileBaseName),
	}
}

// parseCLITraceEnv reads GC_CLI_TRACE. Returns (value, set). Invalid values
// are silently ignored (set=false) so a typo can't enable instrumentation.
func parseCLITraceEnv() (bool, bool) {
	raw, ok := os.LookupEnv("GC_CLI_TRACE")
	if !ok {
		return false, false
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "t", "true", "y", "yes", "on", "enabled":
		return true, true
	case "0", "f", "false", "n", "no", "off", "disabled":
		return false, true
	default:
		return false, false
	}
}

// buildCLIInvocationRecord constructs the JSONL payload. The timestamp is
// the start of the invocation (not now) so callers can reconstruct the
// wall-clock ordering of overlapping runs.
func buildCLIInvocationRecord(root *cobra.Command, args []string, startTime time.Time, exitCode int) cliInvocationRecord {
	return cliInvocationRecord{
		TS:              startTime.UTC().Format(time.RFC3339Nano),
		Cmd:             resolveCmdChain(root, args),
		ArgsTruncated:   truncateCLIArgs(args, cliTraceMaxArgsBytes, cliTraceMaxArgValueBytes),
		DurationMS:      time.Since(startTime).Milliseconds(),
		ExitCode:        exitCode,
		PID:             os.Getpid(),
		PPID:            os.Getppid(),
		GCSessionID:     strings.TrimSpace(os.Getenv("GC_SESSION_ID")),
		GCAlias:         strings.TrimSpace(os.Getenv("GC_ALIAS")),
		GCSessionOrigin: strings.TrimSpace(os.Getenv("GC_SESSION_ORIGIN")),
	}
}

// resolveCmdChain returns the full subcommand chain (e.g. "gc bd update")
// resolved through cobra's Find. Falls back to "gc" when args fail to
// resolve to any registered command.
func resolveCmdChain(root *cobra.Command, args []string) string {
	if root == nil {
		return "gc"
	}
	cmd, _, err := root.Find(args)
	if err != nil || cmd == nil {
		return "gc"
	}
	return cmd.CommandPath()
}

// truncateCLIArgs builds a space-joined args summary capped at maxTotal
// bytes. Individual args longer than maxArg bytes are replaced with a
// <len:N> sentinel so a single huge value (e.g., --description with a long
// markdown body) doesn't blow the budget on its own.
func truncateCLIArgs(args []string, maxTotal, maxArg int) string {
	if len(args) == 0 {
		return ""
	}
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		if len(arg) > maxArg {
			parts = append(parts, fmt.Sprintf("<len:%d>", len(arg)))
			continue
		}
		parts = append(parts, arg)
	}
	joined := strings.Join(parts, " ")
	if len(joined) <= maxTotal {
		return joined
	}
	if maxTotal <= 3 {
		return joined[:maxTotal]
	}
	return joined[:maxTotal-3] + "..."
}

// appendCLIInvocation writes one JSON line to the active log, rotating
// first if the file has crossed cliTraceMaxSizeBytes. All errors are
// reported via warnCLITrace and otherwise swallowed.
func appendCLIInvocation(path string, rec cliInvocationRecord, stderr io.Writer) {
	data, err := json.Marshal(rec)
	if err != nil {
		warnCLITrace(stderr, "marshal: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		warnCLITrace(stderr, "mkdir: %v", err)
		return
	}
	if err := maybeRotateCLITrace(path, cliTraceMaxSizeBytes, stderr); err != nil {
		warnCLITrace(stderr, "rotate: %v", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		warnCLITrace(stderr, "open: %v", err)
		return
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		warnCLITrace(stderr, "write: %v", err)
	}
	if err := f.Close(); err != nil {
		warnCLITrace(stderr, "close: %v", err)
	}
}

// maybeRotateCLITrace rotates path when its current size meets or exceeds
// maxSize. A missing path is treated as "no rotation needed." Always uses
// cliTraceMaxArchives for retention so callers don't need to thread it.
func maybeRotateCLITrace(path string, maxSize int64, stderr io.Writer) error {
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if fi.Size() < maxSize {
		return nil
	}
	return rotateCLITrace(path, cliTraceMaxArchives, stderr)
}

// rotateCLITrace renames the active log to a unique temp name, gzips it
// to a timestamped archive, then prunes archives beyond maxArchives.
// On gzip failure the temp file is restored to the active path so the
// next write does not start with an empty log.
func rotateCLITrace(path string, maxArchives int, stderr io.Writer) error {
	ts := time.Now().UTC().Format("20060102T150405.000000000Z")
	archivePath := fmt.Sprintf("%s.%s.gz", path, ts)
	tempPath := fmt.Sprintf("%s.rotating.%d.%s", path, os.Getpid(), ts)

	if err := os.Rename(path, tempPath); err != nil {
		return fmt.Errorf("rename to temp: %w", err)
	}
	if err := gzipCLITraceFile(tempPath, archivePath); err != nil {
		if restoreErr := os.Rename(tempPath, path); restoreErr != nil {
			warnCLITrace(stderr, "rotate restore failed: %v (original gzip err: %v)", restoreErr, err)
		}
		return fmt.Errorf("gzip: %w", err)
	}
	if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
		warnCLITrace(stderr, "remove temp: %v", err)
	}
	pruneCLITraceArchives(path, maxArchives, stderr)
	return nil
}

// gzipCLITraceFile compresses src into dst. The destination is opened with
// O_EXCL so two racing rotations can't clobber each other.
func gzipCLITraceFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(out)
	if _, err := io.Copy(gz, in); err != nil {
		_ = gz.Close()
		_ = out.Close()
		return err
	}
	if err := gz.Close(); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// pruneCLITraceArchives removes oldest archives until at most maxArchives
// remain. Ordering uses the archive's mtime; ties fall back to filename.
func pruneCLITraceArchives(activePath string, maxArchives int, stderr io.Writer) {
	if maxArchives <= 0 {
		return
	}
	dir := filepath.Dir(activePath)
	base := filepath.Base(activePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		warnCLITrace(stderr, "prune list: %v", err)
		return
	}
	prefix := base + "."
	type archive struct {
		path  string
		mtime time.Time
	}
	var archives []archive
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".gz") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		archives = append(archives, archive{
			path:  filepath.Join(dir, name),
			mtime: info.ModTime(),
		})
	}
	if len(archives) <= maxArchives {
		return
	}
	sort.Slice(archives, func(i, j int) bool {
		if archives[i].mtime.Equal(archives[j].mtime) {
			return archives[i].path < archives[j].path
		}
		return archives[i].mtime.Before(archives[j].mtime)
	})
	for _, a := range archives[:len(archives)-maxArchives] {
		if err := os.Remove(a.path); err != nil {
			warnCLITrace(stderr, "prune remove %s: %v", a.path, err)
		}
	}
}

// warnCLITrace emits a single best-effort warning to stderr.
// Stderr writes that fail are intentionally ignored — instrumentation
// must never break the host command.
func warnCLITrace(stderr io.Writer, format string, args ...any) {
	if stderr == nil {
		return
	}
	fmt.Fprintf(stderr, "gc: cli_trace: "+format+"\n", args...) //nolint:errcheck // best-effort warning
}
