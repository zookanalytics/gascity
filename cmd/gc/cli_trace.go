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
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// CLI invocation tracing. When enabled, the root command wraps Execute()
// and appends one JSON line per invocation to
// <city>/.gc/runtime/gc-invocations.jsonl. Enabled only via the
// GC_CLI_TRACE environment variable (1/true). Defaults to off.
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
// All fields are always present (no omitempty) so the schema is stable
// across invocation contexts; agent-runtime envs that are unset emit "".
type cliInvocationRecord struct {
	TS              string `json:"ts"`
	Cmd             string `json:"cmd"`
	ArgsTruncated   string `json:"args_truncated"`
	DurationMS      int64  `json:"duration_ms"`
	ExitCode        int    `json:"exit_code"`
	PID             int    `json:"pid"`
	PPID            int    `json:"ppid"`
	GCSessionID     string `json:"gc_session_id"`
	GCAlias         string `json:"gc_alias"`
	GCSessionOrigin string `json:"gc_session_origin"`
}

// cliTraceResolved captures the decision of whether to record and where.
// Resolved at most once per process; the disabled path stops at Enabled.
type cliTraceResolved struct {
	Enabled bool
	Path    string
}

// cliTraceResolveOnce caches the per-process resolution so repeated lookups
// (e.g. tests or future call sites) pay the discovery cost at most once.
// Exposed via resetCLITraceResolveCache for tests that need to vary env.
var (
	cliTraceResolveOnce  sync.Once
	cliTraceResolveValue cliTraceResolved
)

// resetCLITraceResolveCache clears the cached resolution. Tests call this
// between cases that vary GC_CLI_TRACE or the city layout.
func resetCLITraceResolveCache() {
	cliTraceResolveOnce = sync.Once{}
	cliTraceResolveValue = cliTraceResolved{}
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
// Default-off is constant-time: when GC_CLI_TRACE is unset or explicitly
// false, this returns immediately with no config or filesystem I/O. Only
// when GC_CLI_TRACE is explicitly true does it walk for the city path.
//
// The legacy city.toml `[instrumentation] cli_trace_enabled` flag is no
// longer consulted — toml-only enablement would require reading config in
// the default-off path, which violates the constant-time requirement.
func resolveCLITrace() cliTraceResolved {
	cliTraceResolveOnce.Do(func() {
		cliTraceResolveValue = resolveCLITraceUncached()
	})
	return cliTraceResolveValue
}

func resolveCLITraceUncached() cliTraceResolved {
	envOverride, envSet := parseCLITraceEnv()

	// Default off. No I/O when env is unset, invalid, or explicitly false.
	if !envSet || !envOverride {
		return cliTraceResolved{}
	}

	// Enabled via env. Only now do we resolve the city path for the
	// trace file location.
	cityPath, err := resolveCity()
	if err != nil || cityPath == "" {
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

// cliTraceSensitiveFlags is the case-insensitive deny-list of flags whose
// next positional value (or `=value` suffix) is replaced with
// `<redacted:N>` where N is the original value's byte length.
//
// The single-letter form `-p` is intentionally omitted: too many legitimate
// CLI patterns use short flags for non-sensitive options, and false
// positives would corrupt the trace. Operators who need exhaustive
// redaction should disable tracing for the affected invocation.
var cliTraceSensitiveFlags = map[string]struct{}{
	"--token":    {},
	"--password": {},
	"--secret":   {},
	"--api-key":  {},
	"--apikey":   {},
	"--auth":     {},
	"--bearer":   {},
}

// cliTraceSensitiveEnvSubstrings are substring matches (case-insensitive)
// against the KEY portion of KEY=VALUE positional args. Any match triggers
// redaction of the VALUE.
var cliTraceSensitiveEnvSubstrings = []string{
	"TOKEN",
	"PASSWORD",
	"SECRET",
	"KEY",
	"AUTH",
}

// truncateCLIArgs builds a space-joined args summary capped at maxTotal
// bytes. Three transformations apply, in order:
//
//  1. Deny-listed flag values are replaced with `<redacted:N>`. Both the
//     separated form (`--token <value>`) and inline form (`--token=<value>`)
//     are handled. The redaction is unconditional — short and long values
//     alike are stripped, since short tokens leak as easily as long ones.
//  2. KEY=VALUE positional args whose KEY contains a sensitive substring
//     (TOKEN, PASSWORD, SECRET, KEY, AUTH — case-insensitive) get the
//     VALUE redacted in the same `<redacted:N>` form.
//  3. Individual args longer than maxArg bytes are replaced with the
//     `<len:N>` sentinel so a single huge value doesn't blow the budget.
//
// After the per-arg transformation the joined string is truncated to
// maxTotal bytes (with `...` ellipsis when it overflows).
func truncateCLIArgs(args []string, maxTotal, maxArg int) string {
	if len(args) == 0 {
		return ""
	}
	parts := make([]string, 0, len(args))
	skipNext := false
	for i, arg := range args {
		if skipNext {
			parts = append(parts, redactedSentinel(len(arg)))
			skipNext = false
			continue
		}

		if redacted, ok := redactSensitiveFlag(arg); ok {
			parts = append(parts, redacted)
			// If the flag was the separated form (no `=` in arg), the
			// next positional is the value and must be redacted.
			if !strings.Contains(arg, "=") && i+1 < len(args) {
				skipNext = true
			}
			continue
		}

		if redacted, ok := redactSensitiveEnvKV(arg); ok {
			parts = append(parts, redacted)
			continue
		}

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

// redactSensitiveFlag handles `--flag`, `--flag=value`, and the
// case-insensitive deny-list. Returns the redacted form and true when the
// arg matched a sensitive flag.
func redactSensitiveFlag(arg string) (string, bool) {
	if !strings.HasPrefix(arg, "--") {
		return "", false
	}
	name := arg
	value := ""
	hasInlineValue := false
	if eq := strings.IndexByte(arg, '='); eq > 0 {
		name = arg[:eq]
		value = arg[eq+1:]
		hasInlineValue = true
	}
	if _, ok := cliTraceSensitiveFlags[strings.ToLower(name)]; !ok {
		return "", false
	}
	if hasInlineValue {
		return name + "=" + redactedSentinel(len(value)), true
	}
	// Separated form: pass the flag through; the next arg is consumed by
	// the caller (skipNext) and replaced with the sentinel.
	return name, true
}

// redactSensitiveEnvKV redacts the VALUE portion of KEY=VALUE positional
// args when KEY contains a sensitive substring. Returns the redacted form
// and true on match.
func redactSensitiveEnvKV(arg string) (string, bool) {
	if strings.HasPrefix(arg, "-") {
		return "", false
	}
	eq := strings.IndexByte(arg, '=')
	if eq <= 0 {
		return "", false
	}
	key := arg[:eq]
	value := arg[eq+1:]
	upperKey := strings.ToUpper(key)
	for _, needle := range cliTraceSensitiveEnvSubstrings {
		if strings.Contains(upperKey, needle) {
			return key + "=" + redactedSentinel(len(value)), true
		}
	}
	return "", false
}

func redactedSentinel(n int) string {
	return fmt.Sprintf("<redacted:%d>", n)
}

// appendCLIInvocation writes one JSON line to the active log, rotating
// first if the file has crossed cliTraceMaxSizeBytes. All errors are
// reported via warnCLITrace and otherwise swallowed. Rotation is
// asynchronous — only the rename happens on the host's critical path.
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
	maybeRotateCLITrace(path, cliTraceMaxSizeBytes, stderr)
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

// maybeRotateCLITrace performs synchronous size detection and rename, then
// kicks off the gzip+prune in a detached goroutine. The function returns
// as soon as the rename completes — the host command never blocks on
// compression. Errors are reported via warnCLITrace and never propagated.
//
// On process exit the goroutine may be interrupted mid-gzip. The renamed
// file remains on disk as `<path>.rotating.<pid>.<ts>` and is treated as
// orphan state the next rotation cycle leaves alone. This is acceptable —
// a missed archive is preferable to a slow `gc` invocation.
func maybeRotateCLITrace(path string, maxSize int64, stderr io.Writer) {
	fi, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			warnCLITrace(stderr, "rotate stat: %v", err)
		}
		return
	}
	if fi.Size() < maxSize {
		return
	}

	ts := time.Now().UTC().Format("20060102T150405.000000000Z")
	archivePath := fmt.Sprintf("%s.%s.gz", path, ts)
	tempPath := fmt.Sprintf("%s.rotating.%d.%s", path, os.Getpid(), ts)

	if err := os.Rename(path, tempPath); err != nil {
		warnCLITrace(stderr, "rotate rename: %v", err)
		return
	}

	cliTraceRotateInFlight.Add(1)
	go func() {
		defer cliTraceRotateInFlight.Done()
		defer func() {
			if r := recover(); r != nil {
				warnCLITrace(stderr, "rotate panic: %v", r)
			}
		}()
		finishCLITraceRotation(tempPath, archivePath, path, stderr)
	}()
}

// cliTraceRotateInFlight tracks background rotations so tests can wait
// for them to settle deterministically.
var cliTraceRotateInFlight sync.WaitGroup

// waitForCLITraceRotations blocks until all in-flight rotation goroutines
// complete. Intended for tests only.
func waitForCLITraceRotations() {
	cliTraceRotateInFlight.Wait()
}

// finishCLITraceRotation runs the slow half of rotation: gzip the renamed
// file, remove the temp, and prune older archives. Errors leave the temp
// file on disk so nothing is silently destroyed.
func finishCLITraceRotation(tempPath, archivePath, activePath string, stderr io.Writer) {
	if err := gzipCLITraceFile(tempPath, archivePath); err != nil {
		warnCLITrace(stderr, "rotate gzip: %v", err)
		return
	}
	if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
		warnCLITrace(stderr, "rotate remove temp: %v", err)
	}
	pruneCLITraceArchives(activePath, cliTraceMaxArchives, stderr)
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
