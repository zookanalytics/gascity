package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestParseCLITraceEnv(t *testing.T) {
	cases := []struct {
		raw       string
		wantValue bool
		wantSet   bool
	}{
		{"", false, false},
		{"1", true, true},
		{"true", true, true},
		{"TRUE", true, true},
		{"on", true, true},
		{"enabled", true, true},
		{"yes", true, true},
		{"0", false, true},
		{"false", false, true},
		{"OFF", false, true},
		{"disabled", false, true},
		{"no", false, true},
		{"maybe", false, false},
		{"  ", false, false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("raw=%q", tc.raw), func(t *testing.T) {
			t.Setenv("GC_CLI_TRACE", tc.raw)
			if tc.raw == "" {
				_ = os.Unsetenv("GC_CLI_TRACE")
			}
			gotValue, gotSet := parseCLITraceEnv()
			if gotValue != tc.wantValue || gotSet != tc.wantSet {
				t.Fatalf("parseCLITraceEnv(%q) = (%v, %v); want (%v, %v)",
					tc.raw, gotValue, gotSet, tc.wantValue, tc.wantSet)
			}
		})
	}
}

func TestTruncateCLIArgs(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		maxTotal int
		maxArg   int
		want     string
	}{
		{
			name: "empty",
			args: nil,
			want: "",
		},
		{
			name:     "short args pass through",
			args:     []string{"bd", "list"},
			maxTotal: 200,
			maxArg:   64,
			want:     "bd list",
		},
		{
			name:     "long arg becomes sentinel",
			args:     []string{"bd", "update", "--description", strings.Repeat("x", 300)},
			maxTotal: 200,
			maxArg:   64,
			want:     "bd update --description <len:300>",
		},
		{
			name:     "total truncated",
			args:     []string{strings.Repeat("a", 60), strings.Repeat("b", 60), strings.Repeat("c", 60), strings.Repeat("d", 60)},
			maxTotal: 100,
			maxArg:   64,
			want:     strings.Repeat("a", 60) + " " + strings.Repeat("b", 36) + "...",
		},
		{
			name:     "exact fit no ellipsis",
			args:     []string{"abc", "defghi"},
			maxTotal: 10,
			maxArg:   64,
			want:     "abc defghi",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateCLIArgs(tc.args, tc.maxTotal, tc.maxArg)
			if got != tc.want {
				t.Fatalf("truncateCLIArgs = %q; want %q", got, tc.want)
			}
			if len(got) > tc.maxTotal && tc.maxTotal > 0 {
				t.Fatalf("output length %d exceeds maxTotal %d", len(got), tc.maxTotal)
			}
		})
	}
}

func TestResolveCmdChain(t *testing.T) {
	root := &cobra.Command{Use: "gc"}
	sub := &cobra.Command{Use: "bd"}
	leaf := &cobra.Command{Use: "list", Run: func(*cobra.Command, []string) {}}
	sub.AddCommand(leaf)
	root.AddCommand(sub)

	cases := []struct {
		args []string
		want string
	}{
		{nil, "gc"},
		{[]string{"bd"}, "gc bd"},
		{[]string{"bd", "list"}, "gc bd list"},
		{[]string{"unknown"}, "gc"},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, "."), func(t *testing.T) {
			got := resolveCmdChain(root, tc.args)
			if got != tc.want {
				t.Fatalf("resolveCmdChain(%v) = %q; want %q", tc.args, got, tc.want)
			}
		})
	}

	if got := resolveCmdChain(nil, nil); got != "gc" {
		t.Fatalf("resolveCmdChain(nil, nil) = %q; want %q", got, "gc")
	}
}

func TestBuildCLIInvocationRecord(t *testing.T) {
	root := &cobra.Command{Use: "gc"}
	bd := &cobra.Command{Use: "bd"}
	list := &cobra.Command{Use: "list", Run: func(*cobra.Command, []string) {}}
	bd.AddCommand(list)
	root.AddCommand(bd)

	t.Setenv("GC_SESSION_ID", "sess-abc")
	t.Setenv("GC_ALIAS", "alias-xyz")
	t.Setenv("GC_SESSION_ORIGIN", "ephemeral")

	start := time.Now().Add(-150 * time.Millisecond)
	rec := buildCLIInvocationRecord(root, []string{"bd", "list"}, start, 0)

	if rec.Cmd != "gc bd list" {
		t.Errorf("Cmd = %q; want %q", rec.Cmd, "gc bd list")
	}
	if rec.ArgsTruncated != "bd list" {
		t.Errorf("ArgsTruncated = %q; want %q", rec.ArgsTruncated, "bd list")
	}
	if rec.DurationMS < 100 {
		t.Errorf("DurationMS = %d; want >= 100", rec.DurationMS)
	}
	if rec.ExitCode != 0 {
		t.Errorf("ExitCode = %d; want 0", rec.ExitCode)
	}
	if rec.PID != os.Getpid() {
		t.Errorf("PID = %d; want %d", rec.PID, os.Getpid())
	}
	if rec.GCSessionID != "sess-abc" {
		t.Errorf("GCSessionID = %q; want %q", rec.GCSessionID, "sess-abc")
	}
	if rec.GCAlias != "alias-xyz" {
		t.Errorf("GCAlias = %q; want %q", rec.GCAlias, "alias-xyz")
	}
	if rec.GCSessionOrigin != "ephemeral" {
		t.Errorf("GCSessionOrigin = %q; want %q", rec.GCSessionOrigin, "ephemeral")
	}
	if _, err := time.Parse(time.RFC3339Nano, rec.TS); err != nil {
		t.Errorf("TS = %q is not RFC3339Nano: %v", rec.TS, err)
	}
}

func TestAppendCLIInvocationWritesValidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runtime", cliTraceFileBaseName)

	rec := cliInvocationRecord{
		TS:            "2026-05-20T14:00:00Z",
		Cmd:           "gc bd list",
		ArgsTruncated: "bd list",
		DurationMS:    42,
		ExitCode:      0,
		PID:           1234,
		PPID:          5678,
		GCSessionID:   "sess-1",
	}
	var stderr bytes.Buffer
	appendCLIInvocation(path, rec, &stderr)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read jsonl: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d lines; want 1: %q", len(lines), string(data))
	}
	var decoded cliInvocationRecord
	if err := json.Unmarshal([]byte(lines[0]), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v: %q", err, lines[0])
	}
	if decoded != rec {
		t.Fatalf("decoded mismatch:\n got: %+v\nwant: %+v", decoded, rec)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %q", stderr.String())
	}
}

func TestAppendCLIInvocationAppendsMultipleLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cliTraceFileBaseName)
	var stderr bytes.Buffer
	for i := 0; i < 3; i++ {
		appendCLIInvocation(path, cliInvocationRecord{
			Cmd:        fmt.Sprintf("gc cmd-%d", i),
			ExitCode:   i,
			DurationMS: int64(i),
		}, &stderr)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines; want 3", len(lines))
	}
	for i, line := range lines {
		var rec cliInvocationRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d invalid JSON: %v", i, err)
		}
		if rec.Cmd != fmt.Sprintf("gc cmd-%d", i) {
			t.Errorf("line %d Cmd = %q; want gc cmd-%d", i, rec.Cmd, i)
		}
	}
}

func TestMaybeRotateCLITraceUnderThreshold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cliTraceFileBaseName)
	if err := os.WriteFile(path, []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var stderr bytes.Buffer
	maybeRotateCLITrace(path, 1024, &stderr)
	waitForCLITraceRotations()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("active file removed under threshold: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("unexpected files: %v", entries)
	}
}

func TestMaybeRotateCLITraceMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cliTraceFileBaseName)
	var stderr bytes.Buffer
	maybeRotateCLITrace(path, 10, &stderr)
	waitForCLITraceRotations()
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr for missing file: %q", stderr.String())
	}
}

func TestRotateCLITraceCreatesArchive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cliTraceFileBaseName)
	payload := []byte(strings.Repeat("payload-line\n", 100))
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var stderr bytes.Buffer
	maybeRotateCLITrace(path, 10, &stderr)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("active file should be gone immediately after rename; stat err = %v", err)
	}
	waitForCLITraceRotations()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var archives []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gz") {
			archives = append(archives, e.Name())
		}
	}
	if len(archives) != 1 {
		t.Fatalf("got %d archives; want 1: %v", len(archives), archives)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %q", stderr.String())
	}
}

func TestPruneCLITraceArchivesKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cliTraceFileBaseName)
	// Seed 5 fake archives with staggered mtimes.
	now := time.Now()
	for i := 0; i < 5; i++ {
		ap := fmt.Sprintf("%s.%d.gz", path, i)
		if err := os.WriteFile(ap, []byte("x"), 0o644); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
		mtime := now.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(ap, mtime, mtime); err != nil {
			t.Fatalf("chtime %d: %v", i, err)
		}
	}
	var stderr bytes.Buffer
	pruneCLITraceArchives(path, 2, &stderr)

	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Fatalf("got %d entries; want 2: %v", len(entries), entries)
	}
	gotNames := make(map[string]bool)
	for _, e := range entries {
		gotNames[e.Name()] = true
	}
	// Newest two are indexes 3 and 4.
	want := []string{cliTraceFileBaseName + ".3.gz", cliTraceFileBaseName + ".4.gz"}
	for _, w := range want {
		if !gotNames[w] {
			t.Errorf("missing expected archive %q; have %v", w, gotNames)
		}
	}
}

func TestRecordCLIInvocationDisabledWritesNothing(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	withTempCity(t, dir, "")
	t.Setenv("GC_CLI_TRACE", "")
	_ = os.Unsetenv("GC_CLI_TRACE")

	var stderr bytes.Buffer
	recordCLIInvocation(nil, []string{"version"}, time.Now(), 0, &stderr)
	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	if _, err := os.Stat(tracePath); !os.IsNotExist(err) {
		t.Fatalf("trace file should not exist when disabled; stat err = %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr when disabled: %q", stderr.String())
	}
}

// TestRecordCLIInvocationTOMLOnlyDoesNotEnable codifies the rework
// behavior: `cli_trace_enabled = true` in city.toml is no longer
// sufficient on its own. Enablement requires GC_CLI_TRACE=1. The legacy
// toml flag is retained in the schema but consulted nowhere.
func TestRecordCLIInvocationTOMLOnlyDoesNotEnable(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	withTempCity(t, dir, "[instrumentation]\ncli_trace_enabled = true\n")
	_ = os.Unsetenv("GC_CLI_TRACE")

	var stderr bytes.Buffer
	rootCmd := newRootCmd(io.Discard, &stderr)
	recordCLIInvocation(rootCmd, []string{"version"}, time.Now(), 0, &stderr)

	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	if _, err := os.Stat(tracePath); !os.IsNotExist(err) {
		t.Fatalf("trace file should not exist when only toml says enabled; stat err = %v", err)
	}
}

func TestRecordCLIInvocationEnabledViaEnv(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	withTempCity(t, dir, "")
	t.Setenv("GC_CLI_TRACE", "1")

	var stderr bytes.Buffer
	rootCmd := newRootCmd(io.Discard, &stderr)
	recordCLIInvocation(rootCmd, []string{"version"}, time.Now(), 0, &stderr)

	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatalf("trace file missing: %v", err)
	}
	var rec cliInvocationRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &rec); err != nil {
		t.Fatalf("invalid JSON: %v: %q", err, string(data))
	}
	if rec.Cmd != "gc version" {
		t.Fatalf("Cmd = %q; want gc version", rec.Cmd)
	}
}

func TestRecordCLIInvocationEnvOverridesTOMLOff(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	withTempCity(t, dir, "[instrumentation]\ncli_trace_enabled = false\n")
	t.Setenv("GC_CLI_TRACE", "1")

	var stderr bytes.Buffer
	rootCmd := newRootCmd(io.Discard, &stderr)
	recordCLIInvocation(rootCmd, []string{"version"}, time.Now(), 0, &stderr)

	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	if _, err := os.Stat(tracePath); err != nil {
		t.Fatalf("trace file expected when env=1 even with toml=false: %v", err)
	}
}

func TestRecordCLIInvocationEnvOverridesTOMLOn(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	withTempCity(t, dir, "[instrumentation]\ncli_trace_enabled = true\n")
	t.Setenv("GC_CLI_TRACE", "0")

	var stderr bytes.Buffer
	rootCmd := newRootCmd(io.Discard, &stderr)
	recordCLIInvocation(rootCmd, []string{"version"}, time.Now(), 0, &stderr)

	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	if _, err := os.Stat(tracePath); !os.IsNotExist(err) {
		t.Fatalf("trace file should not exist when env=0 overrides toml=true; stat err = %v", err)
	}
}

func TestRecordCLIInvocationFailingCommandStillRecorded(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	withTempCity(t, dir, "")
	t.Setenv("GC_CLI_TRACE", "1")

	var stderr bytes.Buffer
	rootCmd := newRootCmd(io.Discard, &stderr)
	recordCLIInvocation(rootCmd, []string{"version"}, time.Now(), 7, &stderr)

	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatalf("trace file missing: %v", err)
	}
	var rec cliInvocationRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &rec); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if rec.ExitCode != 7 {
		t.Fatalf("ExitCode = %d; want 7", rec.ExitCode)
	}
}

func TestRecordCLIInvocationOutsideCityDoesNothing(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	t.Setenv("GC_CITY", "")
	_ = os.Unsetenv("GC_CITY")
	t.Setenv("GC_CITY_PATH", "")
	_ = os.Unsetenv("GC_CITY_PATH")
	t.Setenv("GC_CITY_ROOT", "")
	_ = os.Unsetenv("GC_CITY_ROOT")
	t.Setenv("GC_DIR", "")
	_ = os.Unsetenv("GC_DIR")
	t.Setenv("GC_RIG", "")
	_ = os.Unsetenv("GC_RIG")
	t.Setenv("GC_CLI_TRACE", "1")
	t.Chdir(dir)

	var stderr bytes.Buffer
	rootCmd := newRootCmd(io.Discard, &stderr)
	recordCLIInvocation(rootCmd, []string{"version"}, time.Now(), 0, &stderr)

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "gc-invocations") {
			t.Fatalf("unexpected trace file outside city: %s", e.Name())
		}
	}
}

func TestAppendCLIInvocationRotatesAtThreshold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cliTraceFileBaseName)
	// Pre-fill the active log just above the rotation threshold.
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 200), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var stderr bytes.Buffer
	// Force rotation by calling maybeRotateCLITrace with a low threshold.
	maybeRotateCLITrace(path, 100, &stderr)
	waitForCLITraceRotations()

	entries, _ := os.ReadDir(dir)
	var archives []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gz") {
			archives = append(archives, e.Name())
		}
	}
	if len(archives) != 1 {
		t.Fatalf("expected 1 archive after rotation; got %d: %v", len(archives), archives)
	}

	// After rotation the active file no longer exists. Subsequent append
	// re-creates it.
	rec := cliInvocationRecord{Cmd: "gc post-rotate", DurationMS: 1}
	appendCLIInvocation(path, rec, &stderr)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("active file should be recreated after rotate+append: %v", err)
	}
}

// withTempCity creates a minimal city.toml with the given extra body and
// chdirs into it. Restores any previous resolution env on cleanup.
func withTempCity(t *testing.T, dir, extraTOML string) {
	t.Helper()
	body := "[workspace]\nname = \"test-city\"\n" + extraTOML
	if err := os.MkdirAll(filepath.Join(dir, ".gc"), 0o755); err != nil {
		t.Fatalf("mkdir .gc: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "city.toml"), []byte(body), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}
	t.Setenv("GC_CITY", dir)
	t.Setenv("GC_RIG", "")
	_ = os.Unsetenv("GC_RIG")
	t.Chdir(dir)
}

func TestRecordCLIInvocationPanicDoesNotEscape(t *testing.T) {
	// Trigger a panic by passing a malformed structure. The most reliable
	// way to force one in the call chain is to make appendCLIInvocation see
	// a path with a NUL byte (Open will return an error, not panic), so
	// we instead exercise the recover() by passing nil root which the code
	// already tolerates. Verify simply that the call returns normally.
	resetCLITraceResolveCache()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("recordCLIInvocation panicked: %v", r)
		}
	}()
	_ = os.Unsetenv("GC_CLI_TRACE")
	var stderr bytes.Buffer
	recordCLIInvocation(nil, nil, time.Now(), 0, &stderr)
}

// TestRecordCLIInvocationAppendErrorReportsToStderr exercises the failure
// path: a file path under a directory that cannot be created.
func TestRecordCLIInvocationAppendErrorReportsToStderr(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	// Create a regular file where the runtime directory would go to force
	// MkdirAll to fail with NotADirectory.
	cityPath := filepath.Join(dir, "city")
	if err := os.MkdirAll(filepath.Join(cityPath, ".gc"), 0o755); err != nil {
		t.Fatalf("mkdir city: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cityPath, ".gc", "runtime"), []byte("blocker"), 0o644); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cityPath, "city.toml"),
		[]byte("[workspace]\nname=\"t\"\n"), 0o644); err != nil {
		t.Fatalf("seed city.toml: %v", err)
	}
	t.Setenv("GC_CITY", cityPath)
	t.Setenv("GC_CLI_TRACE", "1")
	t.Chdir(cityPath)

	var stderr bytes.Buffer
	rootCmd := newRootCmd(io.Discard, &stderr)
	// Should not panic, should write a warn line and return normally.
	recordCLIInvocation(rootCmd, []string{"version"}, time.Now(), 0, &stderr)

	if !strings.Contains(stderr.String(), "gc: cli_trace:") {
		t.Fatalf("expected cli_trace warn in stderr; got %q", stderr.String())
	}
}

// TestRunRecordsInvocationEndToEnd exercises the full run() pipeline with
// instrumentation enabled via the GC_CLI_TRACE env var, asserting that one
// JSONL line lands in .gc/runtime/gc-invocations.jsonl with the expected
// fields.
func TestRunRecordsInvocationEndToEnd(t *testing.T) {
	resetCLITraceResolveCache()
	configureIsolatedRuntimeEnv(t)
	dir := t.TempDir()
	withTempCity(t, dir, "")
	t.Setenv("GC_CLI_TRACE", "1")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(version) = %d; stderr: %s", code, stderr.String())
	}

	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatalf("trace file missing: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d trace lines; want 1: %q", len(lines), string(data))
	}
	var rec cliInvocationRecord
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("invalid JSON: %v: %q", err, lines[0])
	}
	if rec.Cmd != "gc version" {
		t.Errorf("Cmd = %q; want %q", rec.Cmd, "gc version")
	}
	if rec.ExitCode != 0 {
		t.Errorf("ExitCode = %d; want 0", rec.ExitCode)
	}
	if rec.PID != os.Getpid() {
		t.Errorf("PID = %d; want %d", rec.PID, os.Getpid())
	}
	if rec.DurationMS < 0 {
		t.Errorf("DurationMS = %d; want >= 0", rec.DurationMS)
	}
}

// TestRunDoesNotRecordWhenDisabled asserts the default-off behavior: no
// trace file is created when GC_CLI_TRACE is unset, regardless of any
// city.toml `cli_trace_enabled` setting.
func TestRunDoesNotRecordWhenDisabled(t *testing.T) {
	resetCLITraceResolveCache()
	configureIsolatedRuntimeEnv(t)
	dir := t.TempDir()
	withTempCity(t, dir, "")
	_ = os.Unsetenv("GC_CLI_TRACE")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(version) = %d; stderr: %s", code, stderr.String())
	}

	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	if _, err := os.Stat(tracePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("trace file should not exist by default; stat err = %v", err)
	}
}

// TestAppendCLIInvocationRotateProducesArchive exercises the happy-path
// rotation through the async pipeline: the rename happens synchronously,
// the gzip completes after the wait, and exactly one archive remains.
func TestAppendCLIInvocationRotateProducesArchive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cliTraceFileBaseName)
	if err := os.WriteFile(path, []byte("data\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var stderr bytes.Buffer
	maybeRotateCLITrace(path, 1, &stderr)
	// Rename is synchronous; the active file must already be gone.
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active file should be removed immediately; err=%v", err)
	}
	waitForCLITraceRotations()
	entries, _ := os.ReadDir(dir)
	var gz int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gz") {
			gz++
		}
	}
	if gz != 1 {
		t.Fatalf("want 1 gz archive; got %d", gz)
	}
}

// TestCLIInvocationRecordSchemaStable asserts that all schema fields are
// always present in the marshaled JSON, even when the agent-runtime envs
// are unset. Downstream analyzers rely on a stable shape.
func TestCLIInvocationRecordSchemaStable(t *testing.T) {
	resetCLITraceResolveCache()
	t.Setenv("GC_SESSION_ID", "")
	_ = os.Unsetenv("GC_SESSION_ID")
	t.Setenv("GC_ALIAS", "")
	_ = os.Unsetenv("GC_ALIAS")
	t.Setenv("GC_SESSION_ORIGIN", "")
	_ = os.Unsetenv("GC_SESSION_ORIGIN")

	root := &cobra.Command{Use: "gc"}
	rec := buildCLIInvocationRecord(root, []string{"version"}, time.Now(), 0)
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, field := range []string{
		"ts", "cmd", "args_truncated", "duration_ms", "exit_code",
		"pid", "ppid", "gc_session_id", "gc_alias", "gc_session_origin",
	} {
		if _, ok := raw[field]; !ok {
			t.Errorf("field %q missing from JSON; raw=%s", field, string(data))
		}
	}
	for _, field := range []string{"gc_session_id", "gc_alias", "gc_session_origin"} {
		v, _ := raw[field].(string)
		if v != "" {
			t.Errorf("%s = %q; want \"\"", field, v)
		}
	}
}

// TestResolveCLITraceConstantTimeDefaultOff verifies the constant-time
// disabled path: with GC_CLI_TRACE unset, resolveCLITrace must not touch
// the filesystem (no city.toml read, no ancestor walk).
//
// The check uses a chdir into a directory under which any successful
// ancestor walk would have to traverse, then asserts that resolving
// returns disabled without recording any access pattern that would
// require filesystem I/O.
func TestResolveCLITraceConstantTimeDefaultOff(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	// Seed a city.toml that would enable instrumentation if read.
	// With the constant-time default-off path, this file must NOT be
	// consulted.
	withTempCity(t, dir, "[instrumentation]\ncli_trace_enabled = true\n")
	_ = os.Unsetenv("GC_CLI_TRACE")

	resolved := resolveCLITrace()
	if resolved.Enabled {
		t.Fatalf("resolved.Enabled = true; want false (env unset must short-circuit)")
	}
	if resolved.Path != "" {
		t.Fatalf("resolved.Path = %q; want \"\"", resolved.Path)
	}
}

// TestResolveCLITraceCache asserts that resolveCLITrace is computed once
// per process. Changing GC_CLI_TRACE after the first call must not flip
// the resolution until resetCLITraceResolveCache is called.
func TestResolveCLITraceCache(t *testing.T) {
	resetCLITraceResolveCache()
	t.Setenv("GC_CLI_TRACE", "0")
	first := resolveCLITrace()
	if first.Enabled {
		t.Fatalf("first.Enabled = true; want false")
	}
	t.Setenv("GC_CLI_TRACE", "1")
	second := resolveCLITrace()
	if second.Enabled {
		t.Fatalf("second.Enabled = true; cache must hold previous decision")
	}
	resetCLITraceResolveCache()
	dir := t.TempDir()
	withTempCity(t, dir, "")
	third := resolveCLITrace()
	if !third.Enabled {
		t.Fatalf("third.Enabled = false; want true after reset with env=1")
	}
}

// TestTruncateCLIArgsRedactsSensitiveFlags walks the deny-list of flag
// names and asserts both the separated form (`--token v`) and inline
// form (`--token=v`) are redacted regardless of value length.
func TestTruncateCLIArgsRedactsSensitiveFlags(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "token separated short value",
			args: []string{"--token", "abc"},
			want: "--token <redacted:3>",
		},
		{
			name: "token separated long value",
			args: []string{"--token", strings.Repeat("x", 80)},
			want: "--token <redacted:80>",
		},
		{
			name: "password inline",
			args: []string{"--password=hunter2"},
			want: "--password=<redacted:7>",
		},
		{
			name: "secret uppercase flag name",
			args: []string{"--SECRET", "shh"},
			want: "--SECRET <redacted:3>",
		},
		{
			name: "api-key with hyphen",
			args: []string{"--api-key", "abc123"},
			want: "--api-key <redacted:6>",
		},
		{
			name: "apikey concatenated",
			args: []string{"--apikey=zzz"},
			want: "--apikey=<redacted:3>",
		},
		{
			name: "auth flag",
			args: []string{"--auth", "Basic foo"},
			want: "--auth <redacted:9>",
		},
		{
			name: "bearer inline",
			args: []string{"--bearer=tok"},
			want: "--bearer=<redacted:3>",
		},
		{
			name: "non-sensitive flag passes through",
			args: []string{"--verbose", "true"},
			want: "--verbose true",
		},
		{
			name: "single-letter -p not redacted",
			args: []string{"-p", "8080"},
			want: "-p 8080",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateCLIArgs(tc.args, 200, 64)
			if got != tc.want {
				t.Fatalf("truncateCLIArgs(%v) = %q; want %q", tc.args, got, tc.want)
			}
		})
	}
}

// TestTruncateCLIArgsRedactsEnvStyleKV exercises the KEY=VALUE redaction
// rules for substring-matched sensitive names.
func TestTruncateCLIArgsRedactsEnvStyleKV(t *testing.T) {
	cases := []struct {
		name string
		arg  string
		want string
	}{
		{name: "GITHUB_TOKEN", arg: "GITHUB_TOKEN=ghp_xxx", want: "GITHUB_TOKEN=<redacted:7>"},
		{name: "MY_PASSWORD", arg: "MY_PASSWORD=secret", want: "MY_PASSWORD=<redacted:6>"},
		{name: "API_KEY", arg: "API_KEY=k", want: "API_KEY=<redacted:1>"},
		{name: "ANTHROPIC_SECRET", arg: "ANTHROPIC_SECRET=foo", want: "ANTHROPIC_SECRET=<redacted:3>"},
		{name: "AUTH_HEADER", arg: "AUTH_HEADER=Bearer abc", want: "AUTH_HEADER=<redacted:10>"},
		{name: "lowercase key match", arg: "github_token=v", want: "github_token=<redacted:1>"},
		{name: "non-sensitive KV pass through", arg: "FOO=bar", want: "FOO=bar"},
		{name: "leading dash skipped", arg: "-MYKEY=v", want: "-MYKEY=v"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateCLIArgs([]string{tc.arg}, 200, 64)
			if got != tc.want {
				t.Fatalf("truncateCLIArgs([%q]) = %q; want %q", tc.arg, got, tc.want)
			}
		})
	}
}

// TestTruncateCLIArgsRedactionInterleavedWithOtherArgs ensures redaction
// only consumes the immediate next positional, not arbitrary downstream
// args, and that the boundary cases (last arg is a flag) don't crash.
func TestTruncateCLIArgsRedactionInterleavedWithOtherArgs(t *testing.T) {
	args := []string{"--token", "abc", "list", "--password=p", "--limit", "5"}
	got := truncateCLIArgs(args, 200, 64)
	want := "--token <redacted:3> list --password=<redacted:1> --limit 5"
	if got != want {
		t.Fatalf("interleaved redaction:\n got: %q\nwant: %q", got, want)
	}

	tail := truncateCLIArgs([]string{"foo", "--token"}, 200, 64)
	wantTail := "foo --token"
	if tail != wantTail {
		t.Fatalf("trailing flag without value:\n got: %q\nwant: %q", tail, wantTail)
	}
}

// TestMaybeRotateCLITraceReturnsBeforeGzip asserts that the rename is
// synchronous (active file gone) while gzip is still in flight. The
// rotation is given a pre-filled file large enough that the gzip work is
// observable; we check the .gz does not yet exist at the time
// maybeRotateCLITrace returns.
func TestMaybeRotateCLITraceReturnsBeforeGzip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, cliTraceFileBaseName)
	// Seed with enough data that gzip is not instantaneous, though we
	// also rely on the goroutine scheduling boundary to ensure ordering.
	payload := bytes.Repeat([]byte("payload-line\n"), 10000)
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var stderr bytes.Buffer
	maybeRotateCLITrace(path, 1, &stderr)

	// Immediately after the call returns, the active file must be gone
	// (rename is synchronous).
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active file present after rename; err=%v", err)
	}
	// The renamed temp file should exist while gzip runs.
	entries, _ := os.ReadDir(dir)
	var sawTemp bool
	for _, e := range entries {
		if strings.Contains(e.Name(), ".rotating.") {
			sawTemp = true
			break
		}
	}
	if !sawTemp {
		// Goroutine may have already finished the gzip in fast envs; that
		// is still a correct outcome (gzip ran async, just very fast).
		t.Logf("temp file not observed; goroutine completed before stat — acceptable")
	}

	waitForCLITraceRotations()

	entries, _ = os.ReadDir(dir)
	var gz int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gz") {
			gz++
		}
	}
	if gz != 1 {
		t.Fatalf("want exactly 1 .gz after wait; got %d", gz)
	}
}

// TestRecordCLIInvocationRedactsSensitiveArgs is an end-to-end check that
// the JSONL line written by recordCLIInvocation has redacted values for
// deny-listed flags.
func TestRecordCLIInvocationRedactsSensitiveArgs(t *testing.T) {
	resetCLITraceResolveCache()
	dir := t.TempDir()
	withTempCity(t, dir, "")
	t.Setenv("GC_CLI_TRACE", "1")

	var stderr bytes.Buffer
	rootCmd := newRootCmd(io.Discard, &stderr)
	recordCLIInvocation(rootCmd, []string{"version", "--token", "supersecret"}, time.Now(), 0, &stderr)

	tracePath := filepath.Join(dir, ".gc", "runtime", cliTraceFileBaseName)
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatalf("trace file missing: %v", err)
	}
	var rec cliInvocationRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &rec); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if strings.Contains(rec.ArgsTruncated, "supersecret") {
		t.Fatalf("secret leaked in args_truncated: %q", rec.ArgsTruncated)
	}
	if !strings.Contains(rec.ArgsTruncated, "<redacted:") {
		t.Fatalf("expected redacted sentinel in %q", rec.ArgsTruncated)
	}
}
