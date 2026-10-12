//go:build integration

package hooks

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/testutil"
)

// TestCodexLaunchHooksLoadOnceInEveryWorkDirShape asks a real Codex, through
// its app-server hooks/list call, which hooks a session launched with
// CodexLaunchArgs loads. It covers the two working-directory shapes Codex
// agents run in: a linked git worktree, where Codex reads project hooks only
// from the main checkout, and a plain directory inside a repository, where
// Codex also reads the directory's own .codex/hooks.json. In both, each
// managed hook must load exactly once, from the launch override, trusted.
func TestCodexLaunchHooksLoadOnceInEveryWorkDirShape(t *testing.T) {
	codexPath, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("codex is not on PATH; this test asks a real Codex which hooks it loads")
	}

	const city = "/city"
	launchArgs, err := CodexLaunchArgs(city)
	if err != nil {
		t.Fatalf("CodexLaunchArgs: %v", err)
	}
	doc, err := managedCodexHooksDoc(city)
	if err != nil {
		t.Fatalf("managedCodexHooksDoc: %v", err)
	}
	want := map[string]int{}
	for event, groups := range doc.Hooks {
		for _, group := range groups {
			for _, handler := range group.Hooks {
				want[event+"\t"+handler.Command]++
			}
		}
	}

	rigRoot, _ := testutil.InitGitRepo(t)
	rigRoot = resolvedDir(t, rigRoot)
	worktree := filepath.Join(resolvedDir(t, t.TempDir()), "worker")
	testutil.RunGit(t, rigRoot, "worktree", "add", "--detach", worktree)
	cityRoot, _ := testutil.InitGitRepo(t)
	cityRoot = resolvedDir(t, cityRoot)
	plainDir := filepath.Join(cityRoot, ".gc", "agents", "worker")

	// Each work dir holds the hooks file Gas City staged there before its
	// Codex hooks moved to the launch command.
	staged, err := readEmbedded(codexManagedHooksAsset)
	if err != nil {
		t.Fatalf("reading the managed codex hooks document: %v", err)
	}
	for _, dir := range []string{worktree, plainDir} {
		if err := os.MkdirAll(filepath.Join(dir, ".codex"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, ".codex", "hooks.json"), staged, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	codexHome := t.TempDir()
	trust := fmt.Sprintf("[projects.%q]\ntrust_level = \"trusted\"\n\n[projects.%q]\ntrust_level = \"trusted\"\n", rigRoot, cityRoot)
	if err := os.WriteFile(filepath.Join(codexHome, "config.toml"), []byte(trust), 0o600); err != nil {
		t.Fatal(err)
	}

	// The staged file is invisible in the worktree and a second registration
	// in the plain directory, which is why the launch override replaces it.
	before := codexHooksList(t, codexPath, codexHome, launchArgs, worktree, plainDir)
	if got := len(before[worktree].Hooks); got != len(want) {
		t.Errorf("worktree holding a staged hooks file: hooks/list loaded %d hooks, want only the %d launch hooks", got, len(want))
	}
	if got := len(before[plainDir].Hooks); got <= len(want) {
		t.Errorf("plain directory holding a staged hooks file: hooks/list loaded %d hooks, want the staged copy on top of the %d launch hooks", got, len(want))
	}

	for _, dir := range []string{worktree, plainDir} {
		if err := StripManagedCodexHooks(fsys.OSFS{}, dir); err != nil {
			t.Fatalf("StripManagedCodexHooks(%s): %v", dir, err)
		}
	}

	after := codexHooksList(t, codexPath, codexHome, launchArgs, worktree, plainDir)
	for _, dir := range []string{worktree, plainDir} {
		entry := after[dir]
		got := map[string]int{}
		for _, hook := range entry.Hooks {
			if hook.TrustStatus != "trusted" {
				t.Errorf("%s: %s hook %q has trust status %q, want trusted", dir, hook.EventName, hook.Command, hook.TrustStatus)
			}
			if hook.Source != "sessionFlags" {
				t.Errorf("%s: %s hook %q came from %q, want the session's -c override", dir, hook.EventName, hook.Command, hook.Source)
			}
			got[strings.ToUpper(hook.EventName[:1])+hook.EventName[1:]+"\t"+hook.Command]++
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: hooks/list loaded\n%s\nwant each managed hook exactly once:\n%s", dir, formatHookCounts(got), formatHookCounts(want))
		}
		if len(entry.Errors) > 0 || len(entry.Warnings) > 0 {
			t.Errorf("%s: hooks/list reported errors %s and warnings %q", dir, entry.Errors, entry.Warnings)
		}
	}
}

func resolvedDir(t *testing.T, dir string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("resolving %s: %v", dir, err)
	}
	return resolved
}

func formatHookCounts(counts map[string]int) string {
	lines := make([]string, 0, len(counts))
	for key, n := range counts {
		lines = append(lines, fmt.Sprintf("  %dx %s", n, key))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

type codexHooksListEntry struct {
	Cwd      string            `json:"cwd"`
	Hooks    []codexListedHook `json:"hooks"`
	Warnings []string          `json:"warnings"`
	Errors   []json.RawMessage `json:"errors"`
}

type codexListedHook struct {
	EventName   string `json:"eventName"`
	Command     string `json:"command"`
	Source      string `json:"source"`
	TrustStatus string `json:"trustStatus"`
}

// codexHooksList runs `codex app-server` with args and returns its hooks/list
// answer for each cwd, keyed by cwd. hooks/list starts no session and calls no
// model.
func codexHooksList(t *testing.T, codexPath, codexHome string, args []string, cwds ...string) map[string]codexHooksListEntry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	cmd := exec.CommandContext(ctx, codexPath, append([]string{"app-server"}, args...)...)
	cmd.Env = append(os.Environ(), "CODEX_HOME="+codexHome)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("starting codex app-server: %v", err)
	}
	defer func() {
		_ = stdin.Close()
		cancel()
		_ = cmd.Wait()
	}()

	reader := bufio.NewReader(stdout)
	send := func(msg map[string]any) {
		data, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stdin.Write(append(data, '\n')); err != nil {
			t.Fatalf("writing to codex app-server: %v (stderr: %s)", err, stderr.String())
		}
	}
	receive := func(id int) json.RawMessage {
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if errors.Is(err, io.EOF) {
					t.Fatalf("codex app-server closed before answering request %d (stderr: %s)", id, stderr.String())
				}
				t.Fatalf("reading from codex app-server: %v", err)
			}
			var msg struct {
				ID     *int            `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  json.RawMessage `json:"error"`
			}
			if err := json.Unmarshal(line, &msg); err != nil || msg.ID == nil || *msg.ID != id {
				continue
			}
			if len(msg.Error) > 0 && string(msg.Error) != "null" {
				t.Fatalf("codex app-server request %d failed: %s", id, msg.Error)
			}
			return msg.Result
		}
	}

	send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{
		"clientInfo": map[string]any{"name": "gascity-test", "version": "0"},
	}})
	receive(1)
	send(map[string]any{"method": "initialized"})
	send(map[string]any{"id": 2, "method": "hooks/list", "params": map[string]any{"cwds": cwds}})
	var resp struct {
		Data []codexHooksListEntry `json:"data"`
	}
	if err := json.Unmarshal(receive(2), &resp); err != nil {
		t.Fatalf("decoding hooks/list: %v", err)
	}
	out := make(map[string]codexHooksListEntry, len(resp.Data))
	for _, entry := range resp.Data {
		out[entry.Cwd] = entry
	}
	for _, cwd := range cwds {
		if _, ok := out[cwd]; !ok {
			t.Fatalf("hooks/list returned no entry for %s: %+v", cwd, resp.Data)
		}
	}
	return out
}
