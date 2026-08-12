package core

import (
	"bytes"
	"io/fs"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

var (
	bareBDSubcommand       = regexp.MustCompile(`\bbd[[:space:]\\]+(?:blocked|children|close|comment|comments|completion|config|count|create|delete|dep|doctor|dolt|epic|export|formula|gate|graph|help|hook|hooks|import|info|init|label|list|migrate|mol|orphans|prime|prune|ready|remember|rename-prefix|reopen|restore|search|show|sql|stale|stats|status|sync|update|version|where|worktree)\b`)
	bareBDDynamicArg       = regexp.MustCompile(`\bbd[[:space:]\\]+["']\$(?:\{)?[A-Za-z_]`)
	bareBDLeadingFlag      = regexp.MustCompile(`\bbd[[:space:]\\]+--[A-Za-z0-9]`)
	bareBDCommand          = regexp.MustCompile(`\bcommand[[:space:]]+bd\b`)
	bareBDSerializedArgv   = regexp.MustCompile(`["']bd["'][[:space:]]*,`)
	bareBDSerializedScalar = regexp.MustCompile(`\b(?:command|cmd|executable)[ \t]*[:=][ \t]*["']?bd["']?(?:[ \t]|$)`)
	bareBDYAMLArgv         = regexp.MustCompile(`(?m)^[ \t]*-[ \t]+bd[ \t]*(?:\r?\n)[ \t]*-[ \t]+[A-Za-z]`)
	gcImmediatelyBefore    = regexp.MustCompile(`(?:^|[^A-Za-z0-9_-])gc(?:[ \t]+--(?:city|rig)(?:=[^ \t\r\n]+|[ \t]+(?:"[^"\r\n]*"|'[^'\r\n]*'|[^ \t\r\n]+)))*(?:[ \t]|\\\r?\n)+$`)
	gcSerializedBefore     = regexp.MustCompile(`["']gc["'][[:space:]]*,(?:(?:[[:space:]]*["']--(?:city|rig)=[^"']+["'][[:space:]]*,)|(?:[[:space:]]*["']--(?:city|rig)["'][[:space:]]*,[[:space:]]*["'][^"']+["'][[:space:]]*,))*[[:space:]]*$`)
)

func findBareBDCommands(data []byte) []int {
	body := string(data)
	offsets := make(map[int]struct{})

	for _, pattern := range []*regexp.Regexp{bareBDSubcommand, bareBDDynamicArg, bareBDLeadingFlag} {
		for _, match := range pattern.FindAllStringIndex(body, -1) {
			if gcImmediatelyBefore.MatchString(body[:match[0]]) {
				continue
			}
			offsets[match[0]] = struct{}{}
		}
	}
	for _, match := range bareBDSerializedArgv.FindAllStringIndex(body, -1) {
		if gcSerializedBefore.MatchString(body[:match[0]]) {
			continue
		}
		offsets[match[0]] = struct{}{}
	}
	for _, pattern := range []*regexp.Regexp{bareBDCommand, bareBDSerializedScalar, bareBDYAMLArgv} {
		for _, match := range pattern.FindAllStringIndex(body, -1) {
			offsets[match[0]] = struct{}{}
		}
	}

	result := make([]int, 0, len(offsets))
	for offset := range offsets {
		result = append(result, offset)
	}
	sort.Ints(result)
	return result
}

func TestCoreShippedAssetsRouteBDCommandsThroughGC(t *testing.T) {
	err := fs.WalkDir(PackFS, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}

		data, err := fs.ReadFile(PackFS, path)
		if err != nil {
			return err
		}
		for _, offset := range findBareBDCommands(data) {
			lineNumber := bytes.Count(data[:offset], []byte{'\n'}) + 1
			lineStart := bytes.LastIndexByte(data[:offset], '\n') + 1
			lineEnd := bytes.IndexByte(data[offset:], '\n')
			if lineEnd < 0 {
				lineEnd = len(data)
			} else {
				lineEnd += offset
			}
			t.Errorf("%s:%d: shipped bd commands must route through gc bd: %s", path, lineNumber, strings.TrimSpace(string(data[lineStart:lineEnd])))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking embedded core pack: %v", err)
	}
}

func TestFindBareBDCommands(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "plain shell", body: `bd show ga-123`, want: 1},
		{name: "wrapped shell", body: "bd \\\n  show ga-123", want: 1},
		{name: "wrapped markdown", body: "`bd\nshow ga-123`", want: 1},
		{name: "serialized argv", body: "[\"bd\",\n \"future-command\", \"ga-123\"]", want: 1},
		{name: "dir-scoped command", body: `bd --dir /tmp/rig show ga-123`, want: 1},
		{name: "leading passthrough flag", body: `bd --no-daemon list`, want: 1},
		{name: "unknown leading passthrough flag", body: `bd --future-routing-bypass list`, want: 1},
		{name: "dynamic subcommand", body: `bd "$verb"`, want: 1},
		{name: "wrapped gc command", body: "gc bd \\\n  show ga-123"},
		{name: "plain gc command", body: `gc bd show ga-123`},
		{name: "explicit gc city", body: `gc --city /tmp/city bd show ga-123`},
		{name: "quoted explicit gc city", body: `gc --city "$CITY" bd list`},
		{name: "explicit gc rig", body: `gc --rig frontend bd list`},
		{name: "explicit gc city and rig", body: `gc --city /tmp/city --rig frontend bd list`},
		{name: "serialized gc argv", body: `["gc", "bd", "show", "ga-123"]`},
		{name: "serialized scoped gc argv", body: `["gc", "--city", "/x", "--rig", "r", "bd", "show"]`},
		{name: "binary prose", body: `the bd CLI reads a bd-managed store`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := len(findBareBDCommands([]byte(tt.body))); got != tt.want {
				t.Fatalf("findBareBDCommands() found %d commands, want %d in %q", got, tt.want, tt.body)
			}
		})
	}
}

func TestCoreMaintenanceExecAssets(t *testing.T) {
	required := []string{
		"assets/scripts/_bd_trace.sh",
		"assets/scripts/_list-helpers.sh",
		"assets/scripts/dolt-target.sh",
		"assets/scripts/escalate.sh",
		"assets/scripts/jsonl-export.sh",
		"assets/scripts/reaper.sh",
		"orders/jsonl-export.toml",
		"orders/reaper.toml",
	}
	for _, path := range required {
		if _, err := fs.Stat(PackFS, path); err != nil {
			t.Fatalf("core pack missing %s: %v", path, err)
		}
	}

	retired := []string{
		"formulas/mol-dog-jsonl.toml",
		"formulas/mol-dog-reaper.toml",
		"orders/mol-dog-jsonl.toml",
		"orders/mol-dog-reaper.toml",
	}
	for _, path := range retired {
		if _, err := fs.Stat(PackFS, path); err == nil {
			t.Fatalf("core pack must not carry retired Dog maintenance asset %s", path)
		}
	}
}

func TestCoreControlDispatcherAgent(t *testing.T) {
	type agentFile struct {
		Description       string   `toml:"description"`
		StartCommand      string   `toml:"start_command"`
		PromptMode        string   `toml:"prompt_mode"`
		ProcessNames      []string `toml:"process_names"`
		MaxActiveSessions *int     `toml:"max_active_sessions"`
		Scope             string   `toml:"scope"`
	}

	data, err := fs.ReadFile(PackFS, "agents/control-dispatcher/agent.toml")
	if err != nil {
		t.Fatalf("core pack missing control-dispatcher agent: %v", err)
	}
	var agent agentFile
	if _, err := toml.Decode(string(data), &agent); err != nil {
		t.Fatalf("Decode(control-dispatcher agent.toml): %v", err)
	}
	if agent.Description == "" {
		t.Fatal("control-dispatcher description is empty")
	}
	if agent.Scope != "" {
		t.Fatalf("control-dispatcher scope = %q, want empty so it expands at city and rig scope", agent.Scope)
	}
	wantStartCommand := `sh -c 'export GC_WORKFLOW_TRACE="${GC_WORKFLOW_TRACE:-${GC_CONTROL_DISPATCHER_TRACE_DEFAULT:-${GC_CITY}/.gc/runtime/control-dispatcher-trace.log}}"; trace_dir="${GC_WORKFLOW_TRACE%/*}"; if [ "$trace_dir" = "$GC_WORKFLOW_TRACE" ]; then trace_dir="."; elif [ -z "$trace_dir" ]; then trace_dir="/"; fi; mkdir -p "$trace_dir"; exec "${GC_BIN:-gc}" convoy control --serve --follow {{.Agent}}'`
	if agent.StartCommand != wantStartCommand {
		t.Fatalf("control-dispatcher start_command = %q, want templated dispatcher command", agent.StartCommand)
	}
	if agent.PromptMode != "none" {
		t.Fatalf("control-dispatcher prompt_mode = %q, want none", agent.PromptMode)
	}
	if !reflect.DeepEqual(agent.ProcessNames, []string{"gc"}) {
		t.Fatalf("control-dispatcher process_names = %v, want [gc]", agent.ProcessNames)
	}
	if agent.MaxActiveSessions == nil || *agent.MaxActiveSessions != 1 {
		t.Fatalf("control-dispatcher max_active_sessions = %v, want 1", agent.MaxActiveSessions)
	}
}

func TestCoreMaintenanceOrdersCarryLegacySkipAliases(t *testing.T) {
	type orderFile struct {
		Order struct {
			SkipAliases []string `toml:"skip_aliases"`
		} `toml:"order"`
	}

	for _, tt := range []struct {
		path string
		want string
	}{
		{path: "orders/jsonl-export.toml", want: "mol-dog-jsonl"},
		{path: "orders/reaper.toml", want: "mol-dog-reaper"},
	} {
		data, err := fs.ReadFile(PackFS, tt.path)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", tt.path, err)
		}
		var parsed orderFile
		if _, err := toml.Decode(string(data), &parsed); err != nil {
			t.Fatalf("Decode(%s): %v", tt.path, err)
		}
		if len(parsed.Order.SkipAliases) != 1 || parsed.Order.SkipAliases[0] != tt.want {
			t.Fatalf("%s skip_aliases = %#v, want [%q]", tt.path, parsed.Order.SkipAliases, tt.want)
		}
	}
}

// TestClaimProtocolFragmentIsEmbedded pins the go:embed inclusion of
// template-fragments/. The fragment render tests in cmd/gc read the on-disk
// source tree, but production cities hydrate the core pack from PackFS —
// dropping all:template-fragments from the embed directive would ship
// pool-worker prompts whose {{ template "claim-protocol" . }} degrades to an
// unexpanded literal while every repo test stayed green.
func TestClaimProtocolFragmentIsEmbedded(t *testing.T) {
	data, err := fs.ReadFile(PackFS, "template-fragments/claim-protocol.template.md")
	if err != nil {
		t.Fatalf("core PackFS is missing template-fragments/claim-protocol.template.md: %v", err)
	}
	for _, want := range []string{`{{ define "claim-protocol" -}}`, "gc hook --claim --drain-ack --json"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("embedded claim-protocol fragment missing %q", want)
		}
	}
}

var (
	// A `set ... pipefail` declaration anywhere in the script.
	pipefailDeclPattern = regexp.MustCompile(`(?m)^[ \t]*set\b[^\n]*\bpipefail\b`)
	// A writer piped into `grep` with a `-q` (quiet) flag, in any flag
	// cluster: -q, -qF, -Fxq, -qiE, -Eq, -qsF, ... `grep -q` exits the
	// instant it matches without draining stdin, so the writer races into a
	// SIGPIPE. The here-string form (`grep -q ... <<<"$x"`) has no pipe and
	// is not matched.
	pipedGrepQPattern = regexp.MustCompile(`\|[[:space:]]*grep[[:space:]]+(-[A-Za-z]+[[:space:]]+)*-[A-Za-z]*q[A-Za-z]*\b`)
)

// findPipefailGrepQPipelines returns the byte offsets of `writer | grep -q`
// pipelines in a script that runs under `set -o pipefail`. It returns nil for
// scripts that do not enable pipefail (where the SIGPIPE is harmless) and
// skips full-line comments so the guard can be documented in prose. See
// TestCoreShippedScriptsAvoidPipefailGrepQPipelines for the invariant.
func findPipefailGrepQPipelines(data []byte) []int {
	if !pipefailDeclPattern.Match(data) {
		return nil
	}
	var offsets []int
	pos := 0
	for _, line := range bytes.SplitAfter(data, []byte("\n")) {
		if trimmed := bytes.TrimLeft(line, " \t"); len(trimmed) == 0 || trimmed[0] != '#' {
			if loc := pipedGrepQPattern.FindIndex(line); loc != nil {
				offsets = append(offsets, pos+loc[0])
			}
		}
		pos += len(line)
	}
	return offsets
}

// TestCoreShippedScriptsAvoidPipefailGrepQPipelines fails if any shell script
// shipped in the core pack feeds a writer into `grep -q` while running under
// `set -o pipefail`. Under pipefail, grep's early exit SIGPIPEs the writer and
// that 141 becomes the pipeline's status, so a present match is reported as
// absent — a false negative that has bitten the maintenance scripts three
// times (d416a0085 in reaper.sh, gc-d760o in orphan-sweep.sh, gc-atrp1 across
// spawn-storm-detect/wisp-compact/escalate). Use a here-string
// (`grep -q ... <<<"$x"`, which pipefail does not apply to) or, for exact-line
// membership, list_contains_line from _list-helpers.sh.
func TestCoreShippedScriptsAvoidPipefailGrepQPipelines(t *testing.T) {
	err := fs.WalkDir(PackFS, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".sh") {
			return nil
		}
		data, err := fs.ReadFile(PackFS, path)
		if err != nil {
			return err
		}
		for _, offset := range findPipefailGrepQPipelines(data) {
			lineNumber := bytes.Count(data[:offset], []byte{'\n'}) + 1
			lineStart := bytes.LastIndexByte(data[:offset], '\n') + 1
			lineEnd := bytes.IndexByte(data[offset:], '\n')
			if lineEnd < 0 {
				lineEnd = len(data)
			} else {
				lineEnd += offset
			}
			t.Errorf("%s:%d: under `set -o pipefail`, piping into `grep -q` SIGPIPEs the writer on grep's early exit and reports a present match as absent — use a here-string (`grep -q ... <<<\"$x\"`) or list_contains_line from _list-helpers.sh: %s",
				path, lineNumber, strings.TrimSpace(string(data[lineStart:lineEnd])))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking embedded core pack: %v", err)
	}
}

func TestFindPipefailGrepQPipelines(t *testing.T) {
	const pipefail = "set -euo pipefail\n"
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "piped grep -q", body: pipefail + `echo "$x" | grep -q foo`, want: 1},
		{name: "piped grep -qF", body: pipefail + `printf '%s' "$x" | grep -qF "$n"`, want: 1},
		{name: "piped grep -Fxq", body: pipefail + `printf '%s\n' "$x" | grep -Fxq -- "$n"`, want: 1},
		{name: "piped grep -qiE", body: pipefail + `echo "$x" | grep -qiE 'a|b'`, want: 1},
		{name: "piped grep -Eq", body: pipefail + `printf '%s' "$x" | grep -Eq '\[.\]$'`, want: 1},
		{name: "here-string grep -q is safe", body: pipefail + `grep -q foo <<<"$x"`},
		{name: "here-string grep -Fxq is safe", body: pipefail + `grep -Fxq -- "$n" <<<"$x"`},
		{name: "list_contains_line is safe", body: pipefail + `list_contains_line "$x" "$n"`},
		{name: "not gated without pipefail", body: `echo "$x" | grep -q foo`},
		{name: "full-line comment is ignored", body: pipefail + `  # bad: echo "$x" | grep -q foo`},
		{name: "grep -v without q is not flagged", body: pipefail + `echo "$x" | grep -v foo`},
		{name: "grep pattern q is not a flag", body: pipefail + `echo "$x" | grep 'q'`},
		{name: "grep -e q pattern is not a flag", body: pipefail + `echo "$x" | grep -e q`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := len(findPipefailGrepQPipelines([]byte(tt.body))); got != tt.want {
				t.Fatalf("findPipefailGrepQPipelines() = %d, want %d for %q", got, tt.want, tt.body)
			}
		})
	}
}
