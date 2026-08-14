package tmux

import (
	"slices"
	"testing"
	"time"
)

func TestProviderEnvSkipsEscapeForPiAlias(t *testing.T) {
	if !providerEnvSkipsEscape("my-pi/tmux") {
		t.Fatal("pi provider alias should skip pre-enter Escape")
	}
}

func TestProviderEnvSkipsEscapeForCopilot(t *testing.T) {
	if !providerEnvSkipsEscape("copilot") {
		t.Fatal("copilot provider should skip pre-enter Escape")
	}
}

// TestComputeExcludingKillSet_SelfCloseExcludesCallerKeepsAgent locks in the
// fix for the self-close wedge: when `gc session close` runs from inside the
// pane it is tearing down, the caller is a descendant of the pane leader (the
// agent). The caller must be excluded from the TERM list so it survives long
// enough to finish cleanup, while the pane leader (agent) is still reached.
func TestComputeExcludingKillSet_SelfCloseExcludesCallerKeepsAgent(t *testing.T) {
	const (
		agentPID  = "100" // pane leader (e.g. the coding agent) — must be killed
		shellPID  = "101" // intermediate shell spawned by the agent
		callerPID = "102" // gc session close — the excluded caller
	)
	exclude := map[string]bool{callerPID: true}

	killList, killPaneLeader := computeExcludingKillSet(
		agentPID,
		[]string{shellPID, callerPID},
		nil,
		exclude,
	)

	if !killPaneLeader {
		t.Error("pane leader (agent) must be killed, but it was reported excluded")
	}
	if slices.Contains(killList, callerPID) {
		t.Errorf("caller %s must be excluded from TERM list, got %v", callerPID, killList)
	}
	if !slices.Contains(killList, shellPID) {
		t.Errorf("non-excluded descendant %s must be in TERM list, got %v", shellPID, killList)
	}
}

// TestComputeExcludingKillSet_ExternalCallerKillsEverything verifies that when
// the caller lives outside the pane (e.g. the supervisor running the close),
// excluding its PID is a harmless no-op: every process in the pane's tree is
// still terminated.
func TestComputeExcludingKillSet_ExternalCallerKillsEverything(t *testing.T) {
	const agentPID = "200"
	exclude := map[string]bool{"999": true} // external caller, not in the pane tree

	killList, killPaneLeader := computeExcludingKillSet(
		agentPID,
		[]string{"201"},
		[]string{"202"},
		exclude,
	)

	if !killPaneLeader {
		t.Error("pane leader must be killed for an external caller")
	}
	if !slices.Contains(killList, "201") || !slices.Contains(killList, "202") {
		t.Errorf("all pane descendants must be killed, got %v", killList)
	}
}

// TestComputeExcludingKillSet_ExcludedPaneLeaderSurvives guards the degenerate
// case where the pane leader itself is in the exclusion set: it must not be
// signaled directly (the final tmux kill-session reaps it instead).
func TestComputeExcludingKillSet_ExcludedPaneLeaderSurvives(t *testing.T) {
	const agentPID = "300"
	exclude := map[string]bool{agentPID: true}

	_, killPaneLeader := computeExcludingKillSet(agentPID, nil, nil, exclude)

	if killPaneLeader {
		t.Error("an excluded pane leader must not be killed directly")
	}
}

func TestTerminateProcessSetReturnsWhenTerminatedProcessesExit(t *testing.T) {
	alive := map[string]bool{"101": true, "102": true}
	var signals []string
	var sleeps []time.Duration
	now := time.Unix(0, 0)

	terminateProcessSet(
		[]string{"101", "102"},
		time.Second,
		func(pid, signal string) {
			signals = append(signals, signal+":"+pid)
			if signal == "TERM" {
				alive[pid] = false
			}
		},
		func(pid string) bool { return alive[pid] },
		func(delay time.Duration) {
			sleeps = append(sleeps, delay)
			now = now.Add(delay)
		},
		func() time.Time { return now },
	)

	if want := []string{"TERM:101", "TERM:102"}; !slices.Equal(signals, want) {
		t.Fatalf("signals = %v, want %v", signals, want)
	}
	if len(sleeps) != 0 {
		t.Fatalf("sleep calls = %v, want none after TERM made every process exit", sleeps)
	}
}

func TestTerminateProcessSetKillsOnlyProcessesStillAliveAfterGracePeriod(t *testing.T) {
	alive := map[string]bool{"201": true, "202": true}
	var signals []string
	var slept time.Duration
	now := time.Unix(0, 0)

	terminateProcessSet(
		[]string{"201", "202"},
		2*processExitCheckInterval,
		func(pid, signal string) {
			signals = append(signals, signal+":"+pid)
			if signal == "TERM" && pid == "201" {
				alive[pid] = false
			}
		},
		func(pid string) bool { return alive[pid] },
		func(delay time.Duration) {
			slept += delay
			now = now.Add(delay)
		},
		func() time.Time { return now },
	)

	want := []string{"TERM:201", "TERM:202", "KILL:202"}
	if !slices.Equal(signals, want) {
		t.Fatalf("signals = %v, want %v", signals, want)
	}
	if slept != 2*processExitCheckInterval {
		t.Fatalf("slept = %s, want full grace period %s for surviving process", slept, 2*processExitCheckInterval)
	}
}

func TestTerminateProcessSetReturnsWhenProcessExitsDuringGracePeriod(t *testing.T) {
	var signals []string
	checks := 0
	slept := time.Duration(0)
	now := time.Unix(0, 0)

	terminateProcessSet(
		[]string{"301"},
		time.Second,
		func(pid, signal string) { signals = append(signals, signal+":"+pid) },
		func(string) bool {
			checks++
			return checks < 3
		},
		func(delay time.Duration) {
			slept += delay
			now = now.Add(delay)
		},
		func() time.Time { return now },
	)

	if want := []string{"TERM:301"}; !slices.Equal(signals, want) {
		t.Fatalf("signals = %v, want %v", signals, want)
	}
	if slept != 2*processExitCheckInterval {
		t.Fatalf("slept = %s, want two observations (%s)", slept, 2*processExitCheckInterval)
	}
}

func TestTerminateProcessSetCountsProbeTimeAgainstGracePeriod(t *testing.T) {
	var signals []string
	slept := time.Duration(0)
	now := time.Unix(0, 0)
	probeDuration := 2 * processExitCheckInterval

	terminateProcessSet(
		[]string{"401"},
		3*processExitCheckInterval,
		func(pid, signal string) { signals = append(signals, signal+":"+pid) },
		func(string) bool {
			now = now.Add(probeDuration)
			return true
		},
		func(delay time.Duration) {
			slept += delay
			now = now.Add(delay)
		},
		func() time.Time { return now },
	)

	if want := []string{"TERM:401", "KILL:401"}; !slices.Equal(signals, want) {
		t.Fatalf("signals = %v, want %v", signals, want)
	}
	if slept != processExitCheckInterval {
		t.Fatalf("slept = %s, want remaining grace budget %s after slow probe", slept, processExitCheckInterval)
	}
}

// knownSet builds a descendant-set lookup from the given pids.
func knownSet(pids ...string) map[string]bool {
	m := make(map[string]bool, len(pids))
	for _, p := range pids {
		m[p] = true
	}
	return m
}

func TestReparentedOrphans_CollectsInitAndSubreaperOrphans(t *testing.T) {
	// leader=100, one live descendant=200. Group also holds:
	//   300 reparented to init (ppid 1) — classic case
	//   400 reparented to systemd --user subreaper (ppid 900) — the case the
	//        old PPID==1 test missed
	//   500 still a child of a live descendant (ppid 200) — owned elsewhere
	//   600 whose parent read failed ("") — must be skipped
	known := knownSet("100", "200")
	parents := map[string]string{
		"300": "1",
		"400": "900", // systemd --user pid, not init
		"500": "200",
		"600": "",
	}
	parentOf := func(pid string) string { return parents[pid] }

	got := reparentedOrphans([]string{"200", "300", "400", "500", "600"}, known, parentOf)
	slices.Sort(got)
	want := []string{"300", "400"}
	if !slices.Equal(got, want) {
		t.Fatalf("reparentedOrphans = %v, want %v", got, want)
	}
}

func TestReparentedOrphans_SkipsKnownDescendants(t *testing.T) {
	known := knownSet("100", "200", "300")
	parentOf := func(string) string { return "1" }
	if got := reparentedOrphans([]string{"200", "300"}, known, parentOf); len(got) != 0 {
		t.Fatalf("reparentedOrphans = %v, want empty (all are known descendants)", got)
	}
}

// listKeysPrefixSample holds `tmux list-keys -T prefix` rows (tmux 3.7b,
// default bindings) copied verbatim, keeping the shapes the parser has to
// survive: column-padded keys, the `-r` repeat flag ahead of `-T`,
// backslash-escaped key names, a command carrying its own `-T` flag, and a Gas
// Town if-shell binding. Two rows are not full copies — the real `<`
// display-menu row runs past 1 KB, so it is cut after the `-T` that matters,
// and the `g` row is a Gas Town binding this package installs rather than a
// tmux default. Column alignment and escaping are load-bearing — do not
// reflow.
const listKeysPrefixSample = `bind-key    -T prefix Space   next-layout
bind-key    -T prefix \"      split-window
bind-key    -T prefix \#      list-buffers
bind-key    -T prefix \$      command-prompt -I "#S" { rename-session "%%" }
bind-key    -T prefix <       display-menu -T "#[align=center]#{window_index}" -x W -y W Kill X { kill-window }
bind-key    -T prefix c       new-window
bind-key    -T prefix g       if-shell echo '#{session_name}' | grep -Eq '^(gc)-' "run-shell 'gt agents menu'" :
bind-key    -T prefix n       next-window
bind-key    -T prefix p       previous-window
bind-key    -T prefix s       choose-tree -Zs
bind-key -r -T prefix Up      select-pane -U`

// TestParseKeyBindingCommand pins the parse of the unfiltered
// `list-keys -T <table>` output. tmux 3.7 broke the positional key filter
// (`list-keys -T prefix s` exits 0 with no output), so selecting the row in Go
// is the only version-stable way to read a binding — which makes this parser
// the load-bearing half of getKeyBinding.
func TestParseKeyBindingCommand(t *testing.T) {
	tests := []struct {
		name  string
		table string
		key   string
		want  string
	}{
		{"single-word command", "prefix", "n", "next-window"},
		{"multi-word command", "prefix", "s", "choose-tree -Zs"},
		{"padded key column", "prefix", "Space", "next-layout"},
		{"repeat flag before -T", "prefix", "Up", "select-pane -U"},
		{"escaped double quote key", "prefix", `"`, "split-window"},
		{"escaped hash key", "prefix", "#", "list-buffers"},
		{"escaped dollar key", "prefix", "$", `command-prompt -I "#S" { rename-session "%%" }`},
		{
			// The command itself contains `-T`; anchoring on the first `-T`
			// must not mistake it for the key table.
			name:  "command containing its own -T flag",
			table: "prefix",
			key:   "<",
			want:  `display-menu -T "#[align=center]#{window_index}" -x W -y W Kill X { kill-window }`,
		},
		{
			// Gas Town filtering belongs to the caller, not the parser.
			name:  "gas town binding returned verbatim",
			table: "prefix",
			key:   "g",
			want:  `if-shell echo '#{session_name}' | grep -Eq '^(gc)-' "run-shell 'gt agents menu'" :`,
		},
		{"unbound key", "prefix", "F12", ""},
		{"key bound in a different table", "root", "n", ""},
		{"empty key", "prefix", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseKeyBindingCommand(listKeysPrefixSample, tc.table, tc.key); got != tc.want {
				t.Errorf("parseKeyBindingCommand(sample, %q, %q) = %q, want %q", tc.table, tc.key, got, tc.want)
			}
		})
	}
}

// TestParseKeyBindingCommand_FailsSafeOnUnparseableOutput pins the fail-safe
// contract: output that does not match the documented
// `bind-key [-r] -T <table> <key> <command...>` shape yields "" so callers
// install their own default rather than a corrupt binding.
func TestParseKeyBindingCommand_FailsSafeOnUnparseableOutput(t *testing.T) {
	tests := []struct {
		name   string
		output string
	}{
		{"empty output", ""},
		{"no -T marker", "bind-key prefix n next-window"},
		{"truncated after table", "bind-key    -T prefix"},
		{"key present but no command", "bind-key    -T prefix n"},
		{"key present, command is whitespace", "bind-key    -T prefix n   "},
		{"unrelated text", "no server running on /tmp/tmux-1000/default"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseKeyBindingCommand(tc.output, "prefix", "n"); got != "" {
				t.Errorf("parseKeyBindingCommand(%q, prefix, n) = %q, want \"\"", tc.output, got)
			}
		})
	}
}

// TestIsGasTownBindingCommand pins the guard that keeps ConfigureGasTownSession
// from wrapping its own if-shell binding in another if-shell on a repeat call.
func TestIsGasTownBindingCommand(t *testing.T) {
	tests := []struct {
		name string
		cmd  string
		want bool
	}{
		{"gas town if-shell binding", `if-shell "echo x | grep -Eq '^(gc)-'" "run-shell 'gt feed --window'" :`, true},
		{"tmux default", "next-window", false},
		{"user binding mentioning gt without if-shell", "run-shell 'gt status'", false},
		{"if-shell without gt", `if-shell "true" "display-message hi" :`, false},
		{"unbound", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isGasTownBindingCommand(tc.cmd); got != tc.want {
				t.Errorf("isGasTownBindingCommand(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}
}
