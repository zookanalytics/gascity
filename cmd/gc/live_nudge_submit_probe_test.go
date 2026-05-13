//go:build liveprobe

// Accelerated repro probe for the "Enter becomes newline" submit failure on
// tmux nudge. Measures how often `gc session nudge` text lands in Claude's
// `❯` input box without being submitted as a turn.
//
// See bead gc-kq4ia for the working hypothesis: on detached panes, the
// 500ms debounce between text and Enter does not actually let Claude process
// the text before SIGWINCH wakes it, so Claude's paste detection classifies
// `text + \r` as a single paste and treats the `\r` as a newline-in-buffer.
//
// This probe DOES NOT modify production code. It re-implements the nudge
// sequence locally so the `wake_before_text` knob can be flipped without
// touching tmux.NudgeSession.
//
// Run via:
//	GC_LIVE_CITY=/path/to/city \
//	GC_LIVE_TARGET=mayor \
//	GC_LIVE_NUDGE_MODE=detached \
//	GC_LIVE_NUDGE_ITERATIONS=200 \
//	go test -tags liveprobe -count=1 -timeout 30m -v \
//	    -run TestLiveNudgeSubmitProbe ./cmd/gc

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	sessiontmux "github.com/gastownhall/gascity/internal/runtime/tmux"
)

const (
	defaultProbeIterations = 200
	defaultProbeDebounceMs = 500
	probeReadyPromptPrefix = "❯"
)

type probeMode string

const (
	probeModeAttached probeMode = "attached"
	probeModeDetached probeMode = "detached"
)

type probeIterOutcome string

const (
	probeIterSubmitted probeIterOutcome = "submitted"
	probeIterStuck     probeIterOutcome = "stuck"
	probeIterError     probeIterOutcome = "error"
	probeIterLost      probeIterOutcome = "lost"
)

type probeIterResult struct {
	Index           int              `json:"index"`
	Token           string           `json:"token"`
	Mode            probeMode        `json:"mode"`
	Outcome         probeIterOutcome `json:"outcome"`
	Err             string           `json:"err,omitempty"`
	AttachedAtStart bool             `json:"attached_at_start"`
	DurationMs      int64            `json:"duration_ms"`
}

type probeModeReport struct {
	Mode       probeMode `json:"mode"`
	Iterations int       `json:"iterations"`
	Submitted  int       `json:"submitted"`
	Stuck      int       `json:"stuck"`
	Error      int       `json:"error"`
	Lost       int       `json:"lost"`
}

type probeReport struct {
	CityPath       string            `json:"city_path"`
	Target         string            `json:"target"`
	Socket         string            `json:"socket"`
	Iterations     int               `json:"iterations"`
	DebounceMs     int               `json:"debounce_ms"`
	WakeBeforeText bool              `json:"wake_before_text"`
	Modes          []probeModeReport `json:"modes"`
	Results        []probeIterResult `json:"results"`
}

// TestLiveNudgeSubmitProbe measures the rate at which the production
// NudgeSession sequence delivers text without submitting it. It runs N
// iterations against a live Claude tmux session, replicating the
// `send-keys -l text → sleep → wake → Enter` sequence locally so it can
// flip the wake/text ordering via the GC_LIVE_NUDGE_WAKE_BEFORE_TEXT knob.
func TestLiveNudgeSubmitProbe(t *testing.T) {
	preferRealBDOnPath(t)

	cityPath := os.Getenv("GC_LIVE_CITY")
	if cityPath == "" {
		cityPath = "/tmp/gc-claude-it"
	}
	target := os.Getenv("GC_LIVE_TARGET")
	if target == "" {
		target = "mayor"
	}
	sessionID := os.Getenv("GC_LIVE_SESSION_ID")

	iterations := defaultProbeIterations
	if raw := os.Getenv("GC_LIVE_NUDGE_ITERATIONS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			t.Fatalf("invalid GC_LIVE_NUDGE_ITERATIONS=%q: %v", raw, err)
		}
		iterations = n
	}

	debounceMs := defaultProbeDebounceMs
	if raw := os.Getenv("GC_LIVE_NUDGE_DEBOUNCE_MS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			t.Fatalf("invalid GC_LIVE_NUDGE_DEBOUNCE_MS=%q: %v", raw, err)
		}
		debounceMs = n
	}

	modes, err := parseProbeModes(os.Getenv("GC_LIVE_NUDGE_MODE"))
	if err != nil {
		t.Fatalf("GC_LIVE_NUDGE_MODE: %v", err)
	}

	wakeBeforeText := false
	if raw := os.Getenv("GC_LIVE_NUDGE_WAKE_BEFORE_TEXT"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatalf("invalid GC_LIVE_NUDGE_WAKE_BEFORE_TEXT=%q: %v", raw, err)
		}
		wakeBeforeText = v
	}

	artifactsDir := os.Getenv("GC_LIVE_NUDGE_ARTIFACTS_DIR")
	if artifactsDir == "" {
		artifactsDir = filepath.Join(t.TempDir(), "nudge-submit-probe")
	}
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		t.Fatalf("mkdir artifacts: %v", err)
	}
	t.Logf("artifacts dir: %s", artifactsDir)

	cfg, err := loadCityConfig(cityPath)
	if err != nil {
		t.Fatalf("loadCityConfig(%q): %v", cityPath, err)
	}
	store, err := openCityStoreAt(cityPath)
	if err != nil {
		t.Fatalf("openCityStoreAt(%q): %v", cityPath, err)
	}
	cityName := resolveLiveNudgeProbeCityName(cfg, cityPath)
	sp, err := newSessionProviderByName(cfg.Session.Provider, cfg.Session, cityName, cityPath)
	if err != nil {
		t.Fatalf("newSessionProviderByName: %v", err)
	}
	mgr := newSessionManagerWithConfig(cityPath, store, sp, cfg)
	id, err := resolveLiveProbeSessionID(cityPath, cfg, store, target, sessionID)
	if err != nil {
		t.Fatalf("resolveLiveProbeSessionID(%q): %v", target, err)
	}
	info, err := mgr.Get(id)
	if err != nil {
		t.Fatalf("mgr.Get(%q): %v", id, err)
	}
	// buildResumeCommand has the side effect of materializing any session
	// hints the session manager needs; we don't use the return values
	// directly because we drive tmux ourselves.
	_, _ = buildResumeCommand(t.TempDir(), cfg, info, "", io.Discard)

	socket := cfg.Session.Socket
	if socket == "" {
		socket = cityName
	}

	tm := sessiontmux.NewTmuxWithConfig(sessiontmux.Config{SocketName: socket})

	// Best-effort reset to an idle prompt before the probe.
	_ = tmuxSendKeys(socket, target, "C-c")
	_ = waitForPane(socket, target, 10*time.Second, func(text string) bool {
		return promptIsIdle(text)
	})

	base := fmt.Sprintf("%d", time.Now().UnixNano())
	report := probeReport{
		CityPath:       cityPath,
		Target:         target,
		Socket:         socket,
		Iterations:     iterations,
		DebounceMs:     debounceMs,
		WakeBeforeText: wakeBeforeText,
	}

	for _, mode := range modes {
		cleanup, err := enterProbeMode(t, tm, socket, target, mode)
		if err != nil {
			t.Fatalf("enter mode %s: %v", mode, err)
		}
		modeReport := probeModeReport{Mode: mode, Iterations: iterations}
		modeArtifactsDir := filepath.Join(artifactsDir, string(mode))
		for i := 0; i < iterations; i++ {
			res := runProbeIteration(t, tm, socket, target, mode, debounceMs, wakeBeforeText, base, i, modeArtifactsDir)
			report.Results = append(report.Results, res)
			switch res.Outcome {
			case probeIterSubmitted:
				modeReport.Submitted++
			case probeIterStuck:
				modeReport.Stuck++
			case probeIterError:
				modeReport.Error++
			case probeIterLost:
				modeReport.Lost++
			}
			if (i+1)%25 == 0 {
				t.Logf("mode=%s progress %d/%d: submitted=%d stuck=%d error=%d lost=%d",
					mode, i+1, iterations,
					modeReport.Submitted, modeReport.Stuck, modeReport.Error, modeReport.Lost)
			}
		}
		report.Modes = append(report.Modes, modeReport)
		cleanup()
	}

	if err := writeProbeReport(filepath.Join(artifactsDir, "summary.json"), report); err != nil {
		t.Fatalf("write summary: %v", err)
	}

	for _, mr := range report.Modes {
		pct := func(n int) float64 {
			if mr.Iterations == 0 {
				return 0
			}
			return 100 * float64(n) / float64(mr.Iterations)
		}
		t.Logf("mode=%s: submitted=%d/%d (%.1f%%) stuck=%d/%d (%.1f%%) error=%d lost=%d (wake_before_text=%v debounce_ms=%d)",
			mr.Mode,
			mr.Submitted, mr.Iterations, pct(mr.Submitted),
			mr.Stuck, mr.Iterations, pct(mr.Stuck),
			mr.Error, mr.Lost,
			report.WakeBeforeText, report.DebounceMs)
	}
}

// runProbeIteration executes a single nudge attempt and classifies the
// outcome. It mirrors tmux.NudgeSession except for two probe-specific
// behaviors: an optional `wakeBeforeText` ordering swap, and Ctrl-C
// cleanup between iterations to discard Claude's response.
func runProbeIteration(t *testing.T, tm *sessiontmux.Tmux, socket, target string, mode probeMode, debounceMs int, wakeBeforeText bool, base string, idx int, modeArtifactsDir string) (res probeIterResult) {
	t.Helper()
	token := fmt.Sprintf("GC_PROBE_%s_%04d", base, idx)
	res = probeIterResult{
		Index:           idx,
		Token:           token,
		Mode:            mode,
		AttachedAtStart: tm.IsSessionAttached(target),
	}
	start := time.Now()
	defer func() {
		res.DurationMs = time.Since(start).Milliseconds()
	}()

	captures := newProbeCapture()

	// Pre-iteration: ensure the prompt is idle. A previous iteration may
	// have left a response in flight or a stuck token in the input box.
	if err := waitForPane(socket, target, 10*time.Second, promptIsIdle); err != nil {
		res.Outcome = probeIterError
		res.Err = "pre-iter idle wait: " + err.Error()
		captures.add(t, "pre-idle-wait", socket, target)
		captures.flush(t, modeArtifactsDir, idx)
		return res
	}
	captures.add(t, "pre", socket, target)

	// 1. Optional wake-before-text (the hypothesis-implied fix).
	if wakeBeforeText {
		tm.WakePaneIfDetached(target)
		captures.add(t, "post-pre-wake", socket, target)
	}

	// 2. Send text in literal mode with retry. Mirrors sendKeysLiteralWithRetry.
	if err := probeSendKeysLiteralRetry(socket, target, token, 10*time.Second); err != nil {
		res.Outcome = probeIterError
		res.Err = "send text: " + err.Error()
		captures.flush(t, modeArtifactsDir, idx)
		return res
	}
	captures.add(t, "post-text", socket, target)

	// 3. Debounce.
	time.Sleep(time.Duration(debounceMs) * time.Millisecond)
	captures.add(t, "post-sleep", socket, target)

	// 4. Wake on detached panes if not already done. Mirrors NudgeSession step 4.
	if !wakeBeforeText {
		tm.WakePaneIfDetached(target)
	}
	captures.add(t, "post-wake", socket, target)

	// 5. Send Enter (up to 3 attempts), mirroring NudgeSession step 5.
	var enterErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		out, err := exec.Command("tmux", "-L", socket, "send-keys", "-t", target, "Enter").CombinedOutput()
		if err == nil {
			enterErr = nil
			break
		}
		enterErr = fmt.Errorf("attempt %d: %v: %s", attempt+1, err, strings.TrimSpace(string(out)))
	}
	if enterErr == nil {
		// 6. Wake again so the submitted turn is processed promptly.
		tm.WakePaneIfDetached(target)
	}
	captures.add(t, "post-enter", socket, target)
	if enterErr != nil {
		res.Outcome = probeIterError
		res.Err = "send enter: " + enterErr.Error()
		captures.flush(t, modeArtifactsDir, idx)
		return res
	}

	// Classify. After Enter, either:
	//   - the input clears and the user message echoes as `> token` → submitted
	//   - the token sits on the `❯` input line → stuck
	//   - neither → lost (text vanished without trace)
	res.Outcome = classifyProbeOutcome(socket, target, token, 3*time.Second)
	captures.add(t, "final", socket, target)

	// Persist captures on stuck (per brief). Also persist on error/lost so
	// reviewers can examine unexpected paths.
	if res.Outcome != probeIterSubmitted {
		captures.flush(t, modeArtifactsDir, idx)
	}

	// Reset Claude's input state between iterations. Ctrl-C alone is not
	// enough: on a stuck Enter, the prompt clears visually but Claude's
	// internal input buffer retains the unsubmitted text, so the next
	// iteration's send-keys would prepend the prior token. Ctrl-U
	// (unix-line-discard) drops the unsubmitted buffer first; Ctrl-C
	// then interrupts any in-flight response.
	_ = tmuxSendKeys(socket, target, "C-u")
	_ = tmuxSendKeys(socket, target, "C-c")
	_ = waitForPane(socket, target, 10*time.Second, promptIsIdle)

	return res
}

// classifyProbeOutcome polls the pane for up to `timeout` and decides which
// outcome category the iteration falls into. The classifier locks in
// SUBMITTED as soon as it sees the token outside the input box; STUCK
// requires the token to remain in the input box across the full polling
// window (transient "token still rendering" states resolve to either
// submitted or lost on the next sample).
func classifyProbeOutcome(socket, target, token string, timeout time.Duration) probeIterOutcome {
	deadline := time.Now().Add(timeout)
	var lastSeen probeIterOutcome
	for {
		text, err := capturePane(socket, target, 220)
		if err != nil {
			if time.Now().After(deadline) {
				if lastSeen != "" {
					return lastSeen
				}
				return probeIterError
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch {
		case paneTokenSubmitted(text, token):
			return probeIterSubmitted
		case paneTokenStuck(text, token):
			lastSeen = probeIterStuck
		case strings.Contains(text, token):
			// Token visible somewhere — likely in scrollback that the
			// strict paneTokenSubmitted predicate missed (long lines
			// wrap and break the `❯ <token>` prefix). Conservative call:
			// treat as submitted, since stuck would require it to be in
			// the current input box.
			return probeIterSubmitted
		default:
			lastSeen = probeIterLost
		}
		if time.Now().After(deadline) {
			if lastSeen == "" {
				return probeIterLost
			}
			return lastSeen
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// findInputBoxLineIndex returns the index of the LAST `❯` line in the pane,
// which Claude only ever uses for the current input box. Returns -1 if no
// prompt line is present. Distinguishing this from earlier `❯` lines is
// essential: Claude echoes submitted user messages with the same `❯` glyph
// in scrollback, so a naive any-line check confuses submitted-and-echoed
// with stuck-in-input.
func findInputBoxLineIndex(text string) int {
	lines := strings.Split(text, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, probeReadyPromptPrefix) {
			return i
		}
	}
	return -1
}

// paneTokenSubmitted reports whether the token appears as an echoed user
// message in scrollback — any `❯ <text>` line that is NOT the current
// input box.
func paneTokenSubmitted(text, token string) bool {
	lines := strings.Split(text, "\n")
	inputIdx := findInputBoxLineIndex(text)
	for i, line := range lines {
		if i == inputIdx {
			continue
		}
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, probeReadyPromptPrefix) {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(trimmed, probeReadyPromptPrefix))
		if strings.Contains(rest, token) {
			return true
		}
	}
	return false
}

// paneTokenStuck reports whether the token sits in the current input box
// (the LAST `❯` line, the only `❯` line that represents pending input).
func paneTokenStuck(text, token string) bool {
	idx := findInputBoxLineIndex(text)
	if idx < 0 {
		return false
	}
	lines := strings.Split(text, "\n")
	trimmed := strings.TrimSpace(lines[idx])
	rest := strings.TrimSpace(strings.TrimPrefix(trimmed, probeReadyPromptPrefix))
	return strings.Contains(rest, token)
}

// promptIsIdle reports whether the latest `❯` prompt line is empty.
// Used to gate iterations and confirm cleanup succeeded.
func promptIsIdle(text string) bool {
	// Find the LAST `❯` line — Claude only ever renders one input prompt
	// at the bottom; earlier matches would be scrollback noise.
	lines := strings.Split(text, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, probeReadyPromptPrefix) {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(trimmed, probeReadyPromptPrefix))
		return rest == ""
	}
	return false
}

// probeSendKeysLiteralRetry mirrors tmux.sendKeysLiteralWithRetry: retries
// "not in a mode" with capped exponential backoff, fails fast otherwise.
func probeSendKeysLiteralRetry(socket, target, text string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	interval := 500 * time.Millisecond
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := exec.Command("tmux", "-L", socket, "send-keys", "-t", target, "-l", text).CombinedOutput()
		if err == nil {
			return nil
		}
		combined := strings.TrimSpace(string(out))
		if !strings.Contains(combined, "not in a mode") {
			return fmt.Errorf("%v: %s", err, combined)
		}
		lastErr = fmt.Errorf("%v: %s", err, combined)
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		sleep := interval
		if sleep > remaining {
			sleep = remaining
		}
		time.Sleep(sleep)
		interval = interval * 3 / 2
		if interval > 2*time.Second {
			interval = 2 * time.Second
		}
	}
	return fmt.Errorf("agent not ready after %s: %w", timeout, lastErr)
}

// enterProbeMode either confirms the session is detached or attaches a
// hidden script-managed client. Returns a cleanup func that reverses any
// attach this call performed.
func enterProbeMode(t *testing.T, tm *sessiontmux.Tmux, socket, target string, mode probeMode) (func(), error) {
	t.Helper()
	switch mode {
	case probeModeDetached:
		if tm.IsSessionAttached(target) {
			return nil, fmt.Errorf("session %q is attached; expected detached. detach all clients before running mode=detached", target)
		}
		return func() {}, nil
	case probeModeAttached:
		if tm.IsSessionAttached(target) {
			return func() {}, nil
		}
		// macOS `script` and util-linux `script` have incompatible argv.
		// Mirror sessiontmux.hiddenAttachScriptArgs so the probe works on
		// both without bringing in shellquote.
		var args []string
		tmuxCmd := fmt.Sprintf("tmux -L %s attach-session -t %s", socket, target)
		if goruntime.GOOS == "darwin" {
			args = []string{"-q", "/dev/null", "tmux", "-L", socket, "attach-session", "-t", target}
		} else {
			args = []string{"-qfc", tmuxCmd, "/dev/null"}
		}
		cmd := exec.Command("script", args...)
		cmd.Env = append(os.Environ(), "TERM=xterm-256color")
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("hidden attach start: %w", err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if tm.IsSessionAttached(target) {
				return func() {
					_ = cmd.Process.Kill()
					_, _ = cmd.Process.Wait()
				}, nil
			}
			time.Sleep(50 * time.Millisecond)
		}
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, fmt.Errorf("hidden attach: session never became attached")
	default:
		return nil, fmt.Errorf("unknown probe mode %q", mode)
	}
}

func parseProbeModes(raw string) ([]probeMode, error) {
	if raw == "" {
		return []probeMode{probeModeDetached}, nil
	}
	switch raw {
	case "detached":
		return []probeMode{probeModeDetached}, nil
	case "attached":
		return []probeMode{probeModeAttached}, nil
	case "both":
		return []probeMode{probeModeDetached, probeModeAttached}, nil
	default:
		return nil, fmt.Errorf("unknown mode %q (want attached|detached|both)", raw)
	}
}

// probeCapture buffers pane snapshots per step and flushes them to disk
// only when needed. Avoids writing 200×4 capture files for clean runs.
type probeCapture struct {
	steps []probeCaptureStep
}

type probeCaptureStep struct {
	name string
	text string
	err  error
}

func newProbeCapture() *probeCapture {
	return &probeCapture{}
}

func (c *probeCapture) add(t *testing.T, step, socket, target string) {
	t.Helper()
	text, err := capturePane(socket, target, 220)
	c.steps = append(c.steps, probeCaptureStep{name: step, text: text, err: err})
}

func (c *probeCapture) flush(t *testing.T, modeDir string, idx int) {
	t.Helper()
	iterDir := filepath.Join(modeDir, fmt.Sprintf("%04d", idx))
	if err := os.MkdirAll(iterDir, 0o755); err != nil {
		t.Logf("mkdir %s: %v", iterDir, err)
		return
	}
	for _, step := range c.steps {
		path := filepath.Join(iterDir, step.name+".txt")
		var body strings.Builder
		if step.err != nil {
			fmt.Fprintf(&body, "capture error: %v\n\n", step.err)
		}
		body.WriteString(step.text)
		if err := os.WriteFile(path, []byte(body.String()), 0o644); err != nil {
			t.Logf("write %s: %v", path, err)
		}
	}
}

func writeProbeReport(path string, report probeReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// resolveLiveNudgeProbeCityName returns the effective city name used as the
// default tmux socket and provider context. It mirrors the resolution rules
// in cmd/gc: prefer the resolved name (set by site bindings), then the
// city.toml workspace name, finally the directory basename. The bare
// city.toml emitted by `gc init --provider claude` has no [workspace] name,
// so without the basename fallback the probe would address the default
// tmux server instead of the per-city socket.
func resolveLiveNudgeProbeCityName(cfg *config.City, cityPath string) string {
	if cfg != nil {
		if n := strings.TrimSpace(cfg.ResolvedWorkspaceName); n != "" {
			return n
		}
		if n := strings.TrimSpace(cfg.Workspace.Name); n != "" {
			return n
		}
	}
	return filepath.Base(filepath.Clean(cityPath))
}
