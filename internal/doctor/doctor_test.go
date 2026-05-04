package doctor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// mockCheck is a configurable Check for testing the runner.
type mockCheck struct {
	name   string
	status CheckStatus
	msg    string
	canFix bool
	fixErr error
	fixed  bool // set by Fix
}

func (m *mockCheck) Name() string { return m.name }
func (m *mockCheck) Run(_ *CheckContext) *CheckResult {
	st := m.status
	if m.fixed {
		st = StatusOK
	}
	return &CheckResult{
		Name:    m.name,
		Status:  st,
		Message: m.msg,
	}
}
func (m *mockCheck) CanFix() bool { return m.canFix }
func (m *mockCheck) Fix(_ *CheckContext) error {
	if m.fixErr != nil {
		return m.fixErr
	}
	m.fixed = true
	return nil
}

func TestDoctor_AllPass(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "a", status: StatusOK, msg: "ok"})
	d.Register(&mockCheck{name: "b", status: StatusOK, msg: "ok"})

	var buf bytes.Buffer
	r := d.Run(&CheckContext{CityPath: "/tmp"}, &buf, false)

	if r.Passed != 2 {
		t.Errorf("Passed = %d, want 2", r.Passed)
	}
	if r.Warned != 0 || r.Failed != 0 || r.Fixed != 0 {
		t.Errorf("unexpected counts: warned=%d failed=%d fixed=%d", r.Warned, r.Failed, r.Fixed)
	}
	if !strings.Contains(buf.String(), "✓ a") {
		t.Errorf("output missing check a: %q", buf.String())
	}
}

func TestDoctor_MixedResults(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "ok-check", status: StatusOK, msg: "fine"})
	d.Register(&mockCheck{name: "warn-check", status: StatusWarning, msg: "hmm"})
	d.Register(&mockCheck{name: "fail-check", status: StatusError, msg: "bad"})

	var buf bytes.Buffer
	r := d.Run(&CheckContext{CityPath: "/tmp"}, &buf, false)

	if r.Passed != 1 {
		t.Errorf("Passed = %d, want 1", r.Passed)
	}
	if r.Warned != 1 {
		t.Errorf("Warned = %d, want 1", r.Warned)
	}
	if r.Failed != 1 {
		t.Errorf("Failed = %d, want 1", r.Failed)
	}

	out := buf.String()
	if !strings.Contains(out, "✓ ok-check") {
		t.Errorf("missing ok icon: %q", out)
	}
	if !strings.Contains(out, "⚠ warn-check") {
		t.Errorf("missing warning icon: %q", out)
	}
	if !strings.Contains(out, "✗ fail-check") {
		t.Errorf("missing error icon: %q", out)
	}
}

func TestDoctor_FixFlow(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "fixable", status: StatusWarning, msg: "problem", canFix: true})

	var buf bytes.Buffer
	r := d.Run(&CheckContext{CityPath: "/tmp"}, &buf, true)

	if r.Fixed != 1 {
		t.Errorf("Fixed = %d, want 1", r.Fixed)
	}
	if r.Passed != 1 {
		t.Errorf("Passed = %d, want 1 (fixed counts as passed)", r.Passed)
	}
	if !strings.Contains(buf.String(), "(fixed)") {
		t.Errorf("output missing (fixed): %q", buf.String())
	}
}

func TestDoctor_FixNotRequested(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "fixable", status: StatusWarning, msg: "problem", canFix: true})

	var buf bytes.Buffer
	r := d.Run(&CheckContext{CityPath: "/tmp"}, &buf, false)

	if r.Fixed != 0 {
		t.Errorf("Fixed = %d, want 0 (fix not requested)", r.Fixed)
	}
	if r.Warned != 1 {
		t.Errorf("Warned = %d, want 1", r.Warned)
	}
}

func TestDoctor_FixFails(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{
		name: "broken-fix", status: StatusError, msg: "bad",
		canFix: true, fixErr: fmt.Errorf("fix failed"),
	})

	var buf bytes.Buffer
	r := d.Run(&CheckContext{CityPath: "/tmp"}, &buf, true)

	if r.Fixed != 0 {
		t.Errorf("Fixed = %d, want 0 (fix errored)", r.Fixed)
	}
	if r.Failed != 1 {
		t.Errorf("Failed = %d, want 1", r.Failed)
	}
	if !strings.Contains(buf.String(), "fix failed: fix failed") {
		t.Errorf("output missing fix error: %q", buf.String())
	}
}

func TestDoctor_FixSucceedsButCheckStillFails(t *testing.T) {
	d := &Doctor{}
	d.Register(&unchangedFixCheck{})

	var buf bytes.Buffer
	r := d.Run(&CheckContext{CityPath: "/tmp"}, &buf, true)

	if r.Fixed != 0 {
		t.Errorf("Fixed = %d, want 0", r.Fixed)
	}
	if r.Failed != 1 {
		t.Errorf("Failed = %d, want 1", r.Failed)
	}
	if !strings.Contains(buf.String(), "fix attempted; check still failing") {
		t.Errorf("output missing fix-attempt signal: %q", buf.String())
	}
}

func TestDoctor_NoChecks(t *testing.T) {
	d := &Doctor{}
	var buf bytes.Buffer
	r := d.Run(&CheckContext{CityPath: "/tmp"}, &buf, false)

	if r.Passed != 0 || r.Warned != 0 || r.Failed != 0 || r.Fixed != 0 {
		t.Errorf("empty doctor should have all zeros: %+v", r)
	}
}

func TestDoctor_VerboseDetails(t *testing.T) {
	d := &Doctor{}
	c := &mockCheck{name: "detail-check", status: StatusOK, msg: "ok"}
	d.Register(c)

	// We need a check that returns details — override with a custom one.
	d2 := &Doctor{}
	d2.Register(&detailCheck{})

	var buf bytes.Buffer
	d2.Run(&CheckContext{CityPath: "/tmp", Verbose: true}, &buf, false)

	if !strings.Contains(buf.String(), "extra info") {
		t.Errorf("verbose output missing details: %q", buf.String())
	}
}

func TestDoctor_VerboseHidden(t *testing.T) {
	d := &Doctor{}
	d.Register(&detailCheck{})

	var buf bytes.Buffer
	d.Run(&CheckContext{CityPath: "/tmp", Verbose: false}, &buf, false)

	if strings.Contains(buf.String(), "extra info") {
		t.Errorf("non-verbose output should hide details: %q", buf.String())
	}
}

func TestPrintSummary(t *testing.T) {
	tests := []struct {
		name   string
		report *Report
		want   string
	}{
		{"all pass", &Report{Passed: 3}, "3 passed"},
		{"mixed", &Report{Passed: 2, Warned: 1, Failed: 1}, "2 passed, 1 warnings, 1 failed"},
		{"with fixes", &Report{Passed: 2, Fixed: 1}, "2 passed, 1 fixed"},
		{"empty", &Report{}, "No checks ran."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			PrintSummary(&buf, tt.report)
			if !strings.Contains(buf.String(), tt.want) {
				t.Errorf("summary = %q, want to contain %q", buf.String(), tt.want)
			}
		})
	}
}

func TestDoctor_FixHint(t *testing.T) {
	d := &Doctor{}
	d.Register(&hintCheck{})

	var buf bytes.Buffer
	d.Run(&CheckContext{CityPath: "/tmp"}, &buf, false)

	if !strings.Contains(buf.String(), "hint: try this") {
		t.Errorf("output missing fix hint: %q", buf.String())
	}
}

// TestCheckStatus_String pins the wire form for each status. The lowercase
// string mapping is the JSON contract — consumers must be able to compare
// against fixed string values.
func TestCheckStatus_String(t *testing.T) {
	tests := []struct {
		status CheckStatus
		want   string
	}{
		{StatusOK, "ok"},
		{StatusWarning, "warning"},
		{StatusError, "error"},
		{CheckStatus(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.want {
			t.Errorf("CheckStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
		}
	}
}

// TestCheckStatus_MarshalJSON ensures the status round-trips as a JSON
// string. JSON consumers receive the human-readable token, not an int.
func TestCheckStatus_MarshalJSON(t *testing.T) {
	tests := []struct {
		status CheckStatus
		want   string
	}{
		{StatusOK, `"ok"`},
		{StatusWarning, `"warning"`},
		{StatusError, `"error"`},
	}
	for _, tt := range tests {
		got, err := json.Marshal(tt.status)
		if err != nil {
			t.Fatalf("json.Marshal(%v): %v", tt.status, err)
		}
		if string(got) != tt.want {
			t.Errorf("json.Marshal(%v) = %s, want %s", tt.status, got, tt.want)
		}
	}
}

// TestDoctor_RunCollect verifies the collect path returns the same
// per-check results and counts as the streaming Run path, but without
// writing any output. This is the seam the JSON renderer plugs into.
func TestDoctor_RunCollect(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "ok-check", status: StatusOK, msg: "fine"})
	d.Register(&mockCheck{name: "warn-check", status: StatusWarning, msg: "hmm"})
	d.Register(&mockCheck{name: "fail-check", status: StatusError, msg: "bad"})
	d.Register(&detailCheck{})

	results, report := d.RunCollect(&CheckContext{CityPath: "/tmp"}, false)

	if len(results) != 4 {
		t.Fatalf("got %d results, want 4", len(results))
	}
	if results[0].Name != "ok-check" || results[0].Status != StatusOK {
		t.Errorf("results[0] = %+v, want ok-check/OK", results[0])
	}
	if results[1].Status != StatusWarning {
		t.Errorf("results[1].Status = %v, want warning", results[1].Status)
	}
	if results[2].Status != StatusError {
		t.Errorf("results[2].Status = %v, want error", results[2].Status)
	}
	if len(results[3].Details) == 0 {
		t.Errorf("results[3] should have Details preserved, got %+v", results[3])
	}

	if report.Passed != 2 || report.Warned != 1 || report.Failed != 1 {
		t.Errorf("report = %+v, want passed=2 warned=1 failed=1", report)
	}
}

// TestDoctor_RunCollectFixFlow confirms RunCollect propagates the same
// Fixed and FixAttempted flags as Run. JSON consumers depend on these to
// distinguish "fix succeeded" from "fix attempted but check still bad."
func TestDoctor_RunCollectFixFlow(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "fixable", status: StatusError, msg: "broken", canFix: true})
	d.Register(&unchangedFixCheck{})
	d.Register(&mockCheck{
		name: "fix-errors", status: StatusError, msg: "bad",
		canFix: true, fixErr: fmt.Errorf("boom"),
	})

	results, report := d.RunCollect(&CheckContext{CityPath: "/tmp"}, true)

	if !results[0].Fixed {
		t.Errorf("fixable check should have Fixed=true, got %+v", results[0])
	}
	if results[1].Fixed || !results[1].FixAttempted {
		t.Errorf("unchanged-fix should have FixAttempted=true and Fixed=false, got %+v", results[1])
	}
	if results[2].FixError == "" || !results[2].FixAttempted {
		t.Errorf("fix-errors should have FixError set and FixAttempted=true, got %+v", results[2])
	}

	if report.Fixed != 1 || report.Failed != 2 {
		t.Errorf("report = %+v, want fixed=1 failed=2", report)
	}
}

// TestRenderJSON_Schema pins the wire shape: top-level checks[] and
// summary{passed,warned,failed,fixed}. Each per-check entry carries
// name, status, message, details, fix_hint, fix_error, fix_attempted,
// fixed. Status is a lowercase string. This is the contract automated
// agents (deacon-patrol etc.) consume; breaking changes here ripple to
// every downstream parser.
func TestRenderJSON_Schema(t *testing.T) {
	results := []*CheckResult{
		{
			Name:    "ok-check",
			Status:  StatusOK,
			Message: "fine",
		},
		{
			Name:         "fail-check",
			Status:       StatusError,
			Message:      "1 finding(s)",
			Details:      []string{"finding A", "finding B"},
			FixHint:      "run `gc fix-thing`",
			FixError:     "fix command not found",
			FixAttempted: true,
			Fixed:        false,
		},
		{
			Name:    "fixed-check",
			Status:  StatusOK,
			Message: "ok now",
			Fixed:   true,
		},
	}
	report := &Report{Passed: 2, Warned: 0, Failed: 1, Fixed: 1}

	var buf bytes.Buffer
	if err := RenderJSON(&buf, results, report); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}

	var got JSONOutput
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, buf.String())
	}

	if len(got.Checks) != 3 {
		t.Fatalf("got %d checks, want 3", len(got.Checks))
	}
	if got.Summary == nil {
		t.Fatal("summary missing from JSON output")
	}
	if got.Summary.Passed != 2 || got.Summary.Failed != 1 || got.Summary.Fixed != 1 {
		t.Errorf("summary = %+v, want passed=2 failed=1 fixed=1", got.Summary)
	}

	// Status must be the lowercase string token, not an int. Re-decode
	// into a generic map to assert the wire form directly.
	var raw map[string]any
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	checks, ok := raw["checks"].([]any)
	if !ok {
		t.Fatalf("checks not a slice: %T", raw["checks"])
	}
	first, ok := checks[0].(map[string]any)
	if !ok {
		t.Fatalf("first check not a map: %T", checks[0])
	}
	if first["status"] != "ok" {
		t.Errorf("status = %v (%T), want \"ok\" string", first["status"], first["status"])
	}
	for _, key := range []string{"name", "status", "message", "fix_attempted", "fixed"} {
		if _, ok := first[key]; !ok {
			t.Errorf("first check missing required key %q: %v", key, first)
		}
	}

	// Optional fields populated on the failing check.
	failing := checks[1].(map[string]any)
	if failing["status"] != "error" {
		t.Errorf("failing.status = %v, want \"error\"", failing["status"])
	}
	if failing["fix_hint"] != "run `gc fix-thing`" {
		t.Errorf("failing.fix_hint = %v, want run `gc fix-thing`", failing["fix_hint"])
	}
	if failing["fix_error"] != "fix command not found" {
		t.Errorf("failing.fix_error = %v, want fix command not found", failing["fix_error"])
	}
	if failing["fix_attempted"] != true {
		t.Errorf("failing.fix_attempted = %v, want true", failing["fix_attempted"])
	}
	details, ok := failing["details"].([]any)
	if !ok || len(details) != 2 {
		t.Errorf("failing.details = %v, want 2-element array", failing["details"])
	}

	// Optional fields omitted on the trivially-passing check.
	passing := checks[0].(map[string]any)
	if _, present := passing["fix_hint"]; present {
		t.Errorf("ok check should omit fix_hint, got %v", passing["fix_hint"])
	}
	if _, present := passing["details"]; present {
		t.Errorf("ok check should omit empty details, got %v", passing["details"])
	}
}

// TestRenderJSON_EmptyResults verifies the renderer always emits a
// well-formed top-level shape even when no checks ran. JSON consumers
// rely on the {checks, summary} envelope being present unconditionally.
func TestRenderJSON_EmptyResults(t *testing.T) {
	var buf bytes.Buffer
	if err := RenderJSON(&buf, nil, &Report{}); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, buf.String())
	}
	checks, ok := raw["checks"].([]any)
	if !ok {
		t.Fatalf("checks must be present (even if empty), got %T", raw["checks"])
	}
	if len(checks) != 0 {
		t.Errorf("checks = %v, want empty slice", checks)
	}
	if _, ok := raw["summary"]; !ok {
		t.Error("summary key must always be present")
	}
}

// TestDoctor_RunStillStreams confirms the legacy Run path still produces
// the human-readable streaming output. RunCollect was a refactor and must
// not regress the existing CLI contract.
func TestDoctor_RunStillStreams(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "alpha", status: StatusOK, msg: "ok"})
	d.Register(&mockCheck{name: "beta", status: StatusWarning, msg: "warn"})

	var buf bytes.Buffer
	r := d.Run(&CheckContext{CityPath: "/tmp"}, &buf, false)

	if r.Passed != 1 || r.Warned != 1 {
		t.Errorf("report = %+v, want passed=1 warned=1", r)
	}
	out := buf.String()
	if !strings.Contains(out, "✓ alpha") || !strings.Contains(out, "⚠ beta") {
		t.Errorf("streaming output missing expected lines: %q", out)
	}
}

// detailCheck returns a result with Details for verbose testing.
type detailCheck struct{}

func (c *detailCheck) Name() string { return "detail-check" }
func (c *detailCheck) Run(_ *CheckContext) *CheckResult {
	return &CheckResult{
		Name:    "detail-check",
		Status:  StatusOK,
		Message: "ok",
		Details: []string{"extra info"},
	}
}
func (c *detailCheck) CanFix() bool              { return false }
func (c *detailCheck) Fix(_ *CheckContext) error { return nil }

// hintCheck returns a failing result with a FixHint.
type hintCheck struct{}

func (c *hintCheck) Name() string { return "hint-check" }
func (c *hintCheck) Run(_ *CheckContext) *CheckResult {
	return &CheckResult{
		Name:    "hint-check",
		Status:  StatusError,
		Message: "problem",
		FixHint: "try this",
	}
}
func (c *hintCheck) CanFix() bool              { return false }
func (c *hintCheck) Fix(_ *CheckContext) error { return nil }

type unchangedFixCheck struct{}

func (c *unchangedFixCheck) Name() string { return "unchanged-fix" }
func (c *unchangedFixCheck) Run(_ *CheckContext) *CheckResult {
	return &CheckResult{
		Name:    "unchanged-fix",
		Status:  StatusError,
		Message: "still bad",
	}
}
func (c *unchangedFixCheck) CanFix() bool              { return true }
func (c *unchangedFixCheck) Fix(_ *CheckContext) error { return nil }
