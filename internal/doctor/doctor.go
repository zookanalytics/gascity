package doctor

import (
	"encoding/json"
	"fmt"
	"io"
)

// Report summarizes the results of a doctor run. JSON tags define the
// wire shape used by `gc doctor --json` (see engdocs/contributors/doctor-json.md).
type Report struct {
	// Passed is the number of checks with StatusOK.
	Passed int `json:"passed"`
	// Warned is the number of checks with StatusWarning.
	Warned int `json:"warned"`
	// Failed is the number of checks with StatusError.
	Failed int `json:"failed"`
	// Fixed is the number of checks remediated by --fix.
	Fixed int `json:"fixed"`
}

// MarshalJSON renders a CheckStatus as its lowercase string form so
// JSON consumers receive "ok"/"warning"/"error" instead of an integer
// whose meaning is not self-evident on the wire.
func (s CheckStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON accepts the lowercase string form emitted by MarshalJSON
// so doctor JSON output can round-trip through Go consumers. Unknown
// values resolve to a sentinel that String() reports as "unknown".
func (s *CheckStatus) UnmarshalJSON(data []byte) error {
	var token string
	if err := json.Unmarshal(data, &token); err != nil {
		return fmt.Errorf("CheckStatus: %w", err)
	}
	switch token {
	case "ok":
		*s = StatusOK
	case "warning":
		*s = StatusWarning
	case "error":
		*s = StatusError
	default:
		return fmt.Errorf("CheckStatus: unknown status %q", token)
	}
	return nil
}

// JSONOutput is the top-level shape emitted by `gc doctor --json`. It pairs
// the per-check results with the summary so consumers receive a single
// document on stdout.
type JSONOutput struct {
	Checks  []*CheckResult `json:"checks"`
	Summary *Report        `json:"summary"`
}

// Doctor runs registered health checks and reports results.
type Doctor struct {
	checks []Check
}

// Register adds a check to the doctor's check list.
func (d *Doctor) Register(c Check) {
	d.checks = append(d.checks, c)
}

// Run executes all registered checks, streaming results to w as each
// completes. When fix is true, fixable checks that fail are remediated
// and re-run. Returns a summary report.
func (d *Doctor) Run(ctx *CheckContext, w io.Writer, fix bool) *Report {
	results, report := d.RunCollect(ctx, fix)
	for _, result := range results {
		printResult(w, result, ctx.Verbose)
	}
	return report
}

// RunCollect executes all registered checks and returns the per-check
// results plus a summary report without writing anything. Callers that
// want streaming human output should use Run; callers that want
// machine-readable output (e.g. `gc doctor --json`) should use this and
// then invoke RenderJSON.
func (d *Doctor) RunCollect(ctx *CheckContext, fix bool) ([]*CheckResult, *Report) {
	results := make([]*CheckResult, 0, len(d.checks))
	r := &Report{}
	for _, c := range d.checks {
		result := c.Run(ctx)

		// Attempt fix if requested and the check supports it.
		if fix && result.Status != StatusOK && c.CanFix() {
			if err := c.Fix(ctx); err == nil {
				// Re-run to verify the fix worked.
				result = c.Run(ctx)
				if result.Status == StatusOK {
					result.Fixed = true
				} else {
					result.FixAttempted = true
				}
			} else {
				result.FixError = err.Error()
				result.FixAttempted = true
			}
		}

		results = append(results, result)

		switch {
		case result.Fixed:
			r.Fixed++
			r.Passed++ // Fixed counts as passed.
		case result.Status == StatusOK:
			r.Passed++
		case result.Status == StatusWarning:
			r.Warned++
		case result.Status == StatusError:
			r.Failed++
		}
	}
	return results, r
}

// RenderJSON writes a single JSON document containing the per-check
// results and the summary report. The output is indented for readability;
// tooling should treat newlines and indentation as cosmetic and consume
// the document as a whole. The schema is the wire contract for
// `gc doctor --json`.
func RenderJSON(w io.Writer, results []*CheckResult, report *Report) error {
	if results == nil {
		results = []*CheckResult{}
	}
	out := JSONOutput{Checks: results, Summary: report}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// printResult writes a single check result line to w.
func printResult(w io.Writer, r *CheckResult, verbose bool) {
	var icon string
	switch {
	case r.Fixed:
		icon = "✓" // Fixed shows as pass.
	case r.Status == StatusOK:
		icon = "✓"
	case r.Status == StatusWarning:
		icon = "⚠"
	case r.Status == StatusError:
		icon = "✗"
	}

	suffix := ""
	if r.Fixed {
		suffix = " (fixed)"
	}
	fmt.Fprintf(w, "  %s %s — %s%s\n", icon, r.Name, r.Message, suffix) //nolint:errcheck // best-effort output
	if verbose {
		for _, d := range r.Details {
			fmt.Fprintf(w, "      %s\n", d) //nolint:errcheck // best-effort output
		}
	}
	if r.FixError != "" && r.Status != StatusOK && !r.Fixed {
		fmt.Fprintf(w, "      fix failed: %s\n", r.FixError) //nolint:errcheck // best-effort output
	} else if r.FixAttempted && r.Status != StatusOK && !r.Fixed {
		fmt.Fprintf(w, "      fix attempted; check still failing\n") //nolint:errcheck // best-effort output
	}
	if r.FixHint != "" && r.Status != StatusOK && !r.Fixed {
		fmt.Fprintf(w, "      hint: %s\n", r.FixHint) //nolint:errcheck // best-effort output
	}
}

// PrintSummary writes the final summary line to w.
func PrintSummary(w io.Writer, r *Report) {
	parts := []string{}
	if r.Passed > 0 {
		parts = append(parts, fmt.Sprintf("%d passed", r.Passed))
	}
	if r.Warned > 0 {
		parts = append(parts, fmt.Sprintf("%d warnings", r.Warned))
	}
	if r.Failed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", r.Failed))
	}
	if r.Fixed > 0 {
		parts = append(parts, fmt.Sprintf("%d fixed", r.Fixed))
	}
	if len(parts) == 0 {
		fmt.Fprintln(w, "\nNo checks ran.") //nolint:errcheck // best-effort output
		return
	}
	fmt.Fprintf(w, "\n") //nolint:errcheck // best-effort output
	for i, p := range parts {
		if i > 0 {
			fmt.Fprintf(w, ", ") //nolint:errcheck // best-effort output
		}
		fmt.Fprintf(w, "%s", p) //nolint:errcheck // best-effort output
	}
	fmt.Fprintf(w, "\n") //nolint:errcheck // best-effort output
}
