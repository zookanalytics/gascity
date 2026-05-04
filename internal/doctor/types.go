// Package doctor provides system health diagnostics for a Gas City workspace.
// It defines a Check interface and runner that executes checks with streaming
// output, optional --fix support, and a summary report.
package doctor

// CheckStatus represents the outcome of a health check.
type CheckStatus int

const (
	// StatusOK means the check passed.
	StatusOK CheckStatus = iota
	// StatusWarning means the check found a non-critical issue.
	StatusWarning
	// StatusError means the check found a critical problem.
	StatusError
)

// String returns the lowercase name of the status, used in JSON output and
// any other machine-readable surface. The mapping is part of the JSON
// contract: "ok", "warning", "error", or "unknown" for unrecognized values.
func (s CheckStatus) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusWarning:
		return "warning"
	case StatusError:
		return "error"
	default:
		return "unknown"
	}
}

// Check is a single diagnostic check. Implementations are registered with
// a Doctor and executed sequentially during Run.
type Check interface {
	// Name returns a short, unique identifier for this check (e.g. "city-config").
	Name() string
	// Run executes the check and returns a result.
	Run(ctx *CheckContext) *CheckResult
	// CanFix reports whether this check supports automatic remediation.
	CanFix() bool
	// Fix attempts to automatically remediate the issue found by Run.
	// Only called when CanFix returns true and Run returned a non-OK status.
	Fix(ctx *CheckContext) error
}

// CheckContext carries shared state for all checks during a doctor run.
type CheckContext struct {
	// CityPath is the absolute path to the city root directory.
	CityPath string
	// Verbose enables extra diagnostic output in check results.
	Verbose bool
}

// CheckResult holds the outcome of a single check execution.
//
// JSON tags define the wire shape used by `gc doctor --json`. The schema
// is documented in engdocs/contributors/doctor-json.md and is the
// machine-stable contract referenced by the human-output design note in
// engdocs/design/beads-dolt-contract-redesign.md.
type CheckResult struct {
	// Name identifies which check produced this result.
	Name string `json:"name"`
	// Status is the outcome: OK, Warning, or Error. Marshaled as the
	// lowercase string returned by CheckStatus.String — see MarshalJSON.
	Status CheckStatus `json:"status"`
	// Message is a human-readable summary of the result.
	Message string `json:"message"`
	// Details holds extra lines shown only in verbose mode. JSON consumers
	// always see the full slice regardless of verbosity, since structured
	// output is the entire point of --json.
	Details []string `json:"details,omitempty"`
	// FixHint is a suggestion shown when the check fails and cannot auto-fix.
	FixHint string `json:"fix_hint,omitempty"`
	// FixError describes why an attempted automatic remediation failed.
	FixError string `json:"fix_error,omitempty"`
	// FixAttempted is true when automatic remediation ran but did not
	// leave the check passing.
	FixAttempted bool `json:"fix_attempted"`
	// Fixed is true when --fix successfully remediated the issue.
	Fixed bool `json:"fixed"`
}
