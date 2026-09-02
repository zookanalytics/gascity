package main

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"text/tabwriter"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/worktree"
	"github.com/spf13/cobra"
)

// worktreeCmdOpts carries the flag values for gc worktree subcommands.
type worktreeCmdOpts struct {
	Repo       string
	Root       string
	Path       string
	Branch     string
	Base       string
	BaseSHA    string
	BeadID     string
	StoreRef   string
	Creator    string
	Owner      string
	Generation string
	Lifecycle  string
	AttemptID  string
	DryRun     bool
	JSON       bool
}

// newWorktreeCmd returns the gc worktree command group. It is the CLI face
// of internal/worktree — the transactional workspace owner (gc-r9fx) that
// sling and formula-managed workspace setup can route through instead of
// running competing ad hoc provisioning.
func newWorktreeCmd(stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worktree",
		Short: "Ensure, verify, and reclaim agent workspace worktrees",
		Long: `Ensure, verify, and reclaim agent workspace worktrees.

gc worktree is the single transactional owner for workspace provisioning.
Postconditions: the path is a direct child of the configured per-rig root and
the root of a worktree of the given repository, with the bead's uniquely named
branch checked out on an attached HEAD (never detached). Durable provenance is
stored in the worktree's private git directory and returned as JSON so callers
can atomically publish the same evidence on the bead. A new branch is created
from --base, resolved verbatim against the local repository. Failed creation
rolls back everything it created; --dry-run plans without mutating anything.

ensure, verify and cleanup each act on one named worktree. reap is the bulk
counterpart: it classifies every per-bead worktree in the city against its work
bead and removes the finished ones.`,
	}
	cmd.AddCommand(newWorktreeEnsureCmd(stdout, stderr))
	cmd.AddCommand(newWorktreeVerifyCmd(stdout, stderr))
	cmd.AddCommand(newWorktreeCleanupCmd(stdout, stderr))
	cmd.AddCommand(newWorktreeReapCmd(stdout, stderr))
	return cmd
}

func worktreeFlagSet(cmd *cobra.Command, opts *worktreeCmdOpts) {
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "repository directory the worktree belongs to (required)")
	cmd.Flags().StringVar(&opts.Root, "root", "", "configured per-rig worktree root; path must be its direct child (required)")
	cmd.Flags().StringVar(&opts.Path, "path", "", "worktree path (required)")
	cmd.Flags().StringVar(&opts.Branch, "branch", "", "branch that must be checked out (required)")
	cmd.Flags().StringVar(&opts.Base, "base", "", "exact base ref used for this worktree (required)")
	cmd.Flags().StringVar(&opts.BaseSHA, "base-sha", "", "recorded base SHA to verify when reusing a worktree")
	cmd.Flags().StringVar(&opts.BeadID, "bead", "", "work bead bound to this worktree (required)")
	cmd.Flags().StringVar(&opts.StoreRef, "store-ref", "", "work bead store reference (required)")
	cmd.Flags().StringVar(&opts.Creator, "creator", "", "mechanism creating the worktree (required)")
	cmd.Flags().StringVar(&opts.Owner, "owner", "", "single selected provisioning owner (required)")
	cmd.Flags().StringVar(&opts.Generation, "generation", "", "provisioning generation fence (required)")
	cmd.Flags().StringVar(&opts.Lifecycle, "lifecycle", worktree.LifecycleActive, "worktree lifecycle state")
	cmd.Flags().BoolVar(&opts.JSON, "json", false, "emit the report as JSON")
	for _, name := range []string{
		"repo", "root", "path", "branch", "base", "bead", "store-ref", "creator", "owner", "generation",
	} {
		_ = cmd.MarkFlagRequired(name) //nolint:errcheck // flags exist
	}
}

func newWorktreeEnsureCmd(stdout, stderr io.Writer) *cobra.Command {
	var opts worktreeCmdOpts
	cmd := &cobra.Command{
		Use:   "ensure",
		Short: "Ensure the worktree exists and satisfies all postconditions",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if runWorktreeEnsure(opts, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	worktreeFlagSet(cmd, &opts)
	cmd.Flags().BoolVarP(&opts.DryRun, "dry-run", "n", false, "plan without mutating anything")
	return cmd
}

func newWorktreeVerifyCmd(stdout, stderr io.Writer) *cobra.Command {
	var opts worktreeCmdOpts
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify the worktree satisfies all postconditions without mutating",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if runWorktreeVerify(opts, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	worktreeFlagSet(cmd, &opts)
	return cmd
}

func newWorktreeCleanupCmd(stdout, stderr io.Writer) *cobra.Command {
	var opts worktreeCmdOpts
	cmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Remove an owned worktree after all safety gates pass",
		Long: `Remove an owned worktree after all safety gates pass.

Cleanup verifies the canonical repository, path, branch, and durable ownership
provenance before acting. It refuses dirty worktrees, commits reachable from no
branch, tag, or remote-tracking ref, and commits not merged into --base.
--attempt-id binds the removal to one exact provisioning attempt, so a stale
request cannot remove a workspace re-created at the same path. There is no
force mode and no recursive-filesystem fallback. An already-absent,
unregistered path is an idempotent success. With --json, safety refusals return
a structured cleanup_pending result for formula automation.`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if runWorktreeCleanup(opts, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	worktreeFlagSet(cmd, &opts)
	cmd.Flags().StringVar(&opts.AttemptID, "attempt-id", "",
		"attempt id returned by the ensure that created this worktree (required)")
	_ = cmd.MarkFlagRequired("attempt-id")
	return cmd
}

func (o worktreeCmdOpts) spec() (worktree.Spec, error) {
	repo, err := filepath.Abs(o.Repo)
	if err != nil {
		return worktree.Spec{}, fmt.Errorf("resolving --repo %q: %w", o.Repo, err)
	}
	path, err := filepath.Abs(o.Path)
	if err != nil {
		return worktree.Spec{}, fmt.Errorf("resolving --path %q: %w", o.Path, err)
	}
	root, err := filepath.Abs(o.Root)
	if err != nil {
		return worktree.Spec{}, fmt.Errorf("resolving --root %q: %w", o.Root, err)
	}
	return worktree.Spec{
		RepoDir:    repo,
		Root:       root,
		Path:       path,
		Branch:     o.Branch,
		Base:       o.Base,
		BaseSHA:    o.BaseSHA,
		BeadID:     o.BeadID,
		StoreRef:   o.StoreRef,
		Creator:    o.Creator,
		Owner:      o.Owner,
		Generation: o.Generation,
		Lifecycle:  o.Lifecycle,
		AttemptID:  o.AttemptID,
		DryRun:     o.DryRun,
	}, nil
}

func runWorktreeEnsure(opts worktreeCmdOpts, stdout, stderr io.Writer) int {
	spec, err := opts.spec()
	if err != nil {
		fmt.Fprintf(stderr, "gc worktree ensure: %v\n", err) //nolint:errcheck
		return 1
	}
	rep, err := worktree.Ensure(spec)
	if err != nil {
		fmt.Fprintf(stderr, "gc worktree ensure: %v\n", err) //nolint:errcheck
		return 1
	}
	return writeWorktreeReport("ensure", rep, opts, stdout, stderr)
}

func runWorktreeVerify(opts worktreeCmdOpts, stdout, stderr io.Writer) int {
	spec, err := opts.spec()
	if err != nil {
		fmt.Fprintf(stderr, "gc worktree verify: %v\n", err) //nolint:errcheck
		return 1
	}
	rep, err := worktree.Verify(spec)
	if err != nil {
		fmt.Fprintf(stderr, "gc worktree verify: %v\n", err) //nolint:errcheck
		return 1
	}
	return writeWorktreeReport("verify", rep, opts, stdout, stderr)
}

func runWorktreeCleanup(opts worktreeCmdOpts, stdout, stderr io.Writer) int {
	spec, err := opts.spec()
	if err != nil {
		report := worktree.CleanupReport{
			Path:           opts.Path,
			Branch:         opts.Branch,
			CleanupPending: true,
			Error: &worktree.CleanupError{
				Code:     worktree.CleanupErrorInvalidSpec,
				Message:  err.Error(),
				ExitCode: 1,
			},
		}
		return writeWorktreeCleanupReport(report, opts, stdout, stderr)
	}
	report, cleanupErr := worktree.Cleanup(spec)
	code := writeWorktreeCleanupReport(report, opts, stdout, stderr)
	if cleanupErr != nil {
		return 1
	}
	return code
}

type worktreeJSONResult struct {
	SchemaVersion string `json:"schema_version"`
	OK            bool   `json:"ok"`
	Command       string `json:"command"`
	Action        string `json:"action"`
	worktree.Report
}

func writeWorktreeReport(action string, rep worktree.Report, opts worktreeCmdOpts, stdout, stderr io.Writer) int {
	if opts.JSON {
		result := worktreeJSONResult{
			SchemaVersion: "1",
			OK:            true,
			Command:       "worktree " + action,
			Action:        action,
			Report:        rep,
		}
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			fmt.Fprintf(stderr, "gc worktree: encoding report: %v\n", err) //nolint:errcheck
			return 1
		}
		return 0
	}
	switch {
	case len(rep.Planned) > 0:
		fmt.Fprintf(stdout, "would run (dry-run):\n") //nolint:errcheck
		for _, action := range rep.Planned {
			fmt.Fprintf(stdout, "  %s\n", action) //nolint:errcheck
		}
	case rep.Created:
		fmt.Fprintf(stdout, "created worktree %s on branch %s at %s\n", rep.Path, rep.Branch, rep.Head) //nolint:errcheck
	default:
		fmt.Fprintf(stdout, "worktree %s on branch %s at %s\n", rep.Path, rep.Branch, rep.Head) //nolint:errcheck
	}
	return 0
}

type worktreeCleanupJSONResult struct {
	SchemaVersion string `json:"schema_version"`
	OK            bool   `json:"ok"`
	Command       string `json:"command"`
	Action        string `json:"action"`
	worktree.CleanupReport
}

func writeWorktreeCleanupReport(rep worktree.CleanupReport, opts worktreeCmdOpts, stdout, stderr io.Writer) int {
	if opts.JSON {
		result := worktreeCleanupJSONResult{
			SchemaVersion: "1",
			OK:            rep.Error == nil,
			Command:       "worktree cleanup",
			Action:        "cleanup",
			CleanupReport: rep,
		}
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			fmt.Fprintf(stderr, "gc worktree cleanup: encoding report: %v\n", err) //nolint:errcheck
			return 1
		}
		if rep.Error != nil {
			fmt.Fprintf(stderr, "gc worktree cleanup: %s\n", rep.Error.Message) //nolint:errcheck
			return 1
		}
		return 0
	}
	if rep.Error != nil {
		fmt.Fprintf(stderr, "gc worktree cleanup: %s\n", rep.Error.Message) //nolint:errcheck
		return 1
	}
	switch {
	case rep.AlreadyAbsent:
		fmt.Fprintf(stdout, "worktree %s is already absent\n", rep.Path) //nolint:errcheck
	case rep.Removed:
		fmt.Fprintf(stdout, "removed worktree %s on branch %s\n", rep.Path, rep.Branch) //nolint:errcheck
	default:
		fmt.Fprintf(stdout, "worktree %s required no cleanup\n", rep.Path) //nolint:errcheck
	}
	return 0
}

const worktreeReapCmdName = "gc worktree reap"

func newWorktreeReapCmd(stdout, stderr io.Writer) *cobra.Command {
	var (
		apply   bool
		jsonOut bool
	)
	cmd := &cobra.Command{
		Use:   "reap",
		Short: "Remove per-bead worktrees whose work bead is closed",
		Long: `Classify every per-bead worktree under .gc/worktrees/<rig>/ and remove the
ones whose work bead is closed and that pass every safety gate: past the
configured freshness quarantine, unreferenced by any non-terminal bead, no live
process or open session working in the tree, and a clean git state that removal
would not orphan commits from. Every gate fails closed — an indeterminate answer
protects the tree.

This is the same classification the reconciler patrol runs when
[daemon] auto_reap_closed_bead_worktrees is enabled, on demand and without
changing the city's configuration. It reports what it would remove and removes
nothing until --apply is passed.`,
		Example: `  gc worktree reap
  gc worktree reap --apply
  gc worktree reap --json`,
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			return exitForCode(cmdWorktreeReap(apply, jsonOut, stdout, stderr))
		},
	}
	cmd.Flags().BoolVar(&apply, "apply", false, "remove the eligible worktrees (default: report only)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit machine-readable JSON")
	return cmd
}

// worktreeReapScope is the resolved city state one reap pass acts on. It is a
// struct rather than four parameters so the command's resolution step and its
// rendering step can be tested apart from each other.
type worktreeReapScope struct {
	cityPath        string
	cfg             *config.City
	rigStores       map[string]beads.Store
	liveSessionDirs []string
}

// worktreeReapEntryJSON is one worktree's verdict in the --json report. Reason
// is set on protected entries and empty on reaped ones.
type worktreeReapEntryJSON struct {
	BeadID string `json:"bead_id"`
	Rig    string `json:"rig"`
	Path   string `json:"path"`
	Branch string `json:"branch,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// worktreeReapJSON is the machine-readable form of one reap pass. DryRun true
// means Reaped lists what --apply would have removed.
type worktreeReapJSON struct {
	DryRun         bool                    `json:"dry_run"`
	LivenessSource string                  `json:"liveness_source,omitempty"`
	Reaped         []worktreeReapEntryJSON `json:"reaped"`
	Protected      []worktreeReapEntryJSON `json:"protected"`
	Errors         []string                `json:"errors,omitempty"`
}

func cmdWorktreeReap(apply, jsonOut bool, stdout, stderr io.Writer) int {
	scope, code := resolveWorktreeReapScope(stderr)
	if code != 0 {
		return code
	}
	// Removals are recorded to the city event log so a manual reclaim is
	// visible to `gc events` alongside the patrol's. A dry run is a read-only
	// question and records nothing: its would-reap and protected verdicts are
	// the same edge-triggered chatter the patrol suppresses, and an operator
	// asking what is reclaimable should not write to the log to find out.
	rec := events.Discard
	if apply {
		if ep, _ := openCityEventEmitProvider(stderr, worktreeReapCmdName); ep != nil {
			defer ep.Close() //nolint:errcheck // best-effort flush
			rec = ep
		}
	}
	return runWorktreeReap(scope, apply, jsonOut, rec, stdout, stderr)
}

// resolveWorktreeReapScope locates the city, loads its configuration with full
// pack expansion (the reaper reads cfg.Agents to guard agent-home directories),
// and opens the work store of every bound rig. A rig whose store will not open
// is fatal rather than skipped: a missing store makes every worktree in that rig
// unresolvable, which would read as "nothing to reap" for the rig that most
// needs looking at.
func resolveWorktreeReapScope(stderr io.Writer) (worktreeReapScope, int) {
	cityPath, err := resolveCity()
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", worktreeReapCmdName, err) //nolint:errcheck // best-effort stderr
		return worktreeReapScope{}, 2
	}
	cfg, err := loadCityConfig(cityPath, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", worktreeReapCmdName, err) //nolint:errcheck // best-effort stderr
		return worktreeReapScope{}, 1
	}
	stores, failures := openStandaloneRigStores(cfg, cityPath)
	if len(failures) > 0 {
		for _, f := range failures {
			fmt.Fprintf(stderr, "%s: rig %q store: %v\n", worktreeReapCmdName, f.rig, f.err) //nolint:errcheck // best-effort stderr
		}
		return worktreeReapScope{}, 1
	}
	scope := worktreeReapScope{cityPath: cityPath, cfg: cfg, rigStores: stores}
	// The liveness gate's authoritative signal is the /proc cwd scan the reaper
	// runs itself; the open-session working directories are the second, weaker
	// signal it cross-checks against. A store that will not open costs the
	// cross-check, not the gate, so it warns rather than failing the command.
	if store, err := openCityStoreAt(cityPath); err != nil {
		fmt.Fprintf(stderr, "%s: warning: open sessions not cross-checked: %v\n", worktreeReapCmdName, err) //nolint:errcheck // best-effort stderr
	} else if snapshot, err := loadSessionBeadSnapshot(store); err != nil {
		fmt.Fprintf(stderr, "%s: warning: open sessions not cross-checked: %v\n", worktreeReapCmdName, err) //nolint:errcheck // best-effort stderr
	} else {
		scope.liveSessionDirs = liveSessionWorktreeDirs(snapshot)
	}
	return scope, 0
}

// runWorktreeReap classifies the scope's worktrees and, when apply is set,
// removes the eligible ones, then renders the outcome. It returns 1 when the
// pass could not scan a rig or could not complete a removal, so a scripted
// caller does not read a partial pass as a clean one.
//
// The reaper's own stderr log is discarded: reapReport carries every verdict
// and every failure the log names, so forwarding it would print the answer
// twice on an operator's terminal.
func runWorktreeReap(scope worktreeReapScope, apply, jsonOut bool, rec events.Recorder, stdout, stderr io.Writer) int {
	if len(scope.rigStores) == 0 {
		if jsonOut {
			return writeWorktreeReapJSON(stdout, stderr, worktreeReapJSON{DryRun: !apply, Reaped: []worktreeReapEntryJSON{}, Protected: []worktreeReapEntryJSON{}})
		}
		fmt.Fprintln(stdout, "No worktrees to classify: the city has no bound rigs.") //nolint:errcheck // best-effort stdout
		return 0
	}

	report := reapClosedBeadWorktrees(scope.cityPath, scope.cfg, scope.rigStores, scope.liveSessionDirs, !apply, rec, nil, io.Discard)

	if report.LivenessSource != "" && report.LivenessSource != liveScanSourceProc {
		fmt.Fprintf(stderr, "%s: liveness scanned via %s (/proc unavailable)\n", worktreeReapCmdName, report.LivenessSource) //nolint:errcheck // best-effort stderr
	}

	code := 0
	for _, msg := range report.Errors {
		fmt.Fprintf(stderr, "%s: %s\n", worktreeReapCmdName, msg) //nolint:errcheck // best-effort stderr
		code = 1
	}

	if jsonOut {
		if jsonCode := writeWorktreeReapJSON(stdout, stderr, worktreeReapJSON{
			DryRun:         report.DryRun,
			LivenessSource: report.LivenessSource,
			Reaped:         worktreeReapEntries(report.Reaped),
			Protected:      worktreeReapEntries(report.Protected),
			Errors:         report.Errors,
		}); jsonCode != 0 {
			return jsonCode
		}
		return code
	}
	renderWorktreeReapText(report, stdout)
	return code
}

// worktreeReapEntries projects the reaper's decisions onto the JSON shape,
// returning an empty (not nil) slice so the encoded report always carries both
// arrays. Entries are ordered by rig then path — see sortReapDecisions.
func worktreeReapEntries(decisions []reapDecision) []worktreeReapEntryJSON {
	entries := make([]worktreeReapEntryJSON, 0, len(decisions))
	for _, d := range sortReapDecisions(decisions) {
		entries = append(entries, worktreeReapEntryJSON{
			BeadID: d.BeadID,
			Rig:    d.Rig,
			Path:   d.Path,
			Branch: d.Branch,
			Reason: d.Reason,
		})
	}
	return entries
}

// sortReapDecisions returns decisions ordered by rig then path, in a fresh
// slice so the caller's report is left as the reaper produced it.
//
// The reaper walks rigs in Go map order, which is randomized per run, so the
// same city reports its rigs in a different order every time. The natural way
// to read this command is to run it, act, and run it again — diffing the two
// answers — and shuffled rig blocks make that diff unreadable.
func sortReapDecisions(decisions []reapDecision) []reapDecision {
	sorted := make([]reapDecision, len(decisions))
	copy(sorted, decisions)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Rig != sorted[j].Rig {
			return sorted[i].Rig < sorted[j].Rig
		}
		return sorted[i].Path < sorted[j].Path
	})
	return sorted
}

func writeWorktreeReapJSON(stdout, stderr io.Writer, payload worktreeReapJSON) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(payload); err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", worktreeReapCmdName, err) //nolint:errcheck // best-effort stderr
		return 1
	}
	return 0
}

// renderWorktreeReapText writes the human report: one line per worktree with
// its verdict, then a summary. The verdict column reads "would reap" in a dry
// run and "reaped" once the removal happened, so the two passes are never
// mistaken for each other in a scrollback.
func renderWorktreeReapText(report reapReport, stdout io.Writer) {
	verdict := "reaped"
	if report.DryRun {
		verdict = "would reap"
	}
	if len(report.Reaped) == 0 && len(report.Protected) == 0 {
		fmt.Fprintln(stdout, "No closed-bead worktrees found.") //nolint:errcheck // best-effort stdout
		return
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	for _, d := range sortReapDecisions(report.Reaped) {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", verdict, d.Rig, d.BeadID, d.Path) //nolint:errcheck // best-effort stdout
	}
	for _, d := range sortReapDecisions(report.Protected) {
		fmt.Fprintf(tw, "protected\t%s\t%s\t%s\t%s\n", d.Rig, d.BeadID, d.Path, d.Reason) //nolint:errcheck // best-effort stdout
	}
	_ = tw.Flush()
	fmt.Fprintln(stdout, worktreeReapSummary(report)) //nolint:errcheck // best-effort stdout
}

// worktreeReapSummary is the closing line of the text report: what the pass did
// or would do, and — in a dry run that found something — how to act on it.
func worktreeReapSummary(report reapReport) string {
	protected := ""
	if len(report.Protected) > 0 {
		protected = fmt.Sprintf(", %d protected", len(report.Protected))
	}
	if len(report.Reaped) == 0 {
		return fmt.Sprintf("No closed-bead worktrees are eligible for reaping%s.", protected)
	}
	if report.DryRun {
		return fmt.Sprintf("%d reclaimable%s. Nothing removed — re-run with --apply to reclaim.", len(report.Reaped), protected)
	}
	return fmt.Sprintf("Reaped %d worktree(s)%s.", len(report.Reaped), protected)
}
