package doctor

import (
	"fmt"
	"os/exec"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/gastownhall/gascity/internal/config"
)

// BinaryFreshnessCheck warns when the running gc build predates the newest
// commit on its source repository's fetched default branch — merged fixes that
// are not executing anywhere.
//
// This is the missing link in a three-part chain, and it is deliberately not
// the same question either existing drift signal answers:
//
//	origin/main  ──(this check)──▶  on-disk binary  ──(gc start)──▶  supervisor
//
// `gc start`'s DetectBinaryDrift compares the supervisor's reported buildID
// against the local binary's, catching a supervisor running a stale image. It
// cannot see the failure mode here, where supervisor and on-disk binary agree
// perfectly and BOTH are days behind main. That state reports clean everywhere:
// the binary's mtime is recent enough to look plausible, the bead is closed, and
// the PR is merged, so the only evidence is code that silently is not running.
//
// The check identifies the source repository by asking which configured rig
// CONTAINS the build commit, rather than by matching a name, path, or remote
// URL. That keeps it fork-agnostic — this repo is regularly built from a fork
// whose origin differs from the module path — and keeps any repository identity
// out of Go, which a name match would smuggle in.
//
// It never fetches. Comparing against the last-fetched remote-tracking ref keeps
// the check free of network I/O and side effects, at the cost of understating
// drift when the checkout itself is stale; the finding names the ref it read so
// the reading is never mistaken for a live comparison.
//
// Severity is advisory, not blocking. The remedy is a rebuild plus a supervisor
// restart, and that restart bounces the tmux server hosting every agent session
// — an operator decision, not something a gate should force. Detection is the
// safe half.
type BinaryFreshnessCheck struct {
	rigs          []config.Rig
	buildRevision string
	gitPath       func(name string) (string, error) // injectable for tests
}

// NewBinaryFreshnessCheckForConfig creates the check over a city's configured
// rigs, reading the running binary's stamped VCS revision.
func NewBinaryFreshnessCheckForConfig(cfg *config.City, cfgErr error) *BinaryFreshnessCheck {
	var rigs []config.Rig
	if cfgErr == nil && cfg != nil {
		rigs = cfg.Rigs
	}
	return &BinaryFreshnessCheck{
		rigs:          rigs,
		buildRevision: runningBuildRevision(),
		gitPath:       exec.LookPath,
	}
}

// NewBinaryFreshnessCheckForRigs creates the check over an explicit rig list
// and build revision. Used by tests.
func NewBinaryFreshnessCheckForRigs(rigs []config.Rig, buildRevision string) *BinaryFreshnessCheck {
	return &BinaryFreshnessCheck{rigs: rigs, buildRevision: buildRevision, gitPath: exec.LookPath}
}

// Name returns the check identifier.
func (c *BinaryFreshnessCheck) Name() string { return "binary-freshness" }

// WarmupEligible returns false: this is a steady-state hygiene signal, and its
// remedy restarts the town, so it must not gate `gc start`.
func (c *BinaryFreshnessCheck) WarmupEligible() bool { return false }

// CanFix returns false: rebuilding and restarting the supervisor is an
// operator action with town-wide blast radius.
func (c *BinaryFreshnessCheck) CanFix() bool { return false }

// Fix is a no-op; the check is report-only.
func (c *BinaryFreshnessCheck) Fix(_ *CheckContext) error { return nil }

// runningBuildRevision returns the vcs.revision stamped into this binary, or
// "" when the toolchain recorded none (-buildvcs=false, or a synthesized
// build). An unstamped build is not a finding: it is a build this check simply
// cannot reason about.
func runningBuildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info == nil {
		return ""
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return strings.TrimSpace(setting.Value)
		}
	}
	return ""
}

// Run reports whether the running build is behind its source repo's fetched
// default branch.
func (c *BinaryFreshnessCheck) Run(_ *CheckContext) *CheckResult {
	r := &CheckResult{Name: c.Name(), Severity: SeverityAdvisory}

	if c.buildRevision == "" {
		r.Status = StatusOK
		r.Message = "running build stamps no vcs.revision — cannot compare against origin"
		return r
	}

	gitBin, err := c.gitPath("git")
	if err != nil {
		r.Status = StatusOK
		r.Message = "git unavailable — cannot compare the running build against origin"
		return r
	}

	rig, ok := c.sourceRig(gitBin)
	if !ok {
		r.Status = StatusOK
		r.Message = fmt.Sprintf("build commit %s is in no configured rig — cannot locate the source repo",
			shortRevision(c.buildRevision))
		return r
	}

	branch := rig.EffectiveDefaultBranch()
	if branch == "" {
		branch = defaultBranchFallback
	}
	trackingRef := "origin/" + branch

	if _, err := runGitCommand(gitBin, rig.Path, "rev-parse", "--verify", "--quiet", trackingRef+"^{commit}"); err != nil {
		r.Status = StatusOK
		r.Message = fmt.Sprintf("rig %q has no %s — nothing fetched to compare the running build against",
			rig.Name, trackingRef)
		return r
	}

	behindOut, err := runGitCommand(gitBin, rig.Path, "rev-list", "--count", c.buildRevision+".."+trackingRef)
	if err != nil {
		r.Status = StatusOK
		r.Message = fmt.Sprintf("rig %q: cannot count commits between the running build and %s",
			rig.Name, trackingRef)
		return r
	}
	behind, err := strconv.Atoi(strings.TrimSpace(behindOut))
	if err != nil {
		r.Status = StatusOK
		r.Message = fmt.Sprintf("rig %q: unreadable commit count for %s", rig.Name, trackingRef)
		return r
	}

	buildTime := commitTime(gitBin, rig.Path, c.buildRevision)
	originTime := commitTime(gitBin, rig.Path, trackingRef)

	if behind == 0 {
		r.Status = StatusOK
		r.Message = fmt.Sprintf("running build %s is current with %s in rig %q",
			shortRevision(c.buildRevision), trackingRef, rig.Name)
		return r
	}

	r.Status = StatusWarning
	r.Message = fmt.Sprintf("running build %s (%s) is %d commit(s) behind %s (%s) in rig %q — those fixes are not executing",
		shortRevision(c.buildRevision), buildTime, behind, trackingRef, originTime, rig.Name)
	r.Details = []string{
		fmt.Sprintf("build commit:  %s  %s", c.buildRevision, buildTime),
		fmt.Sprintf("%s: %s", trackingRef, originTime),
		fmt.Sprintf("compared against the last fetch of %s; run git -C %q fetch for a current reading",
			trackingRef, rig.Path),
	}
	r.FixHint = fmt.Sprintf("rebuild and restart together — a rebuild alone leaves the supervisor on a deleted inode: "+
		"(cd %q && go build -o \"$(command -v gc)\" ./cmd/gc) && systemctl --user restart gascity-supervisor.service "+
		"(the restart bounces every agent session — operator's call)", rig.Path)
	return r
}

// sourceRig returns the first configured rig whose object database contains the
// running build's commit. Containment is the identity test precisely because it
// survives forks and renames: the repo that can resolve the commit is the repo
// the binary was built from.
func (c *BinaryFreshnessCheck) sourceRig(gitBin string) (config.Rig, bool) {
	for _, rig := range c.rigs {
		if strings.TrimSpace(rig.Path) == "" {
			continue
		}
		if _, err := runGitCommand(gitBin, rig.Path, "cat-file", "-e", c.buildRevision+"^{commit}"); err == nil {
			return rig, true
		}
	}
	return config.Rig{}, false
}

// commitTime returns a commit's author date in RFC3339, or "unknown time" when
// git cannot resolve it. A missing timestamp degrades the message rather than
// the finding.
func commitTime(gitBin, dir, rev string) string {
	out, err := runGitCommand(gitBin, dir, "log", "-1", "--format=%cI", rev)
	if err != nil {
		return "unknown time"
	}
	if trimmed := strings.TrimSpace(out); trimmed != "" {
		return trimmed
	}
	return "unknown time"
}

// shortRevision abbreviates a full SHA for operator-facing messages, matching
// git's default abbreviation length.
func shortRevision(rev string) string {
	if len(rev) > 7 {
		return rev[:7]
	}
	return rev
}
