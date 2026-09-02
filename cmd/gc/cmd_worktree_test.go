package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/pathutil"
	"github.com/gastownhall/gascity/internal/testutil"
	"github.com/gastownhall/gascity/internal/worktree"
)

func worktreeTestRepo(t *testing.T) (string, string) {
	t.Helper()
	return testutil.InitGitRepo(t)
}

func managedWorktreeCmdOpts(repo, root, path, base string) worktreeCmdOpts {
	return worktreeCmdOpts{
		Repo:       repo,
		Root:       root,
		Path:       path,
		Branch:     "work/gc-test",
		Base:       base,
		BeadID:     "gc-test",
		StoreRef:   "gascity",
		Creator:    "test",
		Owner:      "gc-sling",
		Generation: "1",
		Lifecycle:  worktree.LifecycleActive,
	}
}

func TestCmdWorktreeEnsureCreatesAndVerifies(t *testing.T) {
	repo, base := worktreeTestRepo(t)
	root := t.TempDir()
	wt := filepath.Join(root, "wt")
	var stdout, stderr bytes.Buffer

	opts := managedWorktreeCmdOpts(repo, root, wt, base)
	opts.JSON = true
	code := runWorktreeEnsure(opts, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("ensure exit = %d, stderr: %s", code, stderr.String())
	}
	var rep struct {
		Path          string `json:"path"`
		Branch        string `json:"branch"`
		Created       bool   `json:"created"`
		BranchCreated bool   `json:"branch_created"`
		Provenance    *struct {
			BaseSHA   string `json:"base_sha"`
			AttemptID string `json:"attempt_id"`
		} `json:"provenance"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal ensure output %q: %v", stdout.String(), err)
	}
	if !rep.Created || !rep.BranchCreated || rep.Branch != "work/gc-test" ||
		rep.Provenance == nil || rep.Provenance.BaseSHA == "" || rep.Provenance.AttemptID == "" {
		t.Errorf("report = %+v, want created managed worktree with publishable provenance", rep)
	}

	// verify must pass on the ensured worktree.
	stdout.Reset()
	stderr.Reset()
	verifyOpts := opts
	verifyOpts.BaseSHA = rep.Provenance.BaseSHA
	code = runWorktreeVerify(verifyOpts, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("verify exit = %d, stderr: %s", code, stderr.String())
	}
}

func TestCmdWorktreeVerifyFailsOnMissing(t *testing.T) {
	repo, _ := worktreeTestRepo(t)
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	opts := managedWorktreeCmdOpts(repo, root, filepath.Join(root, "nope"), "main")
	code := runWorktreeVerify(opts, &stdout, &stderr)
	if code == 0 {
		t.Fatal("verify on missing worktree returned 0, want nonzero")
	}
	if !strings.Contains(stderr.String(), "does not exist") {
		t.Errorf("stderr %q does not explain the missing path", stderr.String())
	}
}

func TestCmdWorktreeEnsureDryRunIsPure(t *testing.T) {
	repo, base := worktreeTestRepo(t)
	root := t.TempDir()
	wt := filepath.Join(root, "wt")
	var stdout, stderr bytes.Buffer
	opts := managedWorktreeCmdOpts(repo, root, wt, base)
	opts.DryRun = true
	opts.JSON = true
	code := runWorktreeEnsure(opts, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dry-run ensure exit = %d, stderr: %s", code, stderr.String())
	}
	if _, err := os.Stat(wt); !os.IsNotExist(err) {
		t.Error("dry-run ensure created the worktree path")
	}
	var rep struct {
		Planned []string `json:"planned"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal dry-run output %q: %v", stdout.String(), err)
	}
	if len(rep.Planned) == 0 {
		t.Error("dry-run output has no planned actions")
	}
}

func TestCmdWorktreeManagedFlagsReachSpec(t *testing.T) {
	repo, base := worktreeTestRepo(t)
	root := t.TempDir()
	path := filepath.Join(root, "gc-test")
	opts := managedWorktreeCmdOpts(repo, root, path, base)
	opts.BaseSHA = strings.Repeat("a", 40)

	spec, err := opts.spec()
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	if spec.Root != root || spec.BeadID != opts.BeadID || spec.StoreRef != opts.StoreRef ||
		spec.BaseSHA != opts.BaseSHA || spec.Creator != opts.Creator || spec.Owner != opts.Owner ||
		spec.Generation != opts.Generation || spec.Lifecycle != opts.Lifecycle {
		t.Fatalf("spec = %+v, want all managed ownership fields from CLI", spec)
	}
}

func TestCmdWorktreeRegistered(t *testing.T) {
	root := newRootCmd(&bytes.Buffer{}, &bytes.Buffer{})
	for _, c := range root.Commands() {
		if c.Name() == "worktree" {
			return
		}
	}
	t.Fatal("gc worktree command is not registered on the root command")
}

func TestCmdWorktreeEnsureStrictJSONContract(t *testing.T) {
	t.Setenv("GC_JSON_CONTRACT_STRICT", "1")
	repo, base := worktreeTestRepo(t)
	root := t.TempDir()
	wt := filepath.Join(root, "wt")
	args := []string{
		"worktree", "ensure", "--json",
		"--repo", repo,
		"--root", root,
		"--path", wt,
		"--branch", "work/gc-test",
		"--base", base,
		"--bead", "gc-test",
		"--store-ref", "gascity",
		"--creator", "test",
		"--owner", "gc-sling",
		"--generation", "1",
	}
	var stdout, stderr bytes.Buffer
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("run(%v) failed: code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "json_unsupported") {
		t.Fatalf("gc worktree ensure still lacks declared JSON support: %s", stdout.String())
	}
	var result struct {
		SchemaVersion string `json:"schema_version"`
		OK            bool   `json:"ok"`
		Command       string `json:"command"`
		Action        string `json:"action"`
		Path          string `json:"path"`
		Provenance    *struct {
			AttemptID string `json:"attempt_id"`
		} `json:"provenance"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal JSON output %q: %v", stdout.String(), err)
	}
	if result.SchemaVersion != "1" || !result.OK || result.Command != "worktree ensure" ||
		result.Action != "ensure" || result.Path != wt ||
		result.Provenance == nil || result.Provenance.AttemptID == "" {
		t.Fatalf("JSON result = %+v, want declared structured ensure output", result)
	}
	validateJSONAgainstResultSchema(t, []string{"worktree", "ensure"}, stdout.Bytes())
}

// ensureAttemptID provisions the worktree described by opts and returns the
// attempt id its ensure reported. Cleanup is authorized per provisioning
// attempt, so a caller that wants to clean up what it just created has to carry
// that id forward; routing the test through the CLI's own JSON output is also
// what proves the two commands agree on the field.
func ensureAttemptID(t *testing.T, opts *worktreeCmdOpts) string {
	t.Helper()
	jsonOpts := *opts
	jsonOpts.JSON = true
	var stdout, stderr bytes.Buffer
	if code := runWorktreeEnsure(jsonOpts, &stdout, &stderr); code != 0 {
		t.Fatalf("runWorktreeEnsure setup exit = %d (stderr: %s)", code, stderr.String())
	}
	var result struct {
		Provenance struct {
			AttemptID string `json:"attempt_id"`
		} `json:"provenance"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal ensure output %q: %v", stdout.String(), err)
	}
	if result.Provenance.AttemptID == "" {
		t.Fatalf("ensure output %q carries no attempt id", stdout.String())
	}
	return result.Provenance.AttemptID
}

func TestCmdWorktreeCleanupJSONReportsPendingSafetyRefusal(t *testing.T) {
	repo, base := worktreeTestRepo(t)
	root := t.TempDir()
	wt := filepath.Join(root, "wt")
	opts := managedWorktreeCmdOpts(repo, root, wt, base)
	opts.AttemptID = ensureAttemptID(t, &opts)
	if err := os.WriteFile(filepath.Join(wt, "keep.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	opts.JSON = true
	var stdout, stderr bytes.Buffer
	code := runWorktreeCleanup(opts, &stdout, &stderr)
	if code == 0 {
		t.Fatal("runWorktreeCleanup dirty worktree returned 0, want nonzero")
	}
	var result struct {
		SchemaVersion  string `json:"schema_version"`
		OK             bool   `json:"ok"`
		Command        string `json:"command"`
		Action         string `json:"action"`
		CleanupPending bool   `json:"cleanup_pending"`
		Error          *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal cleanup output %q: %v", stdout.String(), err)
	}
	if result.SchemaVersion != "1" || result.OK || result.Command != "worktree cleanup" ||
		result.Action != "cleanup" || !result.CleanupPending || result.Error == nil ||
		result.Error.Code != worktree.CleanupErrorDirty || result.Error.Message == "" {
		t.Fatalf("cleanup result = %+v, want structured cleanup_pending dirty refusal", result)
	}
	if _, err := os.Stat(filepath.Join(wt, "keep.txt")); err != nil {
		t.Fatalf("cleanup safety refusal removed WIP: %v", err)
	}
}

// TestCmdWorktreeEnsureDryRunJSONContract schema-validates the DRY-RUN output.
// Only the real-creation path was validated before, which is how planned
// provenance came to emit a zero timestamp and an empty attempt id while the
// doc comment promised both were absent. A consumer publishing that evidence
// onto a bead would have stored 0001-01-01T00:00:00Z as a creation time.
func TestCmdWorktreeEnsureDryRunJSONContract(t *testing.T) {
	t.Setenv("GC_JSON_CONTRACT_STRICT", "1")
	repo, base := worktreeTestRepo(t)
	root := t.TempDir()
	wt := filepath.Join(root, "wt")
	args := []string{
		"worktree", "ensure", "--json", "--dry-run",
		"--repo", repo,
		"--root", root,
		"--path", wt,
		"--branch", "work/gc-test",
		"--base", base,
		"--bead", "gc-test",
		"--store-ref", "gascity",
		"--creator", "test",
		"--owner", "gc-sling",
		"--generation", "1",
	}
	var stdout, stderr bytes.Buffer
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("run(%v) failed: code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
	}
	validateJSONAgainstResultSchema(t, []string{"worktree", "ensure"}, stdout.Bytes())

	if body := stdout.String(); strings.Contains(body, "0001-01-01") {
		t.Errorf("dry-run emitted a zero creation time: %s", body)
	}
	var result struct {
		Provenance map[string]any `json:"provenance"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, absent := range []string{"created_at", "attempt_id"} {
		if _, present := result.Provenance[absent]; present {
			t.Errorf("dry-run provenance carries %q = %v, want it omitted until the worktree exists",
				absent, result.Provenance[absent])
		}
	}
}

// reapScopeFor builds the resolved scope a `gc worktree reap` invocation acts
// on from the shared reaper fixtures, so these tests exercise the same rig
// layout and safety gates the controller patrol does. liveSessionDirs is left
// unset: the open-session cross-check is the reaper's own gate, covered at that
// level, and a test that needs it assigns the field on the returned scope.
func reapScopeFor(cityPath, rigRoot string, store beads.Store) worktreeReapScope {
	return worktreeReapScope{
		cityPath:  cityPath,
		cfg:       reapTestConfig(rigRoot),
		rigStores: map[string]beads.Store{reapTestRigName: store},
	}
}

func TestRunWorktreeReapDryRunListsCandidatesAndRemovesNothing(t *testing.T) {
	cityPath, rigRoot := initReapRig(t)
	wt := addClosedWorktree(t, rigRoot, cityPath, "builder", "ga-dryrun1")
	store := beads.NewMemStoreFrom(1, []beads.Bead{{ID: "ga-dryrun1", Status: "closed"}}, nil)
	injectLiveness(t, liveWorktreeState{scanned: true})

	var stdout, stderr bytes.Buffer
	code := runWorktreeReap(reapScopeFor(cityPath, rigRoot, store), false, false, events.Discard, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "would reap") || !strings.Contains(out, "ga-dryrun1") {
		t.Fatalf("stdout = %q, want a would-reap line naming ga-dryrun1", out)
	}
	if !strings.Contains(out, "--apply") {
		t.Fatalf("stdout = %q, want the summary to name --apply", out)
	}
	if _, err := os.Stat(wt); err != nil {
		t.Fatalf("dry run removed or broke worktree %s: %v", wt, err)
	}
}

func TestRunWorktreeReapApplyRemovesEligibleWorktree(t *testing.T) {
	cityPath, rigRoot := initReapRig(t)
	wt := addClosedWorktree(t, rigRoot, cityPath, "builder", "ga-apply01")
	store := beads.NewMemStoreFrom(1, []beads.Bead{{ID: "ga-apply01", Status: "closed"}}, nil)
	injectLiveness(t, liveWorktreeState{scanned: true})

	var stdout, stderr bytes.Buffer
	code := runWorktreeReap(reapScopeFor(cityPath, rigRoot, store), true, false, events.Discard, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "reaped") || !strings.Contains(out, "ga-apply01") {
		t.Fatalf("stdout = %q, want a reaped line naming ga-apply01", out)
	}
	if strings.Contains(out, "--apply") {
		t.Fatalf("stdout = %q, want no --apply hint once the removal happened", out)
	}
	if _, err := os.Stat(wt); !os.IsNotExist(err) {
		t.Fatalf("worktree %s still present after --apply (stat err=%v)", wt, err)
	}
}

func TestRunWorktreeReapProtectedWorktreeIsReportedWithReason(t *testing.T) {
	cityPath, rigRoot := initReapRig(t)
	wt := addClosedWorktree(t, rigRoot, cityPath, "builder", "ga-live001")
	store := beads.NewMemStoreFrom(1, []beads.Bead{{ID: "ga-live001", Status: "closed"}}, nil)
	injectLiveness(t, liveWorktreeState{scanned: true, cwds: []string{pathutil.NormalizePathForCompare(wt)}})

	var stdout, stderr bytes.Buffer
	code := runWorktreeReap(reapScopeFor(cityPath, rigRoot, store), true, false, events.Discard, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "protected") || !strings.Contains(out, "ga-live001") {
		t.Fatalf("stdout = %q, want a protected line naming ga-live001", out)
	}
	if !strings.Contains(out, "live") {
		t.Fatalf("stdout = %q, want the live-process reason rendered", out)
	}
	if _, err := os.Stat(wt); err != nil {
		t.Fatalf("live worktree %s was removed: %v", wt, err)
	}
}

func TestRunWorktreeReapJSONCarriesVerdicts(t *testing.T) {
	cityPath, rigRoot := initReapRig(t)
	reapable := addClosedWorktree(t, rigRoot, cityPath, "builder", "ga-json001")
	held := addClosedWorktree(t, rigRoot, cityPath, "builder", "ga-json002")
	store := beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "ga-json001", Status: "closed"},
		{ID: "ga-json002", Status: "closed"},
	}, nil)
	injectLiveness(t, liveWorktreeState{scanned: true, cwds: []string{pathutil.NormalizePathForCompare(held)}})

	var stdout, stderr bytes.Buffer
	code := runWorktreeReap(reapScopeFor(cityPath, rigRoot, store), false, true, events.Discard, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}
	var got worktreeReapJSON
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decoding stdout %q: %v", stdout.String(), err)
	}
	if !got.DryRun {
		t.Error("dry_run = false, want true without --apply")
	}
	if len(got.Reaped) != 1 || got.Reaped[0].BeadID != "ga-json001" {
		t.Fatalf("reaped = %+v, want exactly ga-json001", got.Reaped)
	}
	if got.Reaped[0].Rig != reapTestRigName || got.Reaped[0].Path != reapable {
		t.Errorf("reaped entry = %+v, want rig %q path %q", got.Reaped[0], reapTestRigName, reapable)
	}
	if len(got.Protected) != 1 || got.Protected[0].BeadID != "ga-json002" {
		t.Fatalf("protected = %+v, want exactly ga-json002", got.Protected)
	}
	if !strings.Contains(got.Protected[0].Reason, "live") {
		t.Errorf("protected reason = %q, want the live-process reason", got.Protected[0].Reason)
	}
}

func TestRunWorktreeReapNoCandidatesReportsNothingEligible(t *testing.T) {
	cityPath, rigRoot := initReapRig(t)
	store := beads.NewMemStoreFrom(1, nil, nil)
	injectLiveness(t, liveWorktreeState{scanned: true})

	var stdout, stderr bytes.Buffer
	code := runWorktreeReap(reapScopeFor(cityPath, rigRoot, store), false, false, events.Discard, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "No closed-bead worktrees") {
		t.Fatalf("stdout = %q, want a nothing-eligible summary", stdout.String())
	}
}

func TestRunWorktreeReapReportsRigFailureAndExitsNonZero(t *testing.T) {
	cityPath, rigRoot := initReapRig(t)
	notARepo := filepath.Join(t.TempDir(), "not-a-repo")
	if err := os.MkdirAll(notARepo, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	store := beads.NewMemStoreFrom(1, nil, nil)
	scope := reapScopeFor(cityPath, rigRoot, store)
	scope.cfg.Rigs = []config.Rig{{Name: reapTestRigName, Path: notARepo}}
	injectLiveness(t, liveWorktreeState{scanned: true})

	var stdout, stderr bytes.Buffer
	code := runWorktreeReap(scope, false, false, events.Discard, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1 when a rig could not be scanned\nstderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), reapTestRigName) {
		t.Fatalf("stderr = %q, want the unscannable rig named", stderr.String())
	}
}

func TestRunWorktreeReapEmptyScopeReportsNoBoundRigs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	scope := worktreeReapScope{cityPath: t.TempDir(), cfg: &config.City{}}
	code := runWorktreeReap(scope, false, false, events.Discard, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0 for a city with no bound rigs", code)
	}
	if !strings.Contains(stdout.String(), "no bound rigs") {
		t.Fatalf("stdout = %q, want a no-bound-rigs summary", stdout.String())
	}
}

// TestSortReapDecisionsOrdersByRigThenPath pins the report's ordering. The
// reaper walks rigs in Go map order, which is randomized per run, so without
// this the same city lists its rigs differently every invocation and two runs
// of the command cannot be diffed against each other.
func TestSortReapDecisionsOrdersByRigThenPath(t *testing.T) {
	in := []reapDecision{
		{Rig: "zeta", Path: "/wt/b", BeadID: "ga-4"},
		{Rig: "alpha", Path: "/wt/z", BeadID: "ga-2"},
		{Rig: "zeta", Path: "/wt/a", BeadID: "ga-3"},
		{Rig: "alpha", Path: "/wt/a", BeadID: "ga-1"},
	}
	got := sortReapDecisions(in)

	var order []string
	for _, d := range got {
		order = append(order, d.BeadID)
	}
	want := []string{"ga-1", "ga-2", "ga-3", "ga-4"}
	if !slices.Equal(order, want) {
		t.Errorf("order = %v, want %v", order, want)
	}
	if in[0].BeadID != "ga-4" {
		t.Errorf("input was reordered in place (in[0] = %q); the caller's report must be left alone", in[0].BeadID)
	}
}

func TestNewWorktreeReapCmdDefaultsToDryRun(t *testing.T) {
	cmd := newWorktreeReapCmd(&bytes.Buffer{}, &bytes.Buffer{})
	apply, err := cmd.Flags().GetBool("apply")
	if err != nil {
		t.Fatalf("--apply flag missing: %v", err)
	}
	if apply {
		t.Error("--apply defaults to true, want false so the command never deletes unasked")
	}
}
