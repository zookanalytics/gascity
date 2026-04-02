//go:build acceptance_c

// Tier C acceptance tests — real inference agents.
//
// These start cities with real AI models (haiku) and verify end-to-end
// outcomes: work dispatched → agent picks up → implements → result appears.
// Assertions are loose (eventual consistency) because model behavior is
// non-deterministic.
//
// Requires: gc binary, bd binary, tmux, dolt, ANTHROPIC_API_KEY (or OAuth).
// Expected duration: ~5 min per scenario.
// Trigger: manual (make test-acceptance-c), then nightly.
package tierc_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	helpers "github.com/gastownhall/gascity/test/acceptance/helpers"
)

var testEnvC *helpers.Env

func TestMain(m *testing.M) {
	// Tier C needs real inference. Accept either:
	// 1. ANTHROPIC_API_KEY env var (CI mode)
	// 2. GC_TIERC_FORCE=1 env var (local OAuth mode — user asserts Claude is authed)
	// 3. Detect OAuth: check if ~/.claude/ exists with credentials
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	forceRun := os.Getenv("GC_TIERC_FORCE") == "1"
	hasOAuth := oauthCredentialsExist()

	if apiKey == "" && !forceRun && !hasOAuth {
		// No credentials available, skip silently.
		os.Exit(0)
	}

	tmpDir, err := os.MkdirTemp("", "gc-acceptance-c-*")
	if err != nil {
		panic("acceptance-c: creating temp dir: " + err.Error())
	}
	defer os.RemoveAll(tmpDir)

	gcBinary := helpers.BuildGC(tmpDir)

	gcHome := filepath.Join(tmpDir, "gc-home")
	runtimeDir := filepath.Join(tmpDir, "runtime")
	for _, d := range []string{gcHome, runtimeDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			panic("acceptance-c: " + err.Error())
		}
	}
	if err := helpers.WriteSupervisorConfig(gcHome); err != nil {
		panic("acceptance-c: " + err.Error())
	}

	// Configure dolt identity in the isolated home (dolt requires user.name).
	doltCfgDir := filepath.Join(gcHome, ".dolt")
	if err := os.MkdirAll(doltCfgDir, 0o755); err != nil {
		panic("acceptance-c: " + err.Error())
	}
	doltCfg := `{"user.name":"gc-test","user.email":"gc-test@test.local"}`
	if err := os.WriteFile(filepath.Join(doltCfgDir, "config_global.json"), []byte(doltCfg), 0o644); err != nil {
		panic("acceptance-c: " + err.Error())
	}

	// Symlink real Claude credentials into the isolated home so agents
	// can authenticate via OAuth. Only the credentials file is needed.
	realHome, _ := os.UserHomeDir()
	realClaudeDir := filepath.Join(realHome, ".claude")
	testClaudeDir := filepath.Join(gcHome, ".claude")
	if _, err := os.Stat(realClaudeDir); err == nil {
		if err := os.Symlink(realClaudeDir, testClaudeDir); err != nil {
			panic("acceptance-c: symlinking .claude: " + err.Error())
		}
	}

	testEnvC = helpers.NewEnv(gcBinary, gcHome, runtimeDir).
		Without("GC_SESSION"). // use real tmux, not subprocess
		Without("GC_BEADS").   // use real bd (dolt-backed) provider
		Without("GC_DOLT").    // let gc manage dolt (don't skip it)
		With("ANTHROPIC_API_KEY", apiKey).
		With("DOLT_ROOT_PATH", gcHome) // dolt reads config from $DOLT_ROOT_PATH/.dolt/

	// Ensure tmux is available.
	if _, err := exec.LookPath("tmux"); err != nil {
		panic("acceptance-c: tmux not found")
	}

	code := m.Run()

	helpers.RunGC(testEnvC, "", "supervisor", "stop") //nolint:errcheck
	os.Exit(code)
}

// TestSwarm_SlingWorkCoderCommits verifies the swarm end-to-end:
// sling a task → coder picks up → creates a file → committer commits.
//
// This is a loose assertion test: we don't verify intermediate steps,
// only that a commit eventually appears with the expected content.
func TestSwarm_SlingWorkCoderCommits(t *testing.T) {
	if testing.Short() {
		t.Skip("Tier C: skipping in short mode")
	}

	// Create a throwaway git repo as the rig.
	rigDir := setupThrowawayRepo(t)
	rigName := filepath.Base(rigDir)

	// Init a swarm city.
	c := helpers.NewCity(t, testEnvC)
	c.InitFrom(filepath.Join(helpers.ExamplesDir(), "swarm"))

	// Add the rig via gc rig add (initializes beads, hooks, routes).
	c.RigAdd(rigDir, "packs/swarm")

	// Limit pool sizes to reduce cost.
	c.AppendToConfig("\n[[rigs.overrides]]\nagent = \"coder\"\n[rigs.overrides.pool]\nmin = 1\nmax = 1\n")

	c.StartWithSupervisor()

	// Wait for supervisor + dolt + agents to initialize.
	time.Sleep(15 * time.Second)

	// Sling work to the coder pool.
	out, err := c.GC("sling", rigName+"/coder", "Create a file called hello.txt with the text 'hello world'")
	if err != nil {
		t.Fatalf("gc sling: %v\n%s", err, out)
	}
	t.Logf("Slung work: %s", strings.TrimSpace(out))

	// Poll for outcome: a commit should eventually appear that creates hello.txt.
	deadline := 5 * time.Minute
	found := pollForCondition(t, deadline, 10*time.Second, func() bool {
		_, err := os.Stat(filepath.Join(rigDir, "hello.txt"))
		return err == nil
	})

	if !found {
		gitLog := gitCmd(t, rigDir, "log", "--oneline", "-10")
		status, _ := c.GC("status")
		t.Fatalf("hello.txt not created within %s\ngit log:\n%s\nstatus:\n%s", deadline, gitLog, status)
	}

	t.Logf("hello.txt created successfully")
	gitLog := gitCmd(t, rigDir, "log", "--oneline", "-5")
	t.Logf("Recent commits:\n%s", gitLog)
}

// TestGastown_PolecatImplementsRefineryMerges verifies the gastown flow:
// dispatch work to polecat pool → polecat creates branch + commits →
// reassigns to refinery → refinery merges to default branch.
func TestGastown_PolecatImplementsRefineryMerges(t *testing.T) {
	if testing.Short() {
		t.Skip("Tier C: skipping in short mode")
	}

	rigDir, bareRepo := setupThrowawayRepoWithOrigin(t)
	rigName := filepath.Base(rigDir)

	c := helpers.NewCity(t, testEnvC)
	c.InitFrom(filepath.Join(helpers.ExamplesDir(), "gastown"))

	// Add the rig via gc rig add (initializes beads, hooks, routes).
	c.RigAdd(rigDir, "packs/gastown")

	// Keep polecat on-demand so we can assert the queued outer bead before any
	// session claims it.
	c.AppendToConfig("\n[[rigs.overrides]]\nagent = \"polecat\"\n[rigs.overrides.pool]\nmin = 0\nmax = 1\n")

	// Sling work to the polecat pool.
	out, err := c.GC("sling", rigName+"/polecat", "Create a file called feature.txt containing 'new feature'")
	if err != nil {
		t.Fatalf("gc sling: %v\n%s", err, out)
	}
	t.Logf("Slung work to polecat: %s", strings.TrimSpace(out))

	ready := bdListJSON(
		t,
		testEnvC,
		rigDir,
		"ready",
		"--metadata-field", "gc.routed_to="+rigName+"/polecat",
		"--unassigned",
		"--json",
		"--limit=10",
	)
	require.Len(t, ready, 1, "expected exactly one queued outer bead before startup")
	require.NotContains(t, ready[0].ID, ".", "queued pool work should be the outer bead, not an internal step")
	outerBeadID := ready[0].ID

	c.StartWithSupervisor()

	sessionDeadline := 2 * time.Minute
	sessionFound := pollForCondition(t, sessionDeadline, 5*time.Second, func() bool {
		sessions := sessionListJSON(t, c)
		for _, s := range sessions {
			if !strings.Contains(s.ID, "polecat") {
				continue
			}
			if s.ID == rigName+"--polecat" {
				return false
			}
			return strings.HasPrefix(s.ID, "polecat-")
		}
		return false
	})
	if !sessionFound {
		sessionOut, _ := c.GC("session", "list", "--json")
		status, _ := c.GC("status")
		t.Fatalf("polecat session did not wake with a pool session name within %s\nsessions:\n%s\nstatus:\n%s",
			sessionDeadline, sessionOut, status)
	}

	deadline := 8 * time.Minute
	merged := pollForCondition(t, deadline, 10*time.Second, func() bool {
		if _, err := gitCmdErr(rigDir, "fetch", "origin", "main"); err != nil {
			return false
		}
		content, err := gitCmdErr(rigDir, "show", "origin/main:feature.txt")
		if err != nil {
			return false
		}
		return strings.TrimSpace(content) == "new feature"
	})

	if !merged {
		sessionOut, _ := c.GC("session", "list", "--json")
		status, _ := c.GC("status")
		gitLog := gitCmd(t, rigDir, "log", "--all", "--oneline", "-10")
		branches := gitCmd(t, rigDir, "branch", "-a")
		beads := bdCmd(t, testEnvC, rigDir, "list", "--json", "--limit", "20")
		t.Fatalf("feature.txt was not merged to origin/main within %s\nbranches:\n%s\ngit log:\n%s\nsessions:\n%s\nstatus:\n%s\nbeads:\n%s",
			deadline, branches, gitLog, sessionOut, status, beads)
	}

	closed := bdShowJSON(t, testEnvC, rigDir, outerBeadID)
	require.Equal(t, "closed", closed.Status)

	verifyDir := filepath.Join(t.TempDir(), "verify")
	gitMust(t, "", "clone", bareRepo, verifyDir)
	data, err := os.ReadFile(filepath.Join(verifyDir, "feature.txt"))
	require.NoError(t, err, "expected feature.txt on a fresh clone from origin")
	require.Equal(t, "new feature", strings.TrimSpace(string(data)))

	mainLog := gitCmd(t, verifyDir, "log", "--oneline", "-5", "main")
	t.Logf("Verified merge on origin/main:\n%s", mainLog)
}

// TestGastown_PolecatLifecycle verifies the full polecat lifecycle:
// prime -> work -> gt done. This is the test that would have caught
// regressions in polecat session management and worktree creation.
func TestGastown_PolecatLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Tier C: skipping in short mode")
	}

	rigDir := setupThrowawayRepo(t)

	// Write a simple Go file with a TODO for the polecat to fix.
	mainGo := filepath.Join(rigDir, "main.go")
	os.WriteFile(mainGo, []byte("package main\n\n// TODO: add a hello function\nfunc main() {}\n"), 0o644)
	gitCmd(t, rigDir, "add", ".")
	gitCmd(t, rigDir, "commit", "-m", "add main.go with TODO")

	rigName := filepath.Base(rigDir)

	c := helpers.NewCity(t, testEnvC)
	c.InitFrom(filepath.Join(helpers.ExamplesDir(), "gastown"))
	c.RigAdd(rigDir, "packs/gastown")

	// Limit pool to 1 polecat, cap cost.
	c.AppendToConfig("\n[[rigs.overrides]]\nagent = \"polecat\"\n[rigs.overrides.pool]\nmin = 1\nmax = 1\n")

	c.StartWithSupervisor()
	time.Sleep(15 * time.Second) // Wait for init.

	// Sling a small, verifiable task.
	out, err := c.GC("sling", rigName+"/polecat", "Add a function called Hello that prints 'hello world' to main.go")
	require.NoError(t, err, "gc sling: %s", out)
	t.Logf("Slung work: %s", strings.TrimSpace(out))

	// Poll: a new branch should appear (polecat creates a worktree branch).
	deadline := 5 * time.Minute
	branchCreated := pollForCondition(t, deadline, 10*time.Second, func() bool {
		branches := gitCmd(t, rigDir, "branch", "--list", "--no-color", "-a")
		for _, line := range strings.Split(branches, "\n") {
			branch := strings.TrimSpace(strings.TrimPrefix(line, "*"))
			if branch != "" && branch != "main" && branch != "master" {
				return true
			}
		}
		return false
	})

	if !branchCreated {
		status, _ := c.GC("status")
		t.Fatalf("polecat did not create a branch within %s\nstatus:\n%s", deadline, status)
	}

	t.Log("Polecat lifecycle test passed: branch created")
}

// TestGastown_MayorDispatchPipeline tests the full mayor -> polecat -> refinery
// pipeline: send mail to mayor, mayor dispatches work, bead appears.
func TestGastown_MayorDispatchPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Tier C: skipping in short mode")
	}

	rigDir := setupThrowawayRepo(t)

	// Add a simple file for mayor to dispatch work about.
	os.WriteFile(filepath.Join(rigDir, "app.py"), []byte("# TODO: add a greet function\n"), 0o644)
	gitCmd(t, rigDir, "add", ".")
	gitCmd(t, rigDir, "commit", "-m", "add app.py")

	rigName := filepath.Base(rigDir)

	c := helpers.NewCity(t, testEnvC)
	c.InitFrom(filepath.Join(helpers.ExamplesDir(), "gastown"))
	c.RigAdd(rigDir, "packs/gastown")

	// Limit pool sizes.
	c.AppendToConfig("\n[[rigs.overrides]]\nagent = \"polecat\"\n[rigs.overrides.pool]\nmin = 1\nmax = 1\n")

	c.StartWithSupervisor()
	time.Sleep(15 * time.Second)

	// Send mail to mayor asking to implement a feature.
	out, err := c.GC("mail", "send", "mayor", "Please add a greet() function to app.py that prints 'hello'")
	if err != nil {
		t.Fatalf("gc mail send: %v\n%s", err, out)
	}
	t.Logf("Sent mail to mayor: %s", strings.TrimSpace(out))

	// Poll: eventually a bead should be created (mayor dispatches work).
	deadline := 8 * time.Minute
	beadCreated := pollForCondition(t, deadline, 15*time.Second, func() bool {
		out, err := c.GC("bd", "list", "--rig", rigName)
		if err != nil {
			return false
		}
		// Look for any bead (mayor creates one from the mail).
		return strings.Contains(out, "open") || strings.Contains(out, "closed")
	})

	if !beadCreated {
		status, _ := c.GC("status")
		t.Fatalf("mayor did not dispatch work within %s\nstatus:\n%s", deadline, status)
	}

	t.Log("Mayor dispatch pipeline test passed: work dispatched")
}

// --- helpers ---

func setupThrowawayRepo(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "config", "user.email", "test@test.com")
	gitCmd(t, dir, "config", "user.name", "Test")
	readme := filepath.Join(dir, "README.md")
	if err := os.WriteFile(readme, []byte("# Test Repo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", ".")
	gitCmd(t, dir, "commit", "-m", "initial commit")
	return dir
}

func setupThrowawayRepoWithOrigin(t *testing.T) (string, string) {
	t.Helper()
	bare := filepath.Join(t.TempDir(), "origin.git")
	gitMust(t, "", "init", "--bare", bare)

	work := filepath.Join(t.TempDir(), "repo")
	gitMust(t, "", "clone", bare, work)
	gitMust(t, work, "config", "user.email", "test@test.com")
	gitMust(t, work, "config", "user.name", "Test")

	readme := filepath.Join(work, "README.md")
	if err := os.WriteFile(readme, []byte("# Test Repo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitMust(t, work, "add", ".")
	gitMust(t, work, "commit", "-m", "initial commit")
	gitMust(t, work, "push", "origin", "main")

	return work, bare
}

type tierCSessionInfo struct {
	ID       string `json:"id"`
	Template string `json:"template"`
	State    string `json:"state"`
	Alias    string `json:"alias"`
}

type tierCBead struct {
	ID       string            `json:"id"`
	Status   string            `json:"status"`
	Assignee string            `json:"assignee"`
	Metadata map[string]string `json:"metadata"`
}

func gitCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := gitCmdErr(dir, args...)
	if err != nil {
		return out
	}
	return strings.TrimSpace(out)
}

func gitMust(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := gitCmdErr(dir, args...)
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(out)
}

func gitCmdErr(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func bdCmd(t *testing.T, env *helpers.Env, dir string, args ...string) string {
	t.Helper()
	bdPath := helpers.RequireBD(t)
	cmd := exec.Command(bdPath, args...)
	cmd.Dir = dir
	cmd.Env = env.List()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bd %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func bdListJSON(t *testing.T, env *helpers.Env, dir string, args ...string) []tierCBead {
	t.Helper()
	out := bdCmd(t, env, dir, args...)
	var beads []tierCBead
	if err := json.Unmarshal([]byte(out), &beads); err != nil {
		t.Fatalf("parsing bd JSON list: %v\n%s", err, out)
	}
	return beads
}

func bdShowJSON(t *testing.T, env *helpers.Env, dir, id string) tierCBead {
	t.Helper()
	out := bdCmd(t, env, dir, "show", id, "--json")
	var bead tierCBead
	if err := json.Unmarshal([]byte(out), &bead); err != nil {
		t.Fatalf("parsing bd show JSON: %v\n%s", err, out)
	}
	return bead
}

func sessionListJSON(t *testing.T, c *helpers.City) []tierCSessionInfo {
	t.Helper()
	out, err := c.GC("session", "list", "--json")
	if err != nil {
		t.Fatalf("gc session list --json: %v\n%s", err, out)
	}
	var sessions []tierCSessionInfo
	if err := json.Unmarshal([]byte(out), &sessions); err != nil {
		t.Fatalf("parsing session list JSON: %v\n%s", err, out)
	}
	return sessions
}

func pollForCondition(t *testing.T, timeout, interval time.Duration, check func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}

// oauthCredentialsExist checks if Claude CLI OAuth credentials are
// available at ~/.claude/credentials.json. When running locally with
// Claude Max, ANTHROPIC_API_KEY is not set, but the CLI authenticates
// via these OAuth tokens.
func oauthCredentialsExist() bool {
	home, err := os.UserHomeDir()
	if err != nil {
		return false
	}
	// Claude CLI stores OAuth tokens in ~/.claude/
	credFile := filepath.Join(home, ".claude", "credentials.json")
	_, err = os.Stat(credFile)
	return err == nil
}
