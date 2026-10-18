package scripts_test

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// blacksmithOwnerInput is the context value a runs-on expression tests to tell
// the repository Blacksmith's runners serve from any other: the account id of
// the repository's owner, which a fork cannot share and a rename does not
// change. gastownhallOwnerID is the owner Blacksmith serves.
const blacksmithOwnerInput = "github.repository_owner_id"

// forkOwnerID stands in for any other owner: a fork keeps the repository name
// under another account.
const forkOwnerID = "1"

// TestForkPullRequestWorkflowsGetGitHubHostedRunners pins that a fork of this
// repository can run the workflows a pull request starts. Blacksmith's runners
// serve only the gastownhall owner's repository. A fork's job that names a
// Blacksmith label waits for a runner that never comes, and every job that
// needs it is skipped, so the fork's pull requests and pushes test nothing.
// Each runs-on in such a workflow that names a Blacksmith label must therefore
// resolve to a GitHub-hosted label under any other owner, whatever values its
// other inputs take, and still reach a Blacksmith label under gastownhall. A
// runs-on that reads the runner-policy job's outputs is runner_policy.py's
// choice, which test_runner_policy.py covers.
func TestForkPullRequestWorkflowsGetGitHubHostedRunners(t *testing.T) {
	root := repoRoot(t)
	var paths []string
	for _, pattern := range []string{"*.yml", "*.yaml"} {
		matches, err := filepath.Glob(filepath.Join(root, ".github", "workflows", pattern))
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, matches...)
	}
	var pullRequestWorkflows int
	for _, path := range paths {
		name := filepath.Base(path)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			On   yaml.Node `yaml:"on"`
			Jobs map[string]struct {
				RunsOn yaml.Node `yaml:"runs-on"`
			} `yaml:"jobs"`
		}
		if err := yaml.Unmarshal(body, &doc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !workflowTriggeredBy(doc.On, "pull_request", "pull_request_target") {
			continue
		}
		pullRequestWorkflows++
		for _, job := range slices.Sorted(maps.Keys(doc.Jobs)) {
			for _, value := range runsOnValues(doc.Jobs[job].RunsOn) {
				if err := blacksmithRunsOnProblem(value); err != nil {
					t.Errorf("%s jobs.%s.runs-on %q: %v", name, job, value, err)
				}
			}
		}
	}
	if pullRequestWorkflows == 0 {
		t.Fatal("no pull_request workflows found; the walk is broken")
	}
}

// blacksmithRunsOnProblem resolves a runs-on value that names a Blacksmith
// label under every combination of its inputs, once with blacksmithOwnerInput
// set to a fork's owner and once to gastownhallOwnerID. It reports a
// combination that gives a fork no GitHub-hosted label, or a value that never
// reaches Blacksmith under gastownhall. A value that names no Blacksmith label
// is fine.
func blacksmithRunsOnProblem(value string) error {
	if !strings.Contains(value, "blacksmith-") {
		return nil
	}
	inputs, literals, err := runsOnInputs(value)
	if err != nil {
		return err
	}
	combinations := 1
	for range inputs {
		combinations *= len(literals) + 1
		if combinations > 1<<16 {
			return fmt.Errorf("reads %d inputs, too many to check every combination", len(inputs))
		}
	}
	reachesBlacksmith := false
	for n := range combinations {
		ctx := map[string]string{}
		for i, rest := 0, n; i < len(inputs); i, rest = i+1, rest/(len(literals)+1) {
			if choice := rest % (len(literals) + 1); choice > 0 {
				ctx[inputs[i]] = literals[choice-1]
			}
		}
		ctx[blacksmithOwnerInput] = forkOwnerID
		label, err := resolveRunsOn(value, ctx)
		if err != nil {
			return err
		}
		if label == "" || strings.Contains(label, "blacksmith") {
			return fmt.Errorf("resolves to %q under owner id %s with inputs %v; a fork has no Blacksmith runner, so select one only when %s == '%s'", label, forkOwnerID, ctx, blacksmithOwnerInput, gastownhallOwnerID)
		}
		ctx[blacksmithOwnerInput] = gastownhallOwnerID
		if label, err = resolveRunsOn(value, ctx); err == nil && strings.HasPrefix(label, "blacksmith-") {
			reachesBlacksmith = true
		}
	}
	if !reachesBlacksmith {
		return fmt.Errorf("never resolves to a Blacksmith label when %s is '%s'", blacksmithOwnerInput, gastownhallOwnerID)
	}
	return nil
}

func TestBlacksmithRunsOnProblem(t *testing.T) {
	for _, tc := range []struct {
		value string
		ok    bool
	}{
		{"ubuntu-latest", true},
		{"${{ needs.runner-policy.outputs.runner_2vcpu }}", true},
		{"${{ github.repository_owner_id == '255961667' && 'blacksmith-2vcpu-ubuntu-2404' || 'ubuntu-latest' }}", true},
		// The repository's name is not the switch: a fork keeps the name.
		{"${{ github.repository == 'gastownhall/gascity' && 'blacksmith-2vcpu-ubuntu-2404' || 'ubuntu-latest' }}", false},
		{"${{ github.repository_owner_id == '255961667' && (needs.gate.outputs.mode == 'remote' && 'blacksmith-2vcpu-ubuntu-2404' || 'blacksmith-16vcpu-ubuntu-2404') || 'ubuntu-latest' }}", true},
		{"${{ github.repository_owner_id != '255961667' && 'ubuntu-latest' || needs.gate.outputs.mode == 'remote' && 'blacksmith-2vcpu-ubuntu-2404' || 'blacksmith-16vcpu-ubuntu-2404' }}", true},
		// A literal Blacksmith label strands every fork run.
		{"blacksmith-2vcpu-ubuntu-2404", false},
		// A fork gets Blacksmith whenever the mode is not remote.
		{"${{ needs.gate.outputs.mode == 'remote' && 'ubuntu-latest' || 'blacksmith-16vcpu-ubuntu-2404' }}", false},
		// The repository test is bypassed when the mode is remote.
		{"${{ (github.repository_owner_id == '255961667' || needs.rbe.outputs.mode == 'remote') && 'blacksmith-2vcpu-ubuntu-2404' || 'ubuntu-latest' }}", false},
		// Both arms name Blacksmith.
		{"${{ github.repository_owner_id == '255961667' && 'blacksmith-2vcpu-ubuntu-2404' || 'blacksmith-4vcpu-ubuntu-2404' }}", false},
		// The fallback is an input that can be empty or a Blacksmith label.
		{"${{ github.repository_owner_id == '255961667' && 'blacksmith-2vcpu-ubuntu-2404' || inputs.runner }}", false},
		// A wrong owner id moves the canonical repository off Blacksmith.
		{"${{ github.repository_owner_id == '25596166' && 'blacksmith-2vcpu-ubuntu-2404' || 'ubuntu-latest' }}", false},
	} {
		err := blacksmithRunsOnProblem(tc.value)
		if tc.ok && err != nil {
			t.Errorf("blacksmithRunsOnProblem(%q) = %v, want nil", tc.value, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("blacksmithRunsOnProblem(%q) = nil, want a problem", tc.value)
		}
	}
}

// workflowTriggeredBy reports whether a workflow's `on` names any of events,
// in its string, list or mapping form.
func workflowTriggeredBy(on yaml.Node, events ...string) bool {
	switch on.Kind {
	case yaml.ScalarNode:
		return slices.Contains(events, on.Value)
	case yaml.SequenceNode:
		for _, item := range on.Content {
			if slices.Contains(events, item.Value) {
				return true
			}
		}
	case yaml.MappingNode:
		for i := 0; i < len(on.Content); i += 2 {
			if slices.Contains(events, on.Content[i].Value) {
				return true
			}
		}
	}
	return false
}

// runsOnValues returns the strings a job's runs-on holds: a label or
// expression, a list of them, or a mapping's group and labels.
func runsOnValues(node yaml.Node) []string {
	switch node.Kind {
	case yaml.ScalarNode:
		return []string{node.Value}
	case yaml.SequenceNode:
		var values []string
		for _, item := range node.Content {
			values = append(values, runsOnValues(*item)...)
		}
		return values
	case yaml.MappingNode:
		var values []string
		for i := 0; i+1 < len(node.Content); i += 2 {
			values = append(values, runsOnValues(*node.Content[i+1])...)
		}
		return values
	}
	return nil
}

// runsOnInputs returns the context values every ${{ }} in value reads, other
// than blacksmithOwnerInput, and the string literals it compares them with.
func runsOnInputs(value string) (inputs, literals []string, err error) {
	for _, match := range ghInterpolation.FindAllStringSubmatch(value, -1) {
		toks, err := ghTokenize(match[1])
		if err != nil {
			return nil, nil, err
		}
		for _, tok := range toks {
			switch tok.kind {
			case "string":
				if !slices.Contains(literals, tok.val) {
					literals = append(literals, tok.val)
				}
			case "ident":
				if _, numErr := strconv.ParseFloat(tok.val, 64); numErr == nil {
					continue
				}
				switch tok.val {
				case "true", "false", "null", blacksmithOwnerInput:
					continue
				}
				if !slices.Contains(inputs, tok.val) {
					inputs = append(inputs, tok.val)
				}
			}
		}
	}
	return inputs, literals, nil
}

// resolveRunsOn interpolates every ${{ }} in value under ctx.
func resolveRunsOn(value string, ctx map[string]string) (string, error) {
	var firstErr error
	resolved := ghInterpolation.ReplaceAllStringFunc(value, func(expr string) string {
		v, err := evalGHExpr(expr, ctx)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("evaluating %s: %w", expr, err)
		}
		return ghString(v)
	})
	return resolved, firstErr
}
