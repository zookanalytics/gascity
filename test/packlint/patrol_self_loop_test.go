// TestPatrolFormulasSelfLoop guards gc-3nj: patrol formulas (witness,
// deacon, refinery) must drive the LLM to start the next cycle in the
// same session. Without an explicit re-engagement step after burning,
// the agent's turn ends naturally and the session sits idle until an
// external nudge arrives — which manifests as the witness self-trigger
// stall the bug describes.
//
// The required pattern in next-iteration is:
//  1. `gc hook` is invoked AFTER `gc bd mol burn` so the new wisp is
//     surfaced as command output the LLM has to react to.
//  2. The closing prose contains explicit "do NOT end your turn"
//     language so the LLM does not interpret burn as session exit.
package packlint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPatrolFormulasSelfLoop(t *testing.T) {
	formulas := []string{
		"examples/gastown/packs/gastown/formulas/mol-witness-patrol.toml",
		"examples/gastown/packs/gastown/formulas/mol-deacon-patrol.toml",
		"examples/gastown/packs/gastown/formulas/mol-refinery-patrol.toml",
	}

	for _, rel := range formulas {
		t.Run(filepath.Base(rel), func(t *testing.T) {
			path := filepath.Join(repoRoot(), rel)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			body := string(data)

			burnIdx := strings.Index(body, "gc bd mol burn")
			if burnIdx < 0 {
				t.Fatalf("%s: missing `gc bd mol burn` invocation", rel)
			}
			tail := body[burnIdx:]
			if !strings.Contains(tail, "gc hook") {
				t.Errorf("%s: next-iteration step must invoke `gc hook` after `gc bd mol burn` "+
					"so the new wisp surfaces as command output that re-engages the LLM (gc-3nj)", rel)
			}
			if !strings.Contains(tail, "do NOT end your turn") {
				t.Errorf("%s: next-iteration step must contain the imperative 'do NOT end your turn' "+
					"after burn so the LLM does not interpret burn as session exit (gc-3nj)", rel)
			}
		})
	}
}
