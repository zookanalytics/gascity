package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// goEnvAllowlistFiles are the files that build an `env -i` allowlist for `go`
// invocations and forward GOROOT. Each strips the ambient environment and
// re-exports a fixed set of GO* variables, deriving GOROOT from `go env GOROOT`.
//
// The list mirrors the git-config isolation fan-out in gitconfig_isolation_test.go
// and for the same reason: the Makefile's TEST_ENV is the first `env -i` layer,
// the shard scripts re-exec jobs through their OWN `env -i` below it, and CI
// invokes the shard scripts directly with no TEST_ENV above them at all. A
// variable missing from any one layer is silently dropped on the way to `go`,
// so every layer must forward it independently.
var goEnvAllowlistFiles = []string{
	"Makefile",
	"scripts/test-local-parallel",
	"scripts/test-go-test-shard",
	"scripts/test-integration-shard",
}

// TestGoEnvAllowlistsForwardGoToolchainWithGoroot guards gc-vveco: an `env -i`
// allowlist that forwards GOROOT must also forward GOTOOLCHAIN.
//
// GOROOT is derived from `go env GOROOT`, which resolves to the pinned
// toolchain's root whenever GOTOOLCHAIN is set in the surrounding environment —
// as the agent pre-commit hook does (GOTOOLCHAIN=go1.X+auto, so golangci-lint's
// buildir linters can build IR). Forwarding GOROOT while dropping GOTOOLCHAIN
// splits the pair: `env -i` clears the pin, the inner `go` resolves from PATH to
// a different toolchain, and GOROOT then points at a version that tool does not
// match. Every package fails to compile with
// "version go1.X does not match go tool version go1.Y" — not just the staged
// diff — breaking every make/go target run under the hook. GOROOT and
// GOTOOLCHAIN are a matched pair and must travel together.
func TestGoEnvAllowlistsForwardGoToolchainWithGoroot(t *testing.T) {
	root := repoRoot(t)
	for _, rel := range goEnvAllowlistFiles {
		t.Run(rel, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, rel))
			if err != nil {
				t.Fatalf("read %s: %v", rel, err)
			}
			content := string(data)
			if !strings.Contains(content, `GOROOT="`) {
				t.Fatalf("%s no longer forwards GOROOT through an `env -i` allowlist; "+
					"if the test fan-out changed, update goEnvAllowlistFiles deliberately", rel)
			}
			if !strings.Contains(content, `GOTOOLCHAIN="`) {
				t.Errorf("%s forwards GOROOT but not GOTOOLCHAIN through its `env -i` allowlist.\n"+
					"GOROOT is derived from `go env GOROOT` and reflects any pinned toolchain, so dropping "+
					"GOTOOLCHAIN desyncs the pair: `env -i` strips the pin, the inner `go` resolves from PATH, "+
					"and every package fails to compile with "+
					"\"version go1.X does not match go tool version go1.Y\" under the agent pre-commit hook (gc-vveco).", rel)
			}
		})
	}
}
