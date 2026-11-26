package beadstest

import "strings"

// EmbeddedDoltUnsupported reports whether output, from a bd command that
// failed, is bd refusing an embedded Dolt store because the binary was built
// with CGO_ENABLED=0:
//
//	Error: failed to open Dolt store: embedded Dolt requires a CGO build,
//	but this bd binary was built with CGO_ENABLED=0.
//
// A test that needs an embedded store skips on such a bd, as a test that
// needs bd skips where none is installed: the refusal is a property of the
// host's bd build, not of the code under test. The bd release that Bazel and
// CI pin is a CGO build, so those tests still run there.
//
// The match requires both "embedded dolt" and "cgo". Either term alone is an
// ordinary failure that must stay a hard failure, and a bd error reworded to
// drop both fails the test again, which is a loud failure rather than a
// silent skip.
func EmbeddedDoltUnsupported(output string) bool {
	lower := strings.ToLower(output)
	return strings.Contains(lower, "embedded dolt") && strings.Contains(lower, "cgo")
}
