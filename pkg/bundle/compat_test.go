package bundle

import (
	"testing"
)

// TestSchemaVersionCompat is the Phase 0 exit gate: verifies that the runtime
// accepts bundles at schema version N-1 and N, and rejects N+1.
func TestSchemaVersionCompat(t *testing.T) {
	t.Run("AcceptsCurrent", func(t *testing.T) {
		if !CanAccept(SchemaVersion) {
			t.Errorf("CanAccept(N=%d) = false, must accept current version", SchemaVersion)
		}
	})

	t.Run("AcceptsPrevious", func(t *testing.T) {
		prev := SchemaVersion - 1
		if !CanAccept(prev) {
			t.Errorf("CanAccept(N-1=%d) = false, must accept previous version", prev)
		}
	})

	t.Run("RejectsNext", func(t *testing.T) {
		next := SchemaVersion + 1
		if CanAccept(next) {
			t.Errorf("CanAccept(N+1=%d) = true, must reject future version", next)
		}
	})
}
