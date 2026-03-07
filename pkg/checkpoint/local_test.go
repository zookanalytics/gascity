package checkpoint_test

import (
	"testing"

	"github.com/gastownhall/gascity/pkg/checkpoint"
	"github.com/gastownhall/gascity/pkg/checkpoint/checkpointtest"
)

func TestLocalStoreConformance(t *testing.T) {
	checkpointtest.RunStoreTests(t, func() checkpoint.Store {
		return checkpoint.NewLocalStore(t.TempDir())
	})
}
