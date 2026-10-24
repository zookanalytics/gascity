package beads

import (
	"sync"
	"sync/atomic"
)

// bdBriefFlag is the projection flag `bd list` and `bd ready` take from bd
// 1.2.0 on. It drops the free-form text (description, design, acceptance
// criteria, notes, payload, waiters) from bd's SELECT and from every JSON row,
// and leaves every other field, the rows that match and their order as they
// were. `bd ready` takes it only beside --json. `bd query` has no such flag.
const bdBriefFlag = "--brief"

// bdBriefRefusals records, per scope path, that the bd driving the scope
// refused --brief as an unknown flag.
//
// The verdict is kept per scope rather than on the store object because
// cmd/gc builds a store per request and the control dispatcher builds one per
// readiness scan: a verdict kept on the store would cost a bd older than 1.2.0
// one refused subprocess on every scan. Only a refusal is recorded, never
// support, so the worst a stale verdict can do after a bd upgrade is keep
// reading whole rows until the process restarts.
var bdBriefRefusals sync.Map // map[string]*atomic.Bool

// bdBriefRefusalFor returns the refusal verdict shared by every read of dir.
func bdBriefRefusalFor(dir string) *atomic.Bool {
	key := scopeGuardKey(dir)
	if v, ok := bdBriefRefusals.Load(key); ok {
		return v.(*atomic.Bool)
	}
	v, _ := bdBriefRefusals.LoadOrStore(key, new(atomic.Bool))
	return v.(*atomic.Bool)
}

// ReadWithBdBrief runs read for a bd invocation in dir that may carry
// --brief. read gets brief=true unless the bd driving dir has already refused
// the flag. When this run's bd refuses it as an unknown flag, ReadWithBdBrief
// records the refusal for dir and runs read once more with brief=false, so a
// bd older than 1.2.0 answers with whole rows instead of failing the read.
// Any other error is returned as read returned it.
//
// read must put --brief on its command line exactly when brief is true, and
// its error must carry bd's own output: the refusal is recognized from bd's
// "unknown flag: --brief" usage error.
func ReadWithBdBrief[T any](dir string, read func(brief bool) (T, error)) (T, error) {
	refused := bdBriefRefusalFor(dir)
	if refused.Load() {
		return read(false)
	}
	out, err := read(true)
	if err == nil || !isBdUnknownFlagError(err.Error(), bdBriefFlag) {
		return out, err
	}
	refused.Store(true)
	return read(false)
}
