package beads

import (
	"bytes"
	"strings"
	"sync"
)

// bdReadyBriefPageLimit is the row limit of a brief ready read's first page.
// bd answers a bounded ready read faster than its unbounded one, so a brief
// read asks for one page first. A page that comes back full may have stopped
// short of the ready set, so readyBriefOutput then reads the whole set.
const bdReadyBriefPageLimit = 5000

// bdReadyBriefRefusals records each scope, keyed by scopeGuardKey, whose bd
// rejected `bd ready --brief`, a flag bd 1.2.0 added. A control-ready scan
// opens a store per scan, so a verdict kept on the store would re-spend the
// refused call on every scan. Entries live for the process. One left behind by
// a bd upgraded while the process runs costs only the row text a brief read
// would have dropped.
var bdReadyBriefRefusals sync.Map // map[string]struct{}

// readyBriefOutput answers a Brief ready query with bd's whole ready set. It
// reads one page first and reads again with no limit when that page may have
// stopped short of the set (bdReadyPageFull).
func (s *BdStore) readyBriefOutput(q ReadyQuery, includeEphemeral bool) ([]byte, error) {
	out, err := s.readyBriefRead(q, includeEphemeral, bdReadyBriefPageLimit)
	if err != nil || !bdReadyPageFull(out) {
		return out, err
	}
	return s.readyBriefRead(q, includeEphemeral, 0)
}

// bdReadyPageFull reports whether a page read may have stopped short of the
// ready set: bd printed a full page, or rows that cannot be counted. It counts
// the rows bd printed, not the rows that parse, so a corrupt row on a full page
// does not pass for the end of the set.
func bdReadyPageFull(out []byte) bool {
	data := extractJSON(out)
	if len(bytes.TrimSpace(data)) == 0 {
		return false
	}
	rows, err := bdListIssueRows(data)
	return err != nil || len(rows) >= bdReadyBriefPageLimit
}

// readyBriefRead takes one `bd ready` read of at most limit rows, asking for
// --brief rows unless this scope's bd is known to reject the flag. When bd
// rejects it, the read is taken again with full rows and the scope is recorded
// in bdReadyBriefRefusals. Only bd's own unknown-flag error for --brief counts
// as a rejection (isBdUnknownFlagError); any other failure is the read's.
func (s *BdStore) readyBriefRead(q ReadyQuery, includeEphemeral bool, limit int) ([]byte, error) {
	key := scopeGuardKey(s.dir)
	if _, refused := bdReadyBriefRefusals.Load(key); refused {
		return s.runBDTransientRead(bdReadyArgs(q, includeEphemeral, limit)...)
	}
	out, err := s.runBDTransientRead(append(bdReadyArgs(q, includeEphemeral, limit), "--brief")...)
	if err != nil && isBdUnknownFlagError(strings.TrimSpace(string(out))+" "+err.Error(), "--brief") {
		bdReadyBriefRefusals.Store(key, struct{}{})
		return s.runBDTransientRead(bdReadyArgs(q, includeEphemeral, limit)...)
	}
	return out, err
}
