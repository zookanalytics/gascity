package beads

import "bytes"

// bdReadyBriefPageLimit is the row limit of a brief ready read's first page.
// bd answers a bounded ready read faster than its unbounded one, so a brief
// read asks for one page first. A page that comes back full may have stopped
// short of the ready set, so readyBriefOutput then reads the whole set.
const bdReadyBriefPageLimit = 5000

// readyBriefOutput answers a ready query whose field set omits the text with
// bd's whole ready set. It reads one page first and reads again with no limit
// when that page may have stopped short of the set (bdReadyPageFull).
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
// --brief rows unless this scope's bd is known to refuse the flag. A bd that
// refuses it is read again with full rows (ReadWithBdBrief).
func (s *BdStore) readyBriefRead(q ReadyQuery, includeEphemeral bool, limit int) ([]byte, error) {
	return ReadWithBdBrief(s.dir, func(brief bool) ([]byte, error) {
		return s.runBDTransientRead(bdReadyArgs(q, includeEphemeral, limit, brief)...)
	})
}
