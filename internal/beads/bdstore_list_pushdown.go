package beads

import (
	"slices"
	"sort"
	"strings"
)

// bdListIDFlagMaxBytes bounds the value of one --id argument and the length of
// one quoted id. Linux refuses a single argument longer than 128 KiB
// (MAX_ARG_STRLEN), and the bound keeps half of that as margin: about 6,000
// ids of ten bytes in one --id value.
const bdListIDFlagMaxBytes = 64 << 10

// bdIDReads returns the reads that select ids, given listArgs, the bd list
// read the query's other filters spell. Every id is read by id, so the reads
// are bounded by the ids however many there are.
//
// bd list splits an --id value on commas and trims each id. The ids that
// survive that unchanged are sorted, so the argv does not depend on the
// caller's order, and sent in --id values of at most bdListIDFlagMaxBytes,
// one bd list read per value. An id holding a comma or edge whitespace is read
// alone through bd query, whose quoted id= clause selects it exactly from
// either table; includeClosed adds --all to that read, as it does to the bd
// list read. ok is false when an id fits neither read: bd's query lexer reads
// one byte at a time, so a NUL or non-ASCII byte reaches the comparison
// altered, and an id over the bound may not survive exec. The caller then
// reads every row and matches the ids in Go.
func bdIDReads(listArgs, ids []string, includeClosed bool) (reads [][]string, ok bool) {
	var listed []string
	for _, id := range distinctNonEmpty(ids) {
		switch {
		case len(id) > bdListIDFlagMaxBytes:
			return nil, false
		case !strings.Contains(id, ",") && strings.TrimSpace(id) == id:
			listed = append(listed, id)
		case bdQueryCanQuoteID(id):
			read := []string{"query", "--json", "id=" + quoteBdQueryValue(id)}
			if includeClosed {
				read = append(read, "--all")
			}
			reads = append(reads, append(read, "--limit", "0"))
		default:
			return nil, false
		}
	}
	sort.Strings(listed)
	byValue := make([][]string, 0, 1)
	for start := 0; start < len(listed); {
		end, size := start, 0
		for end < len(listed) && (end == start || size+1+len(listed[end]) <= bdListIDFlagMaxBytes) {
			if end > start {
				size++
			}
			size += len(listed[end])
			end++
		}
		read := slices.Clip(listArgs)
		byValue = append(byValue, append(read, "--id="+strings.Join(listed[start:end], ",")))
		start = end
	}
	return append(byValue, reads...), true
}

// bdQueryCanQuoteID reports whether quoteBdQueryString carries id to bd's query
// comparison unchanged: no NUL and no non-ASCII byte.
func bdQueryCanQuoteID(id string) bool {
	for i := 0; i < len(id); i++ {
		if id[i] == 0 || id[i] >= 0x80 {
			return false
		}
	}
	return true
}
