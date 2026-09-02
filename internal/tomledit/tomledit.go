// Package tomledit rewrites TOML documents as text instead of re-encoding
// them. Changing one stanza leaves every other stanza exactly as the author
// wrote it, including its comments, its key order, and its spacing.
//
// A struct round trip is lossy in a way that matters for a checked-in
// config. The encoder emits values, so everything written around those
// values to explain them is gone the next time any command touches the
// file.
package tomledit

import "strings"

// Block is one top-level region of a TOML document: a table header, the
// lines beneath it, and the comment and blank lines written above it.
type Block struct {
	// Header is the table path written between the brackets, so
	// "providers.claude" for [providers.claude] and "rigs" for [[rigs]].
	// The region ahead of the first header has an empty Header.
	Header string
	// Array reports whether the header was written as an array of tables.
	Array bool
	// Lead holds the comment and blank lines written above the header.
	Lead []string
	// Body holds the header line and every line beneath it, including any
	// comments written between the keys.
	Body []string
	// Trail holds the comment and blank lines that end the document. Only
	// a document's last block carries them: every other run of comment and
	// blank lines is the Lead of the header below it.
	Trail []string
}

// Key identifies the table a block declares. Blocks sharing a key are the
// successive elements of one array of tables.
func (b Block) Key() string {
	if b.Array {
		return "[[" + b.Header + "]]"
	}
	return "[" + b.Header + "]"
}

// fields returns Body without its comment and blank lines, so two
// renderings of one stanza compare equal whatever commentary surrounds
// them.
func (b Block) fields() []string {
	out := make([]string, 0, len(b.Body))
	for _, line := range b.Body {
		if isTrivia(line) {
			continue
		}
		out = append(out, line)
	}
	return out
}

// Document is a TOML source split into blocks.
type Document struct {
	Blocks []Block
	// FinalNewline reports whether the source ended with a newline.
	FinalNewline bool
}

// String renders the document back to source, reproducing Split's input
// byte for byte.
func (d Document) String() string {
	var lines []string
	for _, block := range d.Blocks {
		lines = append(lines, block.Lead...)
		lines = append(lines, block.Body...)
		lines = append(lines, block.Trail...)
	}
	if len(lines) == 0 {
		if d.FinalNewline {
			return "\n"
		}
		return ""
	}
	out := strings.Join(lines, "\n")
	if d.FinalNewline {
		out += "\n"
	}
	return out
}

// Split divides source into blocks. It always returns at least one block:
// the region ahead of the first table header, which is empty when the file
// opens with one.
func Split(source string) Document {
	doc := Document{FinalNewline: strings.HasSuffix(source, "\n")}
	if source == "" {
		return doc
	}
	trimmed := strings.TrimSuffix(source, "\n")
	if trimmed == "" {
		return doc
	}

	current := Block{}
	var pending []string
	state := scanState{}
	for _, line := range strings.Split(trimmed, "\n") {
		if state.atTopLevel() && isTrivia(line) {
			// Which block owns a run of comments is not yet decided: a
			// header below it takes it as Lead, anything else leaves it in
			// the block being read.
			pending = append(pending, line)
			continue
		}
		if header, array, ok := tableHeader(line); ok && state.atTopLevel() {
			doc.Blocks = append(doc.Blocks, current)
			current = Block{Header: header, Array: array, Lead: pending, Body: []string{line}}
			pending = nil
			state = state.update(line)
			continue
		}
		current.Body = append(current.Body, pending...)
		pending = nil
		current.Body = append(current.Body, line)
		state = state.update(line)
	}
	current.Trail = pending
	doc.Blocks = append(doc.Blocks, current)
	return doc
}

// Splice renders desired while keeping original's exact bytes for every
// block the edit left alone.
//
// baseline must be the canonical rendering of original: the encoder that
// produced desired, run over the config parsed from original. A block whose
// baseline text still appears in desired is therefore provably untouched,
// and original's bytes for it are carried through verbatim. A block whose
// text changed is re-encoded under the comments written above it.
//
// ok is false when original's blocks do not correspond one for one with
// baseline's, which happens when the file states a table in a form the
// encoder does not emit, such as an inline table or a dotted key. The
// caller must then write desired unchanged: splicing a layout that cannot
// be accounted for risks emitting a stanza twice.
func Splice(original, baseline, desired string) (string, bool) {
	origDoc := Split(original)
	baseDoc := Split(baseline)
	wantDoc := Split(desired)

	origByKey := indexByKey(origDoc.Blocks)
	baseByKey := indexByKey(baseDoc.Blocks)
	wantByKey := indexByKey(wantDoc.Blocks)
	if !sameShape(origByKey, baseByKey) {
		return "", false
	}

	// replacement[i] is the desired block that supersedes original block i;
	// -1 marks a stanza the edit removed. verbatim[i] additionally reports
	// that original's own bytes still state exactly what the encoder would,
	// which is what makes copying them through safe. added[i] lists desired
	// blocks to emit after original block i, which is how a new array
	// element lands beside its siblings instead of at the end of the file.
	replacement := make([]int, len(origDoc.Blocks))
	verbatim := make([]bool, len(origDoc.Blocks))
	added := make([][]int, len(origDoc.Blocks))
	for i := range replacement {
		replacement[i] = -1
	}

	claimed := make(map[int]bool, len(wantDoc.Blocks))
	for key, origPos := range origByKey {
		basePos := baseByKey[key]
		wantPos := wantByKey[key]
		taken := make([]bool, len(wantPos))

		// An element whose canonical text still appears in desired is
		// untouched, wherever the edit moved it to.
		for i, base := range basePos {
			for j, want := range wantPos {
				if taken[j] || !equalLines(baseDoc.Blocks[base].fields(), wantDoc.Blocks[want].fields()) {
					continue
				}
				replacement[origPos[i]] = want
				// Copying original's bytes is only equivalent when they
				// state the same keys the encoder does. A file carrying
				// something the canonical form strips, such as a
				// machine-local rig path, states more, and emitting those
				// bytes verbatim would put it back.
				verbatim[origPos[i]] = equalLines(
					origDoc.Blocks[origPos[i]].fields(),
					baseDoc.Blocks[base].fields(),
				)
				taken[j] = true
				claimed[want] = true
				break
			}
		}
		// Whatever is left pairs in order: an edit that rewrites one field
		// of one stanza leaves exactly that stanza on both sides.
		next := 0
		for i := range basePos {
			if replacement[origPos[i]] >= 0 {
				continue
			}
			for next < len(wantPos) && taken[next] {
				next++
			}
			if next == len(wantPos) {
				break
			}
			replacement[origPos[i]] = wantPos[next]
			taken[next] = true
			claimed[wantPos[next]] = true
		}
		// Elements desired gained follow the key's last existing stanza.
		last := origPos[len(origPos)-1]
		for j, want := range wantPos {
			if taken[j] {
				continue
			}
			added[last] = append(added[last], want)
			claimed[want] = true
		}
	}

	var out []string
	for i, block := range origDoc.Blocks {
		switch {
		case replacement[i] < 0:
			// The edit removed this stanza, and its commentary with it.
		case verbatim[i]:
			out = append(out, block.Lead...)
			out = append(out, block.Body...)
		default:
			out = append(out, block.Lead...)
			out = append(out, mergeBody(block.Body, wantDoc.Blocks[replacement[i]].Body)...)
		}
		for _, want := range added[i] {
			out = append(out, wantDoc.Blocks[want].Lead...)
			out = append(out, wantDoc.Blocks[want].Body...)
		}
	}
	// Tables desired introduces that the file never had go at the end, in
	// the order the encoder emits them.
	for i, block := range wantDoc.Blocks {
		if claimed[i] {
			continue
		}
		out = append(out, block.Lead...)
		out = append(out, block.Body...)
	}
	out = append(out, origDoc.Blocks[len(origDoc.Blocks)-1].Trail...)

	rendered := strings.Join(out, "\n")
	if origDoc.FinalNewline && rendered != "" {
		rendered += "\n"
	}
	return rendered, true
}

// assignment is one key together with the lines stating its value, which is
// more than one line when the value is a multi-line array or string.
type assignment struct {
	key   string
	lead  []string
	lines []string
}

// mergeBody re-states want's values under the comments original wrote above
// each key, so a stanza the edit touched keeps the operator's reasons for
// the keys it left alone.
func mergeBody(original, want []string) []string {
	_, origItems, origTrail := splitBody(original)
	header, wantItems, _ := splitBody(want)
	lead := make(map[string][]string, len(origItems))
	for _, item := range origItems {
		lead[item.key] = item.lead
	}
	out := make([]string, 0, len(want)+len(original))
	if header != "" {
		out = append(out, header)
	}
	for _, item := range wantItems {
		out = append(out, lead[item.key]...)
		out = append(out, item.lines...)
	}
	return append(out, origTrail...)
}

// splitBody separates a block's body into its header line, the assignments
// beneath it, and any comment lines trailing the last one.
func splitBody(body []string) (header string, items []assignment, trail []string) {
	if len(body) == 0 {
		return "", nil, nil
	}
	header = body[0]
	var pending []string
	state := scanState{}
	for _, line := range body[1:] {
		switch {
		case state.atTopLevel() && isTrivia(line):
			pending = append(pending, line)
			continue
		case state.atTopLevel() || len(items) == 0:
			items = append(items, assignment{key: assignmentKey(line), lead: pending, lines: []string{line}})
			pending = nil
		default:
			// A continuation of the value the previous line opened.
			items[len(items)-1].lines = append(items[len(items)-1].lines, line)
		}
		state = state.update(line)
	}
	return header, items, pending
}

// assignmentKey returns the key a line assigns, or "" when it states none.
func assignmentKey(line string) string {
	trimmed := strings.TrimSpace(line)
	for i := 0; i < len(trimmed); {
		switch trimmed[i] {
		case '=':
			return strings.TrimSpace(trimmed[:i])
		case '"':
			i += basicStringLen(trimmed[i:])
		case '\'':
			i += literalStringLen(trimmed[i:])
		default:
			i++
		}
	}
	return ""
}

// indexByKey groups block positions by the table each declares, keeping the
// order they appear in.
func indexByKey(blocks []Block) map[string][]int {
	byKey := make(map[string][]int, len(blocks))
	for i, block := range blocks {
		byKey[block.Key()] = append(byKey[block.Key()], i)
	}
	return byKey
}

// sameShape reports whether two documents declare the same tables the same
// number of times, which is what makes their blocks pairwise comparable.
func sameShape(a, b map[string][]int) bool {
	if len(a) != len(b) {
		return false
	}
	for key, positions := range a {
		if len(b[key]) != len(positions) {
			return false
		}
	}
	return true
}

func equalLines(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// isTrivia reports whether a line carries no TOML value: it is blank or is
// wholly a comment. Only meaningful at top level, where a line cannot be
// the interior of a multi-line string.
func isTrivia(line string) bool {
	trimmed := strings.TrimSpace(line)
	return trimmed == "" || strings.HasPrefix(trimmed, "#")
}

// tableHeader reports whether line declares a table, returning the path
// written between the brackets and whether it is an array of tables.
func tableHeader(line string) (header string, array bool, ok bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "[") {
		return "", false, false
	}
	open := 1
	if strings.HasPrefix(trimmed, "[[") {
		open = 2
		array = true
	}
	end := closingBracket(trimmed[open:])
	if end < 0 {
		return "", false, false
	}
	return strings.TrimSpace(trimmed[open : open+end]), array, true
}

// closingBracket returns the offset of the "]" that ends a table header,
// skipping any bracket written inside a quoted key. It returns -1 when the
// header is unterminated.
func closingBracket(s string) int {
	for i := 0; i < len(s); {
		switch s[i] {
		case ']':
			return i
		case '"':
			i += basicStringLen(s[i:])
		case '\'':
			i += literalStringLen(s[i:])
		default:
			i++
		}
	}
	return -1
}

// scanState tracks the TOML lexical context that carries across lines: an
// open multi-line string and the depth of an unclosed array. A line only
// declares a table when both are clear, so a "[" inside a multi-line array
// or a """ block is never mistaken for a header.
type scanState struct {
	inBasic   bool
	inLiteral bool
	depth     int
}

func (s scanState) atTopLevel() bool {
	return !s.inBasic && !s.inLiteral && s.depth == 0
}

// update returns the state left after reading line.
func (s scanState) update(line string) scanState {
	for i := 0; i < len(line); {
		if s.inBasic || s.inLiteral {
			delim := `"""`
			if s.inLiteral {
				delim = `'''`
			}
			idx := strings.Index(line[i:], delim)
			if idx < 0 {
				return s
			}
			s.inBasic = false
			s.inLiteral = false
			i += idx + len(delim)
			continue
		}
		switch {
		case line[i] == '#':
			return s
		case strings.HasPrefix(line[i:], `"""`):
			s.inBasic = true
			i += 3
		case strings.HasPrefix(line[i:], `'''`):
			s.inLiteral = true
			i += 3
		case line[i] == '"':
			i += basicStringLen(line[i:])
		case line[i] == '\'':
			i += literalStringLen(line[i:])
		case line[i] == '[':
			s.depth++
			i++
		case line[i] == ']':
			if s.depth > 0 {
				s.depth--
			}
			i++
		default:
			i++
		}
	}
	return s
}

// basicStringLen returns the length of the quoted string opening at s[0],
// honoring backslash escapes. An unterminated string consumes the rest of
// the line, which keeps the scanner in step on malformed input.
func basicStringLen(s string) int {
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return len(s)
}

// literalStringLen returns the length of the single-quoted string opening
// at s[0]. Literal strings have no escapes.
func literalStringLen(s string) int {
	if idx := strings.IndexByte(s[1:], '\''); idx >= 0 {
		return idx + 2
	}
	return len(s)
}
