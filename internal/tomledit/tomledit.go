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

// fields returns Body without its comments and blank lines, so two
// renderings of one stanza compare equal whatever commentary surrounds them.
// A comment written at the end of a line is a comment like any other, so it
// goes the same way a whole comment line does.
func (b Block) fields() []string {
	out := make([]string, 0, len(b.Body))
	state := scanState{}
	for _, line := range b.Body {
		if state.atTopLevel() && isTrivia(line) {
			continue
		}
		var code string
		code, state = state.code(line)
		out = append(out, code)
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
	trimmed := strings.TrimSuffix(source, "\n")
	if trimmed == "" {
		doc.Blocks = []Block{{}}
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
// stanza the edit left alone.
//
// baseline must be the canonical rendering of original: the encoder that
// produced desired, run over the config parsed from original. Comparing the
// two canonical renderings is what identifies the stanzas an edit touched,
// and comparing original against its own rendering is what proves its bytes
// still say the same thing the encoder would.
//
// Inside a stanza the edit did change, a key keeps the comments written about
// it only while its value stands. A comment states why the value under or
// beside it is what it is, so an edit that changes that value leaves the
// comment behind rather than carrying it onto something it no longer
// describes.
//
// ok is false unless all three documents state the same tables in the same
// order, which restricts the splice to edits that only change values. Two
// cases are turned away. A file that states a table in a form the encoder
// never emits, an inline table or a dotted key, has no block to compare
// against. An edit that adds or removes a stanza moves the ones after it,
// and a table header inserted between an array element and the sub-tables
// beneath it re-parents them onto the wrong element. The caller writes
// desired unchanged in both cases, which is what it did before the splice
// existed.
func Splice(original, baseline, desired string) (string, bool) {
	origDoc := Split(original)
	baseDoc := Split(baseline)
	wantDoc := Split(desired)
	if !sameSequence(origDoc.Blocks, baseDoc.Blocks) || !sameSequence(baseDoc.Blocks, wantDoc.Blocks) {
		return "", false
	}

	var out []string
	for i, block := range origDoc.Blocks {
		out = append(out, block.Lead...)
		untouched := equalLines(baseDoc.Blocks[i].fields(), wantDoc.Blocks[i].fields())
		// Carrying bytes through is only equivalent when they state the
		// same keys the encoder does. A file holding something the
		// canonical form strips, such as a machine-local rig path, states
		// more, and copying those bytes would put it back.
		canonical := equalLines(block.fields(), baseDoc.Blocks[i].fields())
		if untouched && canonical {
			out = append(out, block.Body...)
			continue
		}
		out = append(out, mergeBody(block.Body, baseDoc.Blocks[i].Body, wantDoc.Blocks[i].Body)...)
	}
	out = append(out, origDoc.Blocks[len(origDoc.Blocks)-1].Trail...)

	rendered := strings.Join(out, "\n")
	if origDoc.FinalNewline && rendered != "" {
		rendered += "\n"
	}
	return rendered, true
}

// sameSequence reports whether two documents declare the same tables in the
// same order, which is what lets their blocks be compared pairwise.
func sameSequence(a, b []Block) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key() != b[i].Key() {
			return false
		}
	}
	return true
}

// assignment is one key together with the lines stating its value, which is
// more than one line when the value is a multi-line array or string.
type assignment struct {
	key   string
	lead  []string
	lines []string
}

// mergeBody re-states want's values under the comments original wrote about
// each key, so a stanza the edit touched keeps the operator's reasons for the
// keys it left alone.
//
// A key's comments state why its value is what it is, so they only survive an
// edit that left that value alone; a key whose value changed is written with
// want's line and nothing else. baseline and want are both the encoder's
// rendering, so comparing them by key asks whether the value changed and not
// how the file happens to write it. For a key whose value stands, original's
// own lines go back in the form they were written in, which keeps any comment
// written at the end of one and the spacing the author chose. baseline is the
// encoder's rendering of the config parsed from original, so a key baseline
// and want state alike is a key original already states the value of.
func mergeBody(original, baseline, want []string) []string {
	origHeader, origItems, origTrail := splitBody(original)
	_, baseItems, _ := splitBody(baseline)
	header, wantItems, _ := splitBody(want)
	orig := itemsByKey(origItems)
	base := itemsByKey(baseItems)
	if origHeader != "" {
		// Both declare the same table, so original's line declares it with
		// whatever comment was written at the end of it.
		header = origHeader
	}

	out := make([]string, 0, len(want)+len(original))
	if header != "" {
		out = append(out, header)
	}
	for _, item := range wantItems {
		from, stated := orig[item.key]
		if !stated || !equalLines(base[item.key].lines, item.lines) {
			out = append(out, item.lines...)
			continue
		}
		out = append(out, from.lead...)
		out = append(out, from.lines...)
	}
	return append(out, origTrail...)
}

// itemsByKey indexes a body's assignments by the key each one states.
func itemsByKey(items []assignment) map[string]assignment {
	out := make(map[string]assignment, len(items))
	for _, item := range items {
		out[item.key] = item
	}
	return out
}

// splitBody separates a block's body into its table header line, the
// assignments beneath it, and any comment lines trailing the last one.
//
// Only the region ahead of the first header lacks a header line of its own.
// Its body opens with an assignment, or with the comments written above one,
// so header is empty there and every line is read as an item or an item's
// lead, the same way the lines inside a table are.
func splitBody(body []string) (header string, items []assignment, trail []string) {
	if len(body) > 0 {
		if _, _, ok := tableHeader(body[0]); ok {
			header = body[0]
			body = body[1:]
		}
	}
	var pending []string
	state := scanState{}
	for _, line := range body {
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
	_, out := s.scan(line)
	return out
}

// code returns line without the comment written at its end, together with the
// state left after reading it.
func (s scanState) code(line string) (string, scanState) {
	comment, out := s.scan(line)
	if comment < 0 {
		return line, out
	}
	return strings.TrimRight(line[:comment], " \t"), out
}

// scan reads line and returns the offset of the "#" that opens a comment on
// it, or -1 when it opens none, together with the state left after the line.
// A "#" inside a string opens no comment, so the scan tracks the same lexical
// context Split does.
func (s scanState) scan(line string) (comment int, out scanState) {
	for i := 0; i < len(line); {
		if s.inBasic || s.inLiteral {
			delim := `"""`
			if s.inLiteral {
				delim = `'''`
			}
			idx := strings.Index(line[i:], delim)
			if idx < 0 {
				return -1, s
			}
			s.inBasic = false
			s.inLiteral = false
			i += idx + len(delim)
			continue
		}
		switch {
		case line[i] == '#':
			return i, s
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
	return -1, s
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
