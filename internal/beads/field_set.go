package beads

// FieldSet names the bead fields a read must populate. A read that asks for
// FieldsCore declares that its caller reads no free-form text, so the backing
// may leave that text unread.
//
// The free-form text is the part of a row whose size nothing the controller
// evaluates bounds: a bead's description and, in bd's schema, its design,
// acceptance criteria, notes, payload and waiters. Bead carries only the
// description of these, but a backing that reads whole rows still moves all
// of them, so one oversized notes field multiplies the cost of every read
// that returns its row.
//
// The zero value is FieldsFull, so a query that names no field set reads
// whole rows. See the package documentation for how each backing honors
// FieldsCore.
type FieldSet uint8

const (
	// FieldsFull populates every field.
	FieldsFull FieldSet = iota
	// FieldsCore populates every field except the free-form text. Every row
	// a FieldsCore read returns has an empty Description, on every backing,
	// and every other field holds what a FieldsFull read of the row returns.
	FieldsCore
)

// omitsText reports whether f leaves the free-form text out of a row. A value
// outside the declared constants reads whole rows.
func (f FieldSet) omitsText() bool {
	return f == FieldsCore
}

// project clears from every row the fields f omits and returns rows. It
// writes through the slice, so the rows must be the caller's own copies.
func (f FieldSet) project(rows []Bead) []Bead {
	if !f.omitsText() {
		return rows
	}
	for i := range rows {
		rows[i].Description = ""
	}
	return rows
}
