package record

import "github.com/luigitni/simpledb/file"

// Layout describes the structure of a record.
// It contains the name, type, length and offset of each field of the table
type Layout struct {
	schema   Schema
	offsets  map[string]int
	slotsize int
}

func NewLayout(schema Schema) Layout {

	offsets := make(map[string]int, len(schema.fields))

	s := file.IntSize
	// compute the offset of each field
	for _, f := range schema.fields {
		offsets[f] = s
		s += lenInBytes(schema, f)
	}

	return Layout{
		schema:   schema,
		offsets:  offsets,
		slotsize: s,
	}
}

func newLayoutFromMetadata(schema Schema, offsets map[string]int, slotSize int) Layout {
	return Layout{
		schema:   schema,
		offsets:  offsets,
		slotsize: slotSize,
	}
}

func lenInBytes(schema Schema, field string) int {
	t := schema.ftype(field)
	switch t {
	case file.INTEGER:
		return file.IntSize
	case file.STRING:
		return file.MaxLength(schema.flen(field))
	}
	panic("unsupported type")
}

func (l Layout) Schema() *Schema {
	return &l.schema
}

func (l Layout) Offset(fname string) int {
	return l.offsets[fname]
}

func (l Layout) SlotSize() int {
	return l.slotsize
}
