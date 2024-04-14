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

	offsets := make(map[string]int, len(schema.Fields()))

	s := file.IntBytes
	// compute the offset of each field
	for _, f := range schema.Fields() {
		offsets[f] = s
		s += lenInBytes(schema, f)
	}

	return Layout{
		schema:   schema,
		offsets:  offsets,
		slotsize: s,
	}
}

func NewLayoutFromMetadata(schema Schema, offsets map[string]int, slotSize int) Layout {
	return Layout{
		schema:   schema,
		offsets:  offsets,
		slotsize: slotSize,
	}
}

func lenInBytes(schema Schema, field string) int {
	t := schema.Type(field)
	switch t {
	case file.INTEGER:
		return file.IntBytes
	case file.STRING:
		return file.MaxLength(schema.Length(field))
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
