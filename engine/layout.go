package engine

import "github.com/luigitni/simpledb/storage"

// Layout describes the structure of a record.
// It contains the name, type, length and offset of each field of the table
type Layout struct {
	schema       Schema
	fieldIndexes map[string]int
	offsets      map[string]int
	sizes        map[string]int
	slotsize     int
}

func NewLayout(schema Schema) Layout {
	offsets := make(map[string]int, len(schema.fields))
	fieldIndexes := make(map[string]int, len(schema.fields))

	s := storage.IntSize
	// compute the offset of each field
	for _, f := range schema.fields {
		fieldIndexes[f] = schema.info[f].Index
		offsets[f] = s
		s += lenInBytes(schema, f)
	}

	return Layout{
		schema:       schema,
		fieldIndexes: fieldIndexes,
		offsets:      offsets,
		slotsize:     s,
	}
}

const varlen = -1

func lenInBytes(schema Schema, field string) int {
	t := schema.ftype(field)
	switch t {
	case storage.INTEGER:
		return int(storage.SizeOfInt)
	case storage.STRING:
		return -1
	}
	panic("unsupported type")
}

func (l Layout) Schema() *Schema {
	return &l.schema
}

func (l Layout) Offset(fname string) int {
	return l.offsets[fname]
}

func (l Layout) FieldIndex(fname string) int {
	idx, ok := l.fieldIndexes[fname]
	if !ok {
		return -1
	}

	return idx
}

func (l Layout) FieldSize(fname string) storage.Size {
	return storage.Size(lenInBytes(l.schema, fname))
}

func (l Layout) FieldsCount() int {
	return len(l.schema.fields)
}

func (l Layout) SlotSize() int {
	return l.slotsize
}
