package engine

import "github.com/luigitni/simpledb/storage"

// Layout describes the structure of a record.
// It contains the name, type, length and offset of each field of the table
type Layout struct {
	schema       Schema
	fieldIndexes map[string]storage.SmallInt
	offsets      map[string]storage.Offset
	slotsize     storage.Offset
}

func NewLayout(schema Schema) Layout {
	offsets := make(map[string]storage.Offset, len(schema.fields))
	fieldIndexes := make(map[string]storage.SmallInt, len(schema.fields))

	var s storage.Offset
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

func lenInBytes(schema Schema, field string) storage.Offset {
	return schema.ftype(field).Size()
}

func (l Layout) Schema() *Schema {
	return &l.schema
}

func (l Layout) Offset(fname string) storage.Offset {
	return l.offsets[fname]
}

func (l Layout) FieldIndex(fname string) int {
	idx, ok := l.fieldIndexes[fname]
	if !ok {
		return -1
	}

	return int(idx)
}

func (l Layout) FieldSize(fname string) storage.Offset {
	return lenInBytes(l.schema, fname)
}

func (l Layout) FieldSizeByIndex(idx int) storage.Offset {
	return lenInBytes(l.schema, l.schema.fields[idx])
}

func (l Layout) FieldsCount() int {
	return len(l.schema.fields)
}

func (l Layout) SlotSize() storage.Offset {
	return l.slotsize
}
