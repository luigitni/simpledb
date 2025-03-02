package engine

import "github.com/luigitni/simpledb/storage"

type fieldInfo struct {
	Type  storage.FieldType
	Index storage.SmallInt
}

// Schema is the record schema of a table.
// It contains the name and type of each field of the table,
// As well as the length of each varchar field
type Schema struct {
	fields []string
	info   map[string]fieldInfo
}

func newSchema() Schema {
	return Schema{
		fields: make([]string, 0),
		info:   map[string]fieldInfo{},
	}
}

func newJoinedSchema(first Schema, second Schema) Schema {
	schema := newSchema()
	schema.addAll(first)
	schema.addAll(second)

	return schema
}

func (s *Schema) ftype(name string) storage.FieldType {
	return s.info[name].Type
}

func (s *Schema) FieldInfo(name string) fieldInfo {
	return s.info[name]
}

func (s *Schema) addField(name string, typ storage.FieldType) {
	s.fields = append(s.fields, name)
	s.info[name] = fieldInfo{
		Type:  typ,
		Index: storage.SmallInt(len(s.fields) - 1),
	}
}

func (s *Schema) setFieldAtIndex(name string, typ storage.FieldType, index storage.SmallInt) {
	s.fields = append(s.fields, name)
	s.info[name] = fieldInfo{
		Type:  typ,
		Index: index,
	}
}

func (s *Schema) add(fname string, schema Schema) {
	t := schema.ftype(fname)
	s.addField(fname, t)
}

func (s *Schema) addAll(schema Schema) {
	for _, f := range schema.fields {
		s.add(f, schema)
	}
}

func (s Schema) HasField(fname string) bool {
	_, ok := s.info[fname]
	return ok
}
