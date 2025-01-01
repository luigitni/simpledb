package record

import "github.com/luigitni/simpledb/types"

type fieldInfo struct {
	Type   types.FieldType
	Lenght int
	Index  int
}

// Schema is the record schema of a table.
// It contains the name and type of each field of the table,
// As well as the length of each varchar field
type Schema struct {
	idx    int
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

func (s *Schema) ftype(name string) types.FieldType {
	return s.info[name].Type
}

func (s *Schema) flen(name string) int {
	return s.info[name].Lenght
}

func (s *Schema) addField(name string, typ types.FieldType) {
	s.fields = append(s.fields, name)
	s.info[name] = fieldInfo{
		Type:  typ,
		Index: s.idx,
	}
	s.idx++
}

func (s *Schema) setFieldAtIndex(name string, typ types.FieldType, index int) {
	s.fields = append(s.fields, name)
	s.info[name] = fieldInfo{
		Type:  typ,
		Index: index,
	}
}

func (s *Schema) addIntField(name string) {
	s.addField(name, types.INTEGER)
}

func (s *Schema) addStringField(name string) {
	s.addField(name, types.STRING)
}

// addFixedLenStringField adds a string field to the schema, of type VARCHAR
// The length is the conceptual length of the field.
// For example, if the field is described as VARCHAR(8), then length is 8
func (s *Schema) addFixedLenStringField(name string, length int) {
	s.addStringField(name)
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
