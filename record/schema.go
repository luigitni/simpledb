package record

import "github.com/luigitni/simpledb/file"

type fieldInfo struct {
	Type   file.FieldType
	Lenght int
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

func (s *Schema) ftype(name string) file.FieldType {
	return s.info[name].Type
}

func (s *Schema) flen(name string) int {
	return s.info[name].Lenght
}

func (s *Schema) addField(name string, typ file.FieldType, lenght int) {
	s.fields = append(s.fields, name)
	s.info[name] = fieldInfo{
		Type:   typ,
		Lenght: lenght,
	}
}

func (s *Schema) addIntField(name string) {
	s.addField(name, file.INTEGER, 0)
}

// addStringField adds a string field to the schema, of type VARCHAR
// The length is the conceptual length of the field.
// For example, if the field is described as VARCHAR(8), then length is 8
func (s *Schema) addStringField(name string, length int) {
	s.addField(name, file.STRING, length)
}

func (s *Schema) add(fname string, schema Schema) {
	t := schema.ftype(fname)
	l := schema.flen(fname)
	s.addField(fname, t, l)
}

func (s *Schema) addAll(schema Schema) {
	for _, f := range schema.fields {
		s.add(f, schema)
	}
}

func (s Schema) hasField(fname string) bool {
	_, ok := s.info[fname]
	return ok
}
