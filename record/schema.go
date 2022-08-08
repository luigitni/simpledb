package record

type FieldType int

const (
	INTEGER FieldType = iota
	STRING
)

type FieldInfo struct {
	Type   FieldType
	Lenght int
}

// Schema is the record schema of a table.
// It contains the name and type of each field of the table,
// As well as the length of each varchar field
type Schema struct {
	fields []string
	info   map[string]FieldInfo
}

func MakeSchema() Schema {
	return Schema{
		fields: make([]string, 8),
		info:   map[string]FieldInfo{},
	}
}

func (s *Schema) Type(name string) FieldType {
	return s.info[name].Type
}

func (s *Schema) Length(name string) int {
	return s.info[name].Lenght
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) AddField(name string, typ FieldType, lenght int) {
	s.fields = append(s.fields, name)
	s.info[name] = FieldInfo{
		Type:   typ,
		Lenght: lenght,
	}
}

func (s *Schema) AddIntField(name string) {
	s.AddField(name, INTEGER, 0)
}

// AddStringField adds a string field to the schema, of type VARCHAR
// The length is the conceptual length of the field.
// For example, if the field is described as VARCHAR(8), then length is 8
func (s *Schema) AddStringField(name string, length int) {
	s.AddField(name, STRING, length)
}

func (s *Schema) Add(fname string, schema Schema) {
	t := schema.Type(fname)
	l := schema.Length(fname)
	s.AddField(fname, t, l)
}

func (s *Schema) AddAll(schema Schema) {
	for _, f := range schema.Fields() {
		s.Add(f, schema)
	}
}

func (s *Schema) HasField(fname string) bool {
	_, ok := s.info[fname]
	return ok
}
