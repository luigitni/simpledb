package record

type Command interface{}

type FieldList []string

type ConstantList []Constant

type InsertData struct {
	Command
	TableName string
	Fields    FieldList
	Values    ConstantList
}

func NewInsertData(table string, fields FieldList, values ConstantList) InsertData {
	return InsertData{
		TableName: table,
		Fields:    fields,
		Values:    values,
	}
}

type DeleteData struct {
	Command
	TableName string
	Predicate Predicate
}

func NewDeleteData(table string) DeleteData {
	return DeleteData{
		TableName: table,
		Predicate: Predicate{},
	}
}

func NewDeleteDataWithPredicate(table string, predicate Predicate) DeleteData {
	return DeleteData{
		TableName: table,
		Predicate: predicate,
	}
}

type ModifyData struct {
	Command
	TableName string
	Field     string
	NewValue  Expression
	Predicate Predicate
}

func NewModifyData(table string, field string, expression Expression) ModifyData {
	return ModifyData{
		TableName: table,
		Field:     field,
		NewValue:  expression,
		Predicate: Predicate{},
	}
}

func NewModifyDataWithPredicate(table string, field string, expression Expression, predicate Predicate) ModifyData {
	m := NewModifyData(table, field, expression)
	m.Predicate = predicate
	return m
}
