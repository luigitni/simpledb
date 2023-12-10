package record

type CreateTableData struct {
	Command
	TableName string
	Schema    Schema
}

func NewCreateTableData(name string, schema Schema) CreateTableData {
	return CreateTableData{
		TableName: name,
		Schema:    schema,
	}
}

type CreateIndexData struct {
	Command
	IndexName   string
	TableName   string
	TargetField string
}

func NewCreateIndexData(name string, table string, field string) CreateIndexData {
	return CreateIndexData{
		IndexName:   name,
		TableName:   table,
		TargetField: field,
	}
}

type CreateViewData struct {
	Command
	ViewName string
	Query    QueryData
}

func NewCreateViewData(name string, query QueryData) CreateViewData {
	return CreateViewData{
		ViewName: name,
		Query:    query,
	}
}

func (cvd CreateViewData) Definition() string {
	return cvd.Query.String()
}