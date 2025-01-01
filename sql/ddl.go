package sql

import "github.com/luigitni/simpledb/types"

type FieldDef struct {
	Name string
	Type types.FieldType
	Len  int
}

type CreateTableCommand struct {
	DDLCommandType
	TableName string
	Fields    []FieldDef
}

func NewCreateTableCommand(name string, fieldsDef []FieldDef) CreateTableCommand {
	return CreateTableCommand{
		TableName: name,
		Fields:    fieldsDef,
	}
}

type CreateIndexCommand struct {
	DDLCommandType
	IndexName   string
	TableName   string
	TargetField string
}

func NewCreateIndexCommand(name string, table string, field string) CreateIndexCommand {
	return CreateIndexCommand{
		IndexName:   name,
		TableName:   table,
		TargetField: field,
	}
}

type CreateViewCommand struct {
	DDLCommandType
	ViewName string
	Query    Query
}

func NewCreateViewCommand(name string, query Query) CreateViewCommand {
	return CreateViewCommand{
		ViewName: name,
		Query:    query,
	}
}

func (cvd CreateViewCommand) Definition() string {
	return cvd.Query.String()
}

func (p Parser) isDDL() bool {
	return p.matchKeyword("create")
}

func (p Parser) ddl() (Command, error) {
	if err := p.eatKeyword("create"); err != nil {
		return nil, err
	}

	if p.matchKeyword("table") {
		return p.createTable()
	}

	if p.matchKeyword("index") {
		return p.createIndex()
	}

	return p.createView()
}

// <CreateTable> := CREATE TABLE TokenIdentifier ( <FieldDefs> )
func (p Parser) createTable() (CreateTableCommand, error) {
	p.eatKeyword("table")

	table, err := p.eatIdentifier()
	if err != nil {
		return CreateTableCommand{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return CreateTableCommand{}, err
	}

	fields, err := p.fieldDefs()
	if err != nil {
		return CreateTableCommand{}, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return CreateTableCommand{}, err
	}

	return NewCreateTableCommand(table, fields), nil
}

func (p Parser) fieldDefs() ([]FieldDef, error) {
	fields, err := p.fieldDef()
	if err != nil {
		return nil, err
	}

	if p.matchTokenType(TokenComma) {
		p.eatTokenType(TokenComma)
		s, err := p.fieldDefs()
		if err != nil {
			return nil, err
		}

		fields = append(fields, s...)
	}

	return fields, nil
}

func (p Parser) fieldDef() ([]FieldDef, error) {
	field, err := p.field()
	if err != nil {
		return nil, err
	}

	return p.fieldType(field)
}

func (p Parser) fieldType(field string) ([]FieldDef, error) {
	var fields []FieldDef
	if p.matchKeyword("int") {
		p.eatKeyword("int")
		fields = append(fields, FieldDef{
			Name: field,
			Type: types.INTEGER,
		})
		return fields, nil
	}

	if p.matchKeyword("text") {
		p.eatKeyword("text")
		fields = append(fields, FieldDef{
			Name: field,
			Type: types.STRING,
		})
		return fields, nil
	}

	// legacy varchar type
	if err := p.eatKeyword("varchar"); err != nil {
		return nil, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return nil, err
	}

	len, err := p.eatIntValue()
	if err != nil {
		return nil, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return nil, err
	}

	fields = append(fields, FieldDef{
		Name: field,
		Type: types.STRING,
		Len:  len,
	})

	return fields, nil
}

// <CreateIndex> := CREATE INDEX TokenIdentifier ON TokenIdentifier ( <Field> )
func (p Parser) createIndex() (CreateIndexCommand, error) {
	p.eatKeyword("index")
	id, err := p.eatIdentifier()
	if err != nil {
		return CreateIndexCommand{}, err
	}

	if err := p.eatKeyword("on"); err != nil {
		return CreateIndexCommand{}, err
	}

	table, err := p.eatIdentifier()
	if err != nil {
		return CreateIndexCommand{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return CreateIndexCommand{}, err
	}

	field, err := p.eatIdentifier()
	if err != nil {
		return CreateIndexCommand{}, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return CreateIndexCommand{}, err
	}

	return NewCreateIndexCommand(id, table, field), nil
}

// <CreateView> := CREATE VIEW TokenIdentifier AS <Query>
func (p Parser) createView() (CreateViewCommand, error) {
	p.eatKeyword("view")
	id, err := p.eatIdentifier()
	if err != nil {
		return CreateViewCommand{}, err
	}

	if err := p.eatKeyword("on"); err != nil {
		return CreateViewCommand{}, err
	}

	query, err := p.Query()
	if err != nil {
		return CreateViewCommand{}, err
	}

	return NewCreateViewCommand(id, query), nil
}
