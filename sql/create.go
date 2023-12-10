package sql

import (
	"github.com/luigitni/simpledb/record"
)

func (p Parser) create() (record.Command, error) {
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
func (p Parser) createTable() (record.CreateTableData, error) {
	p.eatKeyword("table")

	table, err := p.eatIdentifier()
	if err != nil {
		return record.CreateTableData{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return record.CreateTableData{}, err
	}

	fields, err := p.fieldDefs()

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return record.CreateTableData{}, err
	}

	return record.NewCreateTableData(table, fields), nil
}

func (p Parser) fieldDefs() (record.Schema, error) {
	schema, err := p.fieldDef()
	if err != nil {
		return record.Schema{}, err
	}

	if p.matchTokenType(TokenComma) {
		p.eatTokenType(TokenComma)
		s, err := p.fieldDefs()
		if err != nil {
			return record.Schema{}, err
		}

		schema.AddAll(s)
	}

	return schema, nil
}

func (p Parser) fieldDef() (record.Schema, error) {
	field, err := p.Field()
	if err != nil {
		return record.Schema{}, err
	}

	return p.fieldType(field)
}

func (p Parser) fieldType(field string) (record.Schema, error) {
	schema := record.NewSchema()
	if p.matchKeyword("int") {
		p.eatKeyword("int")
		schema.AddIntField(field)
		return schema, nil
	}

	if err := p.eatKeyword("varchar"); err != nil {
		return record.Schema{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return record.Schema{}, err
	}

	len, err := p.eatIntConstant()
	if err != nil {
		return record.Schema{}, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return record.Schema{}, err
	}

	schema.AddStringField(field, len)
	return schema, nil
}

// <CreateIndex> := CREATE INDEX TokenIdentifier ON TokenIdentifier ( <Field> )
func (p Parser) createIndex() (record.CreateIndexData, error) {
	p.eatKeyword("index")
	id, err := p.eatIdentifier()
	if err != nil {
		return record.CreateIndexData{}, err
	}

	if err := p.eatKeyword("on"); err != nil {
		return record.CreateIndexData{}, err
	}

	table, err := p.eatIdentifier()
	if err != nil {
		return record.CreateIndexData{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return record.CreateIndexData{}, err
	}

	field, err := p.eatIdentifier()
	if err != nil {
		return record.CreateIndexData{}, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return record.CreateIndexData{}, err
	}

	return record.NewCreateIndexData(id, table, field), nil
}

// <CreateView> := CREATE VIEW TokenIdentifier AS <Query>
func (p Parser) createView() (record.CreateViewData, error) {
	p.eatKeyword("view")
	id, err := p.eatIdentifier()
	if err != nil {
		return record.CreateViewData{}, err
	}

	if err := p.eatKeyword("on"); err != nil {
		return record.CreateViewData{}, err
	}

	query, err := p.Query()
	if err != nil {
		return record.CreateViewData{}, err
	}

	return record.NewCreateViewData(id, query), nil
}
