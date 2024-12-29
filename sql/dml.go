package sql

import "github.com/luigitni/simpledb/file"

type CommandType byte

const (
	// Query Command Type
	CommandTypeQuery CommandType = iota
	// Data Manipulation Language statement (INSERT, UPDATE, DELETE)
	CommandTypeDML
	// Data Definition Language statement (CREATE, ALTER, TRUNCATE, DROP)
	CommandTypeDDL
	// Transaction control language statement (BEGIN, COMMIT, ROLLBACK)
	CommandTypeTCLBegin
	CommandTypeTCLCommit
	CommandTypeTCLRollback
)

type QueryCommandType struct{}

func (qct QueryCommandType) Type() CommandType {
	return CommandTypeQuery
}

type DMLCommandType struct{}

func (dml DMLCommandType) Type() CommandType {
	return CommandTypeDML
}

type DDLCommandType struct{}

func (DDL DDLCommandType) Type() CommandType {
	return CommandTypeDDL
}

type Command interface {
	Type() CommandType
}

type InsertCommand struct {
	DMLCommandType
	TableName string
	Fields    []string
	Values    []file.Value
}

func NewInsertCommand(table string, fields []string, values []file.Value) InsertCommand {
	return InsertCommand{
		TableName: table,
		Fields:    fields,
		Values:    values,
	}
}

type DeleteCommand struct {
	DMLCommandType
	TableName string
	Predicate Predicate
}

func NewDeleteCommand(table string) DeleteCommand {
	return DeleteCommand{
		TableName: table,
		Predicate: Predicate{},
	}
}

func NewDeleteCommandWithPredicate(table string, predicate Predicate) DeleteCommand {
	return DeleteCommand{
		TableName: table,
		Predicate: predicate,
	}
}

type UpdateCommand struct {
	DMLCommandType
	TableName string
	Field     string
	NewValue  Expression
	Predicate Predicate
}

func NewUpdateCommand(table string, field string, expression Expression) UpdateCommand {
	return UpdateCommand{
		TableName: table,
		Field:     field,
		NewValue:  expression,
		Predicate: Predicate{},
	}
}

func NewUpdateCommandWithPredicate(table string, field string, expression Expression, predicate Predicate) UpdateCommand {
	m := NewUpdateCommand(table, field, expression)
	m.Predicate = predicate
	return m
}

func (p Parser) isDML() bool {
	return p.matchKeyword("insert") || p.matchKeyword("update") || p.matchKeyword("delete")
}

func (p Parser) dml() (Command, error) {
	if p.matchKeyword("insert") {
		return p.insert()
	}

	if p.matchKeyword("update") {
		return p.modify()
	}

	if p.matchKeyword("delete") {
		return p.delete()
	}

	return p.ddl()
}

// <Delete> := DELETE FROM TokenIdentifier [ WHERE <Predicate> ]
func (p Parser) delete() (DeleteCommand, error) {
	p.eatKeyword("delete")

	if err := p.eatKeyword("from"); err != nil {
		return DeleteCommand{}, err
	}

	table, err := p.eatIdentifier()
	if err != nil {
		return DeleteCommand{}, err
	}

	if p.matchKeyword("where") {
		p.eatKeyword("where")
		pred, err := p.predicate()
		if err != nil {
			return DeleteCommand{}, err
		}

		return NewDeleteCommandWithPredicate(table, pred), nil
	}

	return NewDeleteCommand(table), nil
}

// <Insert> := INSERT INTO TokenIdentifier ( <FieldList> ) VALUES ( <ConstList> )
func (p Parser) insert() (InsertCommand, error) {
	if err := p.eatKeyword("insert"); err != nil {
		return InsertCommand{}, err
	}

	if err := p.eatKeyword("into"); err != nil {
		return InsertCommand{}, nil
	}

	table, err := p.eatIdentifier()
	if err != nil {
		return InsertCommand{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return InsertCommand{}, err
	}

	fields, err := p.fieldList()
	if err != nil {
		return InsertCommand{}, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return InsertCommand{}, err
	}

	if err := p.eatKeyword("values"); err != nil {
		return InsertCommand{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return InsertCommand{}, err
	}

	constants, err := p.constantList()
	if err != nil {
		return InsertCommand{}, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return InsertCommand{}, err
	}

	return NewInsertCommand(table, fields, constants), nil
}

func (p Parser) fieldList() ([]string, error) {
	var list []string
	v, err := p.field()
	if err != nil {
		return nil, err
	}

	list = append(list, v)

	if !p.matchTokenType(TokenComma) {
		return list, nil
	}

	p.eatTokenType(TokenComma)

	others, err := p.fieldList()
	if err != nil {
		return nil, err
	}

	list = append(list, others...)

	return list, nil
}

func (p Parser) constantList() ([]file.Value, error) {
	var list []file.Value
	c, err := p.constant()
	if err != nil {
		return nil, err
	}

	list = append(list, c)

	if !p.matchTokenType(TokenComma) {
		return list, nil
	}

	if err := p.eatTokenType(TokenComma); err != nil {
		return nil, err
	}

	others, err := p.constantList()
	if err != nil {
		return nil, err
	}

	list = append(list, others...)

	return list, nil
}

// <Modify> := UPDATE TokenIdentifier SET <Field> = <Expression> [ WHERE <Predicate> ]
func (p Parser) modify() (UpdateCommand, error) {
	if err := p.eatKeyword("update"); err != nil {
		return UpdateCommand{}, err
	}

	table, err := p.eatIdentifier()
	if err != nil {
		return UpdateCommand{}, err
	}

	if err := p.eatKeyword("set"); err != nil {
		return UpdateCommand{}, err
	}

	field, err := p.field()
	if err != nil {
		return UpdateCommand{}, err
	}

	if err := p.eatTokenType(TokenEqual); err != nil {
		return UpdateCommand{}, err
	}

	expr, err := p.expression()
	if err != nil {
		return UpdateCommand{}, err
	}

	if p.matchKeyword("where") {
		if err := p.eatKeyword("where"); err != nil {
			return UpdateCommand{}, err
		}

		pred, err := p.predicate()
		if err != nil {
			return UpdateCommand{}, err
		}

		return NewUpdateCommandWithPredicate(table, field, expr, pred), nil
	}

	return NewUpdateCommand(table, field, expr), nil
}
