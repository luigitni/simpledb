package sql

import (
	"github.com/luigitni/simpledb/record"
)

func (p Parser) UpdateCmd() (record.Command, error) {
	if p.matchKeyword("insert") {
		return p.insert()
	}

	if p.matchKeyword("update") {
		return p.modify()
	}

	if p.matchKeyword("delete") {
		return p.delete()
	}

	return p.create()
}

// <Delete> := DELETE FROM TokenIdentifier [ WHERE <Predicate> ]
func (p Parser) delete() (record.DeleteData, error) {
	p.eatKeyword("delete")

	if err := p.eatKeyword("from"); err != nil {
		return record.DeleteData{}, err
	}

	table, err := p.eatIdentifier()
	if err != nil {
		return record.DeleteData{}, err
	}

	if p.matchKeyword("where") {
		p.eatKeyword("where")
		pred, err := p.Predicate()
		if err != nil {
			return record.DeleteData{}, err
		}

		return record.NewDeleteDataWithPredicate(table, pred), nil
	}

	return record.NewDeleteData(table), nil
}

// <Insert> := INSERT INTO TokenIdentifier ( <FieldList> ) VALUES ( <ConstList> )
func (p Parser) insert() (record.InsertData, error) {
	if err := p.eatKeyword("insert"); err != nil {
		return record.InsertData{}, err
	}

	if err := p.eatKeyword("into"); err != nil {
		return record.InsertData{}, nil
	}

	table, err := p.eatIdentifier()
	if err != nil {
		return record.InsertData{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return record.InsertData{}, err
	}

	fields, err := p.FieldList()
	if err != nil {
		return record.InsertData{}, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return record.InsertData{}, err
	}

	if err := p.eatKeyword("values"); err != nil {
		return record.InsertData{}, err
	}

	if err := p.eatTokenType(TokenLeftParen); err != nil {
		return record.InsertData{}, err
	}

	constants, err := p.ConstantList()
	if err != nil {
		return record.InsertData{}, err
	}

	if err := p.eatTokenType(TokenRightParen); err != nil {
		return record.InsertData{}, err
	}

	return record.NewInsertData(table, fields, constants), nil
}

func (p Parser) FieldList() (record.FieldList, error) {
	var list record.FieldList
	v, err := p.Field()
	if err != nil {
		return nil, err
	}

	list = append(list, v)

	if !p.matchTokenType(TokenComma) {
		return list, nil
	}

	p.eatTokenType(TokenComma)

	others, err := p.FieldList()
	if err != nil {
		return nil, err
	}

	list = append(list, others...)

	return list, nil
}

func (p Parser) ConstantList() (record.ConstantList, error) {
	var list record.ConstantList
	c, err := p.Constant()
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

	others, err := p.ConstantList()
	if err != nil {
		return nil, err
	}

	list = append(list, others...)

	return list, nil
}

// <Modify> := UPDATE TokenIdentifier SET <Field> = <Expression> [ WHERE <Predicate> ]
func (p Parser) modify() (record.ModifyData, error) {
	if err := p.eatKeyword("update"); err != nil {
		return record.ModifyData{}, err
	}

	table, err := p.eatIdentifier()
	if err != nil {
		return record.ModifyData{}, err
	}

	if err := p.eatKeyword("set"); err != nil {
		return record.ModifyData{}, err
	}

	field, err := p.Field()
	if err != nil {
		return record.ModifyData{}, err
	}

	if err := p.eatTokenType(TokenEqual); err != nil {
		return record.ModifyData{}, err
	}

	expr, err := p.Expression()
	if err != nil {
		return record.ModifyData{}, err
	}

	if p.matchKeyword("where") {
		if err := p.eatKeyword("where"); err != nil {
			return record.ModifyData{}, err
		}

		pred, err := p.Predicate()
		if err != nil {
			return record.ModifyData{}, err
		}

		return record.NewModifyDataWithPredicate(table, field, expr, pred), nil
	}

	return record.NewModifyData(table, field, expr), nil
}
