package sql

import "github.com/luigitni/simpledb/file"

// Entire grammar for the SQL subset supported by SimpleDB
// <Field> := TokenIdentifier
// <Constant> := TokenString | TokenNumber
// <Expression> := <Field> | <Constant>
// <Term> := <Expression> = <Expression>
// <Predicate> := <Term> [AND <Predicate>]
// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ] [ORDER BY <Field> [, <FieldList>]]
// <SelectList> := <Field> [, <SelectList> ]
// <TableList> := TokenIdentifier [, <TableList> ]
// <UpdateCmd> := <Insert> | <Delete> | <Modify> | <Create>
// <Create> := <CreateTable> | <CreateView> | <CreateIndex>
// <Insert> := INSERT INTO TokenIdentifier ( <FieldList> ) VALUES ( <ConstList> )
// <FieldList> := <Field> [, <FieldList> ]
// <ConstList> := <Constant> [, <ConstList> ]
// <Delete> := DELETE FROM TokenIdentifier [ WHERE <Predicate> ]
// <Modify> := UPDATE TokenIdentifier SET <Field> = <Expression> [ WHERE <Predicate> ]
// <CreateTable> := CREATE TABLE TokenIdentifier ( <FieldDefs> )
// <FieldDefs> := <FieldDef> [, <FieldDefs> ]
// <FieldDef> := TokenIdentifier <TypeDef>
// <TypeDef> := INT | VARCHAR ( TokenNumber )
// <CreateView> := CREATE VIEW TokenIdentifier AS <Query>
// <CreateIndex> := CREATE INDEX TokenIdentifier ON TokenIdentifier ( <Field> )
// <BegingTransaction> := BEGIN
// <Commit> := COMMIT
// <Rollback> := ROLLBACK

type Parser struct {
	*Lexer
}

func NewParser(src string) Parser {
	return Parser{
		Lexer: NewLexer(newTokenizer(src)),
	}
}

func (p Parser) Parse() (Command, error) {

	if ok, cmd := p.isTCL(); ok {
		return cmd, nil
	}

	if p.isQuery() {
		return p.Query()
	}

	if p.isDML() {
		return p.dml()
	}

	return p.ddl()
}

func (p Parser) field() (string, error) {
	return p.eatIdentifier()
}

func (p Parser) constant() (file.Value, error) {
	if p.matchStringValue() {
		s, err := p.eatStringValue()
		if err != nil {
			return file.Value{}, err
		}
		// remove quotes from the parsed raw string
		return file.ValueFromString(s[1 : len(s)-1]), nil
	}

	v, err := p.eatIntValue()
	if err != nil {
		return file.Value{}, err
	}
	return file.ValueFromInt(v), nil
}

func (p Parser) expression() (Expression, error) {
	if p.matchIdentifier() {
		f, err := p.field()
		if err != nil {
			return Expression{}, err
		}
		return NewExpressionWithField(f), nil
	}

	c, err := p.constant()
	if err != nil {
		return Expression{}, err
	}
	return NewExpressionWithVal(c), nil
}

func (p Parser) term() (Term, error) {
	lhs, err := p.expression()
	if err != nil {
		return Term{}, err
	}

	if err := p.eatTokenType(TokenEqual); err != nil {
		return Term{}, err
	}

	rhs, err := p.expression()
	if err != nil {
		return Term{}, err
	}
	return newTerm(lhs, rhs), nil
}

func (p Parser) predicate() (Predicate, error) {
	term, err := p.term()
	if err != nil {
		return Predicate{}, err
	}

	pred := newPredicateWithTerm(term)
	// check if the next token is an AND
	// if not, we are done, otherwise recursively add another predicate
	if !p.matchTokenType(TokenAnd) {
		return pred, nil
	}

	if err := p.eatTokenType(TokenAnd); err != nil {
		return Predicate{}, err
	}

	other, err := p.predicate()
	if err != nil {
		return Predicate{}, nil
	}

	pred.CojoinWith(other)
	return pred, nil
}

// Query parsing methods
// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ] [ ORDER BY <Field>,]
func (p Parser) Query() (Query, error) {
	if err := p.eatTokenType(TokenSelect); err != nil {
		return Query{}, err
	}

	selects, err := p.selectList()
	if err != nil {
		return Query{}, err
	}

	if err := p.eatTokenType(TokenFrom); err != nil {
		return Query{}, err
	}

	tables, err := p.tableList()
	if err != nil {
		return Query{}, err
	}

	q := NewQuery(selects, tables)

	if p.matchTokenType(TokenWhere) {
		if err := p.eatTokenType(TokenWhere); err != nil {
			return Query{}, err
		}

		pred, err := p.predicate()
		if err != nil {
			return Query{}, err
		}

		q.predicate = pred
	}

	if p.matchTokenType(TokenOrderBy) {
		orderByFields, err := p.orderBy()
		if err != nil {
			return Query{}, err
		}

		q.orderByFields = orderByFields
	}

	return q, nil
}

func (p Parser) orderBy() ([]string, error) {
	p.eatTokenType(TokenOrderBy)

	ff, err := p.fieldList()
	if err != nil {
		return nil, err
	}

	return ff, nil
}

func (p Parser) selectList() ([]string, error) {
	var sl []string
	f, err := p.field()
	if err != nil {
		return nil, err
	}

	sl = append(sl, f)
	if !p.matchTokenType(TokenComma) {
		return sl, nil
	}

	p.eatTokenType(TokenComma)

	other, err := p.selectList()
	if err != nil {
		return nil, err
	}

	sl = append(sl, other...)
	return sl, nil
}

func (p Parser) tableList() ([]string, error) {
	var tl []string
	f, err := p.eatIdentifier()
	if err != nil {
		return nil, err
	}

	tl = append(tl, f)
	if !p.matchTokenType(TokenComma) {
		return tl, nil
	}

	if err := p.eatTokenType(TokenComma); err != nil {
		return nil, err
	}

	other, err := p.tableList()
	if err != nil {
		return nil, err
	}

	tl = append(tl, other...)
	return tl, nil
}
