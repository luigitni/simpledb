package sql

import "github.com/luigitni/simpledb/record"

// Entire grammar for the SQL subset supported by SimpleDB
// <Field> := TokeIdentifier
// <Constant> := TokenString | TokenNumber
// <Expression> := <Field> | <Constant>
// <Term> := <Expression> = <Expression>
// <Predicate> := <Term> [AND <Predicate>]
// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
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

type Parser struct {
	*Lexer
}

func NewParser(src string) Parser {
	return Parser{
		Lexer: NewLexer(newTokenizer(src)),
	}
}

func (p Parser) Field() (string, error) {
	return p.eatIdentifier()
}

func (p Parser) Constant() (record.Constant, error) {
	if p.matchStringConstant() {
		s, err := p.eatStringConstant()
		if err != nil {
			return record.Constant{}, err
		}
		// remove quotes from the parsed raw string
		return record.ConstantFromString(s[1 : len(s)-1]), nil
	}

	v, err := p.eatIntConstant()
	if err != nil {
		return record.Constant{}, err
	}
	return record.ConstantFromInt(v), nil
}

func (p Parser) Expression() (record.Expression, error) {
	if p.matchIdentifier() {
		f, err := p.Field()
		if err != nil {
			return record.Expression{}, err
		}
		return record.NewExpressionWithField(f), nil
	}

	c, err := p.Constant()
	if err != nil {
		return record.Expression{}, err
	}
	return record.NewExpressionWithVal(c), nil
}

func (p Parser) Term() (record.Term, error) {
	lhs, err := p.Expression()
	if err != nil {
		return record.Term{}, err
	}

	if err := p.eatTokenType(TokenEqual); err != nil {
		return record.Term{}, err
	}

	rhs, err := p.Expression()
	if err != nil {
		return record.Term{}, err
	}
	return record.NewTerm(lhs, rhs), nil
}

func (p Parser) Predicate() (record.Predicate, error) {
	term, err := p.Term()
	if err != nil {
		return record.Predicate{}, err
	}

	pred := record.NewPredicateWithTerm(term)
	// check if the next token is an AND
	// if not, we are done, otherwise recursively add another predicate
	if !p.matchTokenType(TokenAnd) {
		return pred, nil
	}

	if err := p.eatTokenType(TokenAnd); err != nil {
		return record.Predicate{}, err
	}

	other, err := p.Predicate()
	if err != nil {
		return record.Predicate{}, nil
	}

	pred.CojoinWith(other)
	return pred, nil
}

// Query parsing methods
// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
func (p Parser) Query() (record.QueryData, error) {
	if err := p.eatTokenType(TokenSelect); err != nil {
		return record.QueryData{}, err
	}

	selects, err := p.SelectList()
	if err != nil {
		return record.QueryData{}, err
	}

	if err := p.eatTokenType(TokenFrom); err != nil {
		return record.QueryData{}, err
	}

	tables, err := p.TableList()
	if err != nil {
		return record.QueryData{}, err
	}

	if !p.matchTokenType(TokenWhere) {
		return record.NewQueryData(selects, tables), nil
	}

	if err := p.eatTokenType(TokenWhere); err != nil {
		return record.QueryData{}, err
	}

	pred, err := p.Predicate()
	if err != nil {
		return record.QueryData{}, err
	}

	return record.NewQueryDataWithPredicate(selects, tables, pred), nil
}

func (p Parser) SelectList() (record.SelectList, error) {
	var sl record.SelectList
	f, err := p.Field()
	if err != nil {
		return nil, err
	}

	sl = append(sl, f)
	if !p.matchTokenType(TokenComma) {
		return sl, nil
	}

	p.eatTokenType(TokenComma)

	other, err := p.SelectList()
	if err != nil {
		return nil, err
	}

	sl = append(sl, other...)
	return sl, nil
}

func (p Parser) TableList() (record.TableList, error) {
	var tl record.TableList
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

	other, err := p.TableList()
	if err != nil {
		return nil, err
	}

	tl = append(tl, other...)
	return tl, nil
}
