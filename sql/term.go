package sql

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
)

// Term is a comparison between two Expressions.
type Term struct {
	lhs Expression
	rhs Expression
}

func NewTerm(lhs Expression, rhs Expression) Term {
	return Term{lhs: lhs, rhs: rhs}
}

func (t Term) IsSatisfied(s Scan) (bool, error) {
	lc, err := t.lhs.Evaluate(s)
	if err != nil {
		return false, err
	}

	rc, err := t.rhs.Evaluate(s)
	if err != nil {
		return false, err
	}

	return lc == rc, nil
}

func (t Term) AppliesTo(schema Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t Term) EquatesWithConstant(fieldName string) (bool, file.Value) {
	if t.lhs.IsFieldName() && t.lhs.fname == fieldName && !t.rhs.IsFieldName() {
		return true, t.lhs.AsConstant()
	}

	if t.rhs.IsFieldName() && t.rhs.fname == fieldName && !t.lhs.IsFieldName() {
		return true, t.rhs.AsConstant()
	}

	return false, file.Value{}
}

func (t Term) EquatesWithField(fieldName string) (bool, string) {
	if t.lhs.IsFieldName() && t.lhs.fname == fieldName && !t.rhs.IsFieldName() {
		return true, t.lhs.AsFieldName()
	}

	if t.rhs.IsFieldName() && t.rhs.fname == fieldName && !t.lhs.IsFieldName() {
		return true, t.rhs.AsFieldName()
	}

	return false, ""
}

func (t Term) String() string {
	return fmt.Sprintf("%s = %s", t.lhs, t.rhs)
}
