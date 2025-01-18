package sql

import (
	"fmt"
	"math"

	"github.com/luigitni/simpledb/types"
)

type Plan interface {
	DistinctValues(fieldName string) int
}

// Term is a comparison between two Expressions.
type Term struct {
	lhs Expression
	rhs Expression
}

func newTerm(lhs Expression, rhs Expression) Term {
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

	return lc.Equals(rc), nil
}

func (t Term) ReductionFactor(p Plan) int {
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		lhsName := t.lhs.AsFieldName()
		rhsName := t.rhs.AsFieldName()
		if m := p.DistinctValues(lhsName); m > p.DistinctValues(rhsName) {
			return m
		}
		return p.DistinctValues(rhsName)
	}

	if t.lhs.IsFieldName() {
		return p.DistinctValues(t.lhs.AsFieldName())
	}

	if t.rhs.IsFieldName() {
		return p.DistinctValues(t.rhs.AsFieldName())
	}

	lc := t.lhs.AsConstant()
	rc := t.rhs.AsConstant()
	if lc.Equals(rc) {
		return 1
	}

	return math.MaxInt
}

func (t Term) AppliesTo(schema Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t Term) EquatesWithConstant(fieldName string) (bool, types.Value) {
	if t.lhs.IsFieldName() && t.lhs.fname == fieldName && !t.rhs.IsFieldName() {
		return true, t.lhs.AsConstant()
	}

	if t.rhs.IsFieldName() && t.rhs.fname == fieldName && !t.lhs.IsFieldName() {
		return true, t.rhs.AsConstant()
	}

	return false, types.Value{}
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
