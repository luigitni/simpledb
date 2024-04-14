package sql

import (
	"strings"

	"github.com/luigitni/simpledb/file"
)

// Predicate specifies a condition that returns
// true or false for each ROW of a given scan.
// If the condition returns true, then
// the row satisfies the predicate.
// In SQL, a Predicate is a Term or a boolean combination
// of Terms.
type Predicate struct {
	terms []Term
}

func NewPredicate() Predicate {
	return Predicate{
		terms: make([]Term, 0),
	}
}

func NewPredicateWithTerm(t Term) Predicate {
	var ts []Term
	ts = append(ts, t)
	return Predicate{terms: ts}
}

func (p *Predicate) CojoinWith(other Predicate) {
	p.terms = append(p.terms, other.terms...)
}

func (p Predicate) IsSatisfied(s Scan) (bool, error) {
	for _, t := range p.terms {
		ok, err := t.IsSatisfied(s)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func (p Predicate) ReductionFactor(plan Plan) int {
	factor := 1
	for _, t := range p.terms {
		factor *= t.ReductionFactor(plan)
	}
	return factor
}

func (p Predicate) SelectSubPredicate(schema Schema) (Predicate, bool) {
	result := Predicate{}
	for _, t := range p.terms {
		if t.AppliesTo(schema) {
			result.terms = append(result.terms, t)
		}
	}

	return result, len(result.terms) > 0
}

func (p Predicate) JoinSubPredicate(joined Schema, first Schema, second Schema) (Predicate, bool) {
	out := Predicate{}

	for _, t := range p.terms {
		if !t.AppliesTo(first) && !t.AppliesTo(second) && t.AppliesTo(joined) {
			out.terms = append(out.terms, t)
		}
	}

	return out, len(out.terms) > 0
}

func (p Predicate) EquatesWithConstant(fieldName string) (file.Value, bool) {
	for _, t := range p.terms {
		ok, c := t.EquatesWithConstant(fieldName)
		if ok {
			return c, true
		}
	}

	return file.Value{}, false
}

func (p Predicate) EquatesWithField(fieldname string) (string, bool) {
	for _, t := range p.terms {
		ok, v := t.EquatesWithField(fieldname)
		if ok {
			return v, true
		}
	}

	return "", false
}

func (p Predicate) String() string {
	var sb strings.Builder
	for i, t := range p.terms {
		sb.WriteString(t.String())
		if i != len(p.terms)-1 {
			sb.WriteString(" AND ")
		}
	}
	return sb.String()
}
