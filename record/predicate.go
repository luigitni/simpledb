package record

import "strings"

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

func (p Predicate) SelectSubPredicate(schema Schema) (bool, Predicate) {
	result := Predicate{}
	for _, t := range p.terms {
		if t.AppliesTo(schema) {
			result.terms = append(result.terms, t)
		}
	}

	return len(result.terms) > 0, result
}

func (p Predicate) JoinSubPredicate(first Schema, second Schema) (bool, Predicate) {
	out := Predicate{}
	schema := NewSchema()
	schema.AddAll(first)
	schema.AddAll(second)

	for _, t := range p.terms {
		if !t.AppliesTo(first) && !t.AppliesTo(second) && t.AppliesTo(schema) {
			out.terms = append(out.terms, t)
		}
	}

	return len(out.terms) > 0, out
}

func (p Predicate) EquatesWithConstant(fieldName string) (bool, Constant) {
	for _, t := range p.terms {
		ok, c := t.EquatesWithConstant(fieldName)
		if ok {
			return true, c
		}
	}

	return false, Constant{}
}

func (p Predicate) EquatesWithField(fieldname string) (bool, string) {
	for _, t := range p.terms {
		ok, v := t.EquatesWithField(fieldname)
		if ok {
			return true, v
		}
	}

	return false, ""
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
