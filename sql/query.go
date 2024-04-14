package sql

import "strings"

type Query struct {
	fields    []string
	tables    []string
	predicate Predicate
}

func (qd Query) Tables() []string {
	return qd.tables
}

func (qd Query) Fields() []string {
	return qd.fields
}

func (qd Query) Predicate() Predicate {
	return qd.predicate
}

func NewQuery(selects []string, tables []string) Query {
	return Query{
		fields: selects,
		tables: tables,
	}
}

func NewQueryWithPredicate(selects []string, tables []string, pred Predicate) Query {
	return Query{
		fields:    selects,
		tables:    tables,
		predicate: pred,
	}
}

func (qd Query) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, f := range qd.fields {
		sb.WriteString(f)
		if i != len(qd.fields)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(" FROM ")
	for i, t := range qd.tables {
		sb.WriteString(t)
		if i != len(qd.tables)-1 {
			sb.WriteString(", ")
		}
	}

	if len(qd.predicate.terms) == 0 {
		return sb.String()
	}

	sb.WriteString(" WHERE ")
	sb.WriteString(qd.predicate.String())
	return sb.String()
}
