package record

import "strings"

type SelectList []string

type TableList []string

type QueryData struct {
	fields SelectList
	tables TableList
	pred Predicate
}

func NewQueryData(selects SelectList, tables TableList) QueryData {
	return QueryData{
		fields: selects,
		tables:  tables,
	}
}

func NewQueryDataWithPredicate(selects SelectList, tables TableList, pred Predicate) QueryData {
	return QueryData{
		fields: selects,
		tables:  tables,
		pred:    pred,
	}
}

func (qd QueryData) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, f := range qd.fields {
		sb.WriteString(f)
		if i != len(qd.fields) - 1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(" FROM ")
	for i, t := range qd.tables {
		sb.WriteString(t)
		if i != len(qd.tables) - 1 {
			sb.WriteString(", ")
		}
	}

	if len(qd.pred.terms) == 0 {
		return sb.String()
	}

	sb.WriteString(" WHERE ")
	sb.WriteString(qd.pred.String())
	return sb.String()
}