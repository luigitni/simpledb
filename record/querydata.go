package record

import "strings"

type SelectList []string

type TableList []string

type QueryData struct {
	Fields SelectList
	Tables TableList
	Pred   Predicate
}

func NewQueryData(selects SelectList, tables TableList) QueryData {
	return QueryData{
		Fields: selects,
		Tables: tables,
	}
}

func NewQueryDataWithPredicate(selects SelectList, tables TableList, pred Predicate) QueryData {
	return QueryData{
		Fields: selects,
		Tables: tables,
		Pred:   pred,
	}
}

func (qd QueryData) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, f := range qd.Fields {
		sb.WriteString(f)
		if i != len(qd.Fields)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(" FROM ")
	for i, t := range qd.Tables {
		sb.WriteString(t)
		if i != len(qd.Tables)-1 {
			sb.WriteString(", ")
		}
	}

	if len(qd.Pred.terms) == 0 {
		return sb.String()
	}

	sb.WriteString(" WHERE ")
	sb.WriteString(qd.Pred.String())
	return sb.String()
}
