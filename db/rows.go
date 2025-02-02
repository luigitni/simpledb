package db

import (
	"fmt"
	"strings"

	"github.com/luigitni/simpledb/storage"
)

const printRowsNoResult = "No records found."

type Col struct {
	Name string
	Type storage.FieldType
}

type Row struct {
	vals []storage.Value
}

type Rows struct {
	cols []Col
	rows []Row
}

func (r Rows) String() string {
	if len(r.rows) == 0 {
		return printRowsNoResult
	}

	padLen := 2

	max := make([]int, len(r.cols))

	for i, cols := range r.cols {
		max[i] = len(cols.Name) + padLen
	}

	for _, row := range r.rows {
		for i, val := range row.vals {
			colType := r.cols[i].Type

			s := val.String(colType)
			l := padLen + len(s)

			if l > max[i] {
				max[i] = l
			}
		}
	}

	var builder strings.Builder

	builder.WriteString("\n")
	builder.WriteString("|")

	for i, col := range r.cols {
		m := max[i]

		builder.WriteString(padString(col.Name, m))
		builder.WriteString("|")
	}
	builder.WriteString("\n|")

	for _, m := range max {
		builder.WriteString(strings.Repeat("-", m))
		builder.WriteString("|")
	}

	builder.WriteString("\n")

	for _, rows := range r.rows {
		builder.WriteString("|")
		for i, val := range rows.vals {
			m := max[i]
			colType := r.cols[i].Type

			builder.WriteString(padString(val.String(colType), m))
			builder.WriteString("|")
		}
		builder.WriteString("\n")
	}

	count := "---\n%d records found."
	if len(r.rows) == 1 {
		count = "---\n%d record found."
	}

	builder.WriteString(fmt.Sprintf(count, len(r.rows)))
	return builder.String()
}

func padString(s string, w int) string {
	return fmt.Sprintf("%*s", -w, fmt.Sprintf("%*s", (w+len(s))/2, s))
}
