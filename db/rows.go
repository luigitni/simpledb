package db

import (
	"fmt"
	"strings"

	"github.com/luigitni/simpledb/file"
)

const printRowsNoResult = "No records"

type Row struct {
	vals []file.Value
}

type Rows struct {
	cols []string
	rows []Row
}

func (r Rows) String() string {

	if len(r.rows) == 0 {
		return printRowsNoResult
	}

	padLen := 4

	max := make([]int, len(r.cols))

	for i, cols := range r.cols {
		max[i] = len(cols) + padLen
	}

	for _, row := range r.rows {
		for i, val := range row.vals {
			s := val.String()
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

		builder.WriteString(padString(col, m))
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
			builder.WriteString(padString(val.String(), m))
			builder.WriteString("|")
		}
		builder.WriteString("\n")
	}

	return builder.String()
}

func padString(s string, w int) string {
	return fmt.Sprintf("%*s", -w, fmt.Sprintf("%*s", (w+len(s))/2, s))
}
