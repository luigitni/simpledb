package db

import (
	"testing"

	"github.com/luigitni/simpledb/types"
)

func TestRowsString(t *testing.T) {
	t.Run("expect no result", func(t *testing.T) {
		rows := Rows{
			cols: []string{
				"first col",
				"second col which is very long",
				"third",
			},
		}

		if s := rows.String(); s != printRowsNoResult {
			t.Fatalf("expected %q, got %q", printRowsNoResult, s)
		}
	})

	t.Run("expect correct table", func(t *testing.T) {
		const expected = "\n" +
			"| first col | second col which is very long |             third             |\n" +
			"|-----------|-------------------------------|-------------------------------|\n" +
			"|    123    |             'abc'             | 'This is a much longer value' |\n" +
			"|     0     | 'This is a much longer value' |            'short'            |\n" +
			"---\n" +
			"2 records found."

		rows := Rows{
			cols: []string{
				"first col",
				"second col which is very long",
				"third",
			},
			rows: []Row{
				{
					vals: []types.Value{
						types.ValueFromInteger[types.Int](types.SizeOfInt, 123),
						types.ValueFromGoString("abc"),
						types.ValueFromGoString("This is a much longer value"),
					},
				},
				{
					vals: []types.Value{
						types.ValueFromInteger[types.Int](types.SizeOfInt, 0),
						types.ValueFromGoString("This is a much longer value"),
						types.ValueFromGoString("short"),
					},
				},
			},
		}

		if s := rows.String(); s != expected {
			t.Fatalf("expected %q, got %q", expected, s)
		}
	})
}
