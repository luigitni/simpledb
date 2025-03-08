package db

import (
	"testing"

	"github.com/luigitni/simpledb/storage"
)

func TestRowsString(t *testing.T) {
	t.Run("expect no result", func(t *testing.T) {
		rows := Rows{
			cols: []Col{
				{Name: "first col", Type: storage.INT},
				{Name: "second col which is very long", Type: storage.TEXT},
				{Name: "third", Type: storage.TEXT},
			},
		}

		if s := rows.String(); s != printRowsNoResult {
			t.Fatalf("expected %q, got %q", printRowsNoResult, s)
		}
	})

	t.Run("expect correct table", func(t *testing.T) {
		const expected = "\n" +
			"| first col | second col which is very long |            third            |\n" +
			"|-----------|-------------------------------|-----------------------------|\n" +
			"|    123    |              abc              | This is a much longer value |\n" +
			"|     0     |  This is a much longer value  |            short            |\n" +
			"---\n" +
			"2 records found."

		rows := Rows{
			cols: []Col{
				{Name: "first col", Type: storage.INT},
				{Name: "second col which is very long", Type: storage.TEXT},
				{Name: "third", Type: storage.TEXT},
			},
			rows: []Row{
				{
					vals: []storage.Value{
						storage.ValueFromInteger[storage.Int](storage.SizeOfInt, 123),
						storage.ValueFromGoString("abc"),
						storage.ValueFromGoString("This is a much longer value"),
					},
				},
				{
					vals: []storage.Value{
						storage.ValueFromInteger[storage.Int](storage.SizeOfInt, 0),
						storage.ValueFromGoString("This is a much longer value"),
						storage.ValueFromGoString("short"),
					},
				},
			},
		}

		if s := rows.String(); s != expected {
			t.Fatalf("expected \n%q \n%q", expected, s)
		}
	})
}
