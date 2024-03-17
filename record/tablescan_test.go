package record

import (
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestTableScan(t *testing.T) {
	schema := NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	layout := NewLayout(schema)
	for _, field := range layout.schema.Fields() {
		t.Logf("field %q has offset %d", field, layout.Offset(field))
	}

	fm, lm, bm := test.MakeManagers(t)

	trans := tx.NewTx(fm, lm, bm)

	scan := NewTableScan(trans, "TEST", layout)
	t.Log("Filling the table with 50 random records")
	scan.BeforeFirst()

	nodel := map[RID]struct{}{}

	for i := 0; i < 50; i++ {
		v := rand.Intn(50)
		scan.Insert()
		if err := scan.SetInt("A", v); err != nil {
			t.Fatal(err)
		}

		s := fmt.Sprintf("rec%d", v)
		if err := scan.SetString("B", s); err != nil {
			t.Fatal(err)
		}

		rid := scan.GetRID()
		if v >= 25 {
			nodel[rid] = struct{}{}
		}

		t.Logf("inserting into slot %s record {%d, %q}", rid, v, s)
	}

	t.Log("Deleting records with A < 25")
	scan.BeforeFirst()
	count := 0
	for {
		err := scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		a, err := scan.GetInt("A")
		if err != nil {
			t.Fatal(err)
		}

		if a < 25 {
			rid := scan.GetRID()
			if _, ok := nodel[rid]; ok {
				t.Fatalf("Unexpected deletion of record %q", rid)
			}

			b, err := scan.GetString("B")
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("Deleting record %s {%d, %q}", rid, a, b)

			if err := scan.Delete(); err != nil {
				t.Fatal(err)
			}
			count++
		}
	}

	t.Logf("%d records were deleted", count)

	t.Log("Printing remaining records:")
	scan.BeforeFirst()

	for {
		err := scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		a, err := scan.GetInt("A")
		if err != nil {
			t.Fatal(err)
		}

		b, err := scan.GetString("B")
		if err != nil {
			t.Fatal(err)
		}

		rid := scan.GetRID()
		if _, ok := nodel[rid]; !ok {
			t.Fatalf("record %s: {%d, %q} was expected to be deleted", rid, a, b)
		}

		t.Logf("slot %s: {%d, %q}", rid, a, b)
	}
}
