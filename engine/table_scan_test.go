package engine

import (
	"fmt"
	"io"
	"testing"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestTableScan(t *testing.T) {
	schema := newSchema()
	schema.addField("id", storage.INT)
	schema.addField("tag", storage.TEXT)

	layout := NewLayout(schema)

	fm, lm, bm := test.MakeManagers(t)

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	scan := newTableScan(x, "employee", layout)
	scan.BeforeFirst()

	tag := func(id storage.Int) string {
		return fmt.Sprintf("employee_%d", id)
	}

	t.Run("test insert one record and delete", func(t *testing.T) {
		const val storage.Int = 10
		fullName := tag(val)

		var size storage.Offset
		size += storage.Offset(storage.SizeOfInt)
		size += storage.Offset(storage.UnsafeSizeOfStringAsVarlen(fullName))

		scan.Insert(size)
		if err := scan.SetVal("id", storage.ValueFromInteger(storage.SizeOfInt, val)); err != nil {
			t.Fatal(err)
		}

		if err := scan.SetVal("tag", storage.ValueFromGoString(fullName)); err != nil {
			t.Fatal(err)
		}

		scan.Delete()

		scan.BeforeFirst()
		err := scan.Next()
		if err == nil {
			t.Fatal("expected EOF")
		}

		if err != io.EOF {
			t.Fatalf("expected EOF, got %s", err)
		}
	})

	record := map[RID]struct{}{}

	t.Run("test insert multiple records", func(t *testing.T) {
		scan.BeforeFirst()

		for v := range 50 {
			v := storage.Int(v)
			s := fmt.Sprintf("employee_%d", v)

			var size storage.Offset
			size += storage.Offset(storage.SizeOfInt)
			size += storage.Offset(storage.UnsafeSizeOfStringAsVarlen(s))

			scan.Insert(size)

			if err := scan.SetVal("id", storage.ValueFromInteger(storage.SizeOfInt, v)); err != nil {
				t.Fatalf("error setting id val: %s", err)
			}

			if err := scan.SetVal("tag", storage.ValueFromGoString(s)); err != nil {
				t.Fatalf("error setting tag val: %s", err)
			}

			record[scan.GetRID()] = struct{}{}
		}
	})

	t.Run("test heap scan", func(t *testing.T) {
		scan.BeforeFirst()

		var id storage.Int
		for {
			err := scan.Next()
			if err == io.EOF {
				break
			}

			if err != nil {
				t.Fatalf("unexpected error while scanning the heap table: %s", err)
			}

			v, err := scan.Val("id")
			if err != nil {
				t.Fatalf("error retrieving id: %s", err)
			}

			if got := v.AsFixedLen().UnsafeAsInt(); got != id {
				t.Fatalf("expected scanned value to be %d, got %d", id, got)
			}

			s, err := scan.Val("tag")
			if err != nil {
				t.Fatalf("error retrieving tag: %s", err)
			}

			if got := storage.UnsafeVarlenToGoString(s.AsVarlen()); got != tag(id) {
				t.Fatalf("expected tag %q, got %q", tag(id), got)
			}

			id++
		}
	})
}
