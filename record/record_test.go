package record

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestRecord(t *testing.T) {
	t.Cleanup(test.ClearTestFolder)

	fm, lm, bm := test.MakeManagers()

	schema := MakeSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	// slot has size 33

	layout := NewLayout(schema)

	for _, fname := range layout.Schema().fields {
		offset := layout.Offset(fname)
		t.Logf("field %q has offset %d", fname, offset)
	}

	trans := tx.NewTx(fm, lm, bm)
	blockID, err := trans.Append("testfile")
	if err != nil {
		t.Fatal(err)
	}

	trans.Pin(blockID)
	recpage := NewRecordPage(trans, blockID, layout)
	if err := recpage.Format(); err != nil {
		t.Fatal(err)
	}

	t.Log("filling the page with random records")
	slot, err := recpage.InsertAfter(-1)
	if err != nil {
		t.Fatal(err)
	}

	nodel := map[int]struct{}{}

	for slot >= 0 {
		n := rand.Intn(50)
		recpage.SetInt(slot, "A", n)
		sv := fmt.Sprintf("rec%d", n)
		recpage.SetString(slot, "B", sv)

		if n >= 25 {
			nodel[slot] = struct{}{}
		}

		t.Logf("Inserting into slot %d: {%d, %q}", slot, n, sv)
		slot, err = recpage.InsertAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Log("Delete records with A values < 25")

	slot, err = recpage.NextAfter(-1)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for slot >= 0 {
		v, err := recpage.GetInt(slot, "A")
		if err != nil {
			t.Fatal(err)
		}

		s, err := recpage.GetString(slot, "B")
		if err != nil {
			t.Fatal(err)
		}

		if v < 25 {
			t.Logf("Delete record at slot %d: {%d, %q}", slot, v, s)

			if _, ok := nodel[slot]; ok {
				t.Fatalf("Expected record at slot %d {%d, %q} to not be deleted.", slot, v, s)
			}

			if err := recpage.Delete(slot); err != nil {
				t.Fatal(err)
			}
			count++
		}

		slot, err = recpage.NextAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("Deleted %d records having 'A' < 25", count)
	t.Log("Here are the remaining records:")

	slot, err = recpage.NextAfter(-1)
	if err != nil {
		t.Fatal(err)
	}

	for slot >= 0 {
		a, err := recpage.GetInt(slot, "A")
		if err != nil {
			t.Fatal(err)
		}

		b, err := recpage.GetString(slot, "B")
		if err != nil {
			t.Fatal(err)
		}

		if _, ok := nodel[slot]; !ok {
			t.Fatalf("expected record at slot %d {%d, %q} to be deleted.", slot, a, b)
		}

		t.Logf("slot %d: {%d, %q}", slot, a, b)
		slot, err = recpage.NextAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}

	trans.Unpin(blockID)
	trans.Commit()
}
