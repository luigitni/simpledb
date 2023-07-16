package meta

import (
	"testing"

	"github.com/luigitni/simpledb/record"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestTableManager(t *testing.T) {

	t.Cleanup(test.ClearTestFolder)

	trans := tx.NewTx(test.MakeManagers())

	tm := NewTableManager()
	tm.Init(trans)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	if err := tm.CreateTable("MyTable", schema, trans); err != nil {
		t.Fatal(err)
	}

	layout, err := tm.Layout("MyTable", trans)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("MyTable has slot size %d", layout.SlotSize())

	t.Log("MyTable fields:")
	sch := layout.Schema()
	for _, fname := range sch.Fields() {
		switch sch.Type(fname) {
		case record.INTEGER:
			t.Logf("%s INTEGER", fname)
		case record.STRING:
			t.Logf("%s VARCHAR(%d)", fname, sch.Length(fname))
		}
	}

	trans.Commit()
}
