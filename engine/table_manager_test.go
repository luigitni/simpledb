package engine

import (
	"testing"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestTableManager(t *testing.T) {
	trans := tx.NewTx(test.MakeManagers(t))

	tm := newTableManager()
	tm.init(trans)

	schema := newSchema()
	schema.addIntField("A")
	schema.addFixedLenStringField("B", 9)

	if err := tm.createTable("MyTable", schema, trans); err != nil {
		t.Fatal(err)
	}

	layout, err := tm.layout("MyTable", trans)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("MyTable has slot size %d", layout.SlotSize())

	t.Log("MyTable fields:")
	sch := layout.Schema()
	for _, fname := range sch.fields {
		switch sch.ftype(fname) {
		case storage.INTEGER:
			t.Logf("%s INTEGER", fname)
		case storage.STRING:
			t.Logf("%s VARCHAR(%d)", fname, sch.flen(fname))
		}
	}

	// check the table catalog that the

	trans.Commit()
}
