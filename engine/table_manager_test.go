package engine

import (
	"testing"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestCreateTable(t *testing.T) {
	x := tx.NewTx(test.MakeManagers(t))
	tm := newTableManager()

	tm.init(x)
	defer x.Commit()

	schema := newSchema()
	schema.addField("name", storage.TEXT)
	schema.addField("surname", storage.TEXT)
	schema.addField("age", storage.TINYINT)
	schema.addField("salary", storage.INT)

	if err := tm.createTable("employees", schema, x); err != nil {
		t.Fatal(err)
	}

	layout, err := tm.layout("employees", x)
	if err != nil {
		t.Fatal(err)
	}

	derived := layout.Schema()
	for _, field := range schema.fields {
		if !derived.HasField(field) {
			t.Fatalf("field %q not found in derived schema", field)
		}

		df := derived.FieldInfo(field)
		if df.Type != schema.ftype(field) {
			t.Fatalf("field %q has type %q, expected %q", field, df.Type, schema.ftype(field))
		}
	}
}
