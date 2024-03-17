package planner

import (
	"io"
	"testing"

	"github.com/luigitni/simpledb/meta"
	"github.com/luigitni/simpledb/record"
	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestBasicQueryPlanner(t *testing.T) {

	fm, lm, bm := test.MakeManagers(t)
	mdm := meta.NewManager()

	// STUDENTS table definition
	schema := record.NewSchema()
	schema.AddStringField("sname", 15)
	schema.AddStringField("ssurname", 20)
	schema.AddIntField("gradyear")
	schema.AddIntField("dob")

	updPlanner := NewBasicUpdatePlanner(mdm)

	cx := tx.NewTx(fm, lm, bm)

	if _, err := updPlanner.ExecuteCreateTable(
		record.NewCreateTableData("students", schema),
		cx,
	); err != nil {
		t.Fatal(err)
	}

	// add records into the table
	vals := []record.ConstantList{
		{
			record.ConstantFromString("Name A"),
			record.ConstantFromString("Surname A"),
			record.ConstantFromInt(2000),
			record.ConstantFromInt(1980),
		},
		{
			record.ConstantFromString("Name B"),
			record.ConstantFromString("Surname B"),
			record.ConstantFromInt(2001),
			record.ConstantFromInt(1981),
		},
		{
			record.ConstantFromString("Name C"),
			record.ConstantFromString("Surname C"),
			record.ConstantFromInt(2002),
			record.ConstantFromInt(1982),
		},
	}

	flist := record.FieldList([]string{"sname", "ssurname", "gradyear", "dob"})

	for _, v := range vals {
		if _, err := updPlanner.ExecuteInsert(
			record.NewInsertData("students", flist, v),
			cx,
		); err != nil {
			t.Fatal(err)
		}
	}

	cx.Commit()

	x := tx.NewTx(fm, lm, bm)

	const q = "SELECT sname, gradyear FROM students"

	parser := sql.NewParser(q)
	data, err := parser.Query()
	if err != nil {
		t.Fatal(err)
	}

	planner := NewBasicQueryPlanner(mdm)

	plan, err := planner.CreatePlan(data, x)
	if err != nil {
		t.Fatal(err)
	}

	scan := plan.Open()
	defer scan.Close()

	records := 0
	for {
		err := scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		name, err := scan.GetString("sname")
		if err != nil {
			t.Fatal(err)
		}

		year, err := scan.GetInt("gradyear")
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("sname: %q, gradyear: %d", name, year)
		records++
	}

	if records != 3 {
		t.Fatalf("expected %d records, got %d", 3, records)
	}
}
