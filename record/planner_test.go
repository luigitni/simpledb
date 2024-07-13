package record

import (
	"io"
	"testing"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
	"github.com/luigitni/simpledb/sql"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func createAndInsertTable(t *testing.T, fm *file.Manager, lm *log.LogManager, bm *buffer.Manager, mdm *MetadataManager) error {
	t.Helper()

	// STUDENTS table definition
	schema := newSchema()
	schema.addStringField("sname", 15)
	schema.addStringField("ssurname", 20)
	schema.addIntField("gradyear")
	schema.addIntField("dob")

	updPlanner := newBasicUpdatePlanner(mdm)

	cx := tx.NewTx(fm, lm, bm)
	defer cx.Commit()

	if mdm.tableManager.tableExists("students", cx) {
		return nil
	}

	if _, err := updPlanner.executeCreateTableFromSchema(
		"students",
		schema,
		cx,
	); err != nil {
		return err
	}

	// add records into the table
	vals := [][]file.Value{
		{
			file.ValueFromString("Name A"),
			file.ValueFromString("Surname A"),
			file.ValueFromInt(2001),
			file.ValueFromInt(1980),
		},
		{
			file.ValueFromString("Name B"),
			file.ValueFromString("Surname B"),
			file.ValueFromInt(2001),
			file.ValueFromInt(1981),
		},
		{
			file.ValueFromString("Name C"),
			file.ValueFromString("Surname C"),
			file.ValueFromInt(2002),
			file.ValueFromInt(1982),
		},
	}

	flist := []string{"sname", "ssurname", "gradyear", "dob"}

	for _, v := range vals {
		if _, err := updPlanner.executeInsert(
			sql.NewInsertCommand("students", flist, v),
			cx,
		); err != nil {
			return err
		}
	}

	return nil
}

func selectRecords(planner BasicQueryPlanner, query string, x tx.Transaction) (Plan, error) {
	parser := sql.NewParser(query)

	data, err := parser.Query()
	if err != nil {
		return nil, err
	}

	plan, err := planner.CreatePlan(data, x)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func TestBasicQueryPlanner(t *testing.T) {

	fm, lm, bm := test.MakeManagers(t)
	mdm := NewMetadataManager()

	if err := createAndInsertTable(t, fm, lm, bm, mdm); err != nil {
		t.Fatal(err)
	}

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

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

	scan, err := plan.Open()
	if err != nil {
		t.Fatal(err)
	}

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

		name, err := scan.String("sname")
		if err != nil {
			t.Fatal(err)
		}

		year, err := scan.Int("gradyear")
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

func TestDeletePlanner(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)
	mdm := NewMetadataManager()

	if err := createAndInsertTable(t, fm, lm, bm, mdm); err != nil {
		t.Fatal(err)
	}

	x := tx.NewTx(fm, lm, bm)
	defer x.Commit()

	const q = "DELETE FROM students WHERE gradyear = 2001"

	parser := sql.NewParser(q)
	data, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	planner := newBasicUpdatePlanner(mdm)

	deleted, err := planner.executeDelete(data.(sql.DeleteCommand), x)
	if err != nil {
		t.Fatal(err)
	}

	// expect 2 records to be deleted
	if deleted != 2 {
		t.Fatalf("expected 2 students to be deleted. Got %d", deleted)
	}

	plan, err := selectRecords(
		NewBasicQueryPlanner(mdm),
		"SELECT sname, gradyear FROM students",
		x,
	)

	if err != nil {
		t.Fatal(err)
	}

	scan, err := plan.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer scan.Close()

	// test that no student has a "gradyear" value of 2001
	records := 0
	for {
		err := scan.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		year, err := scan.Int("gradyear")
		if err != nil {
			t.Fatal(err)
		}

		if year == 2001 {
			t.Fatalf("unexpected %q to be %d", "gradyear", 2001)
		}
		records++
	}

	if records != 1 {
		t.Fatalf("expected %d records, got %d", 1, records)
	}
}
