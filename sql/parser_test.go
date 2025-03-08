package sql

import (
	"slices"
	"testing"

	"github.com/luigitni/simpledb/storage"
)

func TestParseField(t *testing.T) {
	p := NewParser("field")

	v, err := p.field()
	if err != nil {
		t.Fatal(err)
	}
	if v != "field" {
		t.Fatalf("expected %q got %s", "field", v)
	}
}

func TestFieldList(t *testing.T) {
	const src = "first, second, third"
	p := NewParser(src)

	sl, err := p.selectList()
	if err != nil {
		t.Fatal(err)
	}

	if len(sl) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(sl))
	}

	exp := []string{"first", "second", "third"}
	for i := range sl {
		if sl[i] != exp[i] {
			t.Fatalf("expected %q got %q at position %d", exp[i], sl[i], i)
		}
	}
}

func TestConstant(t *testing.T) {
	type test struct {
		src string
		exp string
	}

	for _, v := range []test{
		{
			src: "''",
			exp: "",
		},
		{
			src: "'test'",
			exp: "test",
		},
	} {
		p := NewParser(v.src)

		c, err := p.constant()
		if err != nil {
			t.Fatal(err)
		}

		if s := c.AsVarlen().AsGoString(); s != v.exp {
			t.Fatalf("expected %q, got %s", v.exp, s)
		}
	}
}

func TestQuery(t *testing.T) {
	const src = "SELECT first, second FROM atable WHERE first = 1 AND second = 'second' ORDER BY second"
	p := NewParser(src)

	qd, err := p.Query()
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(qd.Tables(), []string{"atable"}) {
		t.Fatalf("unexpected tables %v", qd.Tables())
	}

	if !slices.Equal(qd.Fields(), []string{"first", "second"}) {
		t.Fatalf("unexpected fields %v", qd.Fields())
	}

	predicate := qd.Predicate()
	if len(predicate.terms) != 2 {
		t.Fatalf("expected 2 terms, got %d", len(predicate.terms))
	}

	feq1 := predicate.terms[0]
	if feq1.lhs.fname != "first" {
		t.Fatalf("expected field to be %q, got %q", "first", feq1.lhs.fname)
	}

	if got := feq1.rhs.AsConstant().AsFixedLen().AsInt(); got != 1 {
		t.Fatalf("expected value to be %d, got %d", 1, got)
	}

	feq2 := predicate.terms[1]
	if feq2.lhs.fname != "second" {
		t.Fatalf("expected field to be %q, got %q", "second", feq2.lhs.fname)
	}

	if got := feq2.rhs.AsConstant().AsVarlen().AsGoString(); got != "second" {
		t.Fatalf("expected value to be %q, got %q", "second", got)
	}

	if !slices.Equal(qd.OrderByFields(), []string{"second"}) {
		t.Fatalf("unexpected order by fields %v", qd.OrderByFields())
	}
}

func TestUpdateCommandSimple(t *testing.T) {
	const src = "UPDATE atable SET col = 5"

	p := NewParser(src)

	cmd, err := p.dml()
	if err != nil {
		t.Fatal(err)
	}

	upd := cmd.(UpdateCommand)

	if len(upd.Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(upd.Fields))
	}

	field := upd.Fields[0]

	if field.Field != "col" {
		t.Fatalf("expected field to be %q, got %s", "col", field.Field)
	}

	if v := field.NewValue.String(storage.INT); v != "5" {
		t.Fatalf("expected newValue to be %q, got %s", "5", field.Field)
	}
}

func TestUpdateCommandPredicate(t *testing.T) {
	const src = "UPDATE atable SET col = 5 WHERE anothercol = 3"

	p := NewParser(src)

	cmd, err := p.dml()
	if err != nil {
		t.Fatal(err)
	}

	upd := cmd.(UpdateCommand)

	if len(upd.Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(upd.Fields))
	}

	field := upd.Fields[0]

	if field.Field != "col" {
		t.Fatalf("expected field to be %q, got %s", "col", field.Field)
	}

	if v := field.NewValue.AsConstant(); storage.ValueAsInteger[storage.Int](v) != 5 {
		t.Fatalf("expected newValue to be %d, got %d", 5, v)
	}

	aeq3 := upd.Predicate.terms
	if len(aeq3) != 1 {
		t.Fatalf("expected 1 term, got %d", len(aeq3))
	}

	if aeq3[0].lhs.fname != "anothercol" {
		t.Fatalf("expected field to be %q, got %q", "anothercol", aeq3[0].lhs.fname)
	}

	if v := aeq3[0].rhs.AsConstant().AsFixedLen().AsInt(); v != 3 {
		t.Fatalf("expected value to be %d, got %d", 3, v)
	}
}

func TestUpdateCommandMultipleFields(t *testing.T) {
	const src = "UPDATE atable SET col = 5, second = 'test' WHERE anothercol = 3"

	p := NewParser(src)

	cmd, err := p.dml()
	if err != nil {
		t.Fatal(err)
	}

	upd := cmd.(UpdateCommand)

	if len(upd.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(upd.Fields))
	}

	type exp struct {
		Field string
		Value interface{}
	}

	for i, e := range []exp{
		{
			Field: "col",
			Value: 5,
		},
		{
			Field: "second",
			Value: "test",
		},
	} {

		field := upd.Fields[i]

		if field.Field != e.Field {
			t.Fatalf("expected field to be %q, got %q", e.Field, field.Field)
		}

		v := field.NewValue.AsConstant()

		switch val := e.Value.(type) {
		case int:
			if storage.ValueAsInteger[storage.Int](v) != storage.Int(val) {
				t.Fatalf("expected newValue to be %d, got %d", val, v)
			}
		case string:
			if v := v.AsVarlen().AsGoString(); v != val {
				t.Fatalf("expected newValue to be %q, got %s", val, v)
			}
		}
	}

	aeq3 := upd.Predicate.terms
	if len(aeq3) != 1 {
		t.Fatalf("expected 1 term, got %d", len(aeq3))
	}

	if aeq3[0].lhs.fname != "anothercol" {
		t.Fatalf("expected field to be %q, got %q", "anothercol", aeq3[0].lhs.fname)
	}

	if v := aeq3[0].rhs.AsConstant().AsFixedLen().AsInt(); v != 3 {
		t.Fatalf("expected value to be %d, got %d", 3, v)
	}
}

func TestDeleteCommand(t *testing.T) {
	const src = "DELETE FROM atable WHERE acol = 5"

	p := NewParser(src)

	cmd, err := p.dml()
	if err != nil {
		t.Fatal(err)
	}

	del := cmd.(DeleteCommand)

	if del.TableName != "atable" {
		t.Fatalf("expected target table to be %q, got %q", "atable", del.TableName)
	}

	if len(del.Predicate.terms) != 1 {
		t.Fatalf("expected 1 term, got %d", len(del.Predicate.terms))
	}

	term := del.Predicate.terms[0]
	if term.lhs.fname != "acol" {
		t.Fatalf("expected field to be %q, got %q", "acol", term.lhs.fname)
	}

	if v := term.rhs.AsConstant().AsFixedLen().AsInt(); v != 5 {
		t.Fatalf("expected value to be %d, got %d", 5, v)
	}
}

func TestInsertCommand(t *testing.T) {
	const src = "INSERT INTO atable (acolumn, anothercolumn) VALUES ('aval', 5)"

	p := NewParser(src)

	cmd, err := p.dml()
	if err != nil {
		t.Fatal(err)
	}

	ins := cmd.(InsertCommand)

	if ins.TableName != "atable" {
		t.Fatalf("expected target table to be %q, got %q", "atable", ins.TableName)
	}

	for i, c := range []string{"acolumn", "anothercolumn"} {
		if f := ins.Fields[i]; f != c {
			t.Fatalf("expected field %q at index %d, got %q", c, i, f)
		}
	}

	if v := ins.Values[0].AsVarlen().AsGoString(); v != "aval" {
		t.Fatalf("expected value to be %q, got %q", "aval", v)
	}

	if v := ins.Values[1]; storage.ValueAsInteger[storage.Int](v) != 5 {
		t.Fatalf("expected value to be %d, got %d", 5, v)
	}
}

func TestCreateTableCommand(t *testing.T) {
	const src = "CREATE TABLE atable (name TEXT, age INT)"

	p := NewParser(src)

	cmd, err := p.ddl()
	if err != nil {
		t.Fatal(err)
	}

	cr, ok := cmd.(CreateTableCommand)
	if !ok {
		t.Fatal("expected CreateTableCommand")
	}

	if cr.TableName != "atable" {
		t.Fatalf("expected table name to be %q, got %q", "atable", cr.TableName)
	}

	expF := []FieldDef{
		{
			Name: "name",
			Type: storage.TEXT,
			Len:  0,
		},
		{
			Name: "age",
			Type: storage.INT,
			Len:  0,
		},
	}

	for i, f := range cr.Fields {
		e := expF[i]
		if f != e {
			t.Fatalf("expected field to be %+v at %d, got %+v", e, i, f)
		}
	}
}

func TestTCLCommands(t *testing.T) {
	t.Parallel()
	type test struct {
		src     string
		cmdType CommandType
	}

	for _, test := range []test{
		{
			src:     "BEGIN",
			cmdType: CommandTypeTCLBegin,
		},
		{
			src:     "COMMIT",
			cmdType: CommandTypeTCLCommit,
		},
		{
			src:     "ROLLBACK",
			cmdType: CommandTypeTCLRollback,
		},
	} {
		tt := test
		t.Run(test.src, func(t *testing.T) {
			t.Parallel()

			p := NewParser(test.src)
			ok, cmd := p.isTCL()
			if !ok {
				t.Fatal("expected TCL command")
			}

			if cmd.Type() != tt.cmdType {
				t.Fatalf("expected %v got %v", test.cmdType, cmd.Type())
			}
		})
	}
}
