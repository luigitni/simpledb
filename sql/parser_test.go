package sql

import (
	"testing"
)

func TestParseField(t *testing.T) {
	p := NewParser("field")

	v, err := p.Field()
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

	sl, err := p.SelectList()

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

		c, err := p.Constant()
		if err != nil {
			t.Fatal(err)
		}

		if s := c.AsStringVal(); s != v.exp {
			t.Fatalf("expected %q, got %s", v.exp, s)
		}
	}
}

func TestQuery(t *testing.T) {
	const src = "SELECT first, second FROM atable WHERE first = 1 AND second = 'second'"
	p := NewParser(src)

	qd, err := p.Query()

	if err != nil {
		t.Fatal(err)
	}

	if qs := qd.String(); qs != src {
		t.Fatalf("unexpected query %q", qs)
	}
}

func TestUpdateCommandSimple(t *testing.T) {
	const src = "UPDATE atable SET col = 5"

	p := NewParser(src)

	cmd, err := p.WriteCmd()
	if err != nil {
		t.Fatal(err)
	}

	upd := cmd.(UpdateCommand)

	if upd.Field != "col" {
		t.Fatalf("expected field to be %q, got %s", "col", upd.Field)
	}

	if v := upd.NewValue.String(); v != "5" {
		t.Fatalf("expected newValue to be %q, got %s", "5", upd.Field)
	}
}

func TestUpdateCommandPredicate(t *testing.T) {
	const src = "UPDATE atable SET col = 5 WHERE anothercol = 3"

	p := NewParser(src)

	cmd, err := p.WriteCmd()
	if err != nil {
		t.Fatal(err)
	}

	upd := cmd.(UpdateCommand)

	if upd.Field != "col" {
		t.Fatalf("expected field to be %q, got %s", "col", upd.Field)
	}

	if v := upd.NewValue.AsConstant().AsIntVal(); v != 5 {
		t.Fatalf("expected newValue to be %d, got %d", 5, v)
	}

	if s := upd.Predicate.String(); s != "anothercol = 3" {
		t.Fatalf("expected predicate to be %q, got %q", "anothercol = 3", s)
	}
}

func TestDeleteCommand(t *testing.T) {
	const src = "DELETE FROM atable WHERE acol = 5"

	p := NewParser(src)

	cmd, err := p.WriteCmd()
	if err != nil {
		t.Fatal(err)
	}

	del := cmd.(DeleteCommand)

	if del.TableName != "atable" {
		t.Fatalf("expected target table to be %q, got %q", "atable", del.TableName)
	}

	if v := del.Predicate.String(); v != "acol = 5" {
		t.Fatalf("expected predicate to be %q, got %q", "acol = 5", v)
	}
}

func TestInsertCommand(t *testing.T) {
	const src = "INSERT INTO atable (acolumn, anothercolumn) VALUES ('aval', 5)"

	p := NewParser(src)

	cmd, err := p.WriteCmd()
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

	if v := ins.Values[0].AsStringVal(); v != "aval" {
		t.Fatalf("expected value to be %q, got %q", "aval", v)
	}

	if v := ins.Values[1].AsIntVal(); v != 5 {
		t.Fatalf("expected value to be %d, got %d", 5, v)
	}
}
