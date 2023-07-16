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
