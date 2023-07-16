package sql

import "testing"

func TestMatchStringConstant(t *testing.T) {
	
	const exp = "'testname'"
	lx := NewLexer(newTokenizer(exp));

	v, err := lx.eatStringConstant()

	if err != nil {
		t.Fatal(err)
	}
	
	if v != exp {
		t.Fatalf("expected a value of %q, got %s", exp, v)
	}
}

func TestMatchIntConstant(t *testing.T) {
	
	lx := NewLexer(newTokenizer("123"));

	v, err := lx.eatIntConstant()

	if err != nil {
		t.Fatal(err)
	}
	
	if v != 123 {
		t.Fatalf("expected a value of 123, got %d", v)
	}
}

func TestMatchKeywords(t *testing.T) {
	for _, v := range []string{
		"select",
		"from",
		"where",
		"and",
		"insert",
		"into",
		"values",
		"delete",
		"update",
		"set",
		"create",
		"table",
		"varchar",
		"int",
		"view",
		"as",
		"index",
		"on",
	} {
		lx := NewLexer(newTokenizer(v))

		if err := lx.eatKeyword(v); err != nil {
			t.Fatalf("unexpected %s error for keyword %q", err, v)
		}
	}

	if lx := NewLexer(newTokenizer("notakeyword")); lx.eatKeyword("notakeyword") == nil {
		t.Fatal("unexpected keyword")
	}
}