package sql

import "testing"

func TestIsKeyword(t *testing.T) {

	if !isKeyword("insert", 0, 6, 1, 5, "nsert") {
		t.Fatalf("expected keyword for 'nsert'")
	}

	if !isKeyword("insert", 0, 6, 2, 4, "sert") {
		t.Fatalf("expected keyword for 'nsert'")
	}

	if !isKeyword("insert", 0, 6, 3, 3, "ert") {
		t.Fatalf("expected keyword for 'nsert'")
	}
}

func TestTokenizer(t *testing.T) {

	const src = "SELECT * FROM table"

	tokenizer := initTokenizer(src)
	tkns, err := tokenize(src, tokenizer)
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range []string{
		"select",
		"*",
		"from",
		"table",
	} {
		if vv := tokenToString(tokenizer.src, tkns[i]); vv != v {
			t.Fatalf("expected token to be %q, got %v", v, vv)
		}
	}

	exp := []Token{
		{
			TokenType: TokenSelect,
			start:     0,
			lenght:    len("SELECT"),
			line:      0,
		},
		{
			TokenType: TokenStar,
			start:     7,
			lenght:    1,
			line:      0,
		},
		{
			TokenType: TokenFrom,
			start:     9,
			lenght:    len("FROM"),
			line:      0,
		},
		{
			TokenType: TokenIdentifier,
			start:     14,
			lenght:    len("table"),
			line:      0,
		},
	}

	for i := range tkns {
		if tkns[i] != exp[i] {
			t.Fatalf("expected token %+v, got %+v", exp[i], tkns[i])
		}
	}
}
