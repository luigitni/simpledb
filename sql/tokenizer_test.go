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

func TestKeywords(t *testing.T) {
	type test struct {
		src string
		exp tokenType
	}

	for _, tc := range []test{
		{
			src: "CREATE",
			exp: TokenCreate,
		},
		{
			src: "DELETE",
			exp: TokenDelete,
		},
		{
			src: "FROM",
			exp: TokenFrom,
		},
		{
			src: "INSERT",
			exp: TokenInsert,
		},
		{
			src: "INTO",
			exp: TokenInto,
		},
		{
			src: "INDEX",
			exp: TokenIndex,
		},
		{
			src: "SELECT",
			exp: TokenSelect,
		},
		{
			src: "UPDATE",
			exp: TokenUpdate,
		},
		{
			src: "WHERE",
			exp: TokenWhere,
		},
		{
			src: "AND",
			exp: TokenAnd,
		},
		{
			src: "VALUES",
			exp: TokenValues,
		},
		{
			src: "SET",
			exp: TokenSet,
		},
		{
			src: "TABLE",
			exp: TokenTable,
		},
		{
			src: "VARCHAR",
			exp: TokenVarchar,
		},
		{
			src: "INT",
			exp: TokenInt,
		},
		{
			src: "VIEW",
			exp: TokenView,
		},
		{
			src: "AS",
			exp: TokenAs,
		},
		{
			src: "ON",
			exp: TokenOn,
		},
	} {
		tokenizer := newTokenizer(tc.src)
		tkn, err := tokenizer.nextToken()
		if err != nil {
			t.Fatal(err)
		}

		if tkn.TokenType != tc.exp {
			t.Fatalf("expected token of type %+v for keyword %q. Got %+v", tc.exp, tc.src, tkn.TokenType)
		}
	}
}

func TestTokenizer(t *testing.T) {

	const src = "SELECT * FROM identifier"

	tokenizer := newTokenizer(src)
	tkns, err := tokenizer.tokenise()
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range []string{
		"select",
		"*",
		"from",
		"identifier",
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
			lenght:    len("itentifier"),
			line:      0,
		},
	}

	for i := range tkns {
		if tkns[i] != exp[i] {
			t.Fatalf("expected token %+v, got %+v", exp[i], tkns[i])
		}
	}
}
