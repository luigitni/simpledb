package sql

import (
	"errors"
	"io"
	"strconv"
	"strings"
)

type tokenType int

const (
	TokenLeftParen tokenType = iota
	TokenRightParen
	TokenSemicolon
	TokenComma
	TokenStar
	TokenEOF
	TokenEqual
	TokenBang
	TokenLess
	TokenGreater
	TokenBangEqual
	TokenEqualEqual
	TokenLessEqual
	TokenGreaterEqual

	TokenString
	TokenNumber

	TokenIdentifier

	// keyword tokens
	keywordTokens
	TokenCreate
	TokenFrom
	TokenDelete
	TokenIndex
	TokenInsert
	TokenInto
	TokenSelect
	TokenUpdate
	TokenWhere

	TokenAnd
	TokenValues
	TokenSet
	TokenTable
	TokenVarchar
	TokenInt
	TokenView
	TokenAs
	TokenOn
)

type Token struct {
	TokenType tokenType
	start     int
	lenght    int
	line      int
}

type tokenizer struct {
	src     string
	start   int
	current int
	line    int
}

func newTokenizer(src string) *tokenizer {
	src = strings.ToLower(src)
	return &tokenizer{
		src: src,
	}
}

func Tokenize(src string) ([]Token, error) {
	t := newTokenizer(src)
	return t.tokenise()
}

func (t *tokenizer) tokenise() ([]Token, error) {
	var tokens []Token

	for {
		tkn, err := t.nextToken()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		tokens = append(tokens, tkn)
	}

	return tokens, nil
}

func tokenToString(src string, tkn Token) string {
	return src[tkn.start : tkn.start+tkn.lenght]
}

func tokenToIntVal(src string, tkn Token) (int, error) {
	if tkn.TokenType != TokenNumber {
		return 0, ErrInvalidSyntax
	}

	sv := tokenToString(src, tkn)
	return strconv.Atoi(sv)
}

func (t *tokenizer) nextToken() (Token, error) {
	t.skipWhitespace()
	t.start = t.current

	if t.isAtEnd() {
		return t.makeToken(TokenEOF), io.EOF
	}

	b := t.advance()

	switch b {
	case '(':
		return t.makeToken(TokenLeftParen), nil
	case ')':
		return t.makeToken(TokenRightParen), nil
	case ';':
		return t.makeToken(TokenSemicolon), nil
	case ',':
		return t.makeToken(TokenComma), nil
	// two chars token
	case '!':
		if t.match('=') {
			return t.makeToken(TokenBangEqual), nil
		}
		return t.makeToken(TokenBang), nil

	case '=':
		if t.match('=') {
			return t.makeToken(TokenEqualEqual), nil
		}
		return t.makeToken(TokenEqual), nil

	case '<':
		if t.match('=') {
			return t.makeToken(TokenLessEqual), nil
		}
		return t.makeToken(TokenGreater), nil

	case '>':
		if t.match('=') {
			return t.makeToken(TokenGreaterEqual), nil
		}
		return t.makeToken(TokenLessEqual), nil
	case '*':
		return t.makeToken(TokenStar), nil
	case '\'':
		return t.string()
	case '\n':
		t.line++
		t.advance()

	default:
		if isAlpha(b) {
			return t.identifier()
		}

		if isDigit(b) {
			return t.number()
		}
	}

	return Token{}, errors.New("unexpected character")
}

func (t *tokenizer) skipWhitespace() {
	for {
		c := t.peek()
		switch c {
		case ' ', '\r', '\t':
			t.advance()
		// sql comments:
		// eat the comment texts
		case '-':
			if t.peekNext() == '-' {
				for t.peek() != '\n' && !t.isAtEnd() {
					t.advance()
				}
			} else {
				return
			}
		default:
			return
		}
	}
}

func (t *tokenizer) match(b byte) bool {
	if t.isAtEnd() {
		return false
	}
	if t.src[t.current] != b {
		return false
	}

	t.current++
	return true
}

func (t *tokenizer) isAtEnd() bool {
	return t.current == len(t.src)
}

func (t *tokenizer) makeToken(typ tokenType) Token {
	return Token{
		TokenType: typ,
		start:     t.start,
		lenght:    t.current - t.start,
		line:      t.line,
	}
}

func (t *tokenizer) advance() byte {
	b := t.src[t.current]
	t.current++
	return b
}

func (t *tokenizer) peek() byte {
	if t.current >= len(t.src) {
		return 0
	}
	return t.src[t.current]
}

// peeks two chars ahead
func (t *tokenizer) peekNext() byte {
	if t.isAtEnd() {
		return 0
	}
	return t.src[t.current+1]
}

func (t *tokenizer) string() (Token, error) {
	for t.peek() != '\'' && !t.isAtEnd() {
		t.advance()
	}

	if t.isAtEnd() {
		return Token{}, errors.New("unterminated string")
	}

	t.advance()

	return t.makeToken(TokenString), nil
}

func (t *tokenizer) number() (Token, error) {
	// SimpleDB just supports integers
	// which means no checks for decimal part are run
	for isDigit(t.peek()) {
		t.advance()
	}

	return t.makeToken(TokenNumber), nil
}

func (t *tokenizer) identifier() (Token, error) {
	for isAlpha(t.peek()) || isDigit(t.peek()) {
		t.advance()
	}

	return t.makeToken(t.identifierType()), nil
}

func (t *tokenizer) isKeyword(start int, len int, keyword string) bool {
	return isKeyword(t.src, t.start, t.current, start, len, keyword)
	// return t.current-t.start == start+len && keyword == t.src[t.start+start:t.start+start+len]
}

func isKeyword(src string, from int, to int, start int, l int, keyword string) bool {
	return to-from == start+l && keyword == src[from+start:from+start+l]
}

func (t *tokenizer) identifierType() tokenType {
	switch t.src[t.start] {
	case 'a':
		if t.isKeyword(1, 1, "s") {
			return TokenAs
		}
		if t.isKeyword(1, 2, "nd") {
			return TokenAnd
		}
	case 'c':
		if t.isKeyword(1, 5, "reate") {
			return TokenCreate
		}
	case 'd':
		if t.isKeyword(1, 5, "elete") {
			return TokenDelete
		}
	case 'f':
		if t.isKeyword(1, 3, "rom") {
			return TokenFrom
		}
	case 'i':
		if t.isKeyword(1, 5, "nsert") {
			return TokenInsert
		}
		if t.isKeyword(1, 2, "nt") {
			return TokenInt
		}
		if t.isKeyword(1, 3, "nto") {
			return TokenInto
		}
		if t.isKeyword(1, 4, "ndex") {
			return TokenIndex
		}
	case 'o':
		if t.isKeyword(1, 1, "n") {
			return TokenOn
		}
	case 's':
		if t.isKeyword(1, 2, "et") {
			return TokenSet
		}
		if t.isKeyword(1, 5, "elect") {
			return TokenSelect
		}
	case 't':
		if t.isKeyword(1, 4, "able") {
			return TokenTable
		}
	case 'u':
		if t.isKeyword(1, 5, "pdate") {
			return TokenUpdate
		}
	case 'w':
		if t.isKeyword(1, 4, "here") {
			return TokenWhere
		}
	case 'v':
		if t.isKeyword(1, 3, "iew") {
			return TokenView
		}
		if t.isKeyword(1, 5, "alues") {
			return TokenValues
		}
		if t.isKeyword(1, 6, "archar") {
			return TokenVarchar
		}
	}
	return TokenIdentifier
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func isAlpha(b byte) bool {
	return b >= 'a' && b <= 'z' ||
		b >= 'A' && b <= 'Z'
}
