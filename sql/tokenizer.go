package sql

import (
	"errors"
	"io"
	"strings"
)

type tokenType int

const (
	TokenLeftParen tokenType = iota
	TokenRightParen
	TokenSemicolon
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
	TokenCreate
	TokenFrom
	TokenDelete
	TokenIndex
	TokenInsert
	TokenInto
	TokenSelect
	TokenUpdate
	TokenWhere
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

func initTokenizer(src string) tokenizer {
	src = strings.ToLower(src)
	return tokenizer{
		src: src,
	}
}

func Tokenize(src string) ([]Token, error) {
	t := initTokenizer(src)
	return tokenize(src, t)
}

func tokenize(src string, t tokenizer) ([]Token, error) {
	var tokens []Token

	for {
		tkn, err := t.scanToken()
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

func (t *tokenizer) scanToken() (Token, error) {
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

	return Token{}, errors.New("Unexpected character.")
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
		return Token{}, errors.New("Unterminated string.")
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
		} else if t.isKeyword(1, 4, "nto") {
			return TokenInto
		} else if t.isKeyword(1, 4, "ndex") {
			return TokenIndex
		}
	case 's':
		if t.isKeyword(1, 5, "elect") {
			return TokenSelect
		}
	case 'u':
		if t.isKeyword(1, 5, "pdate") {
			return TokenUpdate
		}
	case 'w':
		if t.isKeyword(1, 4, "here") {
			return TokenWhere
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
