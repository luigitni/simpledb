package sql

import "errors"

var ErrInvalidSyntax = errors.New("invalid syntax")

type Lexer struct {
	tokenizer *tokenizer
	current   Token
}

func NewLexer(tokenizer *tokenizer) Lexer {
	lx := Lexer{
		tokenizer: tokenizer,
	}
	lx.nextToken()
	return lx
}

func (lexer Lexer) matchTokenType(t tokenType) bool {
	return lexer.current.TokenType == t
}

func (lexer Lexer) matchIntConstant() bool {
	return lexer.matchTokenType(TokenNumber)
}

func (lexer Lexer) matchStringConstant() bool {
	return lexer.matchTokenType(TokenString)
}

func (lexer Lexer) matchKeyword(keyword string) bool {
	return lexer.current.TokenType > keywordTokens &&
		tokenToString(lexer.tokenizer.src, lexer.current) == keyword
}

func (lexer Lexer) matchIdentifier() bool {
	return lexer.current.TokenType == TokenIdentifier
}

func (lexer Lexer) eatTokenType(t tokenType) error {
	if !lexer.matchTokenType(t) {
		return ErrInvalidSyntax
	}
	return lexer.nextToken()
}

func (lexer Lexer) eatIntConstant() (int, error) {
	if !lexer.matchIntConstant() {
		return 0, ErrInvalidSyntax
	}

	defer lexer.nextToken()
	return tokenToIntVal(lexer.tokenizer.src, lexer.current)
}

func (lexer Lexer) eatStringConstant() (string, error) {
	if !lexer.matchStringConstant() {
		return "", ErrInvalidSyntax
	}
	defer lexer.nextToken()
	return tokenToString(lexer.tokenizer.src, lexer.current), nil
}

func (lexer Lexer) eatKeyword(kw string) error {
	if !lexer.matchKeyword(kw) {
		return ErrInvalidSyntax
	}
	lexer.nextToken()
	return nil
}

func (lexer Lexer) eatID() error {
	if !lexer.matchIdentifier() {
		return ErrInvalidSyntax
	}
	lexer.nextToken()
	return nil
}

func (lexer *Lexer) nextToken() error {
	tkn, err := lexer.tokenizer.nextToken()
	if err != nil {
		return ErrInvalidSyntax
	}
	lexer.current = tkn
	return nil
}
