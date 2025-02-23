package parse

import "strings"

type Lexer struct {
	input     string
	keywords  []string
	tokenizer *Tokenizer
}

func NewLexer(input string) *Lexer {
	l := &Lexer{
		input: strings.ToLower(input),
	}
	tokenizer := NewTokenizer(strings.ToLower(input))
	l.tokenizer = tokenizer
	l.initKeywords()
	return l
}

func (lexer *Lexer) matchDelim(d byte) {

}

func (lexer *Lexer) matchIntConstant() {

}

func (lexer *Lexer) matchStringConstant() {

}

func (lexer *Lexer) matchKeyword() {

}

func (lexer *Lexer) matchId() {

}

func (lexer *Lexer) eatDelim(d byte) {

}

func (lexer *Lexer) eatIntConstant() int {
	return -1
}

func (lexer *Lexer) eatStringConstant() string {
	return ""
}

func (lexer *Lexer) eatKeyword(keyword string) {

}

func (lexer *Lexer) eatId() string {
	return ""
}

func (lexer *Lexer) initKeywords() {
	lexer.keywords = []string{
		"select", "from", "where", "and",
		"insert", "into", "values", "delete", "update",
		"set", "create", "table", "varchar",
		"int", "view", "as", "index", "on",
	}
}
