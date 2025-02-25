package parse

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type TokenType int

const (
	TTDelimiter TokenType = iota
	TTNumber
	TTString
	TTOperator
	TTEof
)

type SyntaxError struct {
	Message string
}

func (syn *SyntaxError) Error() string {
	return syn.Message
}

type Token struct {
	tokenType TokenType
	delimiter rune
	number    int
	str       string
	operator  string
}

type Lexer struct {
	input        string
	currentToken Token
	position     int
	keywords     map[string]bool
}

func NewLexer(input string) (*Lexer, error) {
	l := &Lexer{
		input: strings.ToLower(input),
	}
	l.initKeywords()
	if err := l.Next(); err != nil {
		return nil, err
	}
	return l, nil
}

func (lexer *Lexer) matchDelim(d rune) bool {
	return lexer.currentToken.tokenType == TTDelimiter && lexer.currentToken.delimiter == d
}

func (lexer *Lexer) matchIntConstant() bool {
	return lexer.currentToken.tokenType == TTNumber
}

func (lexer *Lexer) matchStringConstant() bool {
	return lexer.currentToken.tokenType == TTString
}

func (lexer *Lexer) matchKeyword(keyword string) bool {
	return lexer.currentToken.tokenType == TTString && lexer.currentToken.str == keyword && lexer.keywords[lexer.currentToken.str]
}

func (lexer *Lexer) matchId() bool {
	return lexer.currentToken.tokenType == TTString && !lexer.keywords[lexer.currentToken.str]
}

func (lexer *Lexer) eatDelim(d rune) error {
	if lexer.currentToken.tokenType != TTDelimiter {
		return &SyntaxError{fmt.Sprintf("Expected a delimiter %c at %d got %v", d, lexer.position, lexer.currentToken.tokenType)}
	}
	if lexer.currentToken.delimiter != d {
		return &SyntaxError{fmt.Sprintf("expected %c,got %c", d, lexer.currentToken.delimiter)}
	}
	return lexer.Next()
}

func (lexer *Lexer) eatIntConstant() (int, error) {
	if lexer.currentToken.tokenType != TTNumber {
		return -1, &SyntaxError{"expected number"}
	}
	val := lexer.currentToken.number
	if err := lexer.Next(); err != nil {
		return -1, err
	}
	return val, nil
}

func (lexer *Lexer) eatStringConstant() (string, error) {
	if lexer.currentToken.tokenType != TTString {
		return "", &SyntaxError{"expected string"}
	}
	val := lexer.currentToken.str
	if err := lexer.Next(); err != nil {
		return "", err
	}
	return val, nil
}

func (lexer *Lexer) eatKeyword(keyword string) error {
	if lexer.currentToken.tokenType != TTString {
		return &SyntaxError{fmt.Sprintf("expected TTString,got %v", lexer.currentToken.tokenType)}
	}
	val := lexer.currentToken.str
	if val != keyword {
		return &SyntaxError{fmt.Sprintf("expected %s,got %s", keyword, val)}
	}
	return lexer.Next()
}

func (lexer *Lexer) eatId() (string, error) {
	if lexer.currentToken.tokenType != TTString {
		return "", &SyntaxError{"expected TTString"}
	}
	val := lexer.currentToken.str
	return val, lexer.Next()
}

func (lexer *Lexer) initKeywords() {
	keywords := []string{
		"select", "from", "where", "and",
		"insert", "into", "values", "delete", "update",
		"set", "create", "table", "varchar",
		"int", "view", "as", "index", "on",
	}
	keywordsMap := make(map[string]bool)
	for _, keyword := range keywords {
		keywordsMap[keyword] = true
	}
	lexer.keywords = keywordsMap
}

func (lexer *Lexer) Next() error {
	lexer.skipWhitespaces()
	if lexer.position >= len(lexer.input) {
		lexer.currentToken = Token{tokenType: TTEof}
		return nil
	}
	nextRune, width := utf8.DecodeRuneInString(lexer.input[lexer.position:])
	switch {
	case isDelimiter(nextRune):
		lexer.position += width
		lexer.currentToken = Token{tokenType: TTDelimiter, delimiter: nextRune}
		return nil
	case isStringStart(nextRune):
		{
			lexer.position++
			var sb strings.Builder
			terminated := false
			for lexer.position < len(lexer.input) {
				if lexer.input[lexer.position] == '\'' {
					terminated = true
					break
				}
				nextRune, width := utf8.DecodeLastRuneInString(lexer.input[lexer.position:])
				lexer.position += width
				sb.WriteRune(nextRune)
			}
			if !terminated {
				return &SyntaxError{"unterminated string"}
			}
			if sb.Len() == 0 {
				return &SyntaxError{"expected a string,got nothing"}
			}
			lexer.currentToken = Token{tokenType: TTString, str: sb.String()}
			return nil
		}
	case isIntStart(nextRune):
		{
			var number strings.Builder
			number.WriteRune(nextRune)
			lexer.position++
			for lexer.position < len(lexer.input) {
				nextRune, width = utf8.DecodeRuneInString(lexer.input[lexer.position:])
				if !unicode.IsDigit(nextRune) {
					break
				}
				number.WriteRune(nextRune)
				lexer.position += width
			}
			num, err := strconv.Atoi(number.String())
			if err != nil {
				return &SyntaxError{"expected number"}
			}
			lexer.currentToken = Token{tokenType: TTNumber, number: num}
			return nil
		}
	case isOperatorStart(nextRune):
		{
			var operator strings.Builder
			operator.WriteRune(nextRune)
			lexer.position++
			nextRune, width = utf8.DecodeRuneInString(lexer.input[lexer.position:])
			lexer.position += width
			if !isOperatorStart(nextRune) {
				return &SyntaxError{fmt.Sprintf("expected a operator,got %c", nextRune)}
			}
			lexer.currentToken = Token{tokenType: TTOperator, operator: operator.String()}
			return nil
		}
	case unicode.IsLetter(nextRune) || nextRune == '_':
		{
			var word strings.Builder
			word.WriteRune(nextRune)
			lexer.position += width
			nextRune, width = utf8.DecodeRuneInString(lexer.input[lexer.position:])
			for unicode.IsLetter(nextRune) || nextRune == '_' {
				word.WriteRune(nextRune)
				lexer.position += width
				nextRune, width = utf8.DecodeRuneInString(lexer.input[lexer.position:])
			}
			lexer.currentToken = Token{tokenType: TTString, str: word.String()}
			return nil
		}
	}
	return fmt.Errorf("unexpected character %c at %d", nextRune, lexer.position)
}

func isDelimiter(r rune) bool {
	delimiters := []rune{',', '(', ')'}
	for _, d := range delimiters {
		if d == r {
			return true
		}
	}
	return false
}

func isStringStart(r rune) bool {
	return r == '\''
}

func isIntStart(r rune) bool {
	return r == '-' || unicode.IsDigit(r)
}

func isOperatorStart(r rune) bool {
	return r == '=' || r == '<' || r == '>' || r == '!'
}

func (lexer *Lexer) skipWhitespaces() {
	for lexer.position < len(lexer.input) {
		nextRune, width := utf8.DecodeRuneInString(lexer.input[lexer.position:])
		if !unicode.IsSpace(nextRune) {
			return
		}
		lexer.position += width
	}
}
