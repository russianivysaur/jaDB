package parse

type Tokenizer struct {
	input string
}

func NewTokenizer(input string) *Tokenizer {
	return &Tokenizer{input}
}
