package query

type Operator int

const (
	Equal Operator = iota
	NotEqual
	LessThan
	LessThanEqual
	GreaterThan
	GreaterThanEqual
)

func (op Operator) String() string {
	switch op {
	case Equal:
		return "="
	case NotEqual:
		return "!="
	case LessThan:
		return "<"
	case LessThanEqual:
		return "<="
	case GreaterThan:
		return ">"
	case GreaterThanEqual:
		return ">="
	}
	return ""
}
