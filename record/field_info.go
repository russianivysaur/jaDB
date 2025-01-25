package record

const (
	INTEGER = iota
	VARCHAR
)

type FieldInfo struct {
	fieldType int
	length    int
}

func NewFieldInfo(fieldType int, length int) FieldInfo {
	return FieldInfo{
		fieldType,
		length,
	}
}
