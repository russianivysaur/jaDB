package parse

type InsertData struct {
	tblName   string
	fieldList []string
	values    []any
}

func NewInsertData(tblName string, fieldList []string, values []any) *InsertData {
	return &InsertData{tblName, fieldList, values}
}

func (i *InsertData) TableName() string {
	return i.tblName
}

func (i *InsertData) Fields() []string {
	return i.fieldList
}

func (i *InsertData) Values() []any {
	return i.values
}
