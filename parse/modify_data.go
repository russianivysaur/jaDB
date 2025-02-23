package parse

import "jadb/query"

type ModifyData struct {
	tableName string
	fields    []string
	values    []any
	predicate *query.Predicate
}

func NewModifyData(tableName string, fields []string, values []any, predicate *query.Predicate) *ModifyData {
	return &ModifyData{
		tableName,
		fields,
		values,
		predicate,
	}
}

func (m *ModifyData) Fields() []string {
	return m.fields
}

func (m *ModifyData) Values() []any {
	return m.values
}

func (m *ModifyData) Predicate() *query.Predicate {
	return m.predicate
}

func (m *ModifyData) TableName() string {
	return m.tableName
}
