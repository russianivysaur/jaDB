package parse

import "jadb/query"

type ModifyData struct {
	tableName string
	field     string
	value     any
	predicate *query.Predicate
}

func NewModifyData(tableName string, field string, value any, predicate *query.Predicate) *ModifyData {
	return &ModifyData{
		tableName,
		field,
		value,
		predicate,
	}
}

func (m *ModifyData) Fields() string {
	return m.field
}

func (m *ModifyData) Values() any {
	return m.value
}

func (m *ModifyData) Predicate() *query.Predicate {
	return m.predicate
}

func (m *ModifyData) TableName() string {
	return m.tableName
}
