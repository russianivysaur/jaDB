package parse

import "jadb/query"

type DeleteData struct {
	tableName string
	predicate *query.Predicate
}

func NewDeleteData(tableName string, predicate *query.Predicate) *DeleteData {
	return &DeleteData{
		tableName, predicate,
	}
}

func (d *DeleteData) TableName() string {
	return d.tableName
}

func (d *DeleteData) Predicate() *query.Predicate {
	return d.predicate
}
