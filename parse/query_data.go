package parse

import (
	"fmt"
	"jadb/query"
	"strings"
)

type QueryData struct {
	fieldList []string
	tableList []string
	pred      *query.Predicate
}

func NewQueryData(fields []string, tables []string, predicate *query.Predicate) *QueryData {
	return &QueryData{fields, tables, predicate}
}

func (q *QueryData) fields() []string {
	return q.fieldList
}

func (q *QueryData) tables() []string {
	return q.tableList
}

func (q *QueryData) predicate() *query.Predicate {
	return q.pred
}

func (q *QueryData) toString() string {
	result := "select "
	fields := ""
	for _, fld := range q.fieldList {
		fields += fmt.Sprintf("%s,", fld)
	}
	fields = strings.TrimRight(fields, ",")
	tables := ""
	for _, tbl := range q.tableList {
		tables += fmt.Sprintf("%s,", tbl)
	}
	tables = strings.TrimRight(tables, ",")

	predicate := ""
	if q.pred.String() != "" {
		predicate += "where " + q.pred.String()
	}
	result = result + fields + tables + predicate
	return result
}
