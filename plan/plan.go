package plan

import (
	"jadb/record"
	"jadb/scan"
)

type Plan interface {
	Open() *scan.Scan
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(string) int
	Schema() *record.Schema
}
