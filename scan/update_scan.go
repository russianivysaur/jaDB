package scan

import "justanotherdb/record"

type UpdateScan interface {
	Scan
	SetInt(string, int)
	SetString(string, string)
	SetVal(string, any)
	Insert()
	Delete()
	GetRid()
	MoveToRid(record.RID)
}
