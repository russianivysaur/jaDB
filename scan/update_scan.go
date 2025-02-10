package scan

import "jadb/record"

type UpdateScan interface {
	Scan
	SetInt(string, int) error
	SetString(string, string) error
	SetVal(string, any) error
	Insert() error
	Delete() error
	GetRid() *record.RID
	MoveToRid(*record.RID) error
}
