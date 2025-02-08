package index

import "jadb/record"

type Index interface {
	BeforeFirst(any) error
	Next() (bool, error)
	GetDataRid() (*record.RID, error)
	Insert(any, *record.RID) error
	Delete(any, *record.RID) error
	Close()
}
