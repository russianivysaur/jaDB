package index

import "justanotherdb/record"

type Index interface {
	BeforeFirst(any)
	Next() bool
	GetDataRid() record.RID
	Insert(any, record.RID)
	Delete(any, record.RID)
	Close()
}
