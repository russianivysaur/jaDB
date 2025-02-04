package index

import "jadb/record"

var _ Index = (*HashIndex)(nil)

type HashIndex struct {
}

func (index *HashIndex) BeforeFirst(searchKey any) {

}

func (index *HashIndex) Next() bool {
	return false
}

func (index *HashIndex) GetDataRid() record.RID {
	return record.RID{}
}

func (index *HashIndex) Insert(dataVal any, dataRid record.RID) {

}

func (index *HashIndex) Delete(dataval any, dataRid record.RID) {

}

func (index *HashIndex) Close() {

}
