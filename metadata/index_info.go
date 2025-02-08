package metadata

import (
	"jadb/index"
	"jadb/index/hash"
	"jadb/record"
	"jadb/tx"
)

type IndexInfo struct {
	idxName   string
	fldName   string
	tblSchema *record.Schema
	idxLayout *record.Layout
	txn       *tx.Transaction
	si        *StatInfo
}

func NewIndexInfo(idxName string, fldName string, tableLayout *record.Layout, txn *tx.Transaction,
	si *StatInfo) IndexInfo {
	return IndexInfo{
		idxName, fldName, tableLayout.Schema(),
		createIdxLayout(tableLayout, fldName),
		txn,
		si,
	}
}

func (info IndexInfo) open() index.Index {
	return hash.NewHashIndex(info.txn, info.idxName, info.idxLayout)
}

func (info IndexInfo) blocksAccessed() int {
	rpb := info.txn.BlockSize() / info.idxLayout.SlotSize()
	numBlocks := info.si.RecordsOutput() / rpb
	return hash.SearchCost(numBlocks, rpb)
}

func (info IndexInfo) recordsOutput() int {
	return info.si.RecordsOutput() / info.si.DistinctValues()
}

func (info IndexInfo) distinctValues(fName string) int {
	if info.fldName == fName {
		return 1
	}
	return info.si.DistinctValues()
}

func createIdxLayout(tableLayout *record.Layout, fldName string) *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	if tableLayout.Schema().Type(fldName) == record.INTEGER {
		schema.AddIntField("dataval")
	} else {
		schema.AddStringField("dataval", tableLayout.Schema().Length(fldName))
	}
	l := record.NewLayout(schema)
	return &l
}
