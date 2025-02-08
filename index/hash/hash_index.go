package hash

import (
	"fmt"
	"jadb/index"
	"jadb/record"
	"jadb/table"
	"jadb/tx"
	"jadb/utils"
)

var _ index.Index = (*HashIndex)(nil)

const NUM_BUCKETS = 100

type HashIndex struct {
	txn       *tx.Transaction
	idxName   string
	layout    *record.Layout
	searchKey any
	ts        *table.TableScan
}

func (h *HashIndex) BeforeFirst(searchKey any) error {
	h.Close()
	h.searchKey = searchKey
	bucket := utils.HashCode(searchKey) % NUM_BUCKETS
	tblName := fmt.Sprintf("%s%d", h.idxName, bucket)
	ts, err := table.NewTableScan(h.txn, tblName, h.layout)
	if err != nil {
		return err
	}
	h.ts = ts
	return nil
}

func (h *HashIndex) Next() (bool, error) {
	for hasNext, err := h.ts.Next(); hasNext; hasNext, err = h.ts.Next() {
		if err != nil {
			return false, err
		}
		if h.ts.GetVal("dataval") == h.searchKey {
			return true, nil
		}
	}
	return false, nil
}

func (h *HashIndex) GetDataRid() (*record.RID, error) {
	blockNumber, err := h.ts.GetInt("block")
	if err != nil {
		return nil, err
	}
	id, err := h.ts.GetInt("id")
	if err != nil {
		return nil, err
	}
	rid := record.NewRID(blockNumber, id)
	return &rid, nil
}

func (h *HashIndex) Insert(val any, rid *record.RID) error {
	if err := h.BeforeFirst(val); err != nil {
		return err
	}
	if err := h.ts.Insert(); err != nil {
		return err
	}
	if err := h.ts.SetInt("block", rid.BlockNumber()); err != nil {
		return err
	}
	if err := h.ts.SetInt("id", rid.Slot()); err != nil {
		return err
	}
	if err := h.ts.SetVal("dataval", val); err != nil {
		return err
	}
	return nil
}

func (h *HashIndex) Delete(key any, rid *record.RID) error {
	if err := h.BeforeFirst(key); err != nil {
		return err
	}
	for hasNext, err := h.Next(); hasNext; hasNext, err = h.Next() {
		if err != nil {
			return err
		}
		if r, err := h.GetDataRid(); r.Equals(rid) {
			if err != nil {
				return err
			}
			if err := h.ts.Delete(); err != nil {
				return err
			}
			return nil
		}
	}
	return fmt.Errorf("key %v not found", key)
}

func (h *HashIndex) Close() {
	if h.ts != nil {
		h.ts.Close()
	}
}

func NewHashIndex(txn *tx.Transaction, idxName string, layout *record.Layout) *HashIndex {
	return &HashIndex{
		txn,
		idxName,
		layout,
		nil, nil,
	}
}

func SearchCost(numBlocks int, rpb int) int {
	return numBlocks / NUM_BUCKETS
}
