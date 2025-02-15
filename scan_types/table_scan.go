package scan_types

import (
	"jadb/file"
	"jadb/record"
	"jadb/scan"
	"jadb/tx"
)

var _ scan.UpdateScan = (*TableScan)(nil)

type TableScan struct {
	tx          *tx.Transaction
	layout      *record.Layout
	filename    string
	rp          *record.RecordPage
	currentSlot int
}

func NewTableScan(tx *tx.Transaction, tablename string, layout *record.Layout) (*TableScan, error) {
	filename := tablename + ".tbl"
	size, err := tx.Size(filename)
	if err != nil {
		return nil, err
	}
	var rp *record.RecordPage
	ts := &TableScan{
		tx,
		layout,
		filename,
		rp,
		-1,
	}

	if size == 0 {
		if err := ts.moveToNewBlock(); err != nil {
			return nil, err
		}
	} else {
		if err := ts.moveToBlock(0); err != nil {
			return nil, err
		}
	}
	return ts, nil
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.Block())
	}
}

func (ts *TableScan) BeforeFirst() error {
	if err := ts.moveToBlock(0); err != nil {
		return err
	}
	return nil
}

func (ts *TableScan) Next() (bool, error) {
	ts.currentSlot = ts.rp.NextAfter(ts.currentSlot)
	for ts.currentSlot < 0 {
		atLast, err := ts.atLastBlock()
		if err != nil {
			return false, err
		}
		if atLast {
			return false, nil
		}
		if err := ts.moveToBlock(ts.rp.Block().GetBlockNumber() + 1); err != nil {
			return false, err
		}
		ts.currentSlot = ts.rp.NextAfter(ts.currentSlot)
	}
	return true, nil
}

func (ts *TableScan) GetInt(fldName string) (int, error) {
	return ts.rp.GetInt(ts.currentSlot, fldName)
}

func (ts *TableScan) GetString(fldName string) (string, error) {
	return ts.rp.GetString(ts.currentSlot, fldName)
}

func (ts *TableScan) GetVal(fldName string) (any, error) {
	if ts.layout.Schema().Type(fldName) == record.INTEGER {
		return ts.GetInt(fldName)
	} else {
		return ts.GetString(fldName)
	}
}

func (ts *TableScan) HasField(fldName string) bool {
	return ts.layout.Schema().HasField(fldName)
}

func (ts *TableScan) SetInt(fldName string, val int) error {
	return ts.rp.SetInt(ts.currentSlot, fldName, val)
}

func (ts *TableScan) SetString(fldName string, val string) error {
	return ts.rp.SetString(ts.currentSlot, fldName, val)
}

func (ts *TableScan) SetVal(fldName string, val any) error {
	if ts.layout.Schema().Type(fldName) == record.INTEGER {
		return ts.SetInt(fldName, val.(int))
	} else {
		return ts.SetString(fldName, val.(string))
	}
}

func (ts *TableScan) Insert() error {
	var err error
	ts.currentSlot, err = ts.rp.InsertAfter(ts.currentSlot)
	if err != nil {
		return err
	}
	for ts.currentSlot < 0 {
		atLast, err := ts.atLastBlock()
		if err != nil {
			return err
		}
		if atLast {
			if err = ts.moveToNewBlock(); err != nil {
				return err
			}
		} else {
			if err = ts.moveToBlock(ts.rp.Block().GetBlockNumber() + 1); err != nil {
				return err
			}
		}
		ts.currentSlot, err = ts.rp.InsertAfter(ts.currentSlot)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ts *TableScan) Delete() error {
	return ts.rp.Delete(ts.currentSlot)
}

func (ts *TableScan) MoveToRid(rid *record.RID) error {
	ts.Close()
	block := file.NewBlock(ts.filename, rid.BlockNumber())
	var err error
	ts.rp, err = record.NewRecordPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}
	ts.currentSlot = rid.Slot()
	return nil
}

func (ts *TableScan) GetRid() *record.RID {
	return record.NewRID(ts.rp.Block().GetBlockNumber(), ts.currentSlot)
}

func (ts *TableScan) moveToNewBlock() error {
	ts.Close()
	block, err := ts.tx.Append(ts.filename)
	if err != nil {
		return err
	}
	if ts.rp, err = record.NewRecordPage(ts.tx, block, ts.layout); err != nil {
		return err
	}
	if err = ts.rp.Format(); err != nil {
		return err
	}
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) moveToBlock(blockNumber int) error {
	ts.Close()
	block := file.NewBlock(ts.filename, blockNumber)
	var err error
	if ts.rp, err = record.NewRecordPage(ts.tx, block, ts.layout); err != nil {
		return err
	}
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) atLastBlock() (bool, error) {
	fileSize, err := ts.tx.Size(ts.filename)
	if err != nil {
		return false, err
	}
	return ts.rp.Block().GetBlockNumber() == fileSize-1, nil
}
