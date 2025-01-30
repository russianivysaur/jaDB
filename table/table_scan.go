package table

import (
	"justanotherdb/file"
	"justanotherdb/record"
	"justanotherdb/scan"
	"justanotherdb/tx"
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

func (ts *TableScan) GetInt(fldname string) (int, error) {
	return ts.rp.GetInt(ts.currentSlot, fldname)
}

func (ts *TableScan) GetString(fldname string) (string, error) {
	return ts.rp.GetString(ts.currentSlot, fldname)
}

func (ts *TableScan) GetVal(fldname string) (any, error) {
	if ts.layout.Schema().Type(fldname) == record.INTEGER {
		return ts.GetInt(fldname)
	} else {
		return ts.GetString(fldname)
	}
}

func (ts *TableScan) HasField(fldname string) bool {
	return ts.layout.Schema().HasField(fldname)
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
