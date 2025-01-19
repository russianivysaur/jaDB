package tx

import (
	"fmt"
	"justanotherdb/constants"
	"justanotherdb/file"
)

type RollbackRecord struct {
	txNum int
}

func NewRollbackRecord(page *file.Page) (*RollbackRecord, error) {
	txOffset := constants.IntSize
	txNum := page.GetInt(txOffset)
	return &RollbackRecord{
		txNum,
	}, nil
}

func (record *RollbackRecord) Op() LogRecordType {
	return ROLLBACK
}

func (record *RollbackRecord) Undo(tx *Transaction) error {
	//nothing to undo
	return nil
}

func (record *RollbackRecord) TxNumber() int {
	return record.txNum
}

func (record *RollbackRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", record.txNum)
}
