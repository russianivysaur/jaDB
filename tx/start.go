package tx

import (
	"fmt"
	"justanotherdb/constants"
	"justanotherdb/file"
)

type StartRecord struct {
	txNum int
}

func NewStartRecord(page *file.Page) (*StartRecord, error) {
	txNumOffset := constants.IntSize
	txNum := page.GetInt(txNumOffset)
	return &StartRecord{
		txNum,
	}, nil
}

func (record *StartRecord) Op() LogRecordType {
	return START
}

func (record *StartRecord) Undo(tx *Transaction) error {
	//nothing to undo
	return nil
}

func (record *StartRecord) TxNumber() int {
	return record.txNum
}

func (record *StartRecord) String() string {
	return fmt.Sprintf("<START %d>", record.txNum)
}
