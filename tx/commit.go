package tx

import (
	"fmt"
	"justanotherdb/constants"
	"justanotherdb/file"
)

type CommitRecord struct {
	txNum int
}

func NewCommitRecord(page *file.Page) (*CommitRecord, error) {
	txOffset := constants.IntSize
	txNum := page.GetInt(txOffset)
	return &CommitRecord{
		txNum,
	}, nil
}

func (record *CommitRecord) Op() LogRecordType {
	return COMMIT
}

func (record *CommitRecord) Undo(tx *Transaction) error {
	//nothing to undo
	return nil
}

func (record *CommitRecord) TxNumber() int {
	return record.txNum
}

func (record *CommitRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", record.txNum)
}
