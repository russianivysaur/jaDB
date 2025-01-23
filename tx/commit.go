package tx

import (
	"fmt"
	"justanotherdb/constants"
	"justanotherdb/file"
	"justanotherdb/log"
)

type CommitRecord struct {
	txNum int
}

func NewCommitRecord(txNum int) *CommitRecord {
	return &CommitRecord{
		txNum,
	}
}

func NewCommitRecordFromPage(page *file.Page) (*CommitRecord, error) {
	txNum := page.GetInt(constants.IntSize)
	return &CommitRecord{
		txNum,
	}, nil
}

func (record *CommitRecord) Op() LogRecordType {
	return COMMIT
}

func (record *CommitRecord) TxNumber() int {
	return record.txNum
}

func (record *CommitRecord) Undo(tx *Transaction) error {
	return nil
}

func (record *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", record.txNum)
}

func (record *CommitRecord) WriteToLog(lm *log.Manager) (int, error) {
	recordTypeOffset := 0
	txNumOffset := recordTypeOffset + constants.IntSize
	recordLen := txNumOffset + constants.IntSize
	buffer := make([]byte, recordLen)
	page := file.NewPageWithBuffer(buffer)
	page.SetInt(recordTypeOffset, int(COMMIT))
	page.SetInt(txNumOffset, record.txNum)
	return lm.Append(page.Contents())
}
