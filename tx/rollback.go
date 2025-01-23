package tx

import (
	"fmt"
	"justanotherdb/constants"
	"justanotherdb/file"
	"justanotherdb/log"
)

type RollbackRecord struct {
	txNum int
}

func NewRollbackRecord(txNum int) *RollbackRecord {
	return &RollbackRecord{
		txNum,
	}
}

func NewRollbackRecordFromPage(page *file.Page) (*RollbackRecord, error) {
	txNum := page.GetInt(constants.IntSize)
	return &RollbackRecord{
		txNum,
	}, nil
}

func (record *RollbackRecord) Op() LogRecordType {
	return ROLLBACK
}

func (record *RollbackRecord) TxNumber() int {
	return record.txNum
}

func (record *RollbackRecord) Undo(tx *Transaction) error {
	return nil
}

func (record *RollbackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", record.txNum)
}

func (record *RollbackRecord) WriteToLog(lm *log.Manager) (int, error) {
	recordTypeOffset := 0
	txNumOffset := recordTypeOffset + constants.IntSize
	recordLen := txNumOffset + constants.IntSize
	page := file.NewPageWithBuffer(make([]byte, recordLen))
	page.SetInt(recordTypeOffset, int(ROLLBACK))
	page.SetInt(txNumOffset, record.txNum)
	return lm.Append(page.Contents())
}
