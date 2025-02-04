package tx

import (
	"fmt"
	"jadb/constants"
	"jadb/file"
	"jadb/log"
)

type StartRecord struct {
	txNum int
}

func NewStartRecord(txNum int) (*StartRecord, error) {
	return &StartRecord{
		txNum,
	}, nil
}

func NewStartRecordFromPage(page *file.Page) (*StartRecord, error) {
	txNum := page.GetInt(constants.IntSize)
	return &StartRecord{
		txNum,
	}, nil
}

func (record *StartRecord) Op() LogRecordType {
	return START
}

func (record *StartRecord) TxNumber() int {
	return record.txNum
}

func (record *StartRecord) Undo(tx *Transaction) error {
	return nil
}

func (record *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d>", record.txNum)
}

func (record *StartRecord) WriteToLog(lm *log.Manager) (int, error) {
	recordTypeOffset := 0
	txNumOffset := recordTypeOffset + constants.IntSize
	recordLen := txNumOffset + constants.IntSize
	buffer := make([]byte, recordLen)
	page := file.NewPageWithBuffer(buffer)
	page.SetInt(recordTypeOffset, int(START))
	page.SetInt(txNumOffset, record.txNum)
	return lm.Append(page.Contents())
}
