package tx

import (
	"fmt"
	"justanotherdb/constants"
	"justanotherdb/file"
	"justanotherdb/log"
)

type CheckpointRecord struct {
}

func NewCheckpointRecord() *CheckpointRecord {
	return &CheckpointRecord{}
}

func (record *CheckpointRecord) Op() LogRecordType {
	return CHECKPOINT
}

func (record *CheckpointRecord) TxNumber() int {
	return -1
}

func (record *CheckpointRecord) Undo(tx *Transaction) error {
	return nil
}

func (record *CheckpointRecord) ToString() string {
	return fmt.Sprintf("<CHECKPOINT>")
}

func (record *CheckpointRecord) WriteToLog(lm *log.Manager) (int, error) {
	recordTypeOffset := 0
	recordLen := constants.IntSize
	buffer := make([]byte, recordLen)
	page := file.NewPageWithBuffer(buffer)
	page.SetInt(recordTypeOffset, int(CHECKPOINT))
	return lm.Append(page.Contents())
}
