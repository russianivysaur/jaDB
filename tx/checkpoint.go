package tx

import "fmt"

type CheckpointRecord struct {
}

func NewCheckpointRecord() (*CheckpointRecord, error) {
	return &CheckpointRecord{}, nil
}

func (record *CheckpointRecord) Op() LogRecordType {
	return CHECKPOINT
}

func (record *CheckpointRecord) Undo(tx *Transaction) error {
	//nothing to undo
	return nil
}

func (record *CheckpointRecord) TxNumber() int {
	//no transaction
	return -1
}

func (record *CheckpointRecord) String() string {
	return fmt.Sprintf("<Checkpoint>")
}

func (record *CheckpointRecord) WriteToLog() {

}
