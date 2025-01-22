package tx

import (
	"fmt"
	"justanotherdb/file"
	"justanotherdb/log"
)

type LogRecordType int

const (
	CHECKPOINT LogRecordType = iota
	START
	COMMIT
	ROLLBACK
	SET_INT
	SET_STRING
)

type LogRecord interface {
	Op() LogRecordType
	TxNumber() int
	Undo(*Transaction) error
	ToString() string
	WriteToLog(*log.Manager) (int, error)
}

func CreateLogRecord(data []byte) (LogRecord, error) {
	page := file.NewPageWithBuffer(data)
	switch LogRecordType(page.GetInt(0)) {
	//case CHECKPOINT:
	//	return NewCheckpointRecord()
	//case START:
	//	return NewStartRecord(page)
	//case COMMIT:
	//	return NewCommitRecord(page)
	//case ROLLBACK:
	//	return NewRollbackRecord(page)
	case SET_INT:
		return NewSetIntRecord(page)
	case SET_STRING:
		return NewSetStringRecord(page)
	}
	return nil, fmt.Errorf("unexpected LogRecordType")
}
