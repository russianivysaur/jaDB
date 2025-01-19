package tx

import (
	"fmt"
	"justanotherdb/file"
)

type LogRecordType int

const (
	CHECKPOINT LogRecordType = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

type LogRecord interface {
	Op() LogRecordType
	TxNumber() int
	Undo(*Transaction) error
	String() string
}

func CreateLogRecord(data []byte) (LogRecord, error) {
	page := file.NewPageWithBuffer(data)
	switch LogRecordType(page.GetInt(0)) {
	case CHECKPOINT:
		return NewCheckpointRecord()
	case START:
		return NewStartRecord(page)
	case COMMIT:
		return NewCommitRecord(page)
	case ROLLBACK:
		return NewRollbackRecord(page)
	case SETINT:
		return NewSetIntRecord(page)
	case SETSTRING:
		return NewSetStringRecord(page)
	}
	return nil, fmt.Errorf("unexpected LogRecordType")
}
