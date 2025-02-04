package tx

import (
	"fmt"
	"jadb/file"
	"jadb/log"
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
	case CHECKPOINT:
		return NewCheckpointRecord(), nil
	case START:
		return NewStartRecordFromPage(page)
	case COMMIT:
		return NewCommitRecordFromPage(page)
	case ROLLBACK:
		return NewRollbackRecordFromPage(page)
	case SET_INT:
		return NewSetIntRecordFromPage(page)
	case SET_STRING:
		return NewSetStringRecordFromPage(page)
	}
	return nil, fmt.Errorf("unexpected LogRecordType")
}
