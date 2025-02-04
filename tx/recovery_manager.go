package tx

import (
	"jadb/buffer"
	"jadb/log"
)

type RecoveryManager struct {
	tx    *Transaction
	txNum int
	lm    *log.Manager
	bm    *buffer.Manager
}

func NewRecoveryManager(tx *Transaction, txNum int, lm *log.Manager, bm *buffer.Manager) (*RecoveryManager, error) {
	return &RecoveryManager{
		tx,
		txNum,
		lm, bm,
	}, nil
}

func (rm *RecoveryManager) commit() error {
	if err := rm.bm.FlushAll(rm.txNum); err != nil {
		return err
	}
	lsn, err := NewCommitRecord(rm.txNum).WriteToLog(rm.lm)
	if err != nil {
		return err
	}
	if err = rm.lm.Flush(lsn); err != nil {
		return err
	}
	return nil
}

func (rm *RecoveryManager) rollback() error {
	if err := rm.doRollback(); err != nil {
		return err
	}
	if err := rm.bm.FlushAll(rm.txNum); err != nil {
		return err
	}

	lsn, err := NewRollbackRecord(rm.txNum).WriteToLog(rm.lm)
	if err = rm.lm.Flush(lsn); err != nil {
		return err
	}
	return nil
}

func (rm *RecoveryManager) doRollback() error {
	iterator, err := rm.lm.GetIterator()
	if err != nil {
		return err
	}
	for iterator.HasNext() {
		record, err := iterator.Next()
		if err != nil {
			return err
		}
		logRecord, err := CreateLogRecord(record)
		if err != nil {
			return err
		}
		if logRecord.TxNumber() == rm.txNum {
			if logRecord.Op() == START {
				return nil
			}
			if err := logRecord.Undo(rm.tx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rm *RecoveryManager) recover() error {
	if err := rm.doRecover(); err != nil {
		return err
	}

	if err := rm.bm.FlushAll(rm.txNum); err != nil {
		return err
	}
	lsn, err := NewCheckpointRecord().WriteToLog(rm.lm)
	if err != nil {
		return err
	}
	if err := rm.lm.Flush(lsn); err != nil {
		return err
	}
	return nil
}

func (rm *RecoveryManager) doRecover() error {
	finishedTxns := make(map[int]bool)
	iterator, err := rm.lm.GetIterator()
	if err != nil {
		return err
	}
	for iterator.HasNext() {
		recordBytes, err := iterator.Next()
		if err != nil {
			return err
		}
		logRecord, err := CreateLogRecord(recordBytes)
		if err != nil {
			return err
		}
		if logRecord.Op() == CHECKPOINT {
			return nil
		}
		if logRecord.Op() == COMMIT || logRecord.Op() == ROLLBACK {
			finishedTxns[logRecord.TxNumber()] = true
		} else if _, ok := finishedTxns[logRecord.TxNumber()]; !ok {
			if err := logRecord.Undo(rm.tx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rm *RecoveryManager) setInt(buff *buffer.Buffer, offset int, newVal int) (int, error) {
	oldVal := buff.Contents().GetInt(offset)
	logRecord := NewSetIntRecord(rm.txNum, buff.Block(), offset, oldVal, newVal)
	return logRecord.WriteToLog(rm.lm)
}

func (rm *RecoveryManager) setString(buff *buffer.Buffer, offset int, newVal string) (int, error) {
	oldVal := buff.Contents().GetString(offset)
	logRecord := NewSetStringRecord(rm.txNum, buff.Block(), offset, oldVal, newVal)
	return logRecord.WriteToLog(rm.lm)
}
