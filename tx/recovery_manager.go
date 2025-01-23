package tx

import (
	"justanotherdb/buffer"
	"justanotherdb/log"
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

func (rm *RecoveryManager) commit() {

}

func (rm *RecoveryManager) rollback() {

}

func (rm *RecoveryManager) recover() {

}

func (rm *RecoveryManager) setInt(buff *buffer.Buffer, offset int, newVal int) (int, error) {
	oldVal := buff.Contents().GetInt(offset)
	logRecord := NewSetIntRecord(rm.txNum, buff.Block(), offset, oldVal, newVal)
	return logRecord.WriteToLog(rm.lm)
}

func (rm *RecoveryManager) setString(buff *buffer.Buffer, offset int, newVal string) {

}
