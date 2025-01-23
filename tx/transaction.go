package tx

import (
	"justanotherdb/buffer"
	"justanotherdb/concurrency"
	"justanotherdb/file"
	"justanotherdb/log"
	"sync"
)

var nextTxNum = 1
var txNumLock sync.Mutex

type Transaction struct {
	rm      *RecoveryManager
	fm      *file.Manager
	bm      *buffer.Manager
	txNum   int
	cm      *concurrency.Manager
	buffers *BufferList
}

func NewTransaction(fm *file.Manager, bm *buffer.Manager, lm *log.Manager, lt *concurrency.LockTable) (*Transaction, error) {
	txNumLock.Lock()
	txNum := nextTxNum
	nextTxNum++
	txNumLock.Unlock()
	myBuffers, err := NewBufferList(bm)
	if err != nil {
		return nil, err
	}
	cm, err := concurrency.NewConcurrencyManager(lt)
	if err != nil {
		return nil, err
	}
	tx := &Transaction{
		txNum:   txNum,
		fm:      fm,
		bm:      bm,
		buffers: myBuffers,
		cm:      cm,
	}
	tx.rm, err = NewRecoveryManager(tx, txNum, lm, bm)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (tx *Transaction) Commit() {

}

func (tx *Transaction) Rollback() {}

func (tx *Transaction) Recover() {}

func (tx *Transaction) pin() {}

func (tx *Transaction) unpin() {}

func (tx *Transaction) getInt() {}

func (tx *Transaction) getString() {}

func (tx *Transaction) setInt(block file.BlockId, offset int, newVal int, log bool) error {
	if err := tx.cm.XLock(block); err != nil {
		return err
	}
	buff, err := tx.buffers.getBuffer(block)
	if err != nil {
		return err
	}
	lsn := -1
	if log {
		lsn, err = tx.rm.setInt(buff, offset, newVal)
		if err != nil {
			return err
		}
	}
	page := buff.Contents()
	page.SetInt(offset, newVal)
	buff.SetModified(tx.txNum, lsn)
	return nil
}

func (tx *Transaction) setString() {}
