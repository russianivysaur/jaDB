package tx

import (
	"fmt"
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

func NewTransaction(fm *file.Manager, lm *log.Manager, bm *buffer.Manager, lt *concurrency.LockTable) (*Transaction, error) {
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

func (tx *Transaction) Commit() error {
	if err := tx.rm.commit(); err != nil {
		return err
	}
	tx.cm.Release()
	tx.buffers.unpinAll()
	fmt.Printf("Transaction %d committed", tx.txNum)
	return nil
}

func (tx *Transaction) Rollback() error {
	if err := tx.rm.rollback(); err != nil {
		return err
	}
	tx.cm.Release()
	tx.buffers.unpinAll()
	fmt.Printf("Transaction %d rolledback", tx.txNum)
	return nil
}

func (tx *Transaction) Recover() error {
	if err := tx.bm.FlushAll(tx.txNum); err != nil {
		return err
	}
	if err := tx.rm.recover(); err != nil {
		return err
	}
	return nil
}

func (tx *Transaction) pin(block *file.BlockId) error {
	if err := tx.buffers.pin(*block); err != nil {
		return err
	}
	return nil
}

func (tx *Transaction) unpin(block *file.BlockId) {
	tx.buffers.unpin(*block)
}

func (tx *Transaction) GetInt(block *file.BlockId, offset int) (int, error) {
	if err := tx.cm.SLock(*block); err != nil {
		return -1, err
	}
	buff, err := tx.buffers.getBuffer(*block)
	if err != nil {
		return -1, err
	}
	page := buff.Contents()
	return page.GetInt(offset), nil
}

func (tx *Transaction) GetString(block *file.BlockId, offset int) (string, error) {
	if err := tx.cm.SLock(*block); err != nil {
		return "", err
	}
	buff, err := tx.buffers.getBuffer(*block)
	if err != nil {
		return "", err
	}
	page := buff.Contents()
	return page.GetString(offset), nil
}

func (tx *Transaction) SetInt(block *file.BlockId, offset int, newVal int, log bool) error {
	if err := tx.cm.XLock(*block); err != nil {
		return err
	}
	buff, err := tx.buffers.getBuffer(*block)
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

func (tx *Transaction) SetString(block *file.BlockId, offset int, newVal string, log bool) error {
	if err := tx.cm.XLock(*block); err != nil {
		return err
	}
	buff, err := tx.buffers.getBuffer(*block)
	if err != nil {
		return err
	}
	lsn := -1
	if log {
		if lsn, err = tx.rm.setString(buff, offset, newVal); err != nil {
			return err
		}
	}
	page := buff.Contents()
	if err = page.SetString(offset, newVal); err != nil {
		return err
	}
	buff.SetModified(tx.txNum, lsn)
	return nil
}
