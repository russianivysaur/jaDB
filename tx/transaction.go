package tx

import (
	"justanotherdb/buffer"
	"justanotherdb/concurrency"
	"justanotherdb/file"
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

func NewTransaction() (*Transaction, error) {
	txNumLock.Lock()
	txNum := nextTxNum
	nextTxNum++
	txNumLock.Unlock()
	return &Transaction{}, nil
}
