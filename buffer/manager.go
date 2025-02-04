package buffer

import (
	"context"
	"errors"
	"jadb/file"
	"jadb/log"
	"sync"
	"time"
)

// MAX_TIME max wait time for buffer to be free
const MAX_TIME = 10

type Manager struct {
	bufferPool  []*Buffer
	available   int
	lock        sync.Mutex
	conditional *sync.Cond
}

func NewBufferManager(fm *file.Manager, lm *log.Manager, bufferPoolCount int) (*Manager, error) {
	pool := make([]*Buffer, bufferPoolCount)
	for i, _ := range pool {
		pool[i] = NewBuffer(fm, lm)
	}
	manager := &Manager{
		bufferPool: pool,
		available:  len(pool),
	}
	cond := sync.NewCond(&manager.lock)
	manager.conditional = cond
	return manager, nil
}

func (manager *Manager) Pin(block *file.BlockId) (*Buffer, error) {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	//magic
	timedContext, cancel := context.WithTimeout(context.Background(), time.Second*MAX_TIME)
	defer cancel()

	//timed out
	timeout := context.AfterFunc(timedContext, func() {
		//beautiful stuff right here
		manager.conditional.L.Lock()
		manager.conditional.Broadcast()
		manager.conditional.L.Unlock()
	})
	defer timeout()
	var err error
	var buff *Buffer
	for {
		if buff, err = manager.tryToPin(block); buff != nil {
			return buff, nil
		}

		if err != nil {
			return nil, err
		}

		manager.conditional.Wait()
		if timedContext.Err() != nil && errors.Is(timedContext.Err(), context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}
	}
}

func (manager *Manager) Unpin(buffer *Buffer) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	buffer.unpin()
	if !buffer.isPinned() {
		manager.available++
		manager.conditional.Broadcast()
	}
}

func (manager *Manager) Available() int {
	return manager.available
}

func (manager *Manager) FlushAll(txNum int) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	for _, buffer := range manager.bufferPool {
		if buffer.modifyingTx() == txNum {
			if err := buffer.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (manager *Manager) tryToPin(block *file.BlockId) (*Buffer, error) {
	buffer := manager.findExistingBuffer(block)
	if buffer != nil {
		return buffer, nil
	}
	for _, buffer := range manager.bufferPool {
		if !buffer.isPinned() {
			if err := buffer.assignToBlock(block); err != nil {
				return nil, err
			}
			manager.available--
			buffer.pin()
			return buffer, nil
		}
	}
	return nil, nil
}

func (manager *Manager) findExistingBuffer(block *file.BlockId) *Buffer {
	for _, buffer := range manager.bufferPool {
		if buffer.block != nil && buffer.block.Equals(block) && buffer.isPinned() {
			return buffer
		}
	}
	return nil
}
