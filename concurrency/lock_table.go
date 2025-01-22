package concurrency

import (
	"context"
	"errors"
	"justanotherdb/file"
	"sync"
	"time"
)

const MAX_TIME = 10

// LockTable
// map values > 0 for shared locks and -1 for exclusive locks
type LockTable struct {
	locks map[file.BlockId]int
	cond  *sync.Cond
}

func NewLockTable() *LockTable {
	return &LockTable{
		locks: make(map[file.BlockId]int),
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

func (table *LockTable) sLock(block file.BlockId) error {
	table.cond.L.Lock()
	defer table.cond.L.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*MAX_TIME)
	defer cancel()

	expiryFunc := context.AfterFunc(ctx, func() {
		table.cond.L.Lock()
		table.cond.Broadcast()
		table.cond.L.Unlock()
	})

	defer expiryFunc()

	for {
		if !table.hasXLock(block) {
			table.locks[block]++
			return nil
		}

		table.cond.Wait()
		if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
	}
}

func (table *LockTable) xLock(block file.BlockId) error {
	table.cond.L.Lock()
	defer table.cond.L.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*MAX_TIME)
	defer cancel()

	expiryFunc := context.AfterFunc(ctx, func() {
		table.cond.L.Lock()
		table.cond.Broadcast()
		table.cond.L.Unlock()
	})

	defer expiryFunc()

	for {
		if !table.hasOtherSLocks(block) {
			table.locks[block] = -1
			return nil
		}
		table.cond.Wait()
		if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
	}
}

func (table *LockTable) unlock(block file.BlockId) {
	table.cond.L.Lock()
	defer table.cond.L.Unlock()
	locks, _ := table.locks[block]
	if locks > 1 {
		table.locks[block]--
		table.cond.Broadcast()
	} else {
		delete(table.locks, block)
		table.cond.Broadcast()
	}
}

func (table *LockTable) hasOtherSLocks(block file.BlockId) bool {
	locks, ok := table.locks[block]
	if !ok || locks <= 1 {
		return false
	}
	return true
}

func (table *LockTable) hasXLock(block file.BlockId) bool {
	lock, ok := table.locks[block]
	if !ok || lock > 0 {
		return false
	}
	return true
}
