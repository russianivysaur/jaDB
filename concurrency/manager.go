package concurrency

import "jadb/file"

type Manager struct {
	lockTable *LockTable
	locks     map[file.BlockId]byte
}

func NewConcurrencyManager(lockTable *LockTable) (*Manager, error) {
	return &Manager{
		lockTable,
		make(map[file.BlockId]byte),
	}, nil
}

func (manager *Manager) SLock(block file.BlockId) error {
	if _, ok := manager.locks[block]; ok {
		return nil
	}
	if err := manager.lockTable.sLock(block); err != nil {
		return err
	}
	manager.locks[block] = 'S'
	return nil
}

func (manager *Manager) XLock(block file.BlockId) error {
	if manager.hasXLock(block) {
		return nil
	}
	if err := manager.SLock(block); err != nil {
		return err
	}
	if err := manager.lockTable.xLock(block); err != nil {
		return err
	}
	manager.locks[block] = 'X'
	return nil
}

func (manager *Manager) Release() {
	for blk, _ := range manager.locks {
		manager.lockTable.unlock(blk)
	}
	manager.locks = make(map[file.BlockId]byte)
}

func (manager *Manager) hasSLock(block file.BlockId) bool {
	return manager.locks[block] == 'S'
}

func (manager *Manager) hasXLock(block file.BlockId) bool {
	return manager.locks[block] == 'X'
}
