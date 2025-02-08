package concurrency

import (
	"context"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/file"
	"sync"
	"testing"
	"time"
)

type TestEnv struct {
	testBlocks []file.BlockId
}

func initEnv() TestEnv {
	blockCount := 100
	blocks := make([]file.BlockId, blockCount)
	for i := range blockCount {
		blocks[i] = *file.NewBlock("test", i)
	}
	return TestEnv{
		blocks,
	}
}

func TestConcurrencyManager(t *testing.T) {
	assert := assertPkg.New(t)
	testEnv := initEnv()
	t.Run("shared lock single block", func(t *testing.T) {
		lt := NewLockTable()
		cm, err := NewConcurrencyManager(lt)
		assert.NoError(err)
		block := testEnv.testBlocks[0]
		err = cm.SLock(block)
		assert.NoError(err)
		assert.Equalf(byte('S'), cm.locks[block], "expected lock flag %c, got %c", 'S', cm.locks[block])
		assert.Equalf(1, lt.locks[block], "expected lock count %d in lock table, got %d", 1, lt.locks[block])
	})

	t.Run("exclusive lock single block", func(t *testing.T) {
		lt := NewLockTable()
		cm, err := NewConcurrencyManager(lt)
		assert.NoError(err)
		testBlock := testEnv.testBlocks[1]
		err = cm.XLock(testBlock)
		assert.NoError(err)
		assert.Equalf(byte('X'), cm.locks[testBlock], "expected lock flag %c,got %c", 'X', cm.locks[testBlock])
		assert.Equalf(-1, lt.locks[testBlock], "expected lock count %d in lock table,got %d", -1, lt.locks[testBlock])
	})

	t.Run("concurrent shared locks", func(t *testing.T) {
		lt := NewLockTable()
		numGoroutines := 10
		block := testEnv.testBlocks[0]
		var wg sync.WaitGroup
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(i int) {
				cm, err := NewConcurrencyManager(lt)
				assert.NoError(err)
				err = cm.SLock(block)
				assert.NoError(err)
				wg.Done()
			}(i)
		}

		wg.Wait()
		assert.Equalf(numGoroutines, lt.locks[block], "expected shared lock count %d, got %d", numGoroutines, lt.locks[block])
	})

	t.Run("timing out while trying for xlock while a slock is taken", func(t *testing.T) {
		lt := NewLockTable()
		block := testEnv.testBlocks[0]
		var wg sync.WaitGroup
		wg.Add(1)
		//goroutine 1
		go func() {
			cm, err := NewConcurrencyManager(lt)
			assert.NoError(err)
			err = cm.SLock(block)
			assert.NoError(err)
			wg.Done()
		}()
		wg.Wait()

		var wg1 sync.WaitGroup

		wg1.Add(1)

		assert.Equal(1, lt.locks[block])

		go func() {
			cm, err := NewConcurrencyManager(lt)
			assert.NoError(err)
			err = cm.XLock(block)
			assert.ErrorIs(err, context.DeadlineExceeded)
			wg1.Done()
		}()

		wg1.Wait()
	})

	t.Run("acquiring XLock when all SLock released", func(t *testing.T) {
		lt := NewLockTable()
		block := testEnv.testBlocks[0]
		sLockGoroutines := 2
		var wg sync.WaitGroup
		wg.Add(sLockGoroutines)
		for i := 0; i < sLockGoroutines; i++ {
			go func() {
				cm, err := NewConcurrencyManager(lt)
				assert.NoError(err)
				err = cm.SLock(block)
				wg.Done()
				time.Sleep(time.Second * 5)
				cm.Release()
			}()
		}
		wg.Wait()

		assert.Equal(sLockGoroutines, lt.locks[block])

		var wg1 sync.WaitGroup
		wg1.Add(1)
		// Goroutine for xLock
		go func() {
			cm, err := NewConcurrencyManager(lt)
			assert.NoError(err)
			err = cm.XLock(block)
			assert.NoError(err)
			wg1.Done()
		}()
		wg1.Wait()
		assert.Equal(-1, lt.locks[block])
	})

}
