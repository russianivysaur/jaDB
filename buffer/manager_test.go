package buffer

import (
	"context"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/file"
	"jadb/log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type TestEnv struct {
	tempDir         string
	fm              *file.Manager
	lm              *log.Manager
	logFile         string
	databaseFile    string
	bufferPoolCount int
}

func initEnv(t *testing.T) TestEnv {
	bufferPoolCount := 100
	tempDir := filepath.Join(os.TempDir(), "test")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		t.Errorf("cannot create temporary directory: %v", err)
	}
	tempLogFileName := "test.log"
	testDatabaseFileName := "test.db"
	blockSize := 100
	var fm *file.Manager
	var lm *log.Manager
	var err error
	if fm, err = file.NewFileManager(tempDir, blockSize); err != nil {
		t.Error(err)
	}
	for i := 0; i <= bufferPoolCount; i++ {
		if _, err = fm.Append(testDatabaseFileName); err != nil {
			t.Error(err)
		}
	}

	if lm, err = log.NewLogManager(fm, tempLogFileName); err != nil {
		t.Error(err)
	}
	return TestEnv{
		tempDir, fm, lm, tempLogFileName, testDatabaseFileName, bufferPoolCount,
	}
}

func TestBufferManager(t *testing.T) {
	assert := assertPkg.New(t)

	env := initEnv(t)
	t.Run("Available", func(t *testing.T) {
		bm, err := NewBufferManager(env.fm, env.lm, env.bufferPoolCount)
		assert.NoError(err)
		assert.Equalf(bm.Available(), env.bufferPoolCount,
			"expected %d buffer pool, got %d", env.bufferPoolCount, bm.Available)
	})

	t.Run("PinUnpinTest", func(t *testing.T) {
		bm, err := NewBufferManager(env.fm, env.lm, env.bufferPoolCount)
		assert.NoError(err)
		testBlock := file.NewBlock(env.databaseFile, 0)
		buffer, err := bm.Pin(testBlock)
		assert.NoErrorf(err, "could not pin block: %v", err)
		assert.Equalf(buffer.block, testBlock, "expected block %v,found block %v in buffer", testBlock,
			buffer.block)

		assert.Equalf(buffer.pins, 1, "expected %d pins,got %d", 1, buffer.pins)
		assert.Equalf(bm.Available(), env.bufferPoolCount-1, "expected %d free buffers,got %d", env.bufferPoolCount-1,
			bm.Available())

		bm.Unpin(buffer)
		assert.Equalf(buffer.pins, 0, "expected %d pins,got %d", 0, buffer.pins)
		assert.Equalf(bm.Available(), env.bufferPoolCount, "expected %d free buffers,got %d", env.bufferPoolCount,
			bm.Available())

	})

	t.Run("MultiplePinUnpinTest", func(t *testing.T) {
		bm, err := NewBufferManager(env.fm, env.lm, env.bufferPoolCount)
		assert.NoError(err)
		testBlock1 := file.NewBlock(env.databaseFile, 0)
		testBlock2 := file.NewBlock(env.databaseFile, 1)
		//get 2 buffers
		buffer1, err := bm.Pin(testBlock1)
		assert.NoErrorf(err, "could not pin block: %v", err)
		_, err = bm.Pin(testBlock2)
		assert.NoErrorf(err, "could not pin block: %v", err)

		assert.Equalf(bm.Available(), env.bufferPoolCount-2, "expected %d free buffers,got %d", env.bufferPoolCount-1,
			bm.Available())

		//unpin one
		bm.Unpin(buffer1)
		assert.Equalf(bm.Available(), env.bufferPoolCount-1, "expected %d free buffers,got %d", env.bufferPoolCount,
			bm.Available())

		//take another
		buffer3, err := bm.Pin(testBlock1)
		assert.NoErrorf(err, "could not pin block: %v", err)
		//test if buffer 1 is allocated back
		assert.Equalf(buffer1, buffer3, "buffer 1 should be allocated")
	})

	t.Run("BufferTimeoutTest", func(t *testing.T) {
		bm, err := NewBufferManager(env.fm, env.lm, env.bufferPoolCount)
		assert.NoError(err)
		for i := 0; i < env.bufferPoolCount; i++ {
			_, err := bm.Pin(file.NewBlock(env.databaseFile, i))
			assert.Equalf(bm.Available(), env.bufferPoolCount-(i+1),
				"expected %d free buffer, found %d free buffers", env.bufferPoolCount-(i+1), bm.Available())
			assert.NoErrorf(err, "could not pin block %d: %v", i, err)
		}

		//try to pin another block - 100
		_, err = bm.Pin(file.NewBlock(env.databaseFile, env.bufferPoolCount))
		assert.ErrorIs(err, context.DeadlineExceeded, "did not timeout")

	})

	t.Run("ConcurrencyTest", func(t *testing.T) {
		bm, err := NewBufferManager(env.fm, env.lm, env.bufferPoolCount)
		assert.NoError(err)
		// there are total of 100 buffers
		// gonna spin up 99 threads to pin 99 blocks
		// spin up 1 thread to fill the last remaining
		// the main thread has to starve for a slot
		// then make the 100th thread unpin the page just in time for 100th thread to acquire it

		var wg sync.WaitGroup
		wg.Add(env.bufferPoolCount - 1)
		for i := 0; i < env.bufferPoolCount-1; i++ {
			go func(i int) {
				_, err := bm.Pin(file.NewBlock(env.databaseFile, i))
				assert.NoError(err, "could not pin buffer %d: %v", i, err)
				wg.Done()
			}(i)
		}

		wg.Wait()

		assert.Equalf(bm.Available(), 1,
			"expected %d free buffer, found %d free buffers", env.bufferPoolCount-2, bm.Available())

		var wg1 sync.WaitGroup
		wg1.Add(1)
		// the nice routine
		go func(i int) {
			buff, err := bm.Pin(file.NewBlock(env.databaseFile, i))
			assert.NoErrorf(err, "could not pin buffer %d: %v", i, err)
			wg1.Done()
			time.Sleep(time.Second * 5)
			bm.Unpin(buff)
		}(env.bufferPoolCount - 1)

		wg1.Wait()
		// the starving thread
		buffer, err := bm.Pin(file.NewBlock(env.databaseFile, env.bufferPoolCount))
		assert.NoErrorf(err, "OOM: %v", err)
		assert.NotEqualf(buffer, nil, "nil buffer")
	})

	t.Run("TimeOutTest", func(t *testing.T) {
		bm, err := NewBufferManager(env.fm, env.lm, env.bufferPoolCount)
		assert.NoError(err)
		// there are total of 100 buffers
		// gonna spin up 99 threads to pin 99 blocks
		// spin up 1 thread to fill the last remaining
		// the main thread has to starve for a slot
		// then make the 100th thread unpin the page just in time for 100th thread to acquire it

		var wg sync.WaitGroup
		wg.Add(env.bufferPoolCount - 1)
		for i := 0; i < env.bufferPoolCount-1; i++ {
			go func(i int) {
				_, err := bm.Pin(file.NewBlock(env.databaseFile, i))
				assert.NoError(err, "could not pin buffer %d: %v", i, err)
				wg.Done()
			}(i)
		}

		wg.Wait()

		assert.Equalf(bm.Available(), 1,
			"expected %d free buffer, found %d free buffers", env.bufferPoolCount-2, bm.Available())

		var wg1 sync.WaitGroup
		wg1.Add(1)
		// the nice routine
		go func(i int) {
			buff, err := bm.Pin(file.NewBlock(env.databaseFile, i))
			assert.NoErrorf(err, "could not pin buffer %d: %v", i, err)
			wg1.Done()
			time.Sleep(time.Second * 15)
			bm.Unpin(buff)
		}(env.bufferPoolCount - 1)

		wg1.Wait()
		// the starving thread
		_, err = bm.Pin(file.NewBlock(env.databaseFile, env.bufferPoolCount))
		assert.ErrorIsf(err, context.DeadlineExceeded, "OOM: %v", err)
	})

	//temp removal
	if err := os.RemoveAll(env.tempDir); err != nil {
		t.Errorf("could not delete temp folder: %v", err)
	}
}
