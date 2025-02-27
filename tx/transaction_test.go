package tx

import (
	"context"
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"jadb/buffer"
	"jadb/concurrency"
	"jadb/file"
	"jadb/log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type TestEnv struct {
	fm        *file.Manager
	lm        *log.Manager
	bm        *buffer.Manager
	blockSize int
	dbFile    string
	logFile   string
	lt        *concurrency.LockTable
	tempDir   string
}

func initEnv(t *testing.T) TestEnv {
	blockSize := 500
	dbFile := "test.db"
	logFile := "test.log"
	tempDir := filepath.Join(os.TempDir(), "test")
	fm, err := file.NewFileManager(tempDir, blockSize)
	require.NoError(t, err)

	lm, err := log.NewLogManager(fm, logFile)
	require.NoError(t, err)
	if err != nil {
		fmt.Println(err)
	}
	bufferCount := 10
	bm, err := buffer.NewBufferManager(fm, lm, bufferCount)
	require.NoError(t, err)
	return TestEnv{
		fm:        fm,
		lm:        lm,
		bm:        bm,
		dbFile:    dbFile,
		logFile:   logFile,
		blockSize: blockSize,
		lt:        concurrency.NewLockTable(),
		tempDir:   tempDir,
	}
}

func TestTransactions(t *testing.T) {
	testEnv := initEnv(t)
	assert := assertPkg.New(t)

	t.Run("single transaction test with commit", func(t *testing.T) {
		// Append a block to db file
		block, err := testEnv.fm.Append(testEnv.dbFile)
		assert.NoErrorf(err, "could not append block: %v", err)
		tx, err := NewTransaction(testEnv.fm, testEnv.lm, testEnv.bm, testEnv.lt)
		assert.NoError(err)
		testOffset := 20
		testVal := 50
		err = tx.SetInt(block, testOffset, testVal, true)
		assert.NoErrorf(err, "could not write int: %v", err)
		actual, err := tx.GetInt(block, testOffset)
		assert.NoError(err)
		assert.Equalf(testVal, actual, "expected %d in tx page,got %d", testVal, actual)
		logIterator, err := testEnv.lm.GetIterator()
		assert.NoError(err)
		assert.True(logIterator.HasNext())
		//get the log record
		recordBytes, err := logIterator.Next()
		assert.NoError(err)

		//check if the log record matches the modification to the block

		logRecord, err := CreateLogRecord(recordBytes)
		assert.NoError(err)
		assert.Equalf(SET_INT, logRecord.Op(), "expected log record type to be %d, got %d", SET_INT, logRecord.Op())
		assert.Equalf(tx.txNum, logRecord.TxNumber(), "expected tx num in log to be %d,got %d", tx.txNum, logRecord.TxNumber())
		assert.Equal(fmt.Sprintf("<SETINT %d %s %d %d %d %d>",
			tx.txNum, block.GetFileName(), block.GetBlockNumber(), testOffset, 0, testVal), logRecord.ToString())
		//write a string to the page
		testStringOffset := 60
		testStringVal := "my name is ankit hehe!"
		err = tx.SetString(block, testStringOffset, testStringVal, true)
		assert.NoError(err)

		//get the latest log record
		logIterator, err = testEnv.lm.GetIterator()
		assert.NoError(err)
		assert.True(logIterator.HasNext())
		recordBytes, err = logIterator.Next()
		logRecord, err = CreateLogRecord(recordBytes)
		assert.NoError(err)

		//check if log record matches string

		assert.Equalf(SET_STRING, logRecord.Op(), "expected log type %d,got %d", SET_STRING, logRecord.Op())
		assert.Equal(fmt.Sprintf("<SETSTRING %d %s %d %d %s %s>",
			tx.txNum, block.GetFileName(), block.GetBlockNumber(), testStringOffset, "", testStringVal), logRecord.ToString())
		assert.Equalf(tx.txNum, logRecord.TxNumber(), "expected log tx number %d, got %d", tx.txNum, logRecord.TxNumber())

		err = tx.Commit()
		assert.NoError(err)

		//check for commit log
		logIterator, err = testEnv.lm.GetIterator()
		assert.NoError(err)
		assert.True(logIterator.HasNext())
		recordBytes, err = logIterator.Next()
		logRecord, err = CreateLogRecord(recordBytes)
		assert.NoError(err)

		assert.Equalf(COMMIT, logRecord.Op(), "expected log type %d,got %d", COMMIT, logRecord.Op())
		assert.Equal(fmt.Sprintf("<COMMIT %d>", tx.txNum), logRecord.ToString())
		assert.Equalf(tx.txNum, logRecord.TxNumber(), "expected log tx number %d, got %d", tx.txNum, logRecord.TxNumber())
	})

	t.Run("transaction with rollback", func(t *testing.T) {
		testIntOffset := 0
		testStringOffset := 40
		block, err := testEnv.fm.Append(testEnv.dbFile)
		assert.NoError(err)
		tx, err := NewTransaction(testEnv.fm, testEnv.lm, testEnv.bm, testEnv.lt)
		assert.NoError(err)
		//Do some stuff
		err = tx.SetInt(block, testIntOffset, 40, true)
		assert.NoError(err)
		err = tx.SetString(block, testStringOffset, "this is a test string", true)
		assert.NoError(err)
		//rollback txn
		err = tx.Rollback()
		assert.NoError(err)

		//check log for rollback record
		iterator, err := testEnv.lm.GetIterator()
		assert.NoError(err)
		assert.True(iterator.HasNext())
		recordBytes, err := iterator.Next()
		assert.NoError(err)
		logRecord, err := CreateLogRecord(recordBytes)
		assert.NoError(err)
		assert.Equalf(ROLLBACK, logRecord.Op(), "expected %d log record type, got %d type", ROLLBACK, logRecord.Op())

		//check if pages have been rolled back
		buff, err := testEnv.bm.Pin(block)
		page := buff.Contents()
		assert.NoError(err)
		assert.Equalf(0, page.GetInt(testIntOffset), "expected %d at offset %d of page, got %d",
			0, testIntOffset, page.GetInt(testIntOffset))
		assert.Equalf("", page.GetString(testStringOffset), "expected %s at offset %s of page, got %s",
			"", testStringOffset, page.GetInt(testStringOffset))
		testEnv.bm.Unpin(buff)
	})

	t.Run("transaction deadlock", func(t *testing.T) {
		responseChannel := make(chan error, 2)

		//append 2 blocks mate
		_, err := testEnv.fm.Append(testEnv.dbFile)
		assert.NoError(err)
		_, err = testEnv.fm.Append(testEnv.dbFile)
		assert.NoError(err)

		var wg sync.WaitGroup
		wg.Add(2)
		//tx 1 acquire block 1 and waits for block 2
		go tx(responseChannel, &wg, testEnv, assert, false)

		//tx 2 acquires block 2 and waits for block 1
		go tx(responseChannel, &wg, testEnv, assert, true)

		//deadlock hehe
		wg.Wait()
		for i := 0; i < 2; i++ {
			response := <-responseChannel
			assert.ErrorIs(response, context.DeadlineExceeded)
		}
	})

	if err := os.RemoveAll(testEnv.tempDir); err != nil {
		t.Error(err)
	}
}

func tx(responseChannel chan<- error,
	wg *sync.WaitGroup,
	testEnv TestEnv,
	assert *assertPkg.Assertions,
	acquireBlock2 bool,
) {
	defer wg.Done()
	tx, err := NewTransaction(testEnv.fm, testEnv.lm, testEnv.bm, testEnv.lt)
	assert.NoError(err)
	block1 := file.NewBlock(testEnv.dbFile, 0)
	block2 := file.NewBlock(testEnv.dbFile, 1)
	//acquire xlock
	var acquirableBlock *file.BlockId
	var nonAcquirableBlock *file.BlockId
	if acquireBlock2 {
		acquirableBlock = block2
		nonAcquirableBlock = block1
	} else {
		nonAcquirableBlock = block2
		acquirableBlock = block1
	}

	err = tx.SetInt(acquirableBlock, 20, 20, true)
	assert.NoError(err)
	time.Sleep(time.Second * 2)

	//acquire XLock on the other block
	// shouldnt be acquirable
	err = tx.SetString(nonAcquirableBlock, 50, "new", true)
	if err != nil {
		err := tx.Rollback()
		assert.NoError(err)
	}
	responseChannel <- err
}

func TestTransactionWriteAndRewrite(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(t)

	txn, err := NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	block, err := txn.Append(env.dbFile)
	assert.NoError(err)

	_, err = txn.GetInt(block, 0)
	assert.NoError(err)
	assert.NoError(txn.SetInt(block, 0, 90, true))
	assert.NoError(txn.Commit())

	txn, err = NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	value, err := txn.GetInt(block, 0)
	assert.NoError(err)
	assert.Equal(value, 90)

	assert.NoError(txn.SetInt(block, 0, 30, true))
	assert.NoError(txn.Commit())

	if err := os.RemoveAll(env.tempDir); err != nil {
		t.Error(err)
	}
}
