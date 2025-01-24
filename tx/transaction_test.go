package tx

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"justanotherdb/buffer"
	"justanotherdb/concurrency"
	"justanotherdb/file"
	"justanotherdb/log"
	"os"
	"path/filepath"
	"testing"
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

	})

	t.Run("transaction deadlock", func(t *testing.T) {

	})

	if err := os.RemoveAll(testEnv.tempDir); err != nil {
		t.Error(err)
	}
}
