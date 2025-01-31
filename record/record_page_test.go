package record

import (
	assertPkg "github.com/stretchr/testify/assert"
	"justanotherdb/buffer"
	"justanotherdb/concurrency"
	"justanotherdb/constants"
	"justanotherdb/file"
	"justanotherdb/log"
	"justanotherdb/tx"
	"os"
	"path/filepath"
	"testing"
)

type TestEnv struct {
	blockSize int
	fm        *file.Manager
	lm        *log.Manager
	bm        *buffer.Manager
	lt        *concurrency.LockTable
	tempDir   string
	dbFile    string
	logFile   string
}

var env TestEnv

func initEnv(assert *assertPkg.Assertions) {
	blockSize := 100
	dbFile := "test.db"
	logFile := "test.log"
	tempDir := filepath.Join(os.TempDir(), "temp")
	fm, err := file.NewFileManager(tempDir, blockSize)
	assert.NoError(err)
	lm, err := log.NewLogManager(fm, logFile)
	assert.NoError(err)
	bufferCount := 10
	bm, err := buffer.NewBufferManager(fm, lm, bufferCount)
	err = os.MkdirAll(tempDir, 0666)
	assert.NoError(err)

	lt := concurrency.NewLockTable()
	assert.NoError(err)
	env = TestEnv{
		blockSize,
		fm, lm, bm, lt, tempDir, dbFile, logFile,
	}
}

func clearEnv(t *testing.T) {
	if err := os.RemoveAll(env.tempDir); err != nil {
		t.Errorf("could not remove temp files %v", err)
	}
}

func TestRecordPage(t *testing.T) {
	assert := assertPkg.New(t)
	initEnv(assert)
	// defining a schema
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 10)
	schema.AddIntField("age")

	// 16+10 = 26
	expectedSlotSize := constants.IntSize*2 + 10

	// layout using the schema
	layout := NewLayout(schema)
	assert.Equal(expectedSlotSize, layout.SlotSize())

	// create a tx
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	// create a dummy block
	block, err := txn.Append(env.dbFile)

	// make a rp
	rp, err := NewRecordPage(txn, block, &layout)
	assert.NoError(err)

	// if we choose blockSize of 100 then 100/26 = 3
	// 3 slots can be fit in a block
	// test for invalid slot
	// slots start from 0
	slot, valid := 3, false
	assert.Equalf(valid, rp.isValidSlot(slot),
		"expected %b for slot %d,got %b", valid, slot, rp.isValidSlot(slot))

	slot, valid = 2, true
	assert.Equalf(valid, rp.isValidSlot(slot),
		"expected %b for slot %d,got %b", valid, slot, rp.isValidSlot(slot))

	slot = 1
	// check if offset returns correct result
	expectedOffset := slot * rp.layout.SlotSize()
	assert.Equalf(expectedOffset, rp.offset(slot), "expected %d offset for slot %d, found %d",
		expectedOffset, slot, rp.offset(slot))

	clearEnv(t)
}
