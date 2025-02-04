package record

import (
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/buffer"
	"jadb/concurrency"
	"jadb/constants"
	"jadb/file"
	"jadb/log"
	"jadb/tx"
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
	blockSize := 200
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
	id := "id"
	name := "name"
	age := "age"
	schema := NewSchema()
	schema.AddIntField(id)
	schema.AddStringField(name, 5)
	schema.AddIntField(age)

	// 16+10 = 26
	expectedSlotSize := constants.IntSize*2 + file.MaxLength(5) + constants.IntSize

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
	slot, valid := 10, false
	assert.Equalf(valid, rp.isValidSlot(slot),
		"expected %b for slot %d,got %b", valid, slot, rp.isValidSlot(slot))

	slot, valid = 1, true
	assert.Equalf(valid, rp.isValidSlot(slot),
		"expected %b for slot %d,got %b", valid, slot, rp.isValidSlot(slot))

	slot = 1
	// check if offset returns correct result
	expectedOffset := slot * rp.layout.SlotSize()
	assert.Equalf(expectedOffset, rp.offset(slot), "expected %d offset for slot %d, found %d",
		expectedOffset, slot, rp.offset(slot))

	type Record struct {
		id   int
		name string
		age  int
	}
	record := Record{1, "ankit", 22}

	// insert into record at slot 0
	err = rp.SetInt(0, id, record.id)
	assert.NoError(err)
	err = rp.SetString(0, name, record.name)
	assert.NoError(err)
	err = rp.SetInt(0, age, record.age)
	assert.NoError(err)
	err = rp.setFlag(0, USED)
	assert.NoError(err)

	//commit transaction
	err = txn.Commit()
	assert.NoError(err)

	// New Transaction for reading
	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	//make the rp
	rp, err = NewRecordPage(txn, block, &layout)
	assert.NoError(err)
	actualId, err := rp.GetInt(0, id)
	assert.Equal(record.id, actualId)
	actualName, err := rp.GetString(0, name)
	assert.Equal(record.name, actualName)
	actualAge, err := rp.GetInt(0, age)
	assert.Equal(record.age, actualAge)
	err = txn.Rollback()
	assert.NoError(err)

	clearEnv(t)
}
