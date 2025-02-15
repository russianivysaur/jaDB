package scan

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/buffer"
	"jadb/concurrency"
	"jadb/file"
	"jadb/log"
	"jadb/record"
	"jadb/tx"
	"os"
	"path/filepath"
	"testing"
)

type TestEnv struct {
	fm        *file.Manager
	lm        *log.Manager
	bm        *buffer.Manager
	dbFile    string
	tempDir   string
	logFile   string
	blockSize int
	lt        *concurrency.LockTable
}

var env TestEnv

func initEnv(assert *assertPkg.Assertions) {
	dbFile := "test.db"
	logFile := "test.log"
	blockSize := 200
	tempDir := filepath.Join(os.TempDir(), "temp")
	fm, err := file.NewFileManager(tempDir, blockSize)
	assert.NoError(err)
	lm, err := log.NewLogManager(fm, logFile)
	assert.NoError(err)
	bm, err := buffer.NewBufferManager(fm, lm, 30)
	assert.NoError(err)
	lt := concurrency.NewLockTable()
	env = TestEnv{
		fm, lm, bm, dbFile, tempDir, logFile, blockSize, lt,
	}
}

func clearEnv(t *testing.T) {
	if err := os.RemoveAll(env.tempDir); err != nil {
		t.Error(err)
	}
}

func TestTableScan(t *testing.T) {
	assert := assertPkg.New(t)
	initEnv(assert)

	//table schema
	schema := record.NewSchema()
	id := "id"
	schema.AddIntField(id)
	name := "name"
	schema.AddStringField(name, 5)
	age := "age"
	schema.AddIntField(age)

	//layout for the schema
	layout := record.NewLayout(schema)

	//new Transaction
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	//new table scan
	type Record struct {
		id   int
		name string
		age  int
	}
	recCount := 20
	ts, err := NewTableScan(txn, "test", layout)
	assert.NoError(err)
	for i := 0; i < recCount; i++ {
		err = ts.Insert()
		assert.NoError(err)
		err = ts.SetInt(id, i)
		assert.NoError(err)
		err = ts.SetString(name, fmt.Sprintf("name is %d", i))
		assert.NoError(err)
		err = ts.SetInt(age, i)
		assert.NoError(err)
	}
	err = txn.Commit()
	assert.NoError(err)
	ts.Close()

	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	ts, err = NewTableScan(txn, "test", layout)
	assert.NoError(err)

	// read data back out
	for i := 0; i < recCount; i++ {
		hasNext, err := ts.Next()
		assert.NoError(err)
		assert.Equal(true, hasNext)
		actualId, err := ts.GetInt(id)
		assert.NoError(err)
		assert.Equal(i, actualId)
		actualName, err := ts.GetString(name)
		assert.NoError(err)
		assert.Equal(fmt.Sprintf("name is %d", i), actualName)
		actualAge, err := ts.GetInt(age)
		assert.NoError(err)
		assert.Equal(i, actualAge)
	}

	err = txn.Rollback()
	assert.NoError(err)
	clearEnv(t)
}
