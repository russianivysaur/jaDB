package metadata

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/buffer"
	"jadb/concurrency"
	"jadb/file"
	"jadb/log"
	"jadb/record"
	"jadb/scan_types"
	"jadb/tx"
	"os"
	"path/filepath"
	"testing"
)

func initStatEnv(assert *assertPkg.Assertions) TestEnv {
	blockSize := 4096
	dbFile := "test.db"
	logFile := "test.log"
	tempDir := filepath.Join(os.TempDir(), "temp")
	fm, err := file.NewFileManager(tempDir, blockSize)
	assert.NoError(err)
	lm, err := log.NewLogManager(fm, logFile)
	assert.NoError(err)
	bm, err := buffer.NewBufferManager(fm, lm, 200)
	assert.NoError(err)
	lt := concurrency.NewLockTable()
	err = os.MkdirAll(tempDir, 0755)
	assert.NoError(err)
	return TestEnv{
		dbFile, logFile, tempDir,
		blockSize,
		fm, lm, bm, lt,
	}
}

func TestStatManager(t *testing.T) {
	assert := assertPkg.New(t)
	env := initStatEnv(assert)

	// Make a table, add a few records in, check statinfo

	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err := NewTableManager(true, txn)
	assert.NoError(err)

	testTableName := "test_table"
	testTableSchema := record.NewSchema()
	testTableSchema.AddIntField("id")
	testTableSchema.AddStringField("name", 5)
	testTableSchema.AddIntField("age")
	err = tableManager.createTable(testTableName, testTableSchema, txn)
	assert.NoError(err)

	testTableLayout := record.NewLayout(testTableSchema)
	ts, err := scan_types.NewTableScan(txn, testTableName, testTableLayout)
	assert.NoError(err)

	testRecordCount := 1000
	requiredBytes := testTableLayout.SlotSize() * testRecordCount
	requiredBlocks := int(requiredBytes / env.blockSize)
	if requiredBytes%env.blockSize > 0 {
		requiredBlocks++
	}
	for i := 0; i < testRecordCount; i++ {
		err = ts.Insert()
		assert.NoError(err)
		assert.NoError(ts.SetInt("id", i+1))
		assert.NoError(ts.SetString("name", fmt.Sprintf("name%d", i+1)))
		assert.NoError(ts.SetInt("age", i*2))
	}
	ts.Close()
	assert.NoError(txn.Commit())

	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err = NewTableManager(false, txn)
	assert.NoError(err)

	statsManager, err := NewStatManager(tableManager, txn)
	assert.NoError(err)

	testTableStats, err := statsManager.getStatInfo(testTableName, testTableLayout, txn)
	assert.NoError(err)

	assert.Equal(testTableStats.numRecs, testRecordCount)
	assert.Equal(testTableStats.numBlocks, requiredBlocks)

	clearEnv(t, env)
}

func TestStatsUpdateAfter100Calls(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)

	// Make a table, add a few records in, check statinfo

	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err := NewTableManager(true, txn)
	assert.NoError(err)

	testTableName := "test_table"
	testTableSchema := record.NewSchema()
	testTableSchema.AddIntField("id")
	testTableSchema.AddStringField("name", 5)
	testTableSchema.AddIntField("age")
	err = tableManager.createTable(testTableName, testTableSchema, txn)
	assert.NoError(err)

	testTableLayout := record.NewLayout(testTableSchema)
	ts, err := scan_types.NewTableScan(txn, testTableName, testTableLayout)
	assert.NoError(err)

	testRecordCount := 100
	requiredBytes := testTableLayout.SlotSize() * testRecordCount
	requiredBlocks := int(requiredBytes / env.blockSize)
	if requiredBytes%env.blockSize > 0 {
		requiredBlocks++
	}
	for i := 0; i < testRecordCount; i++ {
		err = ts.Insert()
		assert.NoError(err)
		assert.NoError(ts.SetInt("id", i+1))
		assert.NoError(ts.SetString("name", fmt.Sprintf("name%d", i+1)))
		assert.NoError(ts.SetInt("age", i*2))
	}
	ts.Close()
	assert.NoError(txn.Commit())

	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err = NewTableManager(false, txn)
	assert.NoError(err)

	statsManager, err := NewStatManager(tableManager, txn)
	assert.NoError(err)

	ts, err = scan_types.NewTableScan(txn, testTableName, testTableLayout)
	assert.NoError(err)
	// add more records in the table

	extraRecordsCount := 300
	extraBytes := extraRecordsCount * testTableLayout.SlotSize()
	extraBlocks := extraBytes / env.blockSize
	if extraBytes%env.blockSize != 0 {
		extraBlocks++
	}
	for i := 0; i < extraRecordsCount; i++ {
		assert.NoError(ts.Insert())
		assert.NoError(ts.SetInt("id", i+1))
		assert.NoError(ts.SetString("name", fmt.Sprintf("name%d", i+1)))
		assert.NoError(ts.SetInt("age", i+1))
	}
	ts.Close()

	//check if stats manager shows old stats
	//100 calls
	var testTableStats *StatInfo
	for i := 0; i < 100; i++ {
		testTableStats, err := statsManager.getStatInfo(testTableName, testTableLayout, txn)
		assert.NoError(err)

		assert.Equal(testTableStats.numRecs, testRecordCount)
		assert.Equal(testTableStats.numBlocks, requiredBlocks)
	}

	//should refresh stats on 101th call
	testTableStats, err = statsManager.getStatInfo(testTableName, testTableLayout, txn)
	assert.NoError(err)

	assert.Equal(testTableStats.numRecs, testRecordCount+extraRecordsCount)
	assert.Equal(testTableStats.numBlocks, requiredBlocks+extraBlocks)
	assert.NoError(txn.Commit())

	clearEnv(t, env)
}
