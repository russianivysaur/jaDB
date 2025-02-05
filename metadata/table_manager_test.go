package metadata

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
	dbFile    string
	logFile   string
	tempDir   string
	blockSize int
	fm        *file.Manager
	lm        *log.Manager
	bm        *buffer.Manager
	lt        *concurrency.LockTable
}

func initEnv(assert *assertPkg.Assertions) TestEnv {
	blockSize := 4096
	dbFile := "test.db"
	logFile := "test.log"
	tempDir := filepath.Join(os.TempDir(), "temp")
	fm, err := file.NewFileManager(tempDir, blockSize)
	assert.NoError(err)
	lm, err := log.NewLogManager(fm, logFile)
	assert.NoError(err)
	bm, err := buffer.NewBufferManager(fm, lm, 100)
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

func clearEnv(t *testing.T, env TestEnv) {
	if err := os.RemoveAll(env.tempDir); err != nil {
		t.Error(err)
	}
}

// TestNewTableManager : checks tblcat and fldcat entries in tblcat and fldcat itself
func TestNewTableManager(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	tableManager, err := NewTableManager(true, txn)
	assert.NoError(err)
	//check if catalog tables are created
	tableCatalogFilePath := filepath.Join(env.tempDir, "tblcat.tbl")
	fldCatalogFilePath := filepath.Join(env.tempDir, "fldcat.tbl")
	_, err = os.Stat(tableCatalogFilePath)
	assert.NoError(err)
	_, err = os.Stat(fldCatalogFilePath)
	assert.NoError(err)

	// check layouts of the tblcat and fldcat
	tblcatLayout, err := tableManager.getLayout("tblcat", txn)
	assert.NoError(err)
	fldcatLayout, err := tableManager.getLayout("fldcat", txn)
	assert.NoError(err)
	tblcatSchema := tblcatLayout.Schema()
	fldcatSchema := fldcatLayout.Schema()
	tblcatFields := []string{"tblname", "slotsize"}
	assert.Equalf(tblcatFields, tblcatSchema.Fields(), "expected fields %v for %s table, got %v",
		tblcatFields, "tblcat", tblcatSchema.Fields())
	fldcatFields := []string{"fldname", "tblname", "type", "length", "offset"}
	assert.Equalf(fldcatFields, fldcatSchema.Fields(), "expectd fields %v for %s table, got %v",
		fldcatFields, "fldcat", fldcatSchema.Fields())
	err = txn.Commit()
	assert.NoError(err)
	//finish test
	clearEnv(t, env)
}

func TestCreateSingleTable(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	tblMgr, err := NewTableManager(true, txn)
	assert.NoError(err)
	tblName := "testtbl"
	tblSchema := record.NewSchema()
	tblSchema.AddIntField("id")
	tblSchema.AddStringField("name", 10)
	err = tblMgr.createTable(tblName, tblSchema, txn)
	assert.NoError(err)
	assert.NoError(txn.Commit())

	// check if tblcat and fldcat have those fields
	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	tblMgr, err = NewTableManager(false, txn)
	assert.NoError(err)
	layout, err := tblMgr.getLayout(tblName, txn)
	assert.NoError(err)
	schema := layout.Schema()
	assert.True(schema.Equals(tblSchema))
	clearEnv(t, env)
}

func TestCreateMultipleTables(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err := NewTableManager(true, txn)
	assert.NoError(err)

	type Table struct {
		name   string
		schema *record.Schema
	}
	testCount := 100
	columnCount := 3
	tables := make([]Table, testCount)
	for i := 0; i < testCount; i++ {
		schema := record.NewSchema()
		for j := 0; j < columnCount; j++ {
			colName := fmt.Sprintf("col%d%d", j, i)
			schema.AddStringField(colName, 10)
		}
		tables[i] = Table{
			fmt.Sprintf("table%d", i),
			schema,
		}
		err := tableManager.createTable(tables[i].name, schema, txn)
		assert.NoError(err)
	}

	assert.NoError(txn.Commit())

	// new transaction and read tables back in
	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err = NewTableManager(false, txn)
	assert.NoError(err)

	for _, table := range tables {
		layout, err := tableManager.getLayout(table.name, txn)
		assert.NoError(err)
		assert.True(layout.Schema().Equals(table.schema))
	}

	clearEnv(t, env)
}
