package metadata

import (
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/buffer"
	"jadb/concurrency"
	"jadb/file"
	"jadb/log"
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
	bm, err := buffer.NewBufferManager(fm, lm, 100)
	assert.NoError(err)
	lt := concurrency.NewLockTable()
	err = os.MkdirAll(tempDir, 0755)
	assert.NoError(err)
	env = TestEnv{
		dbFile, logFile, tempDir,
		blockSize,
		fm, lm, bm, lt,
	}
}

func clearEnv(t *testing.T) {
	if err := os.RemoveAll(env.tempDir); err != nil {
		t.Error(err)
	}
}

func TestNewTableManager(t *testing.T) {
	assert := assertPkg.New(t)
	initEnv(assert)
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
	clearEnv(t)
}

func TestCreateNewTable(t *testing.T) {
	assert := assertPkg.New(t)
	initEnv(assert)

	clearEnv(t)

}
