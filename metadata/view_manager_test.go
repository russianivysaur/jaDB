package metadata

import (
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

func initViewEnv(assert *assertPkg.Assertions) TestEnv {
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

func TestViewManager(t *testing.T) {
	assert := assertPkg.New(t)
	env := initViewEnv(assert)
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	tblMgr, err := NewTableManager(true, txn)
	assert.NoError(err)
	_, err = NewViewManager(true, tblMgr, txn)
	assert.NoError(err)

	viewCatLayout, err := tblMgr.getLayout("viewcat", txn)
	assert.NoError(err)
	schema := record.NewSchema()
	schema.AddStringField("viewname", MAX_NAME)
	schema.AddStringField("viewdef", MAX_VIEW_DEF)
	assert.True(viewCatLayout.Schema().Equals(schema))
	assert.NoError(txn.Commit())
	clearEnv(t, env)
}

func TestAddNewView(t *testing.T) {
	assert := assertPkg.New(t)
	env := initViewEnv(assert)
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	tblMgr, err := NewTableManager(true, txn)
	assert.NoError(err)
	viewMgr, err := NewViewManager(true, tblMgr, txn)
	assert.NoError(err)

	viewCatLayout, err := tblMgr.getLayout("viewcat", txn)
	assert.NoError(err)
	schema := record.NewSchema()
	schema.AddStringField("viewname", MAX_NAME)
	schema.AddStringField("viewdef", MAX_VIEW_DEF)
	assert.True(viewCatLayout.Schema().Equals(schema))

	testViewName := "testview"
	testViewDef := "this is the definition!!!!"
	err = viewMgr.createView(testViewName, testViewDef, txn)
	assert.NoError(err)

	viewCatPath := filepath.Join(env.tempDir, "viewcat.tbl")
	_, err = os.Stat(viewCatPath)
	assert.NoError(err)

	viewDef, err := viewMgr.getViewDef(testViewName, txn)
	assert.NoError(err)
	assert.Equal(testViewDef, viewDef)
	assert.NoError(txn.Commit())

	clearEnv(t, env)
}
