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

func TestAddMultipleViews(t *testing.T) {
	assert := assertPkg.New(t)
	env := initViewEnv(assert)
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err := NewTableManager(true, txn)
	assert.NoError(err)

	viewManager, err := NewViewManager(true, tableManager, txn)
	assert.NoError(err)
	type View struct {
		name string
		def  string
	}

	testCount := 100
	views := make([]View, testCount)
	for i := 0; i < testCount; i++ {
		views[i] = View{
			fmt.Sprintf("view %d", i),
			fmt.Sprintf("view def %d", i),
		}
		err = viewManager.createView(views[i].name, views[i].def, txn)
		assert.NoError(err)
	}

	//commit txn
	assert.NoError(txn.Commit())

	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	// read back views in view cat
	tableManager, err = NewTableManager(false, txn)
	assert.NoError(err)
	viewManager, err = NewViewManager(false, tableManager, txn)
	assert.NoError(err)
	for i := 0; i < testCount; i++ {
		viewDef, err := viewManager.getViewDef(views[i].name, txn)
		assert.NoError(err)
		assert.Equal(views[i].def, viewDef)
	}

	assert.NoError(txn.Rollback())

	clearEnv(t, env)
}
