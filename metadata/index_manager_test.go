package metadata

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/record"
	"jadb/scan"
	"jadb/tx"
	"testing"
)

func TestIndexManager(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)

	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err := NewTableManager(true, txn)
	assert.NoError(err)

	statManager, err := NewStatManager(tableManager, txn)
	assert.NoError(err)

	indexManager, err := NewIndexManager(true, tableManager, statManager, txn)
	assert.NoError(err)

	testTableName := "test_table"
	testTableSchema := record.NewSchema()
	testTableSchema.AddIntField("id")
	testTableSchema.AddStringField("name", 5)
	testTableSchema.AddIntField("age")
	indexFieldName := "name"
	testIndexName := "test_index"

	//create table
	assert.NoError(tableManager.createTable(testTableName, testTableSchema, txn))

	//create the index
	assert.NoError(indexManager.createIndex(testIndexName, testTableName, indexFieldName, txn))
	assert.NoError(txn.Commit())

	//check if the index catalog has the index entry
	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	tableManager, err = NewTableManager(false, txn)
	assert.NoError(err)
	statManager, err = NewStatManager(tableManager, txn)
	assert.NoError(err)
	indexManager, err = NewIndexManager(false, tableManager, statManager, txn)
	assert.NoError(err)
	indexesInfo, err := indexManager.getIndexInfo(testTableName, txn)
	assert.NoError(err)

	assert.Equal(1, len(indexesInfo))
	indexInfo, exists := indexesInfo[indexFieldName]
	assert.True(exists)
	assert.Equal(indexFieldName, indexInfo.fldName)
	assert.Equal(testIndexName, indexInfo.idxName)
	assert.True(testTableSchema.Equals(indexInfo.tableSchema))

	clearEnv(t, env)
}

func TestIndexTraversal(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)
	// create table, create index, add entries into table and index
	// find entries using index
	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err := NewTableManager(true, txn)
	assert.NoError(err)

	statManager, err := NewStatManager(tableManager, txn)
	assert.NoError(err)

	indexManager, err := NewIndexManager(true, tableManager, statManager, txn)
	assert.NoError(err)

	testTableName := "test_table"
	testTableSchema := record.NewSchema()
	testTableSchema.AddIntField("id")
	testTableSchema.AddStringField("name", 5)
	testTableSchema.AddIntField("age")
	assert.NoError(tableManager.createTable(testTableName, testTableSchema, txn))

	testIndexName := "test_index"
	//create index
	assert.NoError(indexManager.createIndex(testIndexName, testTableName, "name", txn))
	indexInfos, err := indexManager.getIndexInfo(testTableName, txn)
	indexInfo, ok := indexInfos["name"]
	assert.True(ok)
	index := indexInfo.Open()

	testRecordCount := 1000
	testMap := make(map[string]record.RID)
	ts, err := scan.NewTableScan(txn, testTableName, record.NewLayout(testTableSchema))
	assert.NoError(err)
	for i := 0; i < testRecordCount; i++ {
		name := fmt.Sprintf("n%d", i)
		assert.NoError(ts.Insert())
		assert.NoError(ts.SetInt("id", i))
		assert.NoError(ts.SetString("name", name))
		assert.NoError(ts.SetInt("age", i))
		assert.NoError(index.Insert(name, ts.GetRid()))
		testMap[name] = *ts.GetRid()
	}
	ts.Close()
	assert.NoError(txn.Commit())

	// now read out the index and check if rid in the test map matches the extracted rid
	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)

	tableManager, err = NewTableManager(false, txn)
	assert.NoError(err)

	statManager, err = NewStatManager(tableManager, txn)
	assert.NoError(err)

	indexManager, err = NewIndexManager(true, tableManager, statManager, txn)
	assert.NoError(err)

	indexInfos, err = indexManager.getIndexInfo(testTableName, txn)
	indexInfo, ok = indexInfos["name"]
	assert.True(ok)
	index = indexInfo.Open()
	testKey := "n564"
	assert.NoError(index.BeforeFirst(testKey))
	for match, err := index.Next(); !match; match, err = index.Next() {
		assert.NoError(err)
	}
	rid, err := index.GetDataRid()
	assert.NoError(err)
	assert.Equal(*rid, testMap[testKey])
	assert.NoError(txn.Commit())

	clearEnv(t, env)
}
