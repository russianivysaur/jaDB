package metadata

import (
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/record"
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

	clearEnv(t, env)
}
