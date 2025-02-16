package scan_types

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/record"
	"jadb/tx"
	"testing"
)

func TestProjectScan(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)

	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	testTableName := "test_table"
	testTableSchema := record.NewSchema()
	testTableSchema.AddIntField("id")
	testTableSchema.AddStringField("name", 10)
	testTableSchema.AddIntField("age")
	ts, err := NewTableScan(txn, testTableName, record.NewLayout(testTableSchema))
	assert.NoError(err)
	//insert a bunch of entries
	testRecordCount := 1000
	for i := 0; i < testRecordCount; i++ {
		assert.NoError(ts.Insert())
		assert.NoError(ts.SetInt("id", i))
		assert.NoError(ts.SetString("name", fmt.Sprintf("nam%d", i)))
		assert.NoError(ts.SetInt("age", i))
	}

	ts.Close()
	assert.NoError(txn.Commit())

	//new project scan
	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	ts, err = NewTableScan(txn, testTableName, record.NewLayout(testTableSchema))

	fldList := make(map[string]bool)
	fldList["name"] = true
	projectScan := NewProjectScan(ts, fldList)

	for i := 0; i < testRecordCount; i++ {
		hasNext, err := projectScan.Next()
		assert.True(hasNext)
		assert.NoError(err)
		actualName, err := projectScan.GetString("name")
		assert.NoError(err)
		assert.Equal(actualName, fmt.Sprintf("nam%d", i))
		_, err = projectScan.GetInt("id")
		assert.Error(err)
	}

	clearEnv(t, env)
}
