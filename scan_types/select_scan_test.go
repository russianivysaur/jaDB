package scan_types

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/query"
	"jadb/record"
	"jadb/tx"
	"testing"
)

func TestSelectScan(t *testing.T) {
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
		assert.NoError(ts.SetString("name", fmt.Sprintf("nam%d%d", i, i)))
		assert.NoError(ts.SetInt("age", i))
	}

	ts.Close()
	assert.NoError(txn.Commit())

	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	ts, err = NewTableScan(txn, testTableName, record.NewLayout(testTableSchema))

	testId := 5
	testName := fmt.Sprintf("nam%d%d", testId, testId)
	predicate := query.NewPredicateFromTerm(
		query.NewTerm(query.NewFieldExpression("id"), query.NewConstantExpression(testId),
			query.Equal))
	selectScan := NewSelectScan(ts, predicate)
	hasNext, err := selectScan.Next()
	assert.NoError(err)
	assert.True(hasNext)

	actualId, err := selectScan.GetInt("id")
	assert.NoError(err)
	assert.Equal(testId, actualId)
	actualName, err := selectScan.GetString("name")
	assert.NoError(err)
	assert.Equal(testName, actualName)

	hasNext, err = selectScan.Next()
	assert.False(hasNext)

	clearEnv(t, env)
}
