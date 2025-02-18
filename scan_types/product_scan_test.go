package scan_types

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/record"
	"jadb/tx"
	"testing"
)

func TestProductScan(t *testing.T) {
	assert := assertPkg.New(t)
	env := initEnv(assert)

	txn, err := tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	assert.NoError(err)
	testTableName1 := "test_table"
	testTableSchema1 := record.NewSchema()
	testTableSchema1.AddIntField("id1")
	testTableSchema1.AddStringField("name1", 20)
	testTableSchema1.AddIntField("age1")
	ts, err := NewTableScan(txn, testTableName1, record.NewLayout(testTableSchema1))
	assert.NoError(err)
	//insert a bunch of entries
	testRecordCount := 1000
	for i := 0; i < testRecordCount; i++ {
		assert.NoError(ts.Insert())
		assert.NoError(ts.SetInt("id1", i))
		assert.NoError(ts.SetString("name1", fmt.Sprintf("nam1%d", i)))
		assert.NoError(ts.SetInt("age1", i))
	}

	ts.Close()

	testTableName2 := "test_table2"
	testTableSchema2 := record.NewSchema()
	testTableSchema2.AddIntField("id2")
	testTableSchema2.AddStringField("name2", 20)
	testTableSchema2.AddIntField("age2")
	ts, err = NewTableScan(txn, testTableName2, record.NewLayout(testTableSchema2))
	assert.NoError(err)
	//insert a bunch of entries
	for i := 1000; i < testRecordCount+1000; i++ {
		assert.NoError(ts.Insert())
		assert.NoError(ts.SetInt("id2", i))
		assert.NoError(ts.SetString("name2", fmt.Sprintf("nam2%d", i)))
		assert.NoError(ts.SetInt("age2", i))
	}

	ts.Close()
	assert.NoError(txn.Commit())

	//new project scan
	txn, err = tx.NewTransaction(env.fm, env.lm, env.bm, env.lt)
	ts, err = NewTableScan(txn, testTableName1, record.NewLayout(testTableSchema1))
	ts2, err := NewTableScan(txn, testTableName2, record.NewLayout(testTableSchema2))

	productScan := NewProductScan(ts, ts2)
	for hasNext, err := productScan.Next(); hasNext; hasNext, err = productScan.Next() {
		assert.NoError(err)
		age1, err := productScan.GetInt("age1")
		assert.NoError(err)
		age2, err := productScan.GetInt("age2")
		assert.NoError(err)
		assert.Equal(age1+1000, age2)
		name1, err := productScan.GetString("name1")
		assert.NoError(err)
		name2, err := productScan.GetString("name2")
		assert.NoError(err)
		assert.Equal(name2, fmt.Sprintf("nam2%d", age2))
		assert.Equal(name1, fmt.Sprintf("nam1%d", age1))
	}

	clearEnv(t, env)
}
