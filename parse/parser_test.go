package parse

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/file"
	"jadb/record"
	"testing"
)

func TestCreateTable(t *testing.T) {
	assert := assertPkg.New(t)
	sql := "CREATE TABLE ANKIT(id int,name varchar(20))"
	parser, err := NewParser(sql)
	assert.NoError(err)
	data, err := parser.updateCmd()
	assert.NoError(err)
	createTableData := data.(*CreateTableData)
	assert.Equal("ankit", createTableData.tableName)
	testSchema := record.NewSchema()
	testSchema.AddIntField("id")
	testSchema.AddStringField("name", file.MaxLength(20))
	assert.True(testSchema.Equals(createTableData.schema))
}

func TestCreateView(t *testing.T) {
	assert := assertPkg.New(t)
	sql := "CREATE VIEW ankit AS SELECT col,col_ FROM test"
	parser, err := NewParser(sql)
	assert.NoError(err)
	data, err := parser.updateCmd()
	assert.NoError(err)
	createIndexData := data.(*CreateViewData)
	assert.Equal("ankit", createIndexData.viewName)
	assert.Equal([]string{"test"}, createIndexData.queryData.tableList)
	assert.Equal([]string{"col", "col_"}, createIndexData.queryData.fieldList)
}

func TestCreateIndex(t *testing.T) {
	assert := assertPkg.New(t)
	sql := "CREATE Index hehe on test(col)"
	parser, err := NewParser(sql)
	assert.NoError(err)
	data, err := parser.updateCmd()
	assert.NoError(err)
	createIndexData := data.(*CreateIndexData)
	assert.Equal("hehe", createIndexData.indexName)
	assert.Equal("test", createIndexData.tableName)
	assert.Equal("col", createIndexData.fieldName)
}

func TestDelete(t *testing.T) {
	assert := assertPkg.New(t)
	testTableName := "test_table"
	testField := "test_field"
	testValue := "test_value"
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s=%s", testTableName, testField, testValue)
	parser, err := NewParser(sql)
	//pred := query.NewPredicateFromTerm(query.NewTerm(query.NewFieldExpression(testField),query.NewConstantExpression(testValue),query.Equal))
	assert.NoError(err)
	data, err := parser.updateCmd()
	assert.NoError(err)
	deleteData := data.(*DeleteData)
	assert.Equal(deleteData.tableName, testTableName)
}
