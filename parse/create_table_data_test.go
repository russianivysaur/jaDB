package parse

import (
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/record"
	"testing"
)

func TestCreateTableData(t *testing.T) {
	assert := assertPkg.New(t)
	newTableName := "test_table"
	newTableSchema := record.NewSchema()
	newTableSchema.AddIntField("id")
	newTableSchema.AddStringField("name", 10)
	newTableSchema.AddIntField("age")
	createTableData := NewCreateTableData(newTableName, newTableSchema)
	assert.Equal(newTableName, createTableData.TableName())
	assert.Equal(newTableSchema, createTableData.Schema())
}
