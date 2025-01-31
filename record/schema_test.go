package record

import (
	assertPkg "github.com/stretchr/testify/assert"
	"justanotherdb/constants"
	"testing"
)

func TestSchema(t *testing.T) {
	assert := assertPkg.New(t)
	fieldNames := []string{"testInt", "testString"}
	schema := NewSchema()
	schema.AddIntField("testInt")
	schema.AddStringField("testString", 80)
	assert.Equalf(fieldNames, schema.Fields(), "expected %v in schema fields, got %v", fieldNames,
		schema.Fields())

	assert.Equal(constants.IntSize, schema.Length("testInt"))
	assert.Equal(80, schema.Length("testString"))

	assert.Equal(false, schema.HasField("idk"))
	assert.Equal(true, schema.HasField("testInt"))

	assert.Equal(INTEGER, schema.Type("testInt"))
	assert.Equal(VARCHAR, schema.Type("testString"))
}
