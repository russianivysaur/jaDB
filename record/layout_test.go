package record

import (
	assertPkg "github.com/stretchr/testify/assert"
	"justanotherdb/constants"
	"testing"
)

func TestLayout(t *testing.T) {
	assert := assertPkg.New(t)

	fields := []string{"testInt1", "testInt2", "testString2"}
	schema := NewSchema()
	schema.AddIntField(fields[0])
	schema.AddIntField(fields[1])
	schema.AddStringField(fields[2], 10)
	expectedSlotSize := constants.IntSize*2 + 10
	layout := NewLayout(schema)

	assert.Equalf(expectedSlotSize, layout.SlotSize(), "expected %d slot size, got %d",
		expectedSlotSize, layout.SlotSize())
	expectedOffset := constants.IntSize
	assert.Equalf(expectedOffset, layout.offset("testInt2"), "expected offset %d,got %d",
		expectedOffset, layout.offset("testInt2"))
}
