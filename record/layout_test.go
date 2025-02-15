package record

import (
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/constants"
	"jadb/file"
	"testing"
)

func TestLayout(t *testing.T) {
	assert := assertPkg.New(t)

	fields := []string{"testInt1", "testInt2", "testString2"}
	schema := NewSchema()
	schema.AddIntField(fields[0])
	schema.AddIntField(fields[1])
	schema.AddStringField(fields[2], 10)
	expectedSlotSize := constants.IntSize*3 + file.MaxLength(10)
	layout := NewLayout(schema)

	assert.Equalf(expectedSlotSize, layout.SlotSize(), "expected %d slot size, got %d",
		expectedSlotSize, layout.SlotSize())
	expectedOffset := constants.IntSize * 2 // count for used and free flag too
	assert.Equalf(expectedOffset, layout.Offset("testInt2"), "expected offset %d,got %d",
		expectedOffset, layout.Offset("testInt2"))
}
