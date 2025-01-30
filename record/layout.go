package record

import (
	"justanotherdb/constants"
	"justanotherdb/file"
)

type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

func NewLayout(schema *Schema) Layout {
	offsets := make(map[string]int)
	offset := 0
	for _, fldName := range schema.Fields() {
		offsets[fldName] = offset
		offset += schema.Length(fldName)
	}
	return Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: offset,
	}
}

func NewLayout1(schema *Schema, offsets map[string]int, slotSize int) Layout {
	return Layout{
		schema,
		offsets,
		slotSize,
	}
}

func (layout Layout) Schema() *Schema {
	return layout.schema
}

func (layout Layout) offset(fldName string) int {
	return layout.offsets[fldName]
}

func (layout Layout) SlotSize() int {
	return layout.slotSize
}

func (layout Layout) lengthInBytes(fldName string) int {
	fldType := layout.schema.Type(fldName)
	if fldType == INTEGER {
		return constants.IntSize
	} else {
		return file.MaxLength(layout.schema.Length(fldName))
	}
}
