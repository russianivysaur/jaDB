package record

import (
	"jadb/constants"
	"jadb/file"
	"slices"
)

type Schema struct {
	info   map[string]FieldInfo
	fields []string
}

func NewSchema() *Schema {
	return &Schema{
		info:   make(map[string]FieldInfo),
		fields: make([]string, 0),
	}
}

func (schema *Schema) AddField(fldName string, fieldType int, length int) {
	schema.fields = append(schema.fields, fldName)
	schema.info[fldName] = NewFieldInfo(fieldType, length)
}

func (schema *Schema) AddIntField(fldName string) {
	schema.AddField(fldName, INTEGER, constants.IntSize)
}

func (schema *Schema) AddStringField(fldName string, length int) {
	schema.AddField(fldName, VARCHAR, file.MaxLength(length))
}

func (schema *Schema) Add(fldName string, sch *Schema) {
	schema.AddField(fldName, sch.Type(fldName), sch.Length(fldName))
}

func (schema *Schema) AddAll(sch *Schema) {
	fields := sch.Fields()
	for _, field := range fields {
		schema.Add(field, sch)
	}
}

func (schema *Schema) Fields() []string {
	return schema.fields
}

func (schema *Schema) Type(fldName string) int {
	return schema.info[fldName].fieldType
}

func (schema *Schema) Length(fldName string) int {
	return schema.info[fldName].length
}

func (schema *Schema) HasField(fldName string) bool {
	_, exists := schema.info[fldName]
	return exists
}

func (schema *Schema) Equals(schema1 *Schema) bool {
	fields1 := schema.Fields()
	fields2 := schema.Fields()
	if !slices.Equal(fields1, fields2) {
		return false
	}
	for i := range len(fields1) {
		if schema.info[fields1[i]] != schema.info[fields2[i]] {
			return false
		}
	}
	return true
}
