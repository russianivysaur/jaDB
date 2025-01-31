package record

import (
	"justanotherdb/constants"
	"justanotherdb/file"
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

func (schema *Schema) addField(fldName string, fieldType int, length int) {
	schema.fields = append(schema.fields, fldName)
	schema.info[fldName] = NewFieldInfo(fieldType, length)
}

func (schema *Schema) AddIntField(fldName string) {
	schema.addField(fldName, INTEGER, constants.IntSize)
}

func (schema *Schema) AddStringField(fldName string, length int) {
	schema.addField(fldName, VARCHAR, file.MaxLength(length))
}

func (schema *Schema) Add(fldName string, sch *Schema) {
	schema.addField(fldName, sch.Type(fldName), sch.Length(fldName))
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
