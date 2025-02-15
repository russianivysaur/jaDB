package query

import (
	"fmt"
	"jadb/record"
	"jadb/scan"
)

type Expression struct {
	value   any
	fldName string
}

func NewFieldExpression(fldName string) *Expression {
	return &Expression{value: nil, fldName: fldName}
}

func NewConstantExpression(value any) *Expression {
	return &Expression{value: value, fldName: ""}
}

func (e *Expression) evaluate(scan scan.Scan) (any, error) {
	if e.value != nil {
		return e.value, nil
	}
	return scan.GetVal(e.fldName)
}

func (e *Expression) IsFieldName() bool {
	return e.fldName != ""
}

func (e *Expression) asConstant() any {
	return e.value
}

func (e *Expression) asFieldName() string {
	return e.fldName
}

func (e *Expression) AppliesTo(schema *record.Schema) bool {
	return e.value != nil || schema.HasField(e.fldName)
}

func (e *Expression) String() string {
	if e.value != nil {
		return fmt.Sprintf("%v", e.value)
	}
	return e.fldName
}
