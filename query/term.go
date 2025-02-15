package query

import (
	"jadb/plan"
	"jadb/record"
	"jadb/scan"
)

type Term struct {
	lhe *Expression
	rhe *Expression
	op  Operator
}

func NewTerm(lhe *Expression, rhe *Expression, op Operator) *Term {
	return &Term{lhe, rhe, op}
}

func (t *Term) IsSatisfied(inputScan scan.Scan) bool {
	var lheVal any
	var err error
	var rheVal any
	if lheVal, err = t.lhe.evaluate(inputScan); err != nil {
		return false
	}
	if rheVal, err = t.rhe.evaluate(inputScan); err != nil {
		return false
	}
	switch t.op {
	case Equal:
		return lheVal == rheVal
	case NotEqual:
		return lheVal != rheVal
	}
	return false
}

func (t *Term) AppliesTo(schema *record.Schema) bool {
	return t.lhe.AppliesTo(schema) && t.rhe.AppliesTo(schema)
}

func (t *Term) reductionFactor(queryPlan plan.Plan) int {
	if t.lhe.IsFieldName() && t.rhe.IsFieldName() {
		return max(queryPlan.DistinctValues(t.lhe.asFieldName()),
			queryPlan.DistinctValues(t.rhe.asFieldName()))
	}
	if t.lhe.IsFieldName() {
		return queryPlan.DistinctValues(t.lhe.asFieldName())
	}
	if t.rhe.IsFieldName() {
		return queryPlan.DistinctValues(t.rhe.asFieldName())
	}
	if t.lhe.asConstant() == t.rhe.asConstant() {
		return 1
	}
	return 1
}

func (t *Term) equatesWithConstant(fldName string) any {
	if t.lhe.IsFieldName() && t.lhe.asFieldName() == fldName && !t.rhe.IsFieldName() {
		return t.rhe.asConstant()
	} else if t.rhe.IsFieldName() && t.rhe.asFieldName() == fldName && !t.lhe.IsFieldName() {
		return t.lhe.asConstant()
	}
	return nil
}

func (t *Term) equatesWithField(fldName string) string {
	if t.lhe.IsFieldName() && t.lhe.asFieldName() == fldName && t.rhe.IsFieldName() {
		return t.rhe.asFieldName()
	} else if t.rhe.IsFieldName() && t.rhe.asFieldName() == fldName && t.lhe.IsFieldName() {
		return t.lhe.asFieldName()
	}
	return ""
}

func (t *Term) String() string {
	return t.lhe.String() + t.op.String() + t.rhe.String()
}
