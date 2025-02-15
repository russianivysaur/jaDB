package query

import (
	"jadb/plan"
	"jadb/record"
	"jadb/scan"
)

type Predicate struct {
	terms []*Term
}

func NewPredicate() *Predicate {
	return &Predicate{terms: make([]*Term, 0)}
}

func NewPredicateFromTerm(term *Term) *Predicate {
	return &Predicate{terms: []*Term{term}}
}

func (p *Predicate) CojoinWith(other *Predicate) {
	p.terms = append(p.terms, other.terms...)
}

func (p *Predicate) IsSatisfied(inputScan scan.Scan) bool {
	for _, term := range p.terms {
		if !term.IsSatisfied(inputScan) {
			return false
		}
	}
	return true
}

func (p *Predicate) ReductionFactor(queryPlan plan.Plan) int {
	factor := 1
	for _, term := range p.terms {
		factor *= term.reductionFactor(queryPlan)
	}
	return factor
}

func (p *Predicate) SelectSubPredicate(schema *record.Schema) *Predicate {
	result := NewPredicate()
	for _, term := range p.terms {
		if term.AppliesTo(schema) {
			result.terms = append(result.terms, term)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

func (p *Predicate) JoinSubPredicate(schema1, schema2 *record.Schema) *Predicate {
	result := NewPredicate()
	unionSchema := record.NewSchema()
	unionSchema.AddAll(schema1)
	unionSchema.AddAll(schema2)

	for _, term := range p.terms {
		if term.AppliesTo(schema1) && term.AppliesTo(schema2) && term.AppliesTo(unionSchema) {
			result.terms = append(result.terms, term)
		}
	}

	if len(result.terms) == 0 {
		return nil
	}
	return result
}

func (p *Predicate) EquatesWithConstant(fldName string) any {
	for _, term := range p.terms {
		if c := term.equatesWithConstant(fldName); c != nil {
			return c
		}
	}
	return nil
}

func (p *Predicate) EquatesWithField(fldName string) string {
	for _, term := range p.terms {
		if f := term.equatesWithField(fldName); f != "" {
			return f
		}
	}
	return ""
}

func (p *Predicate) String() string {
	if len(p.terms) == 0 {
		return ""
	}
	output := p.terms[0].String()
	for _, term := range p.terms[1:] {
		output += " and " + term.String()
	}
	return output
}
