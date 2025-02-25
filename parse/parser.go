package parse

import (
	"fmt"
	"jadb/query"
	"jadb/record"
)

type Parser struct {
	lexer *Lexer
}

func NewParser(input string) (*Parser, error) {
	lexer, err := NewLexer(input)
	if err != nil {
		return nil, err
	}
	return &Parser{lexer: lexer}, nil
}

func (parser *Parser) field() (string, error) {
	return parser.lexer.eatId()
}

func (parser *Parser) constant() (any, error) {
	if parser.lexer.matchStringConstant() {
		return parser.lexer.eatStringConstant()
	}
	if parser.lexer.matchIntConstant() {
		return parser.lexer.eatIntConstant()
	}
	return nil, fmt.Errorf("expected constant,did not find one")
}

func (parser *Parser) expression() (*query.Expression, error) {
	if parser.lexer.matchId() {
		field, err := parser.field()
		if err != nil {
			return nil, err
		}
		return query.NewFieldExpression(field), nil
	}
	constant, err := parser.constant()
	if err != nil {
		return nil, err
	}
	return query.NewConstantExpression(constant), nil
}

func (parser *Parser) term() (*query.Term, error) {
	lhe, err := parser.expression()
	if err != nil {
		return nil, err
	}
	if err := parser.lexer.eatDelim('='); err != nil {
		return nil, err
	}
	rhe, err := parser.expression()
	return query.NewTerm(lhe, rhe, query.Equal), nil
}

func (parser *Parser) predicate() (*query.Predicate, error) {
	term, err := parser.term()
	if err != nil {
		return nil, err
	}
	predicate := query.NewPredicateFromTerm(term)
	if parser.lexer.matchKeyword("and") {
		if err := parser.lexer.eatKeyword("and"); err != nil {
			return nil, err
		}
	}
	return predicate, nil
}

func (parser *Parser) query() (*QueryData, error) {
	err := parser.lexer.eatKeyword("select")
	if err != nil {
		return nil, err
	}
	fields, err := parser.selectList()
	if err != nil {
		return nil, err
	}
	if err := parser.lexer.eatKeyword("from"); err != nil {
		return nil, err
	}
	tables, err := parser.tableList()
	if err != nil {
		return nil, err
	}
	predicate := query.NewPredicate()
	if parser.lexer.matchKeyword("where") {
		if err := parser.lexer.eatKeyword("where"); err != nil {
			return nil, err
		}
		if predicate, err = parser.predicate(); err != nil {
			return nil, err
		}
	}
	return &QueryData{
		fieldList: fields,
		tableList: tables,
		pred:      predicate,
	}, nil
}

func (parser *Parser) selectList() ([]string, error) {
	field, err := parser.field()
	if err != nil {
		return nil, err
	}
	list := []string{field}
	for parser.lexer.matchDelim(',') {
		if err := parser.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		field, err = parser.field()
		list = append(list, field)
	}
	return list, nil
}

func (parser *Parser) tableList() ([]string, error) {
	table, err := parser.field()
	if err != nil {
		return nil, err
	}
	list := []string{table}
	for parser.lexer.matchDelim(',') {
		if err = parser.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		table, err = parser.field()
		if err != nil {
			return nil, err
		}
		list = append(list, table)
	}
	return list, nil
}

func (parser *Parser) updateCmd() (any, error) {
	switch {
	//case parser.lexer.matchKeyword("insert"):
	//	return parser.insert()
	//case parser.lexer.matchKeyword("delete"):
	//	return parser.delete()
	//case parser.lexer.matchKeyword("update"):
	//	return parser.update()
	default:
		return parser.create()
	}
}

//func (parser *Parser) insert() (*InsertData, error) {
//
//}
//
//func (parser *Parser) delete() (*DeleteData, error) {
//
//}
//
//func (parser *Parser) update() (*ModifyData, error) {
//
//}

func (parser *Parser) create() (any, error) {
	if err := parser.lexer.eatKeyword("create"); err != nil {
		return nil, err
	}
	switch {
	case parser.lexer.matchKeyword("table"):
		return parser.createTable()
	case parser.lexer.matchKeyword("view"):
		return parser.createView()
	case parser.lexer.matchKeyword("index"):
		return parser.createIndex()
	}
	return nil, fmt.Errorf("expected table,view or index after create")
}

func (parser *Parser) fieldDefs() (*record.Schema, error) {
	schema, err := parser.fieldDef()
	if err != nil {
		return nil, err
	}
	if parser.lexer.matchDelim(',') {
		if err := parser.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		schema2, err := parser.fieldDefs()
		if err != nil {
			return nil, err
		}
		schema.AddAll(schema2)
	}
	return schema, nil
}

func (parser *Parser) fieldDef() (*record.Schema, error) {
	fldName, err := parser.field()
	if err != nil {
		return nil, err
	}
	return parser.fieldType(fldName)
}

func (parser *Parser) fieldType(fldName string) (*record.Schema, error) {
	schema := record.NewSchema()
	if parser.lexer.matchKeyword("int") {
		if err := parser.lexer.eatKeyword("int"); err != nil {
			return nil, err
		}
		schema.AddIntField(fldName)
	} else if parser.lexer.matchKeyword("varchar") {
		if err := parser.lexer.eatKeyword("varchar"); err != nil {
			return nil, err
		}
		if err := parser.lexer.eatDelim('('); err != nil {
			return nil, err
		}
		size, err := parser.lexer.eatIntConstant()
		if err != nil {
			return nil, err
		}
		schema.AddStringField(fldName, size)
	} else {
		return nil, fmt.Errorf("expected int or varchar after %s", fldName)
	}
	return schema, nil
}

func (parser *Parser) createTable() (*CreateTableData, error) {
	if err := parser.lexer.eatKeyword("table"); err != nil {
		return nil, err
	}
	tableName, err := parser.lexer.eatId()
	if err != nil {
		return nil, err
	}
	if err := parser.lexer.eatDelim('('); err != nil {
		return nil, err
	}
	schema, err := parser.fieldDefs()
	if err != nil {
		return nil, err
	}
	if err := parser.lexer.eatDelim(')'); err != nil {
		return nil, err
	}
	return &CreateTableData{schema: schema, tableName: tableName}, nil

}

func (parser *Parser) createView() (*CreateViewData, error) {
	if err := parser.lexer.eatKeyword("view"); err != nil {
		return nil, err
	}
	viewName, err := parser.lexer.eatId()
	if err != nil {
		return nil, err
	}
	if err := parser.lexer.eatKeyword("as"); err != nil {
		return nil, err
	}
	queryData, err := parser.query()
	if err != nil {
		return nil, err
	}
	return &CreateViewData{viewName: viewName, queryData: queryData}, nil
}

func (parser *Parser) createIndex() (*CreateIndexData, error) {
	if err := parser.lexer.eatKeyword("index"); err != nil {
		return nil, err
	}
	indexName, err := parser.lexer.eatId()
	if err != nil {
		return nil, err
	}
	if err := parser.lexer.eatKeyword("on"); err != nil {
		return nil, err
	}
	tableName, err := parser.lexer.eatId()
	if err != nil {
		return nil, err
	}
	if err := parser.lexer.eatDelim('('); err != nil {
		return nil, err
	}
	columnName, err := parser.lexer.eatId()
	if err != nil {
		return nil, err
	}
	if err := parser.lexer.eatDelim(')'); err != nil {
		return nil, err
	}
	return &CreateIndexData{tableName: tableName, fieldName: columnName, indexName: indexName}, nil
}
