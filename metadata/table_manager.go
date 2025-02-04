package metadata

import (
	"fmt"
	"jadb/record"
	"jadb/table"
	"jadb/tx"
)

const MAX_NAME = 16

type TableManager struct {
	tableCatalogLayout record.Layout
	fldCatalogLayout   record.Layout
}

func NewTableManager(isNew bool, txn *tx.Transaction) (*TableManager, error) {
	//table catalog
	tblCatSchema := record.NewSchema()
	tblCatSchema.AddStringField("tblname", MAX_NAME)
	tblCatSchema.AddIntField("slotsize")

	//field catalog
	fldCatSchema := record.NewSchema()
	fldCatSchema.AddStringField("fldname", MAX_NAME)
	fldCatSchema.AddStringField("tblname", MAX_NAME)
	fldCatSchema.AddIntField("type")
	fldCatSchema.AddIntField("length")
	fldCatSchema.AddIntField("offset")
	tm := &TableManager{
		record.NewLayout(tblCatSchema),
		record.NewLayout(fldCatSchema),
	}
	if isNew {
		if err := tm.createTable("tblcat", tblCatSchema, txn); err != nil {
			return nil, err
		}
		if err := tm.createTable("fldcat", fldCatSchema, txn); err != nil {
			return nil, err
		}
	}

	return tm, nil
}

func (tblMgr *TableManager) createTable(tableName string, schema *record.Schema, txn *tx.Transaction) error {
	// add entry in table catalog
	tblLayout := record.NewLayout(schema)
	ts, err := table.NewTableScan(txn, "tblcat", &tblMgr.tableCatalogLayout)
	if err != nil {
		return err
	}
	if err = ts.Insert(); err != nil {
		return err
	}
	if err = ts.SetString("tblname", tableName); err != nil {
		return err
	}
	if err = ts.SetInt("slotsize", tblLayout.SlotSize()); err != nil {
		return err
	}
	ts.Close()

	//add entries into field catalog
	if ts, err = table.NewTableScan(txn, "fldcat", &tblMgr.fldCatalogLayout); err != nil {
		return err
	}
	for _, field := range schema.Fields() {
		if err = ts.Insert(); err != nil {
			return err
		}
		if err = ts.SetString("fldname", field); err != nil {
			return err
		}
		if err = ts.SetString("tblname", tableName); err != nil {
			return err
		}
		if err = ts.SetInt("type", schema.Type(field)); err != nil {
			return err
		}
		if err = ts.SetInt("length", schema.Length(field)); err != nil {
			return err
		}
		if err = ts.SetInt("offset", tblLayout.Offset(field)); err != nil {
			return err
		}
	}
	ts.Close()
	return nil
}

func (tblMgr *TableManager) getLayout(tblname string, txn *tx.Transaction) (*record.Layout, error) {
	slotsize := -1
	// find table in tblCatalog
	ts, err := table.NewTableScan(txn, "tblcat", &tblMgr.tableCatalogLayout)
	if err != nil {
		return nil, err
	}
	for hasNext, err := ts.Next(); hasNext; hasNext, err = ts.Next() {
		if err != nil {
			return nil, err
		}
		tblName, err := ts.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if tblName == tblname {
			slotsize, err = ts.GetInt("slotsize")
			if err != nil {
				return nil, err
			}
			break
		}
	}
	if slotsize == -1 {
		return nil, fmt.Errorf("no table named %s exists in catalog", tblname)
	}

	//get all the fields of the table
	ts, err = table.NewTableScan(txn, "fldcat", &tblMgr.fldCatalogLayout)
	if err != nil {
		return nil, err
	}
	tblSchema := record.NewSchema()
	offsets := make(map[string]int)
	for hasNext, err := ts.Next(); hasNext; hasNext, err = ts.Next() {
		if err != nil {
			return nil, err
		}
		tblName, err := ts.GetString("tblname")
		if tblName != tblname {
			continue
		}
		fldName, err := ts.GetString("fldname")
		if err != nil {
			return nil, err
		}
		fldType, err := ts.GetInt("type")
		if err != nil {
			return nil, err
		}
		fldOffset, err := ts.GetInt("offset")
		if err != nil {
			return nil, err
		}
		fldLength, err := ts.GetInt("length")
		if err != nil {
			return nil, err
		}
		tblSchema.AddField(fldName, fldType, fldLength)
		offsets[fldName] = fldOffset
	}

	tblLayout := record.NewLayout1(tblSchema, offsets, slotsize)
	return &tblLayout, nil
}
