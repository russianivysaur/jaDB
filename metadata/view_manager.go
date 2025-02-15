package metadata

import (
	"fmt"
	"jadb/record"
	"jadb/scan"
	"jadb/tx"
)

const MAX_VIEW_DEF = 100

type ViewManager struct {
	tblMgr *TableManager
}

func NewViewManager(isNew bool, tblMgr *TableManager, txn *tx.Transaction) (*ViewManager, error) {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("viewname", MAX_NAME)
		schema.AddStringField("viewdef", MAX_VIEW_DEF)
		err := tblMgr.createTable("viewcat", schema, txn)
		if err != nil {
			return nil, err
		}
	}
	return &ViewManager{
		tblMgr,
	}, nil
}

func (manager *ViewManager) createView(viewName string, viewDef string, txn *tx.Transaction) error {
	layout, err := manager.tblMgr.getLayout("viewcat", txn)
	if err != nil {
		return err
	}
	ts, err := scan.NewTableScan(txn, "viewcat", layout)
	if err != nil {
		return err
	}
	if err := ts.Insert(); err != nil {
		return err
	}
	if err := ts.SetString("viewname", viewName); err != nil {
		return err
	}
	if err := ts.SetString("viewdef", viewDef); err != nil {
		return err
	}
	ts.Close()
	return nil
}

func (manager *ViewManager) getViewDef(viewName string, txn *tx.Transaction) (string, error) {
	viewCatalogLayout, err := manager.tblMgr.getLayout("viewcat", txn)
	if err != nil {
		return "", err
	}
	ts, err := scan.NewTableScan(txn, "viewcat", viewCatalogLayout)
	if err != nil {
		return "", err
	}
	hasNext, err := ts.Next()
	for hasNext {
		if err != nil {
			return "", err
		}
		viewname, err := ts.GetString("viewname")
		if err != nil {
			return "", err
		}
		viewdef, err := ts.GetString("viewdef")
		if err != nil {
			return "", err
		}
		if viewname == viewName {
			return viewdef, nil
		}
		hasNext, err = ts.Next()
	}
	return "", fmt.Errorf("view %s not found", viewName)
}
