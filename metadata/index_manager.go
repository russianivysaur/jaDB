package metadata

import (
	"jadb/record"
	"jadb/scan_types"
	"jadb/tx"
)

type IndexManager struct {
	layout  *record.Layout
	tblMgr  *TableManager
	statMgr *StatManager
}

func NewIndexManager(isNew bool, tblMgr *TableManager, statMgr *StatManager, txn *tx.Transaction) (*IndexManager, error) {
	if isNew {
		indexCatalogSchema := record.NewSchema()
		indexCatalogSchema.AddStringField("indexname", MAX_NAME)
		indexCatalogSchema.AddStringField("tablename", MAX_NAME)
		indexCatalogSchema.AddStringField("fieldname", MAX_NAME)
		if err := tblMgr.createTable("idxcat", indexCatalogSchema, txn); err != nil {
			return nil, err
		}
	}

	layout, err := tblMgr.getLayout("idxcat", txn)
	if err != nil {
		return nil, err
	}
	return &IndexManager{
		layout,
		tblMgr,
		statMgr,
	}, nil
}

func (manager *IndexManager) createIndex(idxName string, tblName string, fldName string, txn *tx.Transaction) error {
	ts, err := scan_types.NewTableScan(txn, "idxcat", manager.layout)
	if err != nil {
		return err
	}
	if err := ts.Insert(); err != nil {
		return err
	}
	if err := ts.SetString("indexname", idxName); err != nil {
		return err
	}

	if err := ts.SetString("tablename", tblName); err != nil {
		return err
	}

	if err := ts.SetString("fieldname", fldName); err != nil {
		return err
	}
	ts.Close()
	return nil
}

// getIndexInfo
// get all the indexes of a table
func (manager *IndexManager) getIndexInfo(tblName string, txn *tx.Transaction) (map[string]IndexInfo, error) {
	indexCatalogLayout, err := manager.tblMgr.getLayout("idxcat", txn)
	if err != nil {
		return nil, err
	}
	ts, err := scan_types.NewTableScan(txn, "idxcat", indexCatalogLayout)
	if err != nil {
		return nil, err
	}
	result := make(map[string]IndexInfo)
	hasNext, err := ts.Next()
	for ; hasNext; hasNext, err = ts.Next() {
		if err != nil {
			return nil, err
		}
		tableName, err := ts.GetString("tablename")
		if err != nil {
			return nil, err
		}
		if tableName != tblName {
			continue
		}

		fieldName, err := ts.GetString("fieldname")
		if err != nil {
			return nil, err
		}
		idxName, err := ts.GetString("indexname")
		if err != nil {
			return nil, err
		}
		tableLayout, err := manager.tblMgr.getLayout(tableName, txn)
		if err != nil {
			return nil, err
		}
		si, err := manager.statMgr.getStatInfo(tableName, tableLayout, txn)
		if err != nil {
			return nil, err
		}
		result[fieldName] = NewIndexInfo(idxName, fieldName, tableLayout, txn, si)
	}
	return result, nil
}
