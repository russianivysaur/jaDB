package metadata

import (
	"jadb/record"
	"jadb/tx"
)

type MetadataManager struct {
	tblMgr   *TableManager
	statMgr  *StatManager
	indexMgr *IndexManager
	viewMgr  *ViewManager
}

func NewMetadataManager(tblMgr *TableManager, statMgr *StatManager, indexMgr *IndexManager, viewMgr *ViewManager) *MetadataManager {
	return &MetadataManager{
		tblMgr,
		statMgr,
		indexMgr,
		viewMgr,
	}
}

func (manager *MetadataManager) CreateTable(tblName string, schema *record.Schema, txn *tx.Transaction) error {
	if err := manager.tblMgr.createTable(tblName, schema, txn); err != nil {
		return err
	}
	return nil
}

func (manager *MetadataManager) GetLayout(tblName string, txn *tx.Transaction) (*record.Layout, error) {
	return manager.tblMgr.getLayout(tblName, txn)
}

func (manager *MetadataManager) CreateView(viewName string, viewDef string, txn *tx.Transaction) error {
	return manager.viewMgr.createView(viewName, viewDef, txn)
}

func (manager *MetadataManager) GetViewDef(viewName string, txn *tx.Transaction) (string, error) {
	return manager.viewMgr.getViewDef(viewName, txn)
}

func (manager *MetadataManager) CreateIndex(indexName string, tableName string, fieldName string, txn *tx.Transaction) error {
	return manager.indexMgr.createIndex(indexName, tableName, fieldName, txn)
}

func (manager *MetadataManager) GetIndexInfo(tableName string, txn *tx.Transaction) (map[string]IndexInfo, error) {
	return manager.indexMgr.getIndexInfo(tableName, txn)
}

func (manager *MetadataManager) GetStatInfo(tableName string, layout *record.Layout, txn *tx.Transaction) (*StatInfo, error) {
	return manager.statMgr.getStatInfo(tableName, layout, txn)
}
