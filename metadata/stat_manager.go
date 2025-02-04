package metadata

import (
	"justanotherdb/record"
	"justanotherdb/table"
	"justanotherdb/tx"
	"sync"
)

type StatManager struct {
	tblMgr     *TableManager
	numCalls   int
	tableStats map[string]StatInfo
	lock       sync.Mutex
}

func NewStatManager(tblMgr *TableManager, txn *tx.Transaction) (*StatManager, error) {
	sm := &StatManager{
		tblMgr:     tblMgr,
		numCalls:   0,
		tableStats: make(map[string]StatInfo),
	}
	if err := sm.refreshStatistics(txn); err != nil {
		return nil, err
	}
	return sm, nil
}
func (manager *StatManager) getStatInfo(tableName string, layout *record.Layout, txn *tx.Transaction) (*StatInfo, error) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	manager.numCalls++
	if manager.numCalls > 100 {
		if err := manager.refreshStatistics(txn); err != nil {
			return nil, err
		}
	}
	si, ok := manager.tableStats[tableName]
	if !ok {
		info, err := manager.calcTableStats(tableName, layout, txn)
		if err != nil {
			return nil, err
		}
		si = *info
	}
	return &si, nil
}

func (manager *StatManager) refreshStatistics(txn *tx.Transaction) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	manager.tableStats = make(map[string]StatInfo)
	manager.numCalls = 0
	tableCatalogLayout, err := manager.tblMgr.getLayout("tblcat", txn)
	if err != nil {
		return err
	}
	ts, err := table.NewTableScan(txn, "tblcat", tableCatalogLayout)
	if err != nil {
		return err
	}
	hasNext, err := ts.Next()
	for hasNext {
		tableName, err := ts.GetString("tblname")
		if err != nil {
			return err
		}
		tableLayout, err := manager.tblMgr.getLayout(tableName, txn)
		if err != nil {
			return err
		}
		si, err := manager.calcTableStats(tableName, tableLayout, txn)
		if err != nil {
			return err
		}
		manager.tableStats[tableName] = *si
		hasNext, err = ts.Next()
	}
	return nil
}

func (manager *StatManager) calcTableStats(tableName string, tableLayout *record.Layout, txn *tx.Transaction) (*StatInfo, error) {
	numRecords := 0
	numBlocks := 0
	ts, err := table.NewTableScan(txn, tableName, tableLayout)
	if err != nil {
		return nil, err
	}
	hasNext, err := ts.Next()
	for hasNext {
		numRecords++
		numBlocks = ts.GetRid().BlockNumber() + 1
		hasNext, err = ts.Next()
	}
	ts.Close()
	si := NewStatInfo(numBlocks, numRecords)
	return &si, nil
}
