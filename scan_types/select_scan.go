package scan_types

import (
	"jadb/query"
	"jadb/record"
	"jadb/scan"
)

var _ scan.UpdateScan = (*SelectScan)(nil)

type SelectScan struct {
	s    scan.Scan
	pred *query.Predicate
}

func NewSelectScan(s scan.Scan, pred *query.Predicate) *SelectScan {
	return &SelectScan{s, pred}
}

func (selectScan SelectScan) BeforeFirst() error {
	return selectScan.s.BeforeFirst()
}

func (selectScan SelectScan) Next() (bool, error) {
	for hasNext, err := selectScan.s.Next(); hasNext; hasNext, err = selectScan.Next() {
		if err != nil {
			return false, err
		}
		if selectScan.pred.IsSatisfied(selectScan.s) {
			return true, nil
		}
	}
	return false, nil
}

func (selectScan SelectScan) GetInt(s2 string) (int, error) {
	return selectScan.s.GetInt(s2)
}

func (selectScan SelectScan) GetString(s2 string) (string, error) {
	return selectScan.s.GetString(s2)
}

func (selectScan SelectScan) GetVal(s2 string) (any, error) {
	return selectScan.s.GetVal(s2)
}

func (selectScan SelectScan) HasField(s2 string) bool {
	return selectScan.s.HasField(s2)
}

func (selectScan SelectScan) Close() {
	selectScan.s.Close()
}

func (selectScan SelectScan) SetInt(s2 string, i int) error {
	updateScan := selectScan.s.(scan.UpdateScan)
	return updateScan.SetInt(s2, i)
}

func (selectScan SelectScan) SetString(s3 string, s2 string) error {
	updateScan := selectScan.s.(scan.UpdateScan)
	return updateScan.SetString(s3, s2)
}

func (selectScan SelectScan) SetVal(s2 string, a any) error {
	updateScan := selectScan.s.(scan.UpdateScan)
	return updateScan.SetVal(s2, a)
}

func (selectScan SelectScan) Insert() error {
	updateScan := selectScan.s.(scan.UpdateScan)
	return updateScan.Insert()
}

func (selectScan SelectScan) Delete() error {
	updateScan := selectScan.s.(scan.UpdateScan)
	return updateScan.Delete()
}

func (selectScan SelectScan) GetRid() *record.RID {
	updateScan := selectScan.s.(scan.UpdateScan)
	return updateScan.GetRid()
}

func (selectScan SelectScan) MoveToRid(rid *record.RID) error {
	updateScan := selectScan.s.(scan.UpdateScan)
	return updateScan.MoveToRid(rid)
}
