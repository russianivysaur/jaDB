package scan_types

import (
	"fmt"
	"jadb/scan"
)

var _ scan.Scan = (*ProjectScan)(nil)

type ProjectScan struct {
	s       scan.Scan
	fldList map[string]bool
}

func NewProjectScan(s scan.Scan) *ProjectScan {
	return &ProjectScan{s, make(map[string]bool)}
}

func (p ProjectScan) BeforeFirst() error {
	return p.s.BeforeFirst()
}

func (p ProjectScan) Next() (bool, error) {
	return p.s.Next()
}

func (p ProjectScan) GetInt(s string) (int, error) {
	if p.HasField(s) {
		return p.s.GetInt(s)
	}
	return -1, fmt.Errorf("field %s not found", s)
}

func (p ProjectScan) GetString(s string) (string, error) {
	if p.HasField(s) {
		return p.s.GetString(s)
	}
	return "", fmt.Errorf("field %s not found", s)
}

func (p ProjectScan) GetVal(s string) (any, error) {
	if p.HasField(s) {
		return p.s.GetVal(s)
	}
	return nil, fmt.Errorf("field %s not found", s)
}

func (p ProjectScan) HasField(s string) bool {
	_, ok := p.fldList[s]
	return ok
}

func (p ProjectScan) Close() {
	p.s.Close()
}
