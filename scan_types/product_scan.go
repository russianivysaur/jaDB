package scan_types

import "jadb/scan"

var _ scan.Scan = (*ProductScan)(nil)

type ProductScan struct {
	s1 scan.Scan
	s2 scan.Scan
}

func NewProductScan(s1 scan.Scan, s2 scan.Scan) *ProductScan {
	_, _ = s1.Next()
	return &ProductScan{s1, s2}
}

func (p *ProductScan) BeforeFirst() error {
	if err := p.s1.BeforeFirst(); err != nil {
		return err
	}
	if _, err := p.s1.Next(); err != nil {
		return err
	}
	return p.s2.BeforeFirst()
}

func (p *ProductScan) Next() (bool, error) {
	hasNext2, err := p.s2.Next()
	if err != nil {
		return false, err
	}
	if hasNext2 {
		return true, nil
	}
	if err := p.s2.BeforeFirst(); err != nil {
		return false, err
	}
	hasNext2, err = p.s2.Next()
	if err != nil || !hasNext2 {
		return false, err
	}

	hasNext1, err := p.s1.Next()
	if err != nil || !hasNext1 {
		return false, err
	}
	return true, nil
}

func (p *ProductScan) GetInt(s string) (int, error) {
	if p.s1.HasField(s) {
		return p.s1.GetInt(s)
	}
	return p.s2.GetInt(s)
}

func (p *ProductScan) GetString(s string) (string, error) {
	if p.s1.HasField(s) {
		return p.s1.GetString(s)
	}
	return p.s2.GetString(s)
}

func (p *ProductScan) GetVal(s string) (any, error) {
	if p.s1.HasField(s) {
		return p.s1.GetVal(s)
	}
	return p.s2.GetVal(s)
}

func (p *ProductScan) HasField(s string) bool {
	return p.s1.HasField(s) || p.s2.HasField(s)
}

func (p *ProductScan) Close() {
	p.s1.Close()
	p.s2.Close()
}
