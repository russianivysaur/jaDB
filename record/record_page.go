package record

import (
	"justanotherdb/file"
	"justanotherdb/tx"
)

const (
	EMPTY = iota
	USED
)

type RecordPage struct {
	tx     *tx.Transaction
	blk    *file.BlockId
	layout Layout
}

func NewRecordPage(tx *tx.Transaction, blk *file.BlockId, layout Layout) (*RecordPage, error) {
	if err := tx.Pin(blk); err != nil {
		return nil, err
	}
	return &RecordPage{
		tx,
		blk,
		layout,
	}, nil
}

func (page *RecordPage) getInt(slot int, fldName string) (int, error) {
	fldPos := page.offset(slot) + page.layout.offset(fldName)
	return page.tx.GetInt(page.blk, fldPos)
}

func (page *RecordPage) getString(slot int, fldName string) (string, error) {
	fldPos := page.offset(slot) + page.layout.offset(fldName)
	return page.tx.GetString(page.blk, fldPos)
}

func (page *RecordPage) setInt(slot int, fldName string, val int) error {
	fldPos := page.offset(slot) + page.layout.offset(fldName)
	return page.tx.SetInt(page.blk, fldPos, val, true)
}

func (page *RecordPage) setString(slot int, fldName string, val string) error {
	fldPos := page.offset(slot) + page.layout.offset(fldName)
	return page.tx.SetString(page.blk, fldPos, val, true)
}

func (page *RecordPage) delete(slot int) {
	//page.setFlag(slot,EMPTY)
}

func (page *RecordPage) isValidSlot(slot int) bool {
	return page.offset(slot+1) <= page.tx.BlockSize()
}

func (page *RecordPage) offset(slot int) int {
	return slot * page.layout.SlotSize()
}
