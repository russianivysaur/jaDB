package record

import (
	"jadb/file"
	"jadb/tx"
)

const (
	EMPTY = iota
	USED
)

type RecordPage struct {
	tx     *tx.Transaction
	blk    *file.BlockId
	layout *Layout
}

func NewRecordPage(tx *tx.Transaction, blk *file.BlockId, layout *Layout) (*RecordPage, error) {
	if err := tx.Pin(blk); err != nil {
		return nil, err
	}
	return &RecordPage{
		tx,
		blk,
		layout,
	}, nil
}

func (page *RecordPage) GetInt(slot int, fldName string) (int, error) {
	fldPos := page.offset(slot) + page.layout.Offset(fldName)
	return page.tx.GetInt(page.blk, fldPos)
}

func (page *RecordPage) GetString(slot int, fldName string) (string, error) {
	fldPos := page.offset(slot) + page.layout.Offset(fldName)
	return page.tx.GetString(page.blk, fldPos)
}

func (page *RecordPage) SetInt(slot int, fldName string, val int) error {
	fldPos := page.offset(slot) + page.layout.Offset(fldName)
	return page.tx.SetInt(page.blk, fldPos, val, true)
}

func (page *RecordPage) SetString(slot int, fldName string, val string) error {
	fldPos := page.offset(slot) + page.layout.Offset(fldName)
	return page.tx.SetString(page.blk, fldPos, val, true)
}

func (page *RecordPage) Delete(slot int) error {
	return page.setFlag(slot, EMPTY)
}

func (page *RecordPage) Format() error {
	slot := 0
	for page.isValidSlot(slot) {
		if err := page.tx.SetInt(page.blk, page.offset(slot), EMPTY, false); err != nil {
			return err
		}
		sch := page.layout.Schema()
		for _, fldName := range sch.Fields() {
			fldPos := page.offset(slot) + page.layout.Offset(fldName)
			if sch.Type(fldName) == INTEGER {
				if err := page.tx.SetInt(page.blk, fldPos, 0, false); err != nil {
					return err
				}
			} else {
				if err := page.tx.SetString(page.blk, fldPos, "", false); err != nil {
					return err
				}
			}
		}
		slot++
	}
	return nil
}

func (page *RecordPage) NextAfter(slot int) int {
	return page.searchAfter(slot, USED)
}

func (page *RecordPage) InsertAfter(slot int) (int, error) {
	newSlot := page.searchAfter(slot, EMPTY)
	if newSlot >= 0 {
		if err := page.setFlag(newSlot, USED); err != nil {
			return -1, err
		}
	}
	return newSlot, nil
}

func (page *RecordPage) Block() *file.BlockId {
	return page.blk
}

func (page *RecordPage) searchAfter(slot int, flag int) int {
	for slot += 1; page.isValidSlot(slot); slot++ {
		if val, err := page.tx.GetInt(page.blk, page.offset(slot)); err == nil && val == flag {
			return slot
		}
	}
	return -1
}

func (page *RecordPage) isValidSlot(slot int) bool {
	return page.offset(slot+1) <= page.tx.BlockSize()
}

func (page *RecordPage) offset(slot int) int {
	return slot * page.layout.SlotSize()
}

func (page *RecordPage) setFlag(slot int, flag int) error {
	return page.tx.SetInt(page.blk, page.offset(slot), flag, true)
}
