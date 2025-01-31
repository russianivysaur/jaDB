package tx

import (
	"fmt"
	"justanotherdb/constants"
	"justanotherdb/file"
	"justanotherdb/log"
)

type SetStringRecord struct {
	txNum       int
	filename    string
	block       *file.BlockId
	blockOffset int
	oldVal      string
	newVal      string
}

func NewSetStringRecord(txNum int, block *file.BlockId, offset int, oldVal string, newVal string) *SetStringRecord {
	return &SetStringRecord{
		txNum,
		block.GetFileName(),
		block,
		offset,
		oldVal,
		newVal,
	}
}

func NewSetStringRecordFromPage(page *file.Page) (*SetStringRecord, error) {
	txNumOffset := constants.IntSize
	txNum := page.GetInt(txNumOffset)
	filenameOffset := txNumOffset + constants.IntSize
	filename := page.GetString(filenameOffset)
	blkOffset := filenameOffset + file.MaxLength(len(filename))
	block := file.NewBlock(filename, page.GetInt(blkOffset))
	offsetOffset := blkOffset + constants.IntSize
	blockOffset := page.GetInt(offsetOffset)
	oldValOffset := offsetOffset + constants.IntSize
	oldVal := page.GetString(oldValOffset)
	newValOffset := oldValOffset + file.MaxLength(len(oldVal))
	newVal := page.GetString(newValOffset)
	return &SetStringRecord{
		txNum,
		filename,
		block,
		blockOffset,
		oldVal,
		newVal,
	}, nil
}

func (record *SetStringRecord) Op() LogRecordType {
	return SET_STRING
}

func (record *SetStringRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(record.block); err != nil {
		return err
	}
	if err := tx.SetString(record.block, record.blockOffset, record.oldVal, false); err != nil {
		return err
	}
	tx.Unpin(record.block)
	return nil
}

func (record *SetStringRecord) ToString() string {
	return fmt.Sprintf("<SETSTRING %d %s %d %d %s %s>", record.txNum,
		record.filename, record.block.GetBlockNumber(), record.blockOffset,
		record.oldVal, record.newVal)
}

func (record *SetStringRecord) TxNumber() int {
	return record.txNum
}

func (record *SetStringRecord) WriteToLog(lm *log.Manager) (int, error) {
	recordTypeOffset := 0
	txNumOffset := recordTypeOffset + constants.IntSize
	filenameOffset := txNumOffset + constants.IntSize
	blockNumberOffset := filenameOffset + file.MaxLength(len(record.filename))
	blockOffsetOffset := blockNumberOffset + constants.IntSize
	oldValOffset := blockOffsetOffset + constants.IntSize
	newValOffset := oldValOffset + file.MaxLength(len(record.oldVal))
	recordLength := newValOffset + file.MaxLength(len(record.newVal))
	page := file.NewPageWithBuffer(make([]byte, recordLength))
	page.SetInt(recordTypeOffset, int(SET_STRING))
	page.SetInt(txNumOffset, record.txNum)
	if err := page.SetString(filenameOffset, record.filename); err != nil {
		return -1, err
	}
	page.SetInt(blockNumberOffset, record.block.GetBlockNumber())
	page.SetInt(blockOffsetOffset, record.blockOffset)
	if err := page.SetString(oldValOffset, record.oldVal); err != nil {
		return -1, err
	}

	if err := page.SetString(newValOffset, record.newVal); err != nil {
		return -1, err
	}
	return lm.Append(page.Contents())
}
