package tx

import (
	"fmt"
	"justanotherdb/constants"
	"justanotherdb/file"
	"justanotherdb/log"
)

type SetIntRecord struct {
	txNum       int
	filename    string
	block       *file.BlockId
	blockOffset int
	oldVal      int
	newVal      int
}

func NewSetIntRecord(txNum int, block *file.BlockId, offset int, oldVal int, newVal int) *SetIntRecord {
	return &SetIntRecord{
		txNum,
		block.GetFileName(),
		block,
		offset,
		oldVal,
		newVal,
	}
}

func NewSetIntRecordFromPage(page *file.Page) (*SetIntRecord, error) {
	txNumOffset := constants.IntSize
	txNum := page.GetInt(txNumOffset)
	filenameOffset := txNumOffset + constants.IntSize
	filename := page.GetString(filenameOffset)
	blockNumOffset := filenameOffset + file.MaxLength(len(filename))
	blockNum := page.GetInt(blockNumOffset)
	block := file.NewBlock(filename, blockNum)
	blockOffsetOffset := blockNumOffset + constants.IntSize
	blockOffset := page.GetInt(blockOffsetOffset)
	oldValOffset := blockOffsetOffset + constants.IntSize
	oldVal := page.GetInt(oldValOffset)
	newValOffset := oldValOffset + constants.IntSize
	newVal := page.GetInt(newValOffset)
	return &SetIntRecord{
		txNum,
		filename,
		block,
		blockOffset,
		oldVal,
		newVal,
	}, nil
}

func (record *SetIntRecord) Op() LogRecordType {
	return SET_INT
}

func (record *SetIntRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(record.block); err != nil {
		return err
	}
	if err := tx.SetInt(record.block, record.blockOffset, record.oldVal, false); err != nil {
		return err
	}
	tx.Unpin(record.block)
	return nil
}

func (record *SetIntRecord) TxNumber() int {
	return record.txNum
}

func (record *SetIntRecord) ToString() string {
	return fmt.Sprintf("<SETINT %d %s %d %d %d %d>",
		record.txNum, record.filename,
		record.block.GetBlockNumber(), record.blockOffset,
		record.oldVal, record.newVal)
}

func (record *SetIntRecord) WriteToLog(lm *log.Manager) (int, error) {
	recordTypeOffset := 0
	txNumOffset := recordTypeOffset + constants.IntSize
	filenameOffset := txNumOffset + constants.IntSize
	blockNumberOffset := filenameOffset + file.MaxLength(len(record.filename))
	blockOffsetOffset := blockNumberOffset + constants.IntSize
	oldValOffset := blockOffsetOffset + constants.IntSize
	newValOffset := oldValOffset + constants.IntSize
	recordLength := newValOffset + constants.IntSize
	page := file.NewPageWithBuffer(make([]byte, recordLength))
	page.SetInt(recordTypeOffset, int(SET_INT))
	page.SetInt(txNumOffset, record.txNum)
	if err := page.SetString(filenameOffset, record.filename); err != nil {
		return -1, err
	}
	page.SetInt(blockNumberOffset, record.block.GetBlockNumber())
	page.SetInt(blockOffsetOffset, record.blockOffset)
	page.SetInt(oldValOffset, record.oldVal)
	page.SetInt(newValOffset, record.newVal)
	return lm.Append(page.Contents())
}
