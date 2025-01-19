package tx

import (
	"justanotherdb/constants"
	"justanotherdb/file"
)

type SetIntRecord struct {
	txNum    int
	filename string
	blockNum int
	block    *file.BlockId
	oldVal   int
	newVal   int
}

func NewSetIntRecord(page *file.Page) (*SetIntRecord, error) {
	txNumOffset := constants.IntSize
	txNum := page.GetInt(txNumOffset)
	filenameOffset := txNumOffset + constants.IntSize
	filename := page.GetString(filenameOffset)
	blockNumOffset := filenameOffset + file.MaxLength(len(filename))
	blockNum := page.GetInt(blockNumOffset)
	block := file.NewBlock(filename, blockNum)
	oldValOffset := blockNumOffset + constants.IntSize
	oldVal := page.GetInt(oldValOffset)
	newValOffset := oldValOffset + constants.IntSize
	newVal := page.GetInt(newValOffset)
	return &SetIntRecord{
		txNum,
		filename,
		blockNum,
		block,
		oldVal,
		newVal,
	}, nil
}

func (record *SetIntRecord) Op() LogRecordType {
	return SETINT
}

func (record *SetIntRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(record.block); err != nil {

	}
	
}

func (record *SetIntRecord) TxNumber() int {

}

func (record *SetIntRecord) String() string {
	return ""
}
