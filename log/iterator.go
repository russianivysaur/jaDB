package log

import (
	"justanotherdb/constants"
	"justanotherdb/file"
)

type Iterator struct {
	fileManager *file.Manager
	block       *file.BlockId
	page        *file.Page
	currentPos  int
	boundary    int
}

func NewIterator(fm *file.Manager, block *file.BlockId) (*Iterator, error) {
	page := file.NewPage(fm.BlockSize())
	err := moveToBlock(fm, block, page)
	boundary := page.GetInt(0)
	if err != nil {
		return nil, err
	}
	return &Iterator{
		fileManager: fm,
		block:       block,
		page:        page,
		boundary:    boundary,
		currentPos:  boundary,
	}, nil
}

func (iterator *Iterator) HasNext() bool {
	return iterator.currentPos < iterator.fileManager.BlockSize() || iterator.block.GetBlockNumber() > 0
}

func (iterator *Iterator) Next() ([]byte, error) {
	if iterator.currentPos == iterator.fileManager.BlockSize() && iterator.block.GetBlockNumber() > 0 {
		//previous block now
		block := file.NewBlock(iterator.block.GetFileName(), iterator.block.GetBlockNumber()-1)
		if err := iterator.fileManager.Read(block, iterator.page); err != nil {
			return nil, err
		}
		iterator.block = block
		iterator.boundary = iterator.page.GetInt(0)
		iterator.currentPos = iterator.boundary
	}
	logRecord := iterator.page.GetBytes(iterator.currentPos)
	iterator.currentPos += constants.IntSize + len(logRecord)
	return logRecord, nil
}

func moveToBlock(fileManager *file.Manager, block *file.BlockId, page *file.Page) error {
	if err := fileManager.Read(block, page); err != nil {
		return err
	}
	return nil
}
