package file

type BlockId struct {
	fileName string
	blkNum   int
}

func (block *BlockId) getFileName() string {
	return block.fileName
}

func (block *BlockId) getBlockNumber() int {
	return block.blkNum
}
