package file

type BlockId struct {
	fileName string
	blkNum   int
}

func NewBlock(filename string, blockNumber int) *BlockId {
	return &BlockId{
		filename,
		blockNumber,
	}
}

func (block *BlockId) getFileName() string {
	return block.fileName
}

func (block *BlockId) getBlockNumber() int {
	return block.blkNum
}
