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

func (block *BlockId) GetFileName() string {
	return block.fileName
}

func (block *BlockId) GetBlockNumber() int {
	return block.blkNum
}

func (block *BlockId) Equals(block2 *BlockId) bool {
	return block.fileName == block2.fileName && block.blkNum == block2.blkNum
}
