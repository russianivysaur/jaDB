package file

import "testing"
import assertPkg "github.com/stretchr/testify/assert"

func TestBlockId(t *testing.T) {
	assert := assertPkg.New(t)
	fileName := "test.db"
	blockNum := 200
	block := &BlockId{fileName, blockNum}
	assert.Equalf(block.GetFileName(), fileName, "Block could not set filename %s", fileName)
	assert.Equalf(block.GetBlockNumber(), blockNum, "Block could not set block number %d", blockNum)
}
