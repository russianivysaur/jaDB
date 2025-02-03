package metadata

type StatInfo struct {
	numBlocks int
	numRecs   int
}

func NewStatInfo(numBlocks int, numRecs int) StatInfo {
	return StatInfo{
		numBlocks,
		numRecs,
	}
}

func (info StatInfo) BlocksAccessed() int {
	return info.numBlocks
}

func (info StatInfo) RecordsOutput() int {
	return info.numRecs
}

func (info StatInfo) DistinctValues() int {
	return 1 + (info.numRecs / 3)
}
