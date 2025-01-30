package record

type RID struct {
	blockNumber int
	slot        int
}

func NewRID(blockNumber int, slot int) RID {
	return RID{
		blockNumber,
		slot,
	}
}

func (rid RID) BlockNumber() int {
	return rid.blockNumber
}

func (rid RID) Slot() int {
	return rid.slot
}
