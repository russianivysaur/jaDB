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

func (rid *RID) BlockNumber() int {
	return rid.blockNumber
}

func (rid *RID) Slot() int {
	return rid.slot
}

func (rid *RID) Equals(rid2 *RID) bool {
	return rid.blockNumber == rid2.blockNumber && rid.slot == rid2.slot
}
