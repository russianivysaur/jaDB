package tx

type SetStringRecord struct {
}

func NewSetStringRecord(data []byte) *SetStringRecord {
	return &SetStringRecord{}
}

func (record *SetStringRecord) Op() int {

}

func (record *SetStringRecord) Undo(txNum int) {

}

func (record *SetStringRecord) TxNumber() int {

}
