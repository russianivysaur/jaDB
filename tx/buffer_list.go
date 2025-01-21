package tx

import (
	"justanotherdb/buffer"
)

type pinnedBuffer struct {
	pins   int
	buffer *buffer.Buffer
}

type BufferList struct {
}

func NewBufferList(bm *buffer.Manager) (*BufferList, error) {
	return &BufferList{}, nil
}
