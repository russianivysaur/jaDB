package tx

import (
	"justanotherdb/buffer"
	"justanotherdb/file"
)

type pinnedBuffer struct {
	pins   int
	buffer *buffer.Buffer
}

type BufferList struct {
	buffers map[file.BlockId]*pinnedBuffer
	bm      *buffer.Manager
}

func NewBufferList(bm *buffer.Manager) (*BufferList, error) {
	return &BufferList{
		buffers: make(map[file.BlockId]*pinnedBuffer),
		bm:      bm,
	}, nil
}

func (list *BufferList) GetBuffer(block file.BlockId) *buffer.Buffer {
	if pinnedBuffer, ok := list.buffers[block]; ok {
		return pinnedBuffer.buffer
	}
	return nil
}

func (list *BufferList) pin(block file.BlockId) error {
	buff, err := list.bm.Pin(&block)
	if err != nil {
		return err
	}
	if _, ok := list.buffers[block]; !ok {
		list.buffers[block] = &pinnedBuffer{0, buff}
	}
	list.buffers[block].pins++
	return nil
}

func (list *BufferList) unpin(block file.BlockId) {
	if pinned, ok := list.buffers[block]; ok {
		pins := pinned.pins
		pins--
		if pins <= 0 {
			list.bm.Unpin(pinned.buffer)
			delete(list.buffers, block)
		}
	}
}

func (list *BufferList) unpinAll() {
	for _, pinned := range list.buffers {
		list.bm.Unpin(pinned.buffer)
	}
	list.buffers = make(map[file.BlockId]*pinnedBuffer)
}
