package buffer

import (
	"jadb/file"
	"jadb/log"
)

type Buffer struct {
	fileManager *file.Manager
	logManager  *log.Manager
	contents    *file.Page
	block       *file.BlockId
	pins        int
	txNum       int
	lsn         int
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	return &Buffer{
		fm,
		lm,
		file.NewPage(fm.BlockSize()),
		nil,
		0,
		-1,
		-1,
	}
}

func (buffer *Buffer) Contents() *file.Page {
	return buffer.contents
}

func (buffer *Buffer) Block() *file.BlockId {
	return buffer.block
}

func (buffer *Buffer) isPinned() bool {
	return buffer.pins > 0
}

func (buffer *Buffer) SetModified(txNum int, lsn int) {
	buffer.txNum = txNum
	buffer.lsn = lsn
}

func (buffer *Buffer) pin() {
	buffer.pins++
}

func (buffer *Buffer) unpin() {
	buffer.pins--
}

func (buffer *Buffer) flush() error {
	if buffer.txNum >= 0 {
		if err := buffer.logManager.Flush(buffer.lsn); err != nil {
			return err
		}
		if err := buffer.fileManager.Write(buffer.block, buffer.contents); err != nil {
			return err
		}
		buffer.txNum = -1
	}
	return nil
}

func (buffer *Buffer) assignToBlock(block *file.BlockId) error {
	if err := buffer.flush(); err != nil {
		return err
	}
	if err := buffer.fileManager.Read(block, buffer.contents); err != nil {
		return err
	}
	buffer.block = block
	buffer.pins = 0
	return nil
}

func (buffer *Buffer) modifyingTx() int {
	return buffer.txNum
}
