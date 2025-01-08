package file

import (
	"encoding/binary"
	"fmt"
	"justanotherdb/constants"
	"unicode/utf8"
)

type Page struct {
	page []byte
}

func NewPage(blockSize int) *Page {
	return &Page{
		page: make([]byte, blockSize),
	}
}

func NewPageWithBuffer(buffer []byte) *Page {
	return &Page{
		page: buffer,
	}
}

func (page *Page) setInt(offset int, data int) {
	binary.BigEndian.PutUint64(page.page[offset:], uint64(data))
}

func (page *Page) getInt(offset int) int {
	return int(binary.BigEndian.Uint64(page.page[offset:]))
}

func (page *Page) setString(offset int, data string) error {
	if !utf8.ValidString(data) {
		return fmt.Errorf("not a valid utf-8 string %s", data)
	}
	page.setBytes(offset, []byte(data))
	return nil
}

func (page *Page) getString(offset int) string {
	return string(page.getBytes(offset))
}

func (page *Page) setBytes(offset int, data []byte) {
	length := len(data)
	binary.BigEndian.PutUint64(page.page[offset:], uint64(length))
	offset += constants.IntSize
	copy(page.page[offset:], data)
}

func (page *Page) getBytes(offset int) []byte {
	size := int(binary.BigEndian.Uint64(page.page[offset:]))
	offset += constants.IntSize
	return page.page[offset : offset+size]
}

func MaxLength(strlen int) int {
	return constants.IntSize + (strlen * utf8.UTFMax)
}

func (page *Page) Contents() []byte {
	return page.page
}
