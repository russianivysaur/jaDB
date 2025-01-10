package file

import (
	assertPkg "github.com/stretchr/testify/assert"
	"testing"
)

func TestPage(t *testing.T) {
	assert := assertPkg.New(t)
	blockSize := 400
	// creating a page
	page := NewPage(blockSize)
	assert.Equalf(len(page.page), blockSize, "Page buffer allocation error")

	buffer := make([]byte, blockSize)
	page = NewPageWithBuffer(buffer)
	assert.Equalf(page.page, buffer, "Page not using the specified buffer")

	offset := 0

	t.Run("IntegerDataTest", func(t *testing.T) {
		// integer test
		intData := 1243
		page.SetInt(offset, intData)
		assert.Equalf(page.GetInt(offset), intData, "Integer data does not match in page at offset %d", offset)
	})

	t.Run("ByteDataTest", func(t *testing.T) {
		offset = 10
		// bytes test
		byteData := []byte("This is a test!")
		page.SetBytes(offset, byteData)
		assert.Equalf(page.GetBytes(offset), byteData, "Byte data does not match in page at offset %d", offset)
	})

	t.Run("StringDataTest", func(t *testing.T) {
		// string test
		stringData := "This is another test!"
		err := page.SetString(offset, stringData)
		assert.NoErrorf(err, "Could not get string in page buffer : %v", err)
		assert.Equalf(page.GetString(offset), stringData, "String data does not match in page at offset %d", offset)
	})
}
