package file

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFileManager(t *testing.T) {
	assert := assertPkg.New(t)
	directory := filepath.Join(os.TempDir(), "test")
	blockSize := 500

	defer func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("could not remove temporary files: %v", err)
		}
	}()

	t.Run("NewFileManager", func(t *testing.T) {
		manager, err := NewFileManager(directory, blockSize)
		assert.NoErrorf(err, "Some error occured : %v", err)
		assert.Equalf(manager.dbDirectory, directory, "Directory does not match")
		assert.Equalf(manager.BlockSize(), blockSize, "Block size does not match")
	})

	t.Run("AppendWriteAndRead", func(t *testing.T) {
		filename := "appendWriteAndRead.db"
		manager, err := NewFileManager(directory, blockSize)
		assert.NoErrorf(err, "could not create manager : %v", err)
		blockCount := 10

		for i := 0; i < blockCount; i++ {
			block, err := manager.Append(filename)
			assert.NoErrorf(err, "could not append block : %v", err)
			assert.Equalf(block.getBlockNumber(), i, "expected block number %d, got %d", i, block.getBlockNumber())
			assert.Equalf(block.getFileName(), filename, "expected file name %s,got %s", filename, block.getFileName())
		}
		fileLength, err := manager.Length(filename)
		assert.NoErrorf(err, "could not find file length : %v", err)
		assert.Equalf(blockCount, fileLength, "expected block count %d, got %d", blockCount, fileLength)

		blockNumber := 5
		block := &BlockId{
			filename,
			blockNumber,
		}

		//write string to page
		offset := 3
		testString := "This is a test string!"
		page := NewPage(blockSize)
		err = page.setString(offset, testString)
		assert.NoErrorf(err, "could not set string in page : %v", err)

		// writing page to block
		err = manager.Write(block, page)
		assert.NoErrorf(err, "could not write to block : %v", err)

		// read page and check for string
		page = NewPage(blockSize)
		err = manager.Read(block, page)
		assert.NoErrorf(err, "could not read block to page : %v", err)
		assert.Equalf(testString, page.getString(offset), "test string does not match in page")
	})

	t.Run("Concurrent", func(t *testing.T) {
		filename := "concurrent.db"

		manager, err := NewFileManager(directory, blockSize)
		assert.NoErrorf(err, "cannot create file manager : %v", err)

		numGoroutines := 10
		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				block, err := manager.Append(filename)
				assert.NoErrorf(err, "could not append block : %v", err)

				page := NewPage(blockSize)
				err = manager.Read(block, page)
				assert.NoErrorf(err, "could not read block into page : %v", err)
				testString := fmt.Sprintf("Test text for goroutine %d", index)
				err = page.setString(0, testString)
				assert.NoErrorf(err, "could not write to page : %v", err)

				//write page to block
				err = manager.Write(block, page)
				assert.NoErrorf(err, "could not write page to block : %v", err)

				//read page back
				page = NewPage(blockSize)
				err = manager.Read(block, page)
				assert.NoErrorf(err, "could not read block to page : %v", err)

				extractedString := page.getString(0)
				assert.Equalf(testString, extractedString, "strings do not match for goroutine %d", index)
			}(i)
		}
		wg.Wait()
	})
}
