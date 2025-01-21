package log

import (
	"fmt"
	assertPkg "github.com/stretchr/testify/assert"
	"justanotherdb/constants"
	"justanotherdb/file"
	"os"
	"path/filepath"
	"testing"
)

func TestLogManager(t *testing.T) {
	assert := assertPkg.New(t)

	tempDir := filepath.Join(os.TempDir(), "test")
	blockSize := 400
	fileManager, err := file.NewFileManager(tempDir, blockSize)

	assert.NoErrorf(err, "could not create file manager : %v", err)

	t.Run("TestAppendOneRecord", func(t *testing.T) {
		testLogFileName := "test.log"
		logManager, err := NewLogManager(fileManager, testLogFileName)
		assert.NoErrorf(err, "could not create log manager : %v", err)
		testRecord := []byte("this is a test log record!")
		totalLogSize := len(testRecord) + constants.IntSize
		//append
		lsn, err := logManager.Append(testRecord)

		//check for boundary change
		assert.NoErrorf(err, "could not append log: %v", err)
		assert.Equalf(lsn, 1, "1st lsn should be 1, found %d", lsn)
		boundary := logManager.logPage.GetInt(0)
		expectedBoundary := fileManager.BlockSize() - totalLogSize
		assert.Equalf(boundary, expectedBoundary, "expected boundary to be at %d, found %d", expectedBoundary, boundary)

		iterator, err := logManager.GetIterator()
		assert.NoErrorf(err, "could not get iterator: %v", err)
		assert.Equalf(iterator.HasNext(), true, "iterator should have one value")
		record, err := iterator.Next()
		assert.NoErrorf(err, "iterator err: %v", err)
		assert.Equalf(record, testRecord, "expected %s,got %s", string(testRecord), string(record))
	})

	t.Run("WriteFlushReadTest", func(t *testing.T) {
		testLogFileName := "test1.log"
		logManager, err := NewLogManager(fileManager, testLogFileName)
		assert.NoErrorf(err, "could not create log manager: %v", err)
		for i := 0; i < 100; i++ {
			testRecord := []byte(fmt.Sprintf("this is log number %d", i))
			lsn, err := logManager.Append(testRecord)
			assert.NoErrorf(err, "could not append test log record: %v", err)
			assert.Equalf(lsn, i+1, "expected lsn %d,got %d", i+1, lsn)
		}

		iterator, err := logManager.GetIterator()
		assert.NoErrorf(err, "could not get iterator: %v", err)
		i := 99
		for iterator.HasNext() && i > 0 {
			record, err := iterator.Next()
			assert.NoErrorf(err, "could not get next record in the log: %v", err)
			expected := fmt.Sprintf("this is log number %d", i)
			assert.Equalf(string(record), expected, "expected log record to be '%s', got '%s'", expected, string(record))
			i--
		}
	})
	if err := os.RemoveAll(tempDir); err != nil {
		t.Errorf("could not remove temp files: %v", err)
	}
}
