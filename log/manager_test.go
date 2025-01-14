package log

import (
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
	testLogFileName := "test.log"

	t.Run("TestLogManager", func(t *testing.T) {
		_, err := NewLogManager(fileManager, testLogFileName)
		assert.NoErrorf(err, "could not create log manager : %v", err)
	})

	t.Run("TestAppendOneRecord", func(t *testing.T) {
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
	})

	t.Run("TestAppendMultipleRecords", func(t *testing.T) {

	})

	if err := os.RemoveAll(tempDir); err != nil {
		t.Errorf("could not remove temp files: %v", err)
	}
}
