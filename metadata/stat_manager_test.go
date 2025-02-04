package metadata

import (
	assertPkg "github.com/stretchr/testify/assert"
	"jadb/buffer"
	"jadb/concurrency"
	"jadb/file"
	"jadb/log"
	"os"
	"path/filepath"
	"testing"
)

func initStatEnv(assert *assertPkg.Assertions) TestEnv {
	blockSize := 200
	dbFile := "test.db"
	logFile := "test.log"
	tempDir := filepath.Join(os.TempDir(), "temp")
	fm, err := file.NewFileManager(tempDir, blockSize)
	assert.NoError(err)
	lm, err := log.NewLogManager(fm, logFile)
	assert.NoError(err)
	bm, err := buffer.NewBufferManager(fm, lm, 100)
	assert.NoError(err)
	lt := concurrency.NewLockTable()
	err = os.MkdirAll(tempDir, 0755)
	assert.NoError(err)
	return TestEnv{
		dbFile, logFile, tempDir,
		blockSize,
		fm, lm, bm, lt,
	}
}

func TestStatManager(t *testing.T) {
	assert := assertPkg.New(t)
	env := initStatEnv(assert)

	// TODO

	clearEnv(t, env)
}
