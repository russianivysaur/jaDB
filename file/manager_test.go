package file

import (
	"os"
	"sync"
)

type Manager struct {
	dbDirectory *os.File
	blockSize   int
	openFiles   map[string]*os.File
	lock        sync.Mutex
}

func NewFileManager(directory string, blockSize int) *Manager {
	return &Manager{
		dbDirectory: nil,
		blockSize:   blockSize,
		openFiles:   make(map[string]*os.File, 0),
	}
}
