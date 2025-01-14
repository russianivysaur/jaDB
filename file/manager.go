package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Manager struct {
	dbDirectory string
	blockSize   int
	openFiles   map[string]*os.File
	lock        sync.Mutex
}

func NewFileManager(directory string, blockSize int) (*Manager, error) {
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		err := os.MkdirAll(directory, 0755)
		if err != nil {
			return nil, fmt.Errorf("cannot create directory %v", err)
		}
	}
	//read directory entries
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("cannot read directory %v", err)
	}
	//clean temp files
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "temp") {
			tempFilePath := filepath.Join(directory, entry.Name())
			err := os.RemoveAll(tempFilePath)
			if err != nil {
				return nil, fmt.Errorf("cannot remove temp file : %v", err)
			}
		}
	}
	return &Manager{
		dbDirectory: directory,
		blockSize:   blockSize,
		openFiles:   make(map[string]*os.File),
	}, nil
}

func (manager *Manager) Read(block *BlockId, page *Page) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	file, err := manager.GetFile(block.GetFileName())
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("could not stat file : %v", err)
	}
	size := int(stat.Size())

	blockOffset := block.GetBlockNumber() * manager.blockSize
	if size < blockOffset+manager.blockSize {
		return fmt.Errorf("the block %d does not exist", block.GetBlockNumber())
	}

	if _, err := file.Seek(int64(blockOffset), io.SeekStart); err != nil {
		return fmt.Errorf("could not seek to offset :%v", err)
	}

	n, err := file.Read(page.Contents())
	if err != nil {
		return fmt.Errorf("could not read block into page: %v", err)
	}

	if n != manager.blockSize {
		return fmt.Errorf("got %d bytes expected %d", n, manager.blockSize)
	}
	return nil

}

func (manager *Manager) Write(block *BlockId, page *Page) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	file, err := manager.GetFile(block.GetFileName())
	if err != nil {
		return err
	}
	blockOffset := block.GetBlockNumber() * manager.blockSize
	if _, err := file.Seek(int64(blockOffset), io.SeekStart); err != nil {
		return fmt.Errorf("could not seek to offset %d: %v", blockOffset, err)
	}
	n, err := file.Write(page.Contents())
	if err != nil {
		return fmt.Errorf("could not write page to file: %v", err)
	}
	if n != manager.blockSize {
		return fmt.Errorf("expected %d bytes, wrote %d bytes", manager.blockSize, n)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("fsync error: %v", err)
	}
	return nil
}

func (manager *Manager) Append(filename string) (*BlockId, error) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	file, err := manager.GetFile(filename)
	if err != nil {
		return nil, err
	}

	blockCount, err := manager.Length(filename)
	if err != nil {
		return nil, err
	}

	blockNumber := blockCount

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("could not seek to end of file %s : %v", filename, err)
	}
	n, err := file.Write(make([]byte, manager.blockSize))
	if err != nil {
		return nil, fmt.Errorf("could not append block to file %s : %v", filename, err)
	}

	if n != manager.blockSize {
		return nil, fmt.Errorf("wrote %d bytes, expected to write %d bytes", manager.blockSize, n)
	}

	block := &BlockId{
		filename,
		blockNumber,
	}
	return block, nil
}

func (manager *Manager) Length(filename string) (int, error) {
	file, err := manager.GetFile(filename)
	if err != nil {
		return -1, err
	}
	stat, err := file.Stat()
	if err != nil {
		return -1, fmt.Errorf("could not stat file %s : %v", filename, err)
	}
	size := int(stat.Size())
	blockCount := size / manager.blockSize
	return blockCount, nil
}

func (manager *Manager) GetFile(filename string) (*os.File, error) {
	if file, exists := manager.openFiles[filename]; exists {
		return file, nil
	}
	filePath := filepath.Join(manager.dbDirectory, filename)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not create file : %v", err)
	}
	manager.openFiles[filename] = file
	return file, nil
}

func (manager *Manager) BlockSize() int {
	return manager.blockSize
}
