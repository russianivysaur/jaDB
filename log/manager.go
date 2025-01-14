package log

import (
	"justanotherdb/constants"
	"justanotherdb/file"
	"sync"
)

type Manager struct {
	fileManager  *file.Manager
	logFile      string
	logPage      *file.Page
	currentBlock *file.BlockId
	lastSavedLSN int
	latestLSN    int
	lock         sync.Mutex
}

func NewLogManager(fileManager *file.Manager, logFile string) (*Manager, error) {
	fileLength, err := fileManager.Length(logFile)
	if err != nil {
		return nil, err
	}

	var block *file.BlockId
	latestLSN := 0
	logPage := file.NewPage(fileManager.BlockSize())
	if fileLength == 0 {
		//log is empty
		block, err = fileManager.Append(logFile)
		if err != nil {
			return nil, err
		}
		if err = fileManager.Read(block, logPage); err != nil {
			return nil, err
		}
		logPage.SetInt(0, fileManager.BlockSize())
		if err = fileManager.Write(block, logPage); err != nil {
			return nil, err
		}
	} else {
		//load last block into logPage
		lastBlockNumber := fileLength - 1
		block = file.NewBlock(logFile, lastBlockNumber)
		if err = fileManager.Read(block, logPage); err != nil {
			return nil, err
		}
	}
	return &Manager{
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      logPage,
		currentBlock: block,
		lastSavedLSN: -1,
		latestLSN:    latestLSN,
	}, nil
}

func (manager *Manager) GetIterator() (*Iterator, error) {
	//flush to get an iterator on the latest copy of the page
	if err := manager.flush(); err != nil {
		return nil, err
	}

	return NewIterator(manager.fileManager, manager.currentBlock)
}

func (manager *Manager) Append(data []byte) (int, error) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	boundary := manager.logPage.GetInt(0)
	requiredBytes := len(data) + constants.IntSize
	if boundary-requiredBytes < constants.IntSize {
		//flush this page
		err := manager.flush()
		if err != nil {
			return -1, err
		}
		//new block needed
		block, err := manager.fileManager.Append(manager.logFile)
		if err != nil {
			return -1, err
		}
		if err = manager.fileManager.Read(block, manager.logPage); err != nil {
			return -1, err
		}
		manager.logPage.SetInt(0, manager.fileManager.BlockSize())
		if err = manager.fileManager.Write(block, manager.logPage); err != nil {
			return -1, err
		}
		boundary = manager.logPage.GetInt(0)
		manager.currentBlock = block
	}
	offset := boundary - requiredBytes
	manager.logPage.SetInt(offset, len(data))
	manager.logPage.SetBytes(offset+constants.IntSize, data)
	manager.logPage.SetInt(0, offset)
	manager.latestLSN++
	return manager.latestLSN, nil
}

func (manager *Manager) flush() error {
	if err := manager.fileManager.Write(manager.currentBlock, manager.logPage); err != nil {
		return err
	}
	manager.lastSavedLSN = manager.latestLSN
	return nil
}

func (manager *Manager) Flush(lsn int) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	if lsn > manager.lastSavedLSN {
		return manager.flush()
	}
	return nil
}
