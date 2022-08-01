package file

import (
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

// Implements methods that read and write pages to disk blocks.
// It always reads and writes a block-sized number of bytes from a file, always at a block bounduary.
// This ensures that each call to read, write or apped will incour exactly one disk access
type Manager struct {
	folder    string
	blockSize int
	isNew     bool
	// maps a file name to an open file.
	// files are opened in RWS mode
	openFiles map[string]*os.File
	sync.Mutex
}

func NewFileManager(path string, blockSize int) *Manager {
	_, err := os.Stat(path)

	isNew := os.IsNotExist(err)
	// if the folder does not exists, create one
	if isNew {
		os.MkdirAll(path, os.ModeSticky|os.ModePerm)
	}

	if !isNew && err != nil {
		panic(err)
	}

	// clear all tmp files in the folder
	entries, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}

	for _, v := range entries {
		if strings.HasPrefix(v.Name(), "tmp") {
			os.Remove(v.Name())
		}
	}

	return &Manager{
		folder:    path,
		blockSize: blockSize,
		isNew:     isNew,
		openFiles: make(map[string]*os.File),
	}
}

func (manager *Manager) getFile(fname string) *os.File {
	f, ok := manager.openFiles[fname]
	if !ok {
		p := path.Join(manager.folder, fname)
		table, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0755)
		if err != nil {
			panic(err)
		}
		manager.openFiles[fname] = table
		return table
	}

	return f
}

func (manager *Manager) BlockSize() int {
	return manager.blockSize
}

// todo: code improvement: implement reader and writer interfaces

// Read reads the content of block id blk into Page p
func (manager *Manager) Read(blk BlockID, p *Page) {
	manager.Lock()
	defer manager.Unlock()

	f := manager.getFile(blk.Filename())

	// io.EOF is returned if we are reading too far into the file. This is ok, as we can read an empty block into the page
	if _, err := f.ReadAt(p.contents(), int64(blk.BlockNumber())*int64(manager.blockSize)); err != io.EOF && err != nil {
		panic(err)
	}
}

// Write writes Page p to BlockID block, persisted to a file
func (manager *Manager) Write(blk BlockID, p *Page) {
	manager.Lock()
	defer manager.Unlock()

	f := manager.getFile(blk.Filename())
	f.WriteAt(p.contents(), int64(blk.BlockNumber())*int64(manager.blockSize))
}

func (manager *Manager) Size(filename string) int {
	f := manager.getFile(filename)
	finfo, err := f.Stat()
	if err != nil {
		panic(err)
	}
	return int(finfo.Size() / int64(manager.blockSize))
}

// Append seeks to the end of the file and writes an empty array of bytes to the file
// todo: this might not be needed in go
func (manager *Manager) Append(fname string) BlockID {

	newBlkNum := manager.Size(fname)
	block := NewBlockID(fname, newBlkNum)
	buf := make([]byte, manager.blockSize)

	f := manager.getFile(fname)
	f.WriteAt(buf, int64(block.BlockNumber())*int64(manager.blockSize))
	return block
}
