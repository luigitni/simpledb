package file

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/luigitni/simpledb/storage"
)

const (
	tmpTablePrefix = "__tmp_"
	TmpTablePrefix = tmpTablePrefix + "%d"

	walFolder = "wal"
	WALPath   = "wal/log"
)

// Implements methods that read and write pages to disk blocks.
// It always reads and writes a block-sized number of bytes from a file, always at a block bounduary.
// This ensures that each call to read, write or apped will incour exactly one disk access
type FileManager struct {
	folder    string
	blockSize storage.Long
	isNew     bool
	// maps a file name to an open file.
	// files are opened in RWS mode
	openFiles map[string]*os.File
	sync.Mutex
}

func NewFileManager(root string, blockSize storage.Long) *FileManager {
	_, err := os.Stat(root)

	isNew := os.IsNotExist(err)
	if isNew {
		os.MkdirAll(root, os.ModeSticky|os.ModePerm)
		wp := path.Join(root, walFolder)
		os.MkdirAll(wp, os.ModeSticky|os.ModePerm)
	}

	if !isNew && err != nil {
		panic(err)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		panic(err)
	}

	for _, v := range entries {
		if strings.HasPrefix(v.Name(), tmpTablePrefix) {
			fn := filepath.Join(root, v.Name())
			if err := os.Remove(fn); err != nil {
				panic(err)
			}
		}
	}

	return &FileManager{
		folder:    root,
		blockSize: blockSize,
		isNew:     isNew,
		openFiles: make(map[string]*os.File),
	}
}

func (manager *FileManager) getFile(fname string) *os.File {
	f, ok := manager.openFiles[fname]
	if !ok {
		p := path.Join(manager.folder, fname)

		table, err := manager.openFile(p)
		if err != nil {
			panic(err)
		}

		manager.openFiles[fname] = table

		return table
	}

	return f
}

func (manager *FileManager) Close() error {
	for _, f := range manager.openFiles {
		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}


func (manager *FileManager) openFile(fullPath string) (*os.File, error) {
	if fullPath == WALPath {
		return os.OpenFile(fullPath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0o755)
	}

	return os.OpenFile(fullPath, os.O_CREATE|os.O_RDWR, 0o755)
}

func (manager *FileManager) IsNew() bool {
	return manager.isNew
}

func (manager *FileManager) BlockSize() storage.Offset {
	return storage.PageSize
}

// todo: code improvement: implement reader and writer interfaces

// Read reads the content of block id blk into Page p
func (manager *FileManager) Read(blk storage.Block, p *storage.Page) {
	manager.Lock()
	defer manager.Unlock()

	f := manager.getFile(blk.FileName())

	// io.EOF is returned if we are reading too far into the file. This is ok, as we can read an empty block into the page
	if _, err := f.ReadAt(p.Contents(), int64(blk.Number())*int64(manager.blockSize)); err != io.EOF && err != nil {
		panic(err)
	}
}

// Write writes Page p to BlockID block, persisted to a file
func (manager *FileManager) Write(blk storage.Block, p *storage.Page) {
	manager.Lock()
	defer manager.Unlock()

	f := manager.getFile(blk.FileName())
	f.WriteAt(p.Contents(), int64(blk.Number())*int64(manager.blockSize))
}

// Size returns the size, in blocks, of the given file
func (manager *FileManager) Size(filename string) storage.Long {
	f := manager.getFile(filename)
	finfo, err := f.Stat()
	if err != nil {
		panic(err)
	}
	return storage.Long(finfo.Size()) / manager.blockSize
}

// Append seeks to the end of the file and writes an empty array of bytes to the file
// todo: this might not be needed in go
func (manager *FileManager) Append(fname string) storage.Block {
	newBlkNum := manager.Size(fname)
	block := storage.NewBlock(fname, storage.Long(newBlkNum))
	buf := make([]byte, manager.blockSize)

	f := manager.getFile(fname)
	f.WriteAt(buf, int64(block.Number())*int64(manager.blockSize))
	return block
}
