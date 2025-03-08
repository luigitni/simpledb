package buffer

import "github.com/luigitni/simpledb/storage"

type fileManager interface {
	BlockSize() storage.Offset
	Read(block storage.Block, page *storage.Page)
	Write(block storage.Block, page *storage.Page)
}

type logManager interface {
	Flush(lsn int)
}
