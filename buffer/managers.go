package buffer

import "github.com/luigitni/simpledb/file"

type fileManager interface {
	BlockSize() int
	Read(block file.Block, page *file.Page)
	Write(block file.Block, page *file.Page)
}

type logManager interface {
	Flush(lsn int)
}
