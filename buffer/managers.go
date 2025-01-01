package buffer

import "github.com/luigitni/simpledb/types"

type fileManager interface {
	BlockSize() int
	Read(block types.Block, page *types.Page)
	Write(block types.Block, page *types.Page)
}

type logManager interface {
	Flush(lsn int)
}
