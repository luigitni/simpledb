package log

import "github.com/luigitni/simpledb/file"

type Iterator struct {
	fm         *file.FileManager
	block      file.Block
	page       *file.Page
	currentPos int
	boundary   int
}

// todo: this is a reader in go
func newIterator(fm *file.FileManager, start file.Block) *Iterator {
	it := &Iterator{
		fm:    fm,
		block: start,
		page:  file.NewPage(),
	}

	it.moveToBlock(start)
	return it
}

func (it *Iterator) HasNext() bool {
	return it.currentPos < it.fm.BlockSize() || it.block.Number() > 0
}

func (it *Iterator) Next() []byte {
	if it.currentPos == it.fm.BlockSize() {
		// we are at the end of the block, read the previous one
		it.block = file.NewBlock(it.block.FileName(), it.block.Number()-1)
		it.moveToBlock(it.block)
	}

	record := it.page.Bytes(it.currentPos)
	// move the iterator position forward by the
	it.currentPos += len(record) + file.IntSize
	return record
}

func (it *Iterator) moveToBlock(block file.Block) {
	it.fm.Read(it.block, it.page)
	it.boundary = it.page.Int(0)
	it.currentPos = it.boundary
}
