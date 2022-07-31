package log

import "github.com/luigitni/simpledb/file"

type Iterator struct {
	fm         *file.Manager
	block      file.BlockID
	page       *file.Page
	currentPos int
	boundary   int
}

// todo: this is a reader in go
func newIterator(fm *file.Manager, start file.BlockID) *Iterator {
	it := &Iterator{
		fm:    fm,
		block: start,
		page:  file.NewPageWithSlice(make([]byte, fm.BlockSize())),
	}

	it.moveToBlock(start)
	return it
}

func (it Iterator) HasNext() bool {
	return it.currentPos < it.fm.BlockSize() || it.block.BlockNumber() > 0
}

func (it *Iterator) Next() []byte {
	if it.currentPos == it.fm.BlockSize() {
		// we are at the end of the block, read the previous one
		it.block = file.NewBlockID(it.block.Filename(), it.block.BlockNumber()-1)
		it.moveToBlock(it.block)
	}

	record := it.page.GetBytes(it.currentPos)
	// move the iterator position forward by the
	it.currentPos += len(record) + file.IntBytes
	return record
}

func (it *Iterator) moveToBlock(block file.BlockID) {
	it.fm.Read(it.block, it.page)
	it.boundary = it.page.GetInt(0)
	it.currentPos = it.boundary
}
