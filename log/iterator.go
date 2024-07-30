package log

import "github.com/luigitni/simpledb/file"

// WalIterator iterates over WAL file blocks.
// It reads blocks from disk into a page, and iterates
// records from right to left within each block.
type WalIterator struct {
	fm         *file.FileManager
	block      file.Block
	page       *file.Page
	currentPos int
	boundary   int
}

func newWalIterator(page *file.Page, fm *file.FileManager, start file.Block) *WalIterator {
	it := &WalIterator{
		fm:    fm,
		block: start,
		page:  page,
	}

	it.moveToBlock(start)
	return it
}

// HasNext returns true if there are more records to iterate
func (it *WalIterator) HasNext() bool {
	return it.currentPos < it.fm.BlockSize() || it.block.Number() > 0
}

// Next returns the next record in the WAL
func (it *WalIterator) Next() []byte {
	if it.currentPos == it.fm.BlockSize() {
		// we are at the end of the block, read the previous one
		block := file.NewBlock(it.block.FileName(), it.block.Number()-1)
		it.moveToBlock(block)
	}

	// each record is prepended by its length
	record := it.page.Bytes(it.currentPos)
	// move the iterator pointer to the next record
	it.currentPos += len(record) + file.IntSize
	return record
}

func (it *WalIterator) Close() {
	it.fm = nil
	it.block = file.Block{}

	iteratorPool.Put(it.page)
	it.page = nil
}

func (it *WalIterator) moveToBlock(block file.Block) {
	it.fm.Read(block, it.page)
	// boundary contains the offset of the most recently added record
	// read the boundary from the page
	it.boundary = it.page.Int(0)
	// position the iterator after the boundary offset
	it.currentPos = it.boundary
	it.block = block
}
