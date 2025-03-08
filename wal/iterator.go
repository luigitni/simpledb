package wal

import (
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/storage"
)

// WalIterator iterates over WAL file blocks.
// It reads blocks from disk into a page, and iterates
// records from right to left within each block.
type WalIterator struct {
	fm         *file.FileManager
	block      storage.Block
	page       *storage.Page
	currentPos storage.Offset
	boundary   storage.Offset
}

func newWalIterator(page *storage.Page, fm *file.FileManager, start storage.Block) *WalIterator {
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
		prev := it.block.Number() - 1
		if prev == storage.EOF {
			return nil
		}

		block := storage.NewBlock(it.block.FileName(), prev)
		it.moveToBlock(block)
	}

	// each WAL record is prepended by its size
	record := it.page.GetVarlen(it.currentPos)
	// move the iterator pointer to the next record
	it.currentPos += storage.Offset(record.Size())
	return record.Data()
}

func (it *WalIterator) Close() {
	it.fm = nil
	it.block = storage.Block{}

	iteratorPool.Put(it.page)
	it.page = nil
}

func (it *WalIterator) moveToBlock(block storage.Block) {
	it.fm.Read(block, it.page)
	// boundary contains the offset of the most recently added record
	// read the boundary from the page
	it.boundary = it.page.GetFixedLen(0, storage.SizeOfOffset).AsOffset()
	// position the iterator after the boundary offset
	it.currentPos = it.boundary
	it.block = block
}
