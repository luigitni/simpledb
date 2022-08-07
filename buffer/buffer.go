package buffer

import (
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type Buffer struct {
	fm       *file.Manager
	lm       *log.Manager
	contents *file.Page
	block    file.BlockID
	// how many pins
	// todo: can use an atomic int to leverage CAS
	pins  int
	txnum int
	// log sequence number
	lsn int
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPageWithSize(fm.BlockSize()),
		txnum:    -1,
		lsn:      -1,
	}
}

func (buf *Buffer) Contents() *file.Page {
	return buf.contents
}

func (buf *Buffer) BlockID() file.BlockID {
	return buf.block
}

func (buf *Buffer) SetModified(txnum int, lsn int) {
	buf.txnum = txnum
	if lsn >= 0 {
		buf.lsn = lsn
	}
}

func (buf *Buffer) ModifyingTx() int {
	return buf.txnum
}

func (buf *Buffer) IsPinned() bool {
	return buf.pins > 0
}

// flush ensures that the buffer's assigned disk block has the same values as its page.
// If the underlying page has not been modified (txnum = 0), nothing is written to disk
// otherwise, ensures that the current log is flushed to disk if needed
// and then writes the page to disk.
func (buf *Buffer) flush() {
	if buf.txnum > 0 {
		// flush the log to current block
		buf.lm.Flush(buf.lsn)
		// persist contents of the buffer to the assigned block
		buf.fm.Write(buf.block, buf.contents)
		buf.txnum = -1
	}
}

// assignToBlock associates a buffer with a disk block.
// The buffer is first flushed so that any modifications to the
// previous block are prserved.
// The buffer is then associated with the specified block, reading its contents from disk.
func (buf *Buffer) assignToBlock(block file.BlockID) {
	// flush current contents
	buf.flush()
	buf.block = block
	// reads the block into the buffer page
	buf.fm.Read(buf.block, buf.contents)
	buf.pins = 0
}

func (buf *Buffer) pin() {
	buf.pins++
}

func (buf *Buffer) unpin() {
	buf.pins--
}
