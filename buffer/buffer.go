package buffer

import (
	"sync"

	"github.com/luigitni/simpledb/file"
)

const maxPins = 5

// A Buffer stores pages status information, such as if it's pinned
// and if that's the case, what block it is assigned to.
// Each buffer observes the changes done to its page and it is responsible
// for writing its modifications to disk.
// A Buffer can reduce disk access by delaying flushing:
// for example, if a page is modified several times , then it is more
// efficient to write the page once, after all modifications.
// The Buffer will flush its underlying page only in case the page is
// assigned to a different block, or if the recovery manager needs to write to disk to guard agains a crash.
type Buffer struct {
	sync.RWMutex
	fm       fileManager
	lm       logManager
	contents *file.Page
	block    file.Block
	pins     int
	txnum    int
	lsn      int
}

func newBuffer(fm fileManager, lm logManager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPage(),
		txnum:    -1,
		lsn:      -1,
	}
}

func (buf *Buffer) Contents() *file.Page {
	return buf.contents
}

func (buf *Buffer) Block() file.Block {
	return buf.block
}

// SetModified
func (buf *Buffer) SetModified(txnum int, lsn int) {
	buf.Lock()
	defer buf.Unlock()

	buf.txnum = txnum
	if lsn >= 0 {
		buf.lsn = lsn
	}
}

func (buf *Buffer) modifyingTxNumber() int {
	buf.RLock()
	defer buf.RUnlock()

	return buf.txnum
}

func (buf *Buffer) isPinned() bool {
	buf.RLock()
	defer buf.RUnlock()

	return buf.pins > 0
}

// flush ensures that the buffer's assigned disk block has the same values as its page.
// If the underlying page has not been modified (txnum = 0), nothing is written to disk
// otherwise, ensures that the current log is flushed to disk if needed
// and then writes the page to disk.
func (buf *Buffer) flush() {
	buf.Lock()
	defer buf.Unlock()

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
// previous block are preserved.
// The buffer is then associated with the specified block, reading its contents from disk.
func (buf *Buffer) assignBlock(block file.Block) {
	buf.Lock()
	defer buf.Unlock()
	// flush current contents
	buf.block = block
	// reads the block into the buffer page
	buf.fm.Read(buf.block, buf.contents)
}

func (buf *Buffer) resetPins() {
	buf.Lock()
	defer buf.Unlock()

	buf.pins = 0
}

func (buf *Buffer) pin() {
	buf.Lock()
	defer buf.Unlock()

	if buf.pins < maxPins {
		buf.pins++
	}
}

func (buf *Buffer) unpin() bool {
	buf.Lock()
	defer buf.Unlock()

	if buf.pins == 0 {
		return false
	}

	buf.pins--

	return buf.pins == 0
}
