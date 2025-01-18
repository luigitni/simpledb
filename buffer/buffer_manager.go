package buffer

import (
	"sync"
	"time"

	"github.com/luigitni/simpledb/types"
)

const maxRetryTime = 5 * time.Second

type bufferFreeList struct {
	sync.RWMutex
	list []*Buffer
}

func newBufferFreeListFromSlice(bufs []*Buffer) *bufferFreeList {
	return &bufferFreeList{
		list: bufs,
	}
}

func (list *bufferFreeList) len() int {
	list.RLock()
	defer list.RUnlock()

	return len(list.list)
}

func (list *bufferFreeList) append(buf *Buffer, onAppend func()) {
	list.Lock()
	defer list.Unlock()

	list.list = append(list.list, buf)
	onAppend()
}

func (list *bufferFreeList) pop() *Buffer {
	if list.len() == 0 {
		return nil
	}

	list.Lock()
	defer list.Unlock()

	if l := len(list.list); l > 0 {
		b := list.list[l-1]
		list.list = list.list[:l-1]

		return b
	}

	return nil
}

// BufferManager is the BufferManager of the system.
// The buffer manager is the component of the engine which is responsible
// for the pages that hold user data.
// The buffer manager allocates a fixed set of pages, shared across all the system.
// In order to access a block, a client interacts with the Buffer BufferManager as follows:
// 1. The client asks the BM to pin a page from the buffer pool to that block
// 2. The client accesses the contents of that page as much as it desires.
// 3. When the client is done with the buffer, it requests the BM to unpin it
//
// Each page in the buffer pool has associated status information, such as
// wether it is pinned and, if, so, what block it is assigned to.
type BufferManager struct {
	freeList *bufferFreeList
	blockMap sync.Map
	sync.RWMutex
}

// NewBufferManager pre-allocates all shared buffers, as indicated by the size
// argument.
// A hashmap allows for fast access when looking for buffers assigned to a given block.
func NewBufferManager(fm fileManager, lm logManager, size int) *BufferManager {
	p := make([]*Buffer, size)
	for i := 0; i < len(p); i++ {
		p[i] = newBuffer(fm, lm)
	}

	return &BufferManager{
		freeList: newBufferFreeListFromSlice(p),
	}
}

func (man *BufferManager) Available() int {
	return man.freeList.len()
}

// FlushAll iterates over the assigned blocks and flush them to disk
// if and only if they belong to the current transaction
func (man *BufferManager) FlushAll(txnum types.TxID) {
	man.blockMap.Range(func(key, value any) bool {
		buf := value.(*Buffer)
		if buf.modifyingTxNumber() == txnum {
			buf.flush()
		}
		return true
	})
}

// Unpin unpins the specified buffer
func (man *BufferManager) Unpin(buf *Buffer) {
	buf.unpin()
}

// Pin tries to pin a buffer to the given block.
// If no buffer is available, clients will be put on wait until timeout.
// If no buffer becomes available until time out, an ErrClientTimeout is returned.
func (man *BufferManager) Pin(block types.Block) (*Buffer, error) {
	const maxDelay = 100 * time.Millisecond

	ts := time.Now()
	buf := man.tryToPin(block)
	var delay time.Duration

	for {
		if buf != nil {
			break
		}

		if time.Since(ts) > maxRetryTime {
			return nil, ErrClientTimeout
		}

		time.Sleep(delay)
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
		if delay == 0 {
			delay = 1 * time.Millisecond
		}

		buf = man.tryToPin(block)
	}

	return buf, nil
}

// tryToPin returns a buffer associated with the specified block, if available.
// otherwise it returns nil.
// The method first looks for an existing buffer assigned to the block and returns it if such buffer exists.
// Otherwise it looks for an unpinned buffer to assign to the block.
// The unpinned buffer is then flushed
// Returns nil if no buffer is available
func (man *BufferManager) tryToPin(block types.Block) *Buffer {
	buf := man.findExistingBuffer(block)

	if buf == nil {
		buf = man.chooseUnpinnedBuffer()

		if buf == nil {
			return nil
		}

		man.assignBufferToBlock(buf, block)
	}

	buf.pin()

	return buf
}

// findExistingBuffer tries to find a buffer that has already been assigned the given block.
// if found, the buffer is returned, otherwise the method returns nil
func (man *BufferManager) findExistingBuffer(block types.Block) *Buffer {
	if v, ok := man.blockMap.Load(block.ID()); ok {
		return v.(*Buffer)
	}

	return nil
}

// chooseUnpinnedBuffer looks for an unpinned buffer and returns it.
func (man *BufferManager) chooseUnpinnedBuffer() *Buffer {
	if b := man.freeList.pop(); b != nil {
		return b
	}

	man.markAndSweep()

	return man.freeList.pop()
}

func (man *BufferManager) markAndSweep() {
	man.blockMap.Range(func(key, value any) bool {
		buf := value.(*Buffer)

		if !buf.isPinned() {
			man.freeList.append(buf, func() {
				man.blockMap.Delete(key)
			})

			return false
		}

		buf.unpin()

		return true
	})
}

func (man *BufferManager) assignBufferToBlock(buf *Buffer, block types.Block) {
	man.blockMap.Store(block.ID(), buf)
	buf.flush()
	buf.assignBlock(block)
	buf.resetPins()
}
