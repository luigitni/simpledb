package buffer

import (
	"sync"
	"time"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

const maxTimeSeconds = 5 * time.Second

// Manager is the BufferManager of the system.
// The buffer manager is the component of the engine which is responsible
// for the pages that hold user data.
// The buffer manager allocates a fixed set of pages, called the buffer pool.
// In order to access a block, a client interacts with the Buffer Manager as follows:
// 1. The client asks the BM to pin a page from the buffer pool to that block
// 2. The client accesses the contents of that page as much as it desires.
// 3. When the client is done with the buffer, it requests the BM to unpin it
//
// Each page in the buffer pool has associated status information, such as
// whether it is pinned and, if, so, what block it is assigned to.
// A buffer it the object that contains this information.
type Manager struct {
	pool      []*Buffer
	available int
	sync.Mutex
}

func NewBufferManager(fm *file.Manager, lm *log.LogManager, size int) *Manager {
	p := make([]*Buffer, size)
	for i := 0; i < len(p); i++ {
		p[i] = NewBuffer(fm, lm)
	}
	return &Manager{
		pool:      p,
		available: size,
	}
}

func (man *Manager) Available() int {
	man.Lock()
	defer man.Unlock()
	return man.available
}

func (man *Manager) FlushAll(txnum int) {
	man.Lock()
	defer man.Unlock()
	for _, b := range man.pool {
		if b.ModifyingTx() == txnum {
			b.flush()
		}
	}
}

// Unpin unpins the specified buffer
func (man *Manager) Unpin(buf *Buffer) {
	man.Lock()
	defer man.Unlock()

	buf.unpin()

	if !buf.IsPinned() {
		man.available++
	}
}

// Pin tries to pin a buffer to the given block.
// If no buffer is available, clients will be put on wait until timeout.
// If no buffer becomes available until time out, an ErrClientTimeout is returned.
func (man *Manager) Pin(block file.BlockID) (*Buffer, error) {
	ts := time.Now()
	buf := man.tryToPin(block)

	for {
		if buf != nil {
			break
		}

		if time.Since(ts) > maxTimeSeconds {
			return nil, ErrClientTimeout
		}

		// the original Java implementation uses Thread.wait to suspend the thread, waiting for a call
		// to notifyAll in Unpin.
		// There is no such out of the box mechanism in go, but by calling time.Sleep the runtime scheduler
		// will allocate execution time to another goroutine, generating an analogous result.
		time.Sleep(time.Millisecond * 300)

		buf = man.tryToPin(block)
	}

	return buf, nil
}

// tryToPin returns a buffer associated with the specified block, if available.
// otherwise it returns nil.
// The method first looks for an existing buffer assigned to the block and returns it if such buffer exists.
// Otherwise it looks for an unpinned buffer to assign to the block.
// Returns nil if no buffer is available
func (man *Manager) tryToPin(block file.BlockID) *Buffer {
	man.Lock()
	defer man.Unlock()

	b := man.findExistingBuffer(block)
	if b == nil {
		b = man.chooseUnpinnedBuffer()
		if b == nil {
			return nil
		}
		b.assignToBlock(block)
	}

	if !b.IsPinned() {
		man.available--
	}

	b.pin()

	return b
}

// findExistingBuffer tries to find a buffer that has already been assigned the given block.
// if found, the buffer is returned, otherwise the method returns nil
func (man *Manager) findExistingBuffer(block file.BlockID) *Buffer {
	for _, b := range man.pool {
		if b.block.Equals(block) {
			return b
		}
	}

	return nil
}

// chooseUnpinnedBuffer trivially searches for the first unpinned buffer and returns it.
// returns nil if no unpinned buffer is available
// Changing strategy to select an unpinned buffer is key to performance optimization (e.g. avoid thrashing of the same buffer)
func (man *Manager) chooseUnpinnedBuffer() *Buffer {
	for _, b := range man.pool {
		if !b.IsPinned() {
			return b
		}
	}

	return nil
}
