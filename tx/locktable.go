package tx

import (
	"errors"
	"sync"
	"time"

	"github.com/luigitni/simpledb/types"
)

const lockReqTimeout = 3 * time.Second

const (
	Slock  = "S"
	Xlock  = "X"
	Unlock = "UNLOCK"
)

var ErrLockAcquisitionTimeout = errors.New("could not grant a lock on requested table")

// LockTable provides methods to lock and unlock blocks.
// If a transaction requests a lock that causes a conflict with an existing lock,
// the lock request is retired until it either succeeds or times out.
// NOTE: this algorithm differs from the one implemented by Sciore in the book.
// Also note: the algorithm implemeted here is not perfect:
// Requests that fail to acquire the lock are queued again against a non buffered channel.
// The order in which each request is then satisfied depends ultimately on the scheduler.
type LockTable struct {
	locks             map[types.BlockID]int
	lockRequestChan   chan lockRequest
	unlockRequestChan chan unlockRequest
	done              chan struct{}
}

type unlockRequest types.BlockID

func (req unlockRequest) BlockID() types.BlockID {
	return types.BlockID(req)
}

type lockRequest struct {
	timestamp time.Time
	key       types.BlockID
	lockType  string
	res       chan error
}

func (req lockRequest) done() <-chan error {
	return req.res
}

func makeLockRequest(key types.BlockID, lockType string) lockRequest {
	return lockRequest{
		timestamp: time.Now(),
		key:       key,
		lockType:  lockType,
		res:       make(chan error),
	}
}

func makeSLockRequest(block types.Block) lockRequest {
	return makeLockRequest(block.ID(), Slock)
}

func makeXLockRequest(block types.Block) lockRequest {
	return makeLockRequest(block.ID(), Xlock)
}

func makeUnlockRequest(block types.Block) unlockRequest {
	return unlockRequest(block.ID())
}

var lockTable *LockTable
var locktableOnce sync.Once

func GetLockTable() *LockTable {
	locktableOnce.Do(func() {
		lockTable = makeLockTable()
	})
	return lockTable
}

// MakeLockTable allocates and returns a LockTable object.
// The method also creates a loop that runs in a dedicated goroutine and which is responsible
// to resolve lock requests coming from clients.
func makeLockTable() *LockTable {
	lt := &LockTable{
		locks:             map[types.BlockID]int{},
		lockRequestChan:   make(chan lockRequest),
		unlockRequestChan: make(chan unlockRequest),
		done:              make(chan struct{}),
	}

	// this goroutine manages locking and unlocking.
	// for each lock requests it receives, checks if the lock can be granted.
	// if not, the request is queued again and retried until it either times out or succeeds
	go func(table *LockTable) {
		for {
			select {
			case req := <-lt.lockRequestChan:
				// lock request has timed out, return the error to the channel
				if time.Since(req.timestamp) > lockReqTimeout {
					req.res <- ErrLockAcquisitionTimeout
					close(req.res)
					continue
				}

				// get the request
				switch req.lockType {
				case Slock:
					if lt.hasXLock(req.key) {
						// requeue the request
						go func() { lt.lockRequestChan <- req }()
					} else {
						// grant the lock
						lt.locks[req.key]++
						close(req.res)
					}
				case Xlock:
					if lt.hasOtherSLocks(req.key) {
						// requeue the request until it either times out or is accepted
						go func() { lt.lockRequestChan <- req }()
					} else {
						lt.locks[req.key] = -1
						close(req.res)
					}
				}
			case ureq := <-lt.unlockRequestChan:
				lt.unlock(ureq.BlockID())
			case <-lt.done:
				return
			}
		}
	}(lt)

	return lt
}

func (lt *LockTable) Close() error {
	lt.done <- struct{}{}
	close(lt.done)
	return nil
}

// hasXlock returns true if an X lock exists for the given block
func (lt *LockTable) hasXLock(blockKey types.BlockID) bool {
	return lt.getLockVal(blockKey) < 0
}

// hasOtherSlocks returns true if at least one S lock exists for the given block
func (lt *LockTable) hasOtherSLocks(blockKey types.BlockID) bool {
	return lt.getLockVal(blockKey) > 1
}

// getLockVal returns -1 if the given block has an X lock associated
// if at least one S lock has been granted for the block, it returns the number of S locks
// returns 0 if no locks of any type have been granted to the block
func (lt *LockTable) getLockVal(key types.BlockID) int {
	return lt.locks[key]
}

func (lt *LockTable) unlock(key types.BlockID) {
	v := lt.getLockVal(key)
	if v > 1 {
		lt.locks[key]--
	} else {
		delete(lt.locks, key)
	}
}

// SLock grants a shared lock on the specified block.
// If an exclusive lock exists on the block when the method is called,
// then the calling thread will be placed on a wait list until the Xlock is released.
// If the goroutine remains on the wait list for longer than lockReqTimeout
// an ErrLockAcquisitionTimeout error is returned
func (lt LockTable) SLock(block types.Block) error {
	req := makeSLockRequest(block)
	lt.lockRequestChan <- req
	err := <-req.done()
	return err
}

// XLock grants an X lock on the specified block.
// If a lock of any type exists when the method is called,
// the calling client will block until either an X lock is granted
// or the timeout is reached, in which case it returns an ErrLockAcquisitionTimeout error.
func (lt LockTable) XLock(block types.Block) error {
	req := makeXLockRequest(block)
	lt.lockRequestChan <- req
	return <-req.done()
}

// Unlock releases a lock on the specified block.
func (lt LockTable) Unlock(block types.Block) {
	lt.unlockRequestChan <- makeUnlockRequest(block)
}

// Unlock releases a lock on the specified block using the block string identifier (String())
func (lt LockTable) UnlockByBlockId(id types.BlockID) {
	lt.unlockRequestChan <- unlockRequest(id)
}
