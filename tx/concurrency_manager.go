package tx

import "github.com/luigitni/simpledb/file"

// ConcurrencyManager is transaction specific.
// It keeps track of which locks the transaction currently has
// and interacts with the global lock table as needed.
type ConcurrencyManager struct {
	lockTable *LockTable
	locks     map[file.BlockID]string
}

func NewConcurrencyManager() ConcurrencyManager {
	return ConcurrencyManager{
		lockTable: GetLockTable(),
		locks:     map[file.BlockID]string{},
	}
}

// Slock attempts to obtain a shared lock on the block.
// The method will ask the locktable for an Slock if the tx has no locks on that block.
func (cm ConcurrencyManager) SLock(block file.Block) error {
	if _, ok := cm.locks[block.ID()]; !ok {
		if err := cm.lockTable.SLock(block); err != nil {
			return err
		}
		cm.locks[block.ID()] = Slock
	}
	return nil
}

// XLock attempts to obtain an exclusive lock on the block.
// If the tx does not have an Xlock on that block
// it first tries to S-lock the block, if needed, then upgrades it to an X lock.
func (cm ConcurrencyManager) XLock(block file.Block) error {
	if !cm.hasXLock(block) {
		// attempt to obtain an S lock first
		if err := cm.SLock(block); err != nil {
			return err
		}
		// upgrade ths S lock to an Xlock
		if err := cm.lockTable.XLock(block); err != nil {
			return err
		}
		cm.locks[block.ID()] = Xlock
	}
	return nil
}

// Releases all locks, by requesting the LockTable to unlock each one.
func (cm *ConcurrencyManager) Release() {
	for k := range cm.locks {
		cm.lockTable.UnlockByBlockId(k)
	}
	cm.locks = map[file.BlockID]string{}
}

// hasXLock returns true if the block has already an XLock assigned
func (cm ConcurrencyManager) hasXLock(block file.Block) bool {
	return cm.locks[block.ID()] == Xlock
}
