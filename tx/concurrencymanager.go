package tx

import "github.com/luigitni/simpledb/file"

// ConcurrencyManager is transaction specific.
// It keeps track of which locks the transaction currently has
// and interacts with the global lock table as needed.
type ConcurrencyManager struct {
	lockTable *LockTable
	locks     map[string]string
}

func NewConcurrencyManager() ConcurrencyManager {
	return ConcurrencyManager{
		lockTable: GetLockTable(),
		locks:     map[string]string{},
	}
}

// Slock attempts to obtain a shared lock on the block.
// The method will ask the locktable for an Slock if the tx has no locks on that block.
func (cm ConcurrencyManager) SLock(block file.BlockID) error {
	if _, ok := cm.locks[block.String()]; !ok {
		if err := cm.lockTable.SLock(block); err != nil {
			return err
		}
		cm.locks[block.String()] = Slock
	}
	return nil
}

// XLock attempts to obtain an exclusive lock on the block.
// If the tx does not have an Xlock on that block
// it first tries to S-lock the block, if needed, then upgrades it to an X lock.
func (cm ConcurrencyManager) XLock(block file.BlockID) error {
	if !cm.hasXLock(block) {
		// attempt to obtain an S lock first
		if err := cm.SLock(block); err != nil {
			return err
		}
		// upgrade ths S lock to an Xlock
		if err := cm.lockTable.XLock(block); err != nil {
			return err
		}
		cm.locks[block.String()] = Xlock
	}
	return nil
}

// Releases all locks, by requesting the LockTable to unlock each one.
func (cm *ConcurrencyManager) Release() {
	for k := range cm.locks {
		cm.lockTable.UnlockByBlockId(k)
	}
	cm.locks = map[string]string{}
}

// hasXLock returns true if the block has already an XLock assigned
func (cm ConcurrencyManager) hasXLock(block file.BlockID) bool {
	return cm.locks[block.String()] == Xlock
}
