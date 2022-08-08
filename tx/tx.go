package tx

import (
	"sync/atomic"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

var nextTxNum int64

type Transaction interface {
	// Commit commits the current transaction
	// Flushes all the modified buffers and their log records
	// writes and flushes a commit record to the log
	// and finally releases all locks and unpins any pinned buffers.
	Commit()

	// Rollback rolls back the current transaction:
	// it undoes any modified values,
	// flushes the underlying buffers
	// writes and flushes a rollback record to the log
	// and finally releases all locks and unpins any pinned buffers
	Rollback()

	// Recover flushes all the modified buffers then goes through the log
	// rolling back all uncommitted transactions.
	// Finally, it writes a quiescent checkpoint record to the log.
	// This method is called during system startup, before user transactions begin.
	Recover()

	// Pin pins the specified block.
	// The transaction wraps and manages the buffer for the client.
	Pin(blockID file.BlockID)

	// Unpin unpins the specified block.
	// The transaction looks up the buffer pinned to this block and unpins it
	Unpin(blockID file.BlockID)

	// GetInt returns the integer value stored at the specified offset of the specified block.
	// It first attempts to obtrain an Slock on the block and then it calls the buffer to retrieve the value.
	// Returns ErrLockAcquisitionTimeout if the Slock can't be acquired
	GetInt(blockID file.BlockID, offset int) (int, error)

	// GetString returns the string value stored at offset of the given block.
	// It first attempts to obtain an S lock on the block, and then retrieves the value from the underlying buffers
	// Returns ErrLockAcquisitionTimeout if the Slock can't be acquired
	GetString(blockID file.BlockID, offset int) (string, error)

	// SetInt stores an integer at the specified offset of the given block.
	// It first obtains an X lock on the block, then creates a SETINT log record.
	// Finally, it writes the value to the underlying buffer, passing in the log sequence number
	// Returns ErrLockAcquisitionTimeout if the Xlock can't be acquired
	SetInt(blockID file.BlockID, offset int, val int, shouldLog bool) error

	// SetString stores a string at the specified offset of the given block.
	// It first attempts to obtain an X lock on the block, then creates a SETSTRING log record.
	// Finally, it writes the value to the underlying buffer, passing in the log sequence number.
	// Returns ErrLockAcquisitionTimeout if the Xlock can't be acquired
	SetString(blockID file.BlockID, offset int, val string, shouldLog bool) error

	// Size returns the number of blocks in the specified file.
	// It first obtains an Slock on the "end of file" block
	// before asking the file manager to return the file size, to avoid phantoms
	// Returns ErrLockAcquisitionTimeout if the Slock can't be acquired
	Size(fname string) (int, error)

	// Append attempts to append a new block to the end of the specific file and returns a reference to it
	// It first attempts to obtain an X lock on the "end of file" block.
	// Returns ErrLockAcquisitionTimeout if the X lock can't be acquired
	Append(fname string) (file.BlockID, error)

	// BlockSize returns the size of a block
	BlockSize() int

	// AvailableBuffers returns the number of unpinned buffers
	AvailableBuffers() int
}

// incrTxNum generates transaction ids
func incrTxNum() int {
	return int(atomic.AddInt64(&nextTxNum, 1))
}

type TransactionImpl struct {
	// todo: add recovery and concurrency managers
	bufMan     *buffer.Manager
	fileMan    *file.Manager
	recoverMan RecoveryManager
	concMan    ConcurrencyManager
	buffers    BufferList
	num        int
}

func NewTx(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) Transaction {
	tx := TransactionImpl{
		bufMan:  bm,
		fileMan: fm,
		num:     incrTxNum(),
		concMan: NewConcurrencyManager(),
		buffers: MakeBufferList(bm),
	}

	// assign the recovery manager to the tx
	// todo: this is ugly in Go, will refactor at a later stage.
	tx.recoverMan = NewRecoveryManagerForTx(tx, tx.num, lm, bm)

	return tx
}

func (tx TransactionImpl) Commit() {
	tx.recoverMan.Commit()
	// release all locks
	tx.concMan.Release()
	tx.buffers.UnpinAll()
}

func (tx TransactionImpl) Rollback() {
	tx.recoverMan.Rollback()
	tx.concMan.Release()
	tx.buffers.UnpinAll()
}

func (tx TransactionImpl) Recover() {
	tx.bufMan.FlushAll(tx.num)
	tx.recoverMan.Recover()
}

func (tx TransactionImpl) Pin(block file.BlockID) {
	tx.buffers.Pin(block)
}

func (tx TransactionImpl) Unpin(block file.BlockID) {
	tx.buffers.Unpin(block)
}

func (tx TransactionImpl) GetInt(block file.BlockID, offset int) (int, error) {
	if err := tx.concMan.SLock(block); err != nil {
		return 0, err
	}

	buf := tx.buffers.GetBuffer(block)
	v := buf.Contents().GetInt(offset)
	return v, nil
}

func (tx TransactionImpl) GetString(block file.BlockID, offset int) (string, error) {
	if err := tx.concMan.SLock(block); err != nil {
		return "", err
	}

	buf := tx.buffers.GetBuffer(block)
	v := buf.Contents().GetString(offset)
	return v, nil
}

func (tx TransactionImpl) SetInt(block file.BlockID, offset int, val int, shouldLog bool) error {
	if err := tx.concMan.XLock(block); err != nil {
		return err
	}

	buf := tx.buffers.GetBuffer(block)
	lsn := -1
	if shouldLog {
		lsn = tx.recoverMan.SetInt(buf, offset, val)
	}
	p := buf.Contents()
	p.SetInt(offset, val)
	// flag the underlying buffer as dirty to signal that a flush might be needed
	buf.SetModified(tx.num, lsn)
	return nil
}

func (tx TransactionImpl) SetString(block file.BlockID, offset int, val string, shouldLog bool) error {
	if err := tx.concMan.XLock(block); err != nil {
		return err
	}

	buf := tx.buffers.GetBuffer(block)
	lsn := -1
	if shouldLog {
		lsn = tx.recoverMan.SetString(buf, offset, val)
	}
	p := buf.Contents()
	p.SetString(offset, val)
	buf.SetModified(tx.num, lsn)
	return nil
}

func (tx TransactionImpl) Size(fname string) (int, error) {
	dummy := file.NewBlockID(fname, file.EOF)
	if err := tx.concMan.SLock(dummy); err != nil {
		return -1, err
	}

	return tx.fileMan.Size(fname), nil
}

func (tx TransactionImpl) Append(fname string) (file.BlockID, error) {
	dummy := file.NewBlockID(fname, file.EOF)
	if err := tx.concMan.XLock(dummy); err != nil {
		return file.BlockID{}, err
	}
	return tx.fileMan.Append(fname), nil
}

func (tx TransactionImpl) AvailableBuffers() int {
	return tx.bufMan.Available()
}

func (tx TransactionImpl) BlockSize() int {
	return tx.fileMan.BlockSize()
}
