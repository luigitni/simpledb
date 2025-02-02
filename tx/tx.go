package tx

import (
	"sync/atomic"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/storage"
)

var nextTxNum uint32 = uint32(storage.TxIDStart)

type Transaction interface {
	// Id returns the transaction id
	Id() storage.TxID

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
	Pin(blockID storage.Block)

	// Unpin unpins the specified block.
	// The transaction looks up the buffer pinned to this block and unpins it
	Unpin(blockID storage.Block)

	// Int returns the integer value stored at the specified offset of the specified block.
	// It first attempts to obtrain an Slock on the block and then it calls the buffer to retrieve the value.
	// Returns ErrLockAcquisitionTimeout if the Slock can't be acquired
	FixedLen(blockID storage.Block, offset storage.Offset, size storage.Size) (storage.FixedLen, error)

	// String returns the string value stored at offset of the given block.
	// It first attempts to obtain an S lock on the block, and then retrieves the value from the underlying buffers
	// Returns ErrLockAcquisitionTimeout if the Slock can't be acquired
	VarLen(blockID storage.Block, offset storage.Offset) (storage.Varlen, error)

	// SetInt stores an integer at the specified offset of the given block.
	// It first obtains an X lock on the block, then creates a SETINT log record.
	// Finally, it writes the value to the underlying buffer, passing in the log sequence number
	// Returns ErrLockAcquisitionTimeout if the Xlock can't be acquired
	SetFixedLen(blockID storage.Block, offset storage.Offset, size storage.Size, val storage.FixedLen, shouldLog bool) error

	// SetString stores a string at the specified offset of the given block.
	// It first attempts to obtain an X lock on the block, then creates a SETSTRING log record.
	// Finally, it writes the value to the underlying buffer, passing in the log sequence number.
	// Returns ErrLockAcquisitionTimeout if the Xlock can't be acquired
	SetVarLen(blockID storage.Block, offset storage.Offset, val storage.Varlen, shouldLog bool) error

	// Size returns the number of blocks in the specified file.
	// It first obtains an Slock on the "end of file" block
	// before asking the file manager to return the file size, to avoid phantoms
	// Returns ErrLockAcquisitionTimeout if the Slock can't be acquired
	Size(fname string) (storage.Long, error)

	// Append attempts to append a new block to the end of the specific file and returns a reference to it
	// It first attempts to obtain an X lock on the "end of file" block.
	// Returns ErrLockAcquisitionTimeout if the X lock can't be acquired
	Append(fname string) (storage.Block, error)

	// BlockSize returns the size of a block
	BlockSize() storage.Offset
}

// incrTxNum generates transaction ids
func incrTxNum() storage.TxID {
	return storage.TxID(atomic.AddUint32(&nextTxNum, 1))
}

var _ Transaction = transactionImpl{}

type transactionImpl struct {
	bufMan     *buffer.BufferManager
	fileMan    *file.FileManager
	recoverMan recoveryManager
	concMan    ConcurrencyManager
	buffers    bufferList
	num        storage.TxID
}

func NewTx(fm *file.FileManager, lm logManager, bm *buffer.BufferManager) Transaction {
	tx := transactionImpl{
		bufMan:  bm,
		fileMan: fm,
		num:     incrTxNum(),
		concMan: NewConcurrencyManager(),
		buffers: makeBufferList(bm),
	}

	// assign the recovery manager to the tx
	// todo: this is ugly in Go, will refactor at a later stage.
	tx.recoverMan = newRecoveryManagerForTx(tx, tx.num, lm, bm)

	return tx
}

func (tx transactionImpl) Id() storage.TxID {
	return tx.num
}

func (tx transactionImpl) Commit() {
	tx.recoverMan.commit()
	// release all locks
	tx.concMan.Release()
	tx.buffers.unpinAll()
}

func (tx transactionImpl) Rollback() {
	tx.recoverMan.rollback()
	tx.concMan.Release()
	tx.buffers.unpinAll()
}

func (tx transactionImpl) Recover() {
	tx.bufMan.FlushAll(tx.num)
	tx.recoverMan.recover()
}

func (tx transactionImpl) Pin(block storage.Block) {
	tx.buffers.pin(block)
}

func (tx transactionImpl) Unpin(block storage.Block) {
	tx.buffers.unpin(block)
}

func (tx transactionImpl) FixedLen(block storage.Block, offset storage.Offset, size storage.Size) (storage.FixedLen, error) {
	if err := tx.concMan.SLock(block); err != nil {
		return nil, err
	}

	buf := tx.buffers.buffer(block)
	v := buf.Contents().UnsafeGetFixedLen(offset, size)
	return v, nil
}

func (tx transactionImpl) VarLen(block storage.Block, offset storage.Offset) (storage.Varlen, error) {
	if err := tx.concMan.SLock(block); err != nil {
		return storage.Varlen{}, err
	}

	buf := tx.buffers.buffer(block)
	v := buf.Contents().UnsafeGetVarlen(offset)
	return v, nil
}

func (tx transactionImpl) SetFixedLen(block storage.Block, offset storage.Offset, size storage.Size, val storage.FixedLen, shouldLog bool) error {
	if err := tx.concMan.XLock(block); err != nil {
		return err
	}

	buf := tx.buffers.buffer(block)
	lsn := -1
	if shouldLog {
		lsn = tx.recoverMan.setFixedLen(buf, offset, size, val)
	}
	p := buf.Contents()
	p.UnsafeSetFixedLen(offset, size, val)
	// flag the underlying buffer as dirty to signal that a flush might be needed
	buf.SetModified(tx.num, lsn)
	return nil
}

func (tx transactionImpl) SetVarLen(block storage.Block, offset storage.Offset, val storage.Varlen, shouldLog bool) error {
	if err := tx.concMan.XLock(block); err != nil {
		return err
	}

	buf := tx.buffers.buffer(block)
	lsn := -1
	if shouldLog {
		lsn = tx.recoverMan.setVarLen(buf, offset, val)
	}
	p := buf.Contents()
	p.UnsafeSetVarlen(offset, val)
	buf.SetModified(tx.num, lsn)
	return nil
}

func (tx transactionImpl) Size(fname string) (storage.Long, error) {
	dummy := storage.NewBlock(fname, storage.EOF)
	if err := tx.concMan.SLock(dummy); err != nil {
		return 0, err
	}

	return tx.fileMan.Size(fname), nil
}

func (tx transactionImpl) Append(fname string) (storage.Block, error) {
	dummy := storage.NewBlock(fname, storage.EOF)
	if err := tx.concMan.XLock(dummy); err != nil {
		return storage.Block{}, err
	}
	return tx.fileMan.Append(fname), nil
}

// availableBuffers returns the number of unpinned buffers
func (tx transactionImpl) availableBuffers() int {
	return tx.bufMan.Available()
}

func (tx transactionImpl) BlockSize() storage.Offset {
	return tx.fileMan.BlockSize()
}
