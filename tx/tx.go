package tx

import (
	"sync/atomic"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

var nextTxNum int64

type Transaction interface {
	Commit()

	Rollback()

	Recover()

	Pin(blockID file.BlockID)

	Unpin(blockID file.BlockID)

	GetInt(blockID file.BlockID, offset int) int

	GetString(blockID file.BlockID, offset int) string

	SetInt(blockID file.BlockID, offset int, val int, shouldLog bool)

	SetString(blockID file.BlockID, offset int, val string, shouldLog bool)

	Size(fname string) int

	Append(fname string) file.BlockID

	BlockSize() int

	AvailableBuffers() int
}

type TransactionImpl struct {
	// todo: add recovery and concurrency managers
	bufMan     *buffer.Manager
	fileMan    *file.Manager
	recoverMan Manager
	num        int
	buffers    []*buffer.Buffer
}

func NewTx(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) Transaction {
	tx := TransactionImpl{
		bufMan:  bm,
		fileMan: fm,
		num:     incrTxNum(),
		buffers: []*buffer.Buffer{}, // todo
	}

	tx.recoverMan = NewRecoveryManagerForTx(tx, tx.num, lm, bm)

	return tx
}

// Append implements Transaction
func (TransactionImpl) Append(fname string) file.BlockID {
	panic("unimplemented")
}

// AvailableBuffers implements Transaction
func (TransactionImpl) AvailableBuffers() int {
	panic("unimplemented")
}

// BlockSize implements Transaction
func (TransactionImpl) BlockSize() int {
	panic("unimplemented")
}

// GetInt implements Transaction
func (TransactionImpl) GetInt(blockID file.BlockID, offset int) int {
	panic("unimplemented")
}

// GetString implements Transaction
func (TransactionImpl) GetString(blockID file.BlockID, offset int) string {
	panic("unimplemented")
}

// Recover flushes all the modified buffers then goes through the log
// rolling back all uncommitted transactions.
// Finally, it writes a quiescent checkpoint record to the log.
// This method is called during system startup, before user transactions begin.
func (tx TransactionImpl) Recover() {
	tx.bufMan.FlushAll(tx.num)
	tx.recoverMan.Recover()
}

// SetInt implements Transaction
func (TransactionImpl) SetInt(blockID file.BlockID, offset int, val int, shouldLog bool) {
	panic("unimplemented")
}

// Size implements Transaction
func (TransactionImpl) Size(fname string) int {
	panic("unimplemented")
}

func incrTxNum() int {
	return int(atomic.AddInt64(&nextTxNum, 1))
}

func (tx TransactionImpl) Commit() {
	// todo
}

func (tx TransactionImpl) Rollback() {
	// todo
}

func (tx TransactionImpl) Pin(blockID file.BlockID) {
	// todo
}

func (tx TransactionImpl) Unpin(blockId file.BlockID) {
	// todo
}

func (tx TransactionImpl) SetString(blockId file.BlockID, offset int, v string, mustLog bool) {
	// todo
}
