package tx

import (
	"sync/atomic"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/log"
)

type logManager interface {
	Flush(lsn int)
	Append(record []byte) int
	Iterator() *log.WalIterator
}

// recoveryManager is the Recovery manager.
// It can be seen as a wrapper around a transaction
// (and its method could be implemented within the tx implementation tbh, out of a Java design)
// As the name indicates, it manages transaction recovery from the WAL.
// The recoveryManager has three roles:
// 1. to write WAL records
// 2. to rollback transactions
// 3. to recover the database after a system crash
type recoveryManager struct {
	lm    logManager
	bm    *buffer.BufferManager
	tx    Transaction
	txnum int
}

// RecoveryManagerForTx returns a recovery manager for the given transaction and txnum
func newRecoveryManagerForTx(tx Transaction, txnum int, lm logManager, bm *buffer.BufferManager) recoveryManager {
	man := recoveryManager{
		lm:    lm,
		bm:    bm,
		tx:    tx,
		txnum: txnum,
	}
	logStart(lm, txnum)
	return man
}

// setInt writes a SETINT record to the log and returns its lsn.
// buff is the buffer containing the page
// offset is the offset of the value within the page
// val is the value to be written
// todo: why is the actual implementation passing the oldval?
func (man recoveryManager) setInt(buff *buffer.Buffer, offset int, val int) int {
	oldval := buff.Contents().Int(offset)
	block := buff.Block()
	return logSetInt(man.lm, man.txnum, block, offset, oldval)
}

// setString writes a SETSTRING record to the log and return its lsn.
// buff is the buffer containing the page,
// offset is the offset of the value within the page
// newval is the value to be written.
// WHY IS IT PASSING OLDVAL??
func (man recoveryManager) setString(buff *buffer.Buffer, offset int, val string) int {
	oldval := buff.Contents().String(offset)
	block := buff.Block()
	return logSetString(man.lm, man.txnum, block, offset, oldval)
}

// Write a commit record to the log and flushes it to disk
func (man recoveryManager) commit() {
	man.bm.FlushAll(man.txnum)
	lsn := logCommit(man.lm, man.txnum)
	man.lm.Flush(lsn)
}

// rollback writes a rollback record to the log and flushes it to disk
func (man recoveryManager) rollback() {
	man.doRollback()
	man.bm.FlushAll(man.txnum)
	lsn := logRollback(man.lm, man.txnum)
	man.lm.Flush(lsn)
}

// doRollback rolls the transaction back by iterating through log records
// until it finds the transaction's START record, calling tx.Undo() for each of the TX log records.
func (man recoveryManager) doRollback() {
	reader := man.lm.Iterator()
	defer reader.Close()

	for {
		if !reader.HasNext() {
			break
		}

		// get the next log entry - remember, the log is written from right to left
		bytes := reader.Next()
		record := createLogRecord(bytes)
		if record.TxNumber() == man.txnum {
			if record.Op() == START {
				return
			}
			record.Undo(man.tx)
		}
	}
}

// recover recovers uncompleted transactions from the log
// and then writes a quiescent checkpoint record to the log and flushes it
func (man recoveryManager) recover() {
	maxTx := man.doRecover()
	man.bm.FlushAll(man.txnum)
	lsn := logCheckpoint(man.lm)
	man.lm.Flush(lsn)

	// set the next tx number to the max transaction number
	atomic.StoreInt64(&nextTxNum, int64(maxTx))
}

// doRecover does a complete database recovery.
// The method iterates through the log records.
// Whenever it finds a log record for an unfinished transaction,
// it calls undo() on that record.
// The method stops when it encounters a CHECKPOINT record or the end of the log file
func (man recoveryManager) doRecover() int {
	finishedTxs := map[int]struct{}{}
	reader := man.lm.Iterator()
	defer reader.Close()

	maxTxNum := 0

	for {
		if !reader.HasNext() {
			break
		}

		bytes := reader.Next()
		record := createLogRecord(bytes)
		if record.Op() == CHECKPOINT {
			if record.TxNumber() > maxTxNum {
				maxTxNum = record.TxNumber()
			}

			return maxTxNum
		}
		// if the transaction ended with a commit or a rollback,
		// add it to the list of completed txs
		if record.Op() == COMMIT || record.Op() == ROLLBACK {

			txNum := record.TxNumber()
			finishedTxs[txNum] = struct{}{}

			if txNum > maxTxNum {
				maxTxNum = txNum
			}

			// otherwise, and if the record's transaction does not belong to the list of finished txs
			// undo it.
		} else if _, ok := finishedTxs[record.TxNumber()]; !ok {
			record.Undo(man.tx)
		}
	}

	return maxTxNum
}
