package tx

import (
	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/log"
)

// RecoveryManager is the Recovery manager.
// It can be seen as a wrapper around a transaction
// (and its method could be implemented within the tx implementation tbh, out of a Java design)
// As the name indicates, it manages transaction recovery from the WAL
type RecoveryManager struct {
	lm    *log.Manager
	bm    *buffer.Manager
	tx    Transaction
	txnum int
}

// RecoveryManagerForTx returns a recovery manager for the given transaction and txnum
func NewRecoveryManagerForTx(tx Transaction, txnum int, lm *log.Manager, bm *buffer.Manager) RecoveryManager {
	man := RecoveryManager{
		lm:    lm,
		bm:    bm,
		tx:    tx,
		txnum: txnum,
	}
	LogStart(lm, txnum)
	return man
}

// SetInt writes a SETINT record to the log and returns its lsn.
// buff is the buffer containing the page
// offset is the offset of the value within the page
// val is the value to be written
// todo: why is the actual implementation passing the oldval?
func (man RecoveryManager) SetInt(buff *buffer.Buffer, offset int, val int) int {
	oldval := buff.Contents().GetInt(offset)
	block := buff.BlockID()
	return LogSetInt(man.lm, man.txnum, block, offset, oldval)
}

// SetString writes a SETSTRING record to the log and return its lsn.
// buff is the buffer containing the page,
// offset is the offset of the value within the page
// newval is the value to be written.
// WHY IS IT PASSING OLDVAL??
func (man RecoveryManager) SetString(buff *buffer.Buffer, offset int, val string) int {
	oldval := buff.Contents().GetString(offset)
	block := buff.BlockID()
	return LogSetString(man.lm, man.txnum, block, offset, oldval)
}

// Write a commit record to the log and flushes it to disk
func (man RecoveryManager) Commit() {
	man.bm.FlushAll(man.txnum)
	lsn := LogCommit(man.lm, man.txnum)
	man.lm.Flush(lsn)
}

// Rollback writes a rollback record to the log and flushes it to disk
func (man RecoveryManager) Rollback() {
	man.doRollback()
	man.bm.FlushAll(man.txnum)
	lsn := LogRollback(man.lm, man.txnum)
	man.lm.Flush(lsn)
}

// doRollback rolls the transaction back by iterating through log records
// until it finds the transaction's START record, calling tx.Undo() for each of the TX log records.
func (man RecoveryManager) doRollback() {
	reader := man.lm.Iterator()
	for {
		if !reader.HasNext() {
			break
		}

		// get the next log entry - remember, the log is written from right to left
		bytes := reader.Next()
		record := CreateLogRecord(bytes)
		if record.TxNumber() == man.txnum {
			if record.Op() == START {
				return
			}
			record.Undo(man.tx)
		}
	}
}

// Recover recovers uncompleted transactions from the log
// and then writes a quiescent checkpoint record to the log and flushes it
func (man RecoveryManager) Recover() {
	man.doRecover()
	man.bm.FlushAll(man.txnum)
	lsn := LogCheckpoint(man.lm)
	man.lm.Flush(lsn)
}

// doRecover does a complete database recovery.
// The method iterates through the log records.
// Whenever it finds a log record for an unfinished transaction,
// it calls undo() on that record.
// The method stops when it encounters a CHECKPOINT record or the end of the log file
func (man RecoveryManager) doRecover() {
	finishedTxs := map[int]struct{}{}
	reader := man.lm.Iterator()
	for {
		if !reader.HasNext() {
			break
		}

		bytes := reader.Next()
		record := CreateLogRecord(bytes)
		if record.Op() == CHECKPOINT {
			return
		}
		// if the transaction ended with a commit or a rollback,
		// add it to the list of completed txs
		if record.Op() == COMMIT || record.Op() == ROLLBACK {
			finishedTxs[record.TxNumber()] = struct{}{}

			// otherwise, and if the record's transaction does not belong to the list of finished txs
			// undo it.
		} else if _, ok := finishedTxs[record.TxNumber()]; !ok {
			record.Undo(man.tx)
		}
	}
}
