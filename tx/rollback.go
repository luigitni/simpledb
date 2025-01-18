package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/types"
)

type rollbackLogRecord struct {
	txnum types.TxID
}

func newRollbackRecord(record recordBuffer) rollbackLogRecord {
	_ = record.readFixedLen(types.SizeOfTinyInt)

	return rollbackLogRecord{
		txnum: types.UnsafeFixedToInteger[types.TxID](record.readFixedLen(types.SizeOfTxID)),
	}
}

func (record rollbackLogRecord) Op() txType {
	return ROLLBACK
}

func (record rollbackLogRecord) TxNumber() types.TxID {
	return record.txnum
}

func (record rollbackLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record rollbackLogRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", record.txnum)
}

func logRollback(lm logManager, txnum types.TxID) int {
	p := logPools.small2ints.Get().(*[]byte)
	defer logPools.small2ints.Put(p)

	writeRollback(p, txnum)

	return lm.Append(*p)
}

func writeRollback(dst *[]byte, txnum types.TxID) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeFixedLen(
		types.SizeOfTinyInt,
		types.UnsafeIntegerToFixed[types.TinyInt](types.SizeOfTinyInt, types.TinyInt(ROLLBACK)),
	)
	rbuf.writeFixedLen(
		types.SizeOfTxID,
		types.UnsafeIntegerToFixed[types.TxID](types.SizeOfTxID, txnum),
	)
}
