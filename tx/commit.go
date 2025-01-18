package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/types"
)

type commitLogRecord struct {
	txnum types.TxID
}

func newCommitRecord(record recordBuffer) commitLogRecord {
	_ = record.readFixedLen(types.SizeOfTinyInt)

	return commitLogRecord{
		txnum: types.UnsafeFixedToInteger[types.TxID](record.readFixedLen(types.SizeOfTxID)),
	}
}

func (record commitLogRecord) Op() txType {
	return COMMIT
}

func (record commitLogRecord) TxNumber() types.TxID {
	return record.txnum
}

func (record commitLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record commitLogRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", record.txnum)
}

func logCommit(lm logManager, txnum types.TxID) int {
	p := logPools.small2ints.Get().(*[]byte)
	defer logPools.small2ints.Put(p)

	writeCommit(p, txnum)

	return lm.Append(*p)
}

func writeCommit(dst *[]byte, txnum types.TxID) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeFixedLen(
		types.SizeOfTinyInt,
		types.UnsafeIntegerToFixed[types.TinyInt](types.SizeOfTinyInt, types.TinyInt(COMMIT)),
	)
	rbuf.writeFixedLen(
		types.SizeOfTxID,
		types.UnsafeIntegerToFixed[types.TxID](types.SizeOfTxID, txnum),
	)
}
