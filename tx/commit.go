package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/storage"
)

type commitLogRecord struct {
	txnum storage.TxID
}

func newCommitRecord(record recordBuffer) commitLogRecord {
	_ = record.readFixedLen(storage.SizeOfTinyInt)

	return commitLogRecord{
		txnum: storage.UnsafeFixedToInteger[storage.TxID](record.readFixedLen(storage.SizeOfTxID)),
	}
}

func (record commitLogRecord) Op() txType {
	return COMMIT
}

func (record commitLogRecord) TxNumber() storage.TxID {
	return record.txnum
}

func (record commitLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record commitLogRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", record.txnum)
}

func logCommit(lm logManager, txnum storage.TxID) int {
	p := logPools.small2ints.Get().(*[]byte)
	defer logPools.small2ints.Put(p)

	writeCommit(p, txnum)

	return lm.Append(*p)
}

func writeCommit(dst *[]byte, txnum storage.TxID) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeFixedLen(
		storage.SizeOfTinyInt,
		storage.UnsafeIntegerToFixed[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(COMMIT)),
	)
	rbuf.writeFixedLen(
		storage.SizeOfTxID,
		storage.UnsafeIntegerToFixed[storage.TxID](storage.SizeOfTxID, txnum),
	)
}
