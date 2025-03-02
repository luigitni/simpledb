package tx

import (
	"fmt"
	"unsafe"

	"github.com/luigitni/simpledb/storage"
)

type commitLogRecord struct {
	txnum storage.TxID
}

const sizeOfCommitRecord = int(unsafe.Sizeof(commitLogRecord{})) + int(storage.SizeOfTinyInt)

func newCommitRecord(record *recordBuffer) commitLogRecord {
	f := record.readFixedLen(storage.SizeOfTinyInt)
	if v := txTypeFromFixedLen(f); v != COMMIT {
		panic(fmt.Sprintf("bad %s record: %s", COMMIT, v))
	}

	return commitLogRecord{
		txnum: storage.FixedLenToInteger[storage.TxID](record.readFixedLen(storage.SizeOfTxID)),
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
	buf := make([]byte, sizeOfCommitRecord)
	writeCommit(buf, txnum)

	return lm.Append(buf)
}

func writeCommit(dst []byte, txnum storage.TxID) {
	rbuf := recordBuffer{bytes: dst}
	rbuf.writeFixedLen(
		storage.SizeOfTinyInt,
		storage.IntegerToFixedLen[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(COMMIT)),
	)
	rbuf.writeFixedLen(
		storage.SizeOfTxID,
		storage.IntegerToFixedLen[storage.TxID](storage.SizeOfTxID, txnum),
	)
}
