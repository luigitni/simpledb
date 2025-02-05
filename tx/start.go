package tx

import (
	"fmt"
	"unsafe"

	"github.com/luigitni/simpledb/storage"
)

type startLogRecord struct {
	txnum storage.TxID
}

const sizeOfStartRecord = int(unsafe.Sizeof(startLogRecord{})) + int(storage.SizeOfTinyInt)

func newStartLogRecord(record *recordBuffer) startLogRecord {
	_ = record.readFixedLen(storage.SizeOfTinyInt)

	return startLogRecord{
		txnum: storage.UnsafeFixedToInteger[storage.TxID](record.readFixedLen(storage.SizeOfTxID)),
	}
}

func (record startLogRecord) Op() txType {
	return START
}

func (record startLogRecord) TxNumber() storage.TxID {
	return record.txnum
}

func (record startLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record startLogRecord) String() string {
	return fmt.Sprintf("<START %d>", record.txnum)
}

func logStart(lm logManager, txnum storage.TxID) int {
	buf := make([]byte, sizeOfStartRecord)
	writeStart(buf, txnum)

	return lm.Append(buf)
}

func writeStart(dst []byte, txnum storage.TxID) {
	rbuf := recordBuffer{bytes: dst}
	rbuf.writeFixedLen(
		storage.SizeOfTinyInt,
		storage.UnsafeIntegerToFixed[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(START)),
	)
	rbuf.writeFixedLen(
		storage.SizeOfTxID,
		storage.UnsafeIntegerToFixed[storage.TxID](storage.SizeOfTxID, txnum),
	)
}
