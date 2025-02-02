package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/storage"
)

type startLogRecord struct {
	txnum storage.TxID
}

func newStartLogRecord(record recordBuffer) startLogRecord {
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
	p := logPools.small2ints.Get().(*[]byte)
	defer logPools.small2ints.Put(p)

	writeStart(p, txnum)

	return lm.Append(*p)
}

func writeStart(dst *[]byte, txnum storage.TxID) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeFixedLen(
		storage.SizeOfTinyInt,
		storage.UnsafeIntegerToFixed[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(START)),
	)
	rbuf.writeFixedLen(
		storage.SizeOfTxID,
		storage.UnsafeIntegerToFixed[storage.TxID](storage.SizeOfTxID, txnum),
	)
}
