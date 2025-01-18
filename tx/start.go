package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/types"
)

type startLogRecord struct {
	txnum types.TxID
}

func newStartLogRecord(record recordBuffer) startLogRecord {
	_ = record.readFixedLen(types.SizeOfTinyInt)

	return startLogRecord{
		txnum: types.UnsafeFixedToInteger[types.TxID](record.readFixedLen(types.SizeOfTxID)),
	}
}

func (record startLogRecord) Op() txType {
	return START
}

func (record startLogRecord) TxNumber() types.TxID {
	return record.txnum
}

func (record startLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record startLogRecord) String() string {
	return fmt.Sprintf("<START %d>", record.txnum)
}

func logStart(lm logManager, txnum types.TxID) int {
	p := logPools.small2ints.Get().(*[]byte)
	defer logPools.small2ints.Put(p)

	writeStart(p, txnum)

	return lm.Append(*p)
}

func writeStart(dst *[]byte, txnum types.TxID) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeFixedLen(
		types.SizeOfTinyInt,
		types.UnsafeIntegerToFixed[types.TinyInt](types.SizeOfTinyInt, types.TinyInt(START)),
	)
	rbuf.writeFixedLen(
		types.SizeOfTxID,
		types.UnsafeIntegerToFixed[types.TxID](types.SizeOfTxID, txnum),
	)
}
