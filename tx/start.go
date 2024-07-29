package tx

import (
	"fmt"
)

type startLogRecord struct {
	txnum int
}

func newStartLogRecord(record recordBuffer) startLogRecord {
	return startLogRecord{
		txnum: record.readInt(),
	}
}

func (record startLogRecord) Op() txType {
	return START
}

func (record startLogRecord) TxNumber() int {
	return record.txnum
}

func (record startLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record startLogRecord) String() string {
	return fmt.Sprintf("<START %d>", record.txnum)
}

func LogStart(lm logManager, txnum int) int {
	p := logPools.small2ints.Get().(*[]byte)
	defer logPools.small2ints.Put(p)

	logStart(p, txnum)
	return lm.Append(*p)
}

func logStart(dst *[]byte, txnum int) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeInt(int(START))
	rbuf.writeInt(txnum)
}
