package tx

import (
	"fmt"
)

type rollbackLogRecord struct {
	txnum int
}

func newRollbackRecord(record recordBuffer) rollbackLogRecord {
	return rollbackLogRecord{
		txnum: record.readInt(),
	}
}

func (record rollbackLogRecord) Op() txType {
	return ROLLBACK
}

func (record rollbackLogRecord) TxNumber() int {
	return record.txnum
}

func (record rollbackLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record rollbackLogRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", record.txnum)
}

func LogRollback(lm logManager, txnum int) int {
	p := logPools.small2ints.Get().(*[]byte)
	defer logPools.small2ints.Put(p)
	logRollback(p, txnum)
	return lm.Append(*p)
}

func logRollback(dst *[]byte, txnum int) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeInt(int(ROLLBACK))
	rbuf.writeInt(txnum)
}
