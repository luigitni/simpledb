package tx

import (
	"fmt"
)

type commitLogRecord struct {
	txnum int
}

func newCommitRecord(record recordBuffer) commitLogRecord {
	return commitLogRecord{
		txnum: record.readInt(),
	}
}

func (record commitLogRecord) Op() txType {
	return COMMIT
}

func (record commitLogRecord) TxNumber() int {
	return record.txnum
}

func (record commitLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record commitLogRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", record.txnum)
}

func LogCommit(lm logManager, txnum int) int {
	p := logPools.small2ints.Get().(*[]byte)
	defer logPools.small2ints.Put(p)
	logCommit(p, txnum)
	return lm.Append(*p)
}

func logCommit(dst *[]byte, txnum int) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeInt(int(COMMIT))
	rbuf.writeInt(txnum)
}
