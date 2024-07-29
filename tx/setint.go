package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
)

// setIntLogRecord represents an update record of the WAL
// for a SETINT operation
// The record can be represented as
// <SETINT, txnum, filename, blockId, blockOffset, value>
type setIntLogRecord struct {
	txnum  int
	offset int
	block  file.Block
	val    int
}

func newSetIntRecord(record recordBuffer) setIntLogRecord {
	rec := setIntLogRecord{}

	rec.txnum = record.readInt()
	blockID, fname := record.readInt(), record.readString()
	rec.block = file.NewBlock(fname, blockID)
	rec.offset = record.readInt()
	rec.val = record.readInt()

	return rec
}

func (si setIntLogRecord) String() string {
	return fmt.Sprintf("<SETINT %d %s %d %d>", si.txnum, si.block, si.offset, si.val)
}

func (si setIntLogRecord) Op() txType {
	return SETINT
}

func (si setIntLogRecord) TxNumber() int {
	return si.txnum
}

func (si setIntLogRecord) Undo(tx Transaction) {
	tx.Pin(si.block)
	tx.SetInt(si.block, si.offset, si.val, false)
	tx.Unpin(si.block)
}

// LogSetInt appends a string records to the log file, by calling log.Manager.Append
// An int log entry has the following layout:
// | log type | tx number | filename | block number | offset | value |
func LogSetInt(lm logManager, txnum int, block file.Block, offset int, val int) int {
	p := logPools.setInt.Get().(*[]byte)
	defer logPools.setInt.Put(p)
	logSetInt(p, txnum, block, offset, val)

	return lm.Append(*p)
}

func logSetInt(dst *[]byte, txnum int, block file.Block, offset int, val int) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeInt(int(SETINT))
	rbuf.writeInt(txnum)
	rbuf.writeString(block.FileName())
	rbuf.writeInt(block.Number())
	rbuf.writeInt(offset)
	rbuf.writeInt(val)
}
