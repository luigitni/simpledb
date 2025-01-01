package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/types"
)

// setStringLogRecord represents a log record for setting a string value
// The record can be represented as
// <SETSTRING, txnum, filename, blockId, blockOffset, value>
type setStringLogRecord struct {
	txnum  int
	offset int
	block  types.Block
	val    string
}

// NewSetStringRecord constructs a SetStringLogRecord
// by reading from the given page.
// The layout of a string log record is populated according to WriteStringToLog
func newSetStringRecord(record recordBuffer) setStringLogRecord {
	rec := setStringLogRecord{}

	rec.txnum = record.readInt()
	rec.block = record.readBlock()
	rec.offset = record.readInt()
	rec.val = record.readString()

	return rec
}

func (ss setStringLogRecord) Op() txType {
	return SETSTRING
}

func (ss setStringLogRecord) TxNumber() int {
	return ss.txnum
}

func (ss setStringLogRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %s %d %s>", ss.txnum, ss.block, ss.offset, ss.val)
}

// Undo replaces the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block, calls tx.SetString to restore the saved value,
// and unpins the buffer
func (ss setStringLogRecord) Undo(tx Transaction) {
	tx.Pin(ss.block)
	tx.SetString(ss.block, ss.offset, ss.val, false)
	tx.Unpin(ss.block)
}

// logSetString appends a string records to the log file, by calling log.Manager.Append
// A string log entry has the following layout:
// | log type | tx number | filename | block number | offset | value |
func logSetString(lm logManager, txnum int, block types.Block, offset int, val string) int {
	pool := logPools.poolForString(val)
	p := pool.Get().(*[]byte)
	defer pool.Put(p)
	writeString(p, txnum, block, offset, val)

	return lm.Append(*p)
}

func writeString(dst *[]byte, txnum int, block types.Block, offset int, val string) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeInt(int(SETSTRING))
	rbuf.writeInt(txnum)
	rbuf.writeBlock(block)
	rbuf.writeInt(offset)
	rbuf.writeString(val)
}
