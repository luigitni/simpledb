package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/types"
)

// setVarLenLogRecord represents a log record for setting a string value
// The record can be represented as
// <SETVARLEN, txnum, filename, blockId, blockOffset, value>
type setVarLenLogRecord struct {
	txnum  types.TxID
	offset types.Offset
	block  types.Block
	val    types.Varlen
}

// NewSetStringRecord constructs a SetStringLogRecord
// by reading from the given page.
// The layout of a string log record is populated according to WriteStringToLog
func newSetVarLenRecord(record recordBuffer) setVarLenLogRecord {
	rec := setVarLenLogRecord{}

	// skip the first byte, which is the record type
	_ = record.readFixedLen(types.SizeOfTinyInt)
	// read the transaction number
	rec.txnum = types.UnsafeFixedToInteger[types.TxID](record.readFixedLen(types.SizeOfTxID))
	// read the block name
	rec.block = record.readBlock()
	// read the block offset
	rec.offset = types.Offset(types.UnsafeFixedToInteger[types.Offset](record.readFixedLen(types.SizeOfOffset)))
	// read the value
	rec.val = record.readVarlen()

	return rec
}

func (ss setVarLenLogRecord) Op() txType {
	return SETSTRING
}

func (ss setVarLenLogRecord) TxNumber() types.TxID {
	return ss.txnum
}

func (ss setVarLenLogRecord) String() string {
	return fmt.Sprintf("<SETVARLEN %d %s %d %v>", ss.txnum, ss.block, ss.offset, ss.val)
}

// Undo replaces the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block, calls tx.SetString to restore the saved value,
// and unpins the buffer
func (ss setVarLenLogRecord) Undo(tx Transaction) {
	tx.Pin(ss.block)
	tx.SetVarLen(ss.block, ss.offset, ss.val, false)
	tx.Unpin(ss.block)
}

// logSetVarlen appends a string records to the log file, by calling log.Manager.Append
// A string log entry has the following layout:
// | log type | tx number | filename | block number | offset | value |
func logSetVarlen(lm logManager, txnum types.TxID, block types.Block, offset types.Offset, val types.Varlen) int {
	pool := logPools.poolForSize(types.Size(val.Size()))
	p := pool.Get().(*[]byte)
	defer pool.Put(p)
	writeVarlen(p, txnum, block, offset, val)

	return lm.Append(*p)
}

func writeVarlen(dst *[]byte, txnum types.TxID, block types.Block, offset types.Offset, val types.Varlen) {
	rbuf := recordBuffer{bytes: *dst}

	rbuf.writeFixedLen(types.SizeOfTinyInt, types.UnsafeIntegerToFixed[types.TinyInt](types.SizeOfTinyInt, types.TinyInt(SETSTRING)))
	rbuf.writeFixedLen(types.SizeOfTxID, types.UnsafeIntegerToFixed[types.TxID](types.SizeOfTinyInt, txnum))
	rbuf.writeBlock(block)
	// write the offset as a fixed length integer
	rbuf.writeFixedLen(types.SizeOfOffset, types.UnsafeIntegerToFixed[types.Offset](types.SizeOfOffset, offset))
	rbuf.writeVarLen(val) // write offset
}
