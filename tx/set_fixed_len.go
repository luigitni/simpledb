package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/types"
)

// setFixedLenRecord represents an update record of the WAL
// for a SETFIXED operation
// The record can be represented as
// <SETFIXED:size, txnum, filename, blockId, blockOffset, value>
type setFixedLenRecord struct {
	txnum  types.TxID
	offset types.Offset
	size   types.Size
	block  types.Block
	val    types.FixedLen
}

func newSetFixedLenRecord(record recordBuffer) setFixedLenRecord {
	rec := setFixedLenRecord{}

	// skip the first byte, which is the record type
	_ = record.readFixedLen(types.SizeOfTinyInt)
	// read the transaction number
	rec.txnum = types.UnsafeFixedToInteger[types.TxID](record.readFixedLen(types.SizeOfTxID))
	// read the block name
	rec.block = record.readBlock()
	// read the block offset
	rec.offset = types.Offset(types.UnsafeFixedToInteger[types.Offset](record.readFixedLen(types.SizeOfOffset)))
	// read the size of the value
	rec.size = types.Size(types.UnsafeFixedToInteger[types.Size](record.readFixedLen(types.SizeOfSize)))
	// read the value
	rec.val = record.readFixedLen(rec.size)

	return rec
}

func (si setFixedLenRecord) String() string {
	return fmt.Sprintf("<SETFIXED:%d %d %s %d %v>", si.size, si.txnum, si.block, si.offset, si.val)
}

func (si setFixedLenRecord) Op() txType {
	return SETFIXED
}

func (si setFixedLenRecord) TxNumber() types.TxID {
	return si.txnum
}

func (si setFixedLenRecord) Undo(tx Transaction) {
	tx.Pin(si.block)
	tx.SetFixedLen(si.block, si.offset, si.size, si.val, false)
	tx.Unpin(si.block)
}

// logSetFixedLen appends a string records to the log file, by calling log.Manager.Append
// An int log entry has the following layout:
// | log type | tx number | filename | block number | offset | value |
func logSetFixedLen(lm logManager, txnum types.TxID, block types.Block, offset types.Offset, size types.Size, val types.FixedLen) int {
	p := logPools.setInt.Get().(*[]byte)
	defer logPools.setInt.Put(p)
	writeFixedLen(p, txnum, block, offset, size, val)

	return lm.Append(*p)
}

func writeFixedLen(dst *[]byte, txnum types.TxID, block types.Block, offset types.Offset, size types.Size, val types.FixedLen) {
	rbuf := recordBuffer{bytes: *dst}

	rbuf.writeFixedLen(types.SizeOfTinyInt, types.UnsafeIntegerToFixed[types.TinyInt](types.SizeOfTinyInt, types.TinyInt(SETFIXED)))
	rbuf.writeFixedLen(types.SizeOfTxID, types.UnsafeIntegerToFixed[types.TxID](types.SizeOfTxID, txnum))
	rbuf.writeBlock(block)
	rbuf.writeFixedLen(types.SizeOfOffset, types.UnsafeIntegerToFixed[types.Offset](types.SizeOfOffset, offset))
	rbuf.writeFixedLen(types.SizeOfSize, types.UnsafeIntegerToFixed[types.Size](types.SizeOfSize, types.Size(size)))
	rbuf.writeFixedLen(size, val)
}
