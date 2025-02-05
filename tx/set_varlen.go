package tx

import (
	"fmt"
	"unsafe"

	"github.com/luigitni/simpledb/storage"
)

// setVarLenLogRecord represents a log record for setting a string value
// The record can be represented as
// <SETVARLEN, txnum, filename, blockId, blockOffset, value>
type setVarLenLogRecord struct {
	txnum  storage.TxID
	offset storage.Offset
	block  storage.Block
	val    storage.Varlen
}

const sizeOfVarlenRecord = int(unsafe.Sizeof(setVarLenLogRecord{})) + int(storage.SizeOfTinyInt)

// NewSetStringRecord constructs a SetStringLogRecord
// by reading from the given page.
// The layout of a string log record is populated according to WriteStringToLog
func newSetVarLenRecord(record *recordBuffer) setVarLenLogRecord {
	rec := setVarLenLogRecord{}

	// skip the first byte, which is the record type
	_ = record.readFixedLen(storage.SizeOfTinyInt)
	// read the transaction number
	rec.txnum = storage.UnsafeFixedToInteger[storage.TxID](record.readFixedLen(storage.SizeOfTxID))
	// read the block name
	rec.block = record.readBlock()
	// read the block offset
	rec.offset = storage.Offset(storage.UnsafeFixedToInteger[storage.Offset](record.readFixedLen(storage.SizeOfOffset)))
	// read the value
	rec.val = record.readVarlen()

	return rec
}

func (ss setVarLenLogRecord) Op() txType {
	return SETSTRING
}

func (ss setVarLenLogRecord) TxNumber() storage.TxID {
	return ss.txnum
}

func (ss setVarLenLogRecord) String() string {
	return fmt.Sprintf("<SETVARLEN %d %s %d %s>", ss.txnum, ss.block.ID(), ss.offset, ss.val)
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
func logSetVarlen(lm logManager, txnum storage.TxID, block storage.Block, offset storage.Offset, val storage.Varlen) int {
	l := sizeOfVarlenRecord + int(val.Size())
	buf := make([]byte, l)
	written := writeVarlen(buf, txnum, block, offset, val)

	return lm.Append(buf[:written])
}

func writeVarlen(dst []byte, txnum storage.TxID, block storage.Block, offset storage.Offset, val storage.Varlen) storage.Offset {
	rbuf := recordBuffer{bytes: dst}

	rbuf.writeFixedLen(storage.SizeOfTinyInt, storage.UnsafeIntegerToFixed[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(SETSTRING)))
	rbuf.writeFixedLen(storage.SizeOfTxID, storage.UnsafeIntegerToFixed[storage.TxID](storage.SizeOfTinyInt, txnum))
	rbuf.writeBlock(block)
	// write the offset as a fixed length integer
	rbuf.writeFixedLen(storage.SizeOfOffset, storage.UnsafeIntegerToFixed[storage.Offset](storage.SizeOfOffset, offset))
	rbuf.writeVarLen(val) // write offset

	return rbuf.offset
}
