package tx

import (
	"fmt"
	"unsafe"

	"github.com/luigitni/simpledb/storage"
)

// setFixedLenRecord represents an update record of the WAL
// for a SETFIXED operation
// The record can be represented as
// <SETFIXED:size, txnum, filename, blockId, blockOffset, value>
type setFixedLenRecord struct {
	txnum  storage.TxID
	offset storage.Offset
	size   storage.Size
	block  storage.Block
	val    storage.FixedLen
}

const sizeOfFixedLenRecord = int(unsafe.Sizeof(setFixedLenRecord{})) + int(storage.SizeOfTinyInt)

func newSetFixedLenRecord(record *recordBuffer) setFixedLenRecord {
	rec := setFixedLenRecord{}

	f := record.readFixedLen(storage.SizeOfTinyInt)
	if v := txTypeFromFixedLen(f); v != SETFIXEDLEN {
		panic(fmt.Sprintf("bad %s record: %s", SETFIXEDLEN, v))
	}

	// read the transaction number
	rec.txnum = storage.UnsafeFixedToInteger[storage.TxID](record.readFixedLen(storage.SizeOfTxID))
	// read the block name
	rec.block = record.readBlock()
	// read the block offset
	rec.offset = storage.Offset(storage.UnsafeFixedToInteger[storage.Offset](record.readFixedLen(storage.SizeOfOffset)))
	// read the size of the value
	rec.size = storage.Size(storage.UnsafeFixedToInteger[storage.Size](record.readFixedLen(storage.SizeOfSize)))
	// read the value
	rec.val = record.readFixedLen(rec.size)

	return rec
}

func (si setFixedLenRecord) String() string {
	return fmt.Sprintf("<SETFIXED:%d %d %s %d %d>", si.size, si.txnum, si.block.ID(), si.offset, si.val)
}

func (si setFixedLenRecord) Op() txType {
	return SETFIXEDLEN
}

func (si setFixedLenRecord) TxNumber() storage.TxID {
	return si.txnum
}

func (si setFixedLenRecord) Undo(tx Transaction) {
	tx.Pin(si.block)
	tx.SetFixedlen(si.block, si.offset, si.size, si.val, false)
	tx.Unpin(si.block)
}

// logSetFixedLen appends a string records to the log file, by calling log.Manager.Append
// An int log entry has the following layout:
// | log type | tx number | filename | block number | offset | value |
func logSetFixedLen(lm logManager, txnum storage.TxID, block storage.Block, offset storage.Offset, size storage.Size, val storage.FixedLen) int {
	blocknameSize := storage.UnsafeSizeOfStringAsVarlen(block.FileName())

	l := sizeOfFixedLenRecord + int(size) + int(blocknameSize)
	buf := make([]byte, l)
	written := writeFixedLen(buf, txnum, block, offset, size, val)

	return lm.Append(buf[:written])
}

func writeFixedLen(dst []byte, txnum storage.TxID, block storage.Block, offset storage.Offset, size storage.Size, val storage.FixedLen) storage.Offset {
	rbuf := recordBuffer{bytes: dst}

	rbuf.writeFixedLen(storage.SizeOfTinyInt, storage.UnsafeIntegerToFixedlen[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(SETFIXEDLEN)))
	rbuf.writeFixedLen(storage.SizeOfTxID, storage.UnsafeIntegerToFixedlen[storage.TxID](storage.SizeOfTxID, txnum))
	rbuf.writeBlock(block)
	rbuf.writeFixedLen(storage.SizeOfOffset, storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, offset))
	rbuf.writeFixedLen(storage.SizeOfSize, storage.UnsafeIntegerToFixedlen[storage.Size](storage.SizeOfSize, storage.Size(size)))
	rbuf.writeFixedLen(size, val)

	return rbuf.offset
}
