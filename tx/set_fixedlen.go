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
	size   storage.Offset
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
	rec.txnum = storage.FixedLenToInteger[storage.TxID](record.readFixedLen(storage.SizeOfTxID))
	// read the block name
	rec.block = record.readBlock()
	// read the block offset
	rec.offset = storage.FixedLenToInteger[storage.Offset](record.readFixedLen(storage.SizeOfOffset))
	// read the size of the value
	rec.size = storage.FixedLenToInteger[storage.Offset](record.readFixedLen(storage.SizeOfOffset))
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
func logSetFixedLen(lm logManager, txnum storage.TxID, block storage.Block, offset storage.Offset, size storage.Offset, val storage.FixedLen) int {
	blocknameSize := storage.SizeOfStringAsVarlen(block.FileName())

	l := sizeOfFixedLenRecord + int(size) + int(blocknameSize)
	buf := make([]byte, l)
	written := writeFixedLen(buf, txnum, block, offset, size, val)

	return lm.Append(buf[:written])
}

func writeFixedLen(dst []byte, txnum storage.TxID, block storage.Block, offset storage.Offset, size storage.Offset, val storage.FixedLen) storage.Offset {
	rbuf := recordBuffer{bytes: dst}

	rbuf.writeFixedLen(storage.SizeOfTinyInt, storage.IntegerToFixedLen[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(SETFIXEDLEN)))
	rbuf.writeFixedLen(storage.SizeOfTxID, storage.IntegerToFixedLen[storage.TxID](storage.SizeOfTxID, txnum))
	rbuf.writeBlock(block)
	rbuf.writeFixedLen(storage.SizeOfOffset, storage.IntegerToFixedLen[storage.Offset](storage.SizeOfOffset, offset))
	rbuf.writeFixedLen(storage.SizeOfOffset, storage.IntegerToFixedLen[storage.Offset](storage.SizeOfOffset, size))
	rbuf.writeFixedLen(size, val)

	return rbuf.offset
}
