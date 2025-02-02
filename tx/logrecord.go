package tx

import (
	"github.com/luigitni/simpledb/storage"
)

type recordBuffer struct {
	offset storage.Offset
	bytes  []byte
}

func (r *recordBuffer) resetOffset() {
	r.offset = 0
}

func (r *recordBuffer) writeFixedLen(size storage.Size, val storage.FixedLen) {
	copy(r.bytes[r.offset:], val)
	r.offset += storage.Offset(size)
}

func (r *recordBuffer) writeVarLen(v storage.Varlen) {
	storage.UnsafeWriteVarlenToBytes(r.bytes[r.offset:], v)
	r.offset += storage.Offset(v.Size())
}

func (r *recordBuffer) writeBlock(block storage.Block) {
	v := storage.UnsafeNewVarlenFromGoString(block.FileName())
	r.writeVarLen(v)
	r.writeFixedLen(storage.SizeOfLong, storage.UnsafeIntegerToFixed[storage.Long](storage.SizeOfLong, block.Number()))
}

func (r *recordBuffer) readFixedLen(size storage.Size) storage.FixedLen {
	v := storage.UnsafeByteSliceToFixed(r.bytes[r.offset : r.offset+storage.Offset(size)])
	r.offset += storage.Offset(size)
	return v
}

func (r *recordBuffer) readVarlen() storage.Varlen {
	v := storage.UnsafeBytesToVarlen(r.bytes[r.offset:])
	r.offset += storage.Offset(v.Size())

	return v
}

func (r *recordBuffer) readBlock() storage.Block {
	fileName := r.readVarlen()
	blockID := storage.UnsafeFixedToInteger[storage.Long](r.readFixedLen(storage.SizeOfLong))

	return storage.NewBlock(
		storage.UnsafeVarlenToGoString(fileName),
		blockID,
	)
}

type logRecord interface {
	// op returns the log record's type
	Op() txType

	// txNumber returns the tx id stored with the log record
	TxNumber() storage.TxID

	// undo undoes the operation encoded by this log recod.
	Undo(tx Transaction)
}

type txType storage.TinyInt

const (
	CHECKPOINT txType = 1 + iota
	START
	COMMIT
	ROLLBACK
	SETFIXED
	SETSTRING
)

func createLogRecord(bytes []byte) logRecord {
	rbuf := recordBuffer{bytes: bytes}
	tt := storage.UnsafeFixedToInteger[storage.TinyInt](
		rbuf.readFixedLen(storage.SizeOfTinyInt),
	)

	rbuf.resetOffset()

	switch txType(tt) {
	case CHECKPOINT:
		return newCheckpointRecord(rbuf)
	case START:
		return newStartLogRecord(rbuf)
	case COMMIT:
		return newCommitRecord(rbuf)
	case ROLLBACK:
		return newRollbackRecord(rbuf)
	case SETFIXED:
		return newSetFixedLenRecord(rbuf)
	case SETSTRING:
		return newSetVarLenRecord(rbuf)
	}

	return nil
}
