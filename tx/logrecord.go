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

func (r *recordBuffer) writeRaw(data []byte) {
	copy(r.bytes[r.offset:], data)
	r.offset += storage.Offset(len(data))
}

func (r *recordBuffer) writeBlock(block storage.Block) {
	v := storage.UnsafeNewVarlenFromGoString(block.FileName())
	r.writeVarLen(v)
	r.writeFixedLen(storage.SizeOfLong, storage.UnsafeIntegerToFixedlen[storage.Long](storage.SizeOfLong, block.Number()))
}

func (r *recordBuffer) readFixedLen(size storage.Size) storage.FixedLen {
	s := storage.Offset(size)
	v := storage.UnsafeByteSliceToFixedlen(r.bytes[r.offset : r.offset+s])
	r.offset += s
	return v
}

func (r *recordBuffer) readVarlen() storage.Varlen {
	v := storage.UnsafeBytesToVarlen(r.bytes[r.offset:])
	r.offset += storage.Offset(v.Size())

	return v
}

func (r *recordBuffer) readBlock() storage.Block {
	fileName := r.readVarlen()
	fixed := r.readFixedLen(storage.SizeOfLong)
	blockID := storage.UnsafeFixedToInteger[storage.Long](fixed)

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

var txTypeToString = [...]string{
	CHECKPOINT:  "CHECKPOINT",
	START:       "START",
	COMMIT:      "COMMIT",
	ROLLBACK:    "ROLLBACK",
	SETFIXEDLEN: "SETFIXED",
	SETVARLEN:   "SETSTRING",
	COPY:        "COPY",
}

func txTypeFromFixedLen(f storage.FixedLen) txType {
	return txType(storage.UnsafeFixedToInteger[storage.TinyInt](f))
}

func (t txType) String() string {
	return txTypeToString[t]
}

const (
	CHECKPOINT txType = 1 + iota
	START
	COMMIT
	ROLLBACK
	SETFIXEDLEN
	SETVARLEN
	COPY
)

func createLogRecord(bytes []byte) logRecord {
	rbuf := &recordBuffer{bytes: bytes}
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
	case SETFIXEDLEN:
		return newSetFixedLenRecord(rbuf)
	case SETVARLEN:
		return newSetVarLenRecord(rbuf)
	case COPY:
		return newCopyRecord(rbuf)
	}

	return nil
}
