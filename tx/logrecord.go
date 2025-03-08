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

func (r *recordBuffer) writeFixedLen(size storage.Offset, val storage.FixedLen) {
	copy(r.bytes[r.offset:], val)
	r.offset += size
}

func (r *recordBuffer) writeVarLen(v storage.Varlen) {
	storage.WriteVarlenToBytes(r.bytes[r.offset:], v)
	r.offset += storage.Offset(v.Size())
}

func (r *recordBuffer) writeString(s string) {
	storage.WriteVarlenToBytesFromGoString(r.bytes[r.offset:], s)
	r.offset += storage.Offset(
		storage.SizeOfStringAsVarlen(s),
	)
}

func (r *recordBuffer) writeRaw(data []byte) {
	copy(r.bytes[r.offset:], data)
	r.offset += storage.Offset(len(data))
}

func (r *recordBuffer) writeBlock(block storage.Block) {
	r.writeString(block.FileName())
	r.writeFixedLen(storage.SizeOfLong, storage.IntegerToFixedLen[storage.Long](storage.SizeOfLong, block.Number()))
}

func (r *recordBuffer) readFixedLen(size storage.Offset) storage.FixedLen {
	v := storage.ByteSliceToFixedlen(r.bytes[r.offset : r.offset+size])
	r.offset += size
	return v
}

func (r *recordBuffer) readVarlen() storage.Varlen {
	v := storage.BytesToVarlen(r.bytes[r.offset:])
	r.offset += storage.Offset(v.Size())

	return v
}

func (r *recordBuffer) readBlock() storage.Block {
	fileName := r.readVarlen()
	fixed := r.readFixedLen(storage.SizeOfLong)
	blockID := storage.FixedLenToInteger[storage.Long](fixed)

	return storage.NewBlock(
		storage.VarlenToGoString(fileName),
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
	return txType(storage.FixedLenToInteger[storage.TinyInt](f))
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
	tt := storage.FixedLenToInteger[storage.TinyInt](
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
	case COPY:
		return newCopyRecord(rbuf)
	}

	return nil
}
