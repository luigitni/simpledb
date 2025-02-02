package tx

import (
	"sync"

	"github.com/luigitni/simpledb/storage"
)

type pools struct {
	tiny1int       sync.Pool
	small2ints     sync.Pool
	setInt         sync.Pool
	setSmallString sync.Pool
	setLString     sync.Pool
	setXLString    sync.Pool
}

var logPools = pools{
	tiny1int: sync.Pool{
		New: func() interface{} {
			b := make([]byte, storage.SizeOfTinyInt)
			return &b
		},
	},
	small2ints: sync.Pool{
		New: func() interface{} {
			s := make([]byte, 2*storage.SizeOfSmallInt)
			return &s
		},
	},
	setInt: sync.Pool{
		New: func() interface{} {
			s := make([]byte, storage.SizeOfInt)
			return &s
		},
	},
	setSmallString: sync.Pool{
		New: func() interface{} {
			s := make([]byte, 512)
			return &s
		},
	},
	setLString: sync.Pool{
		New: func() interface{} {
			s := make([]byte, 1024)
			return &s
		},
	},
	setXLString: sync.Pool{
		New: func() interface{} {
			s := make([]byte, 3072)
			return &s
		},
	},
}

// todo: the poolforsize is not too useful and it's not sure it's worth it
func (p *pools) poolForSize(s storage.Size) *sync.Pool {
	switch {
	case s < 512:
		return &p.setSmallString
	case s < 1024:
		return &p.setLString
	default:
		return &p.setXLString
	}
}

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
