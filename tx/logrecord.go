package tx

import (
	"sync"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/types"
)

type pools struct {
	tiny1int       sync.Pool
	small2ints     sync.Pool
	setInt         sync.Pool
	setSmallString sync.Pool
	setLString     sync.Pool
	setXLString    sync.Pool
}

const (
	logUpdateHeaderSize = types.IntSize * 4
	logSetIntSize       = logUpdateHeaderSize + file.MaxLoggedTableFileNameSize
)

var logPools = pools{
	tiny1int: sync.Pool{
		New: func() interface{} {
			b := make([]byte, types.IntSize)
			return &b
		},
	},
	small2ints: sync.Pool{
		New: func() interface{} {
			s := make([]byte, 2*types.IntSize)
			return &s
		},
	},
	setInt: sync.Pool{
		New: func() interface{} {
			s := make([]byte, logSetIntSize)
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
func (p *pools) poolForSize(s types.Size) *sync.Pool {
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
	offset types.Offset
	bytes  []byte
}

func (r *recordBuffer) resetOffset() {
	r.offset = 0
}

func (r *recordBuffer) writeFixedLen(size types.Size, val types.FixedLen) {
	copy(r.bytes[r.offset:], val)
	r.offset += types.Offset(size)
}

func (r *recordBuffer) writeVarLen(v types.Varlen) {
	types.UnsafeWriteVarlenToBytes(r.bytes[r.offset:], v)
	r.offset += types.Offset(v.Size())
}

func (r *recordBuffer) writeBlock(block types.Block) {
	v := types.UnsafeNewVarlenFromGoString(block.FileName())
	r.writeVarLen(v)
	r.writeFixedLen(types.SizeOfLong, types.UnsafeIntegerToFixed[types.Long](types.SizeOfLong, block.Number()))
}

func (r *recordBuffer) readFixedLen(size types.Size) types.FixedLen {
	v := types.UnsafeByteSliceToFixed(r.bytes[r.offset : r.offset+types.Offset(size)])
	r.offset += types.Offset(size)
	return v
}

func (r *recordBuffer) readVarlen() types.Varlen {
	v := types.UnsafeBytesToVarlen(r.bytes[r.offset:])
	r.offset += types.Offset(v.Size())

	return v
}

func (r *recordBuffer) readBlock() types.Block {
	fileName := r.readVarlen()
	blockID := types.UnsafeFixedToInteger[types.Long](r.readFixedLen(types.SizeOfLong))

	return types.NewBlock(
		types.UnsafeVarlenToGoString(fileName),
		blockID,
	)
}

type logRecord interface {
	// op returns the log record's type
	Op() txType

	// txNumber returns the tx id stored with the log record
	TxNumber() types.TxID

	// undo undoes the operation encoded by this log recod.
	Undo(tx Transaction)
}

type txType types.TinyInt

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
	tt := types.UnsafeFixedToInteger[types.TinyInt](
		rbuf.readFixedLen(types.SizeOfTinyInt),
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
