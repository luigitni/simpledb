package tx

import (
	"encoding/binary"
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

func (p *pools) poolForString(s string) *sync.Pool {
	l := len(s) + logSetIntSize // account for header size
	switch {
	case l < 512:
		return &p.setSmallString
	case l < 1024:
		return &p.setLString
	default:
		return &p.setXLString
	}
}

type recordBuffer struct {
	offset int
	bytes  []byte
}

func (r *recordBuffer) writeInt(v int) {
	binary.LittleEndian.PutUint64(r.bytes[r.offset:], uint64(v))
	r.offset += types.IntSize
}

func (r *recordBuffer) writeString(v string) {
	l := len(v)
	r.writeInt(l)
	copy(r.bytes[r.offset:], []byte(v))
	r.offset += l
}

func (r *recordBuffer) writeBlock(block types.Block) {
	r.writeString(block.FileName())
	r.writeInt(block.Number())
}

func (r *recordBuffer) readInt() int {
	v := binary.LittleEndian.Uint64(r.bytes[r.offset:])
	r.offset += types.IntSize
	return int(v)
}

func (r *recordBuffer) readString() string {
	length := int(binary.LittleEndian.Uint64(r.bytes[r.offset:]))
	r.offset += types.IntSize
	str := string(r.bytes[r.offset : r.offset+length])
	r.offset += length
	return str
}

func (r *recordBuffer) readBlock() types.Block {
	fileName := r.readString()
	blockID := r.readInt()
	return types.NewBlock(fileName, blockID)
}

type logRecord interface {
	// op returns the log record's type
	Op() txType

	// txNumber returns the tx id stored with the log record
	TxNumber() int

	// undo undoes the operation encoded by this log recod.
	Undo(tx Transaction)
}

type txType int

const (
	CHECKPOINT txType = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

func createLogRecord(bytes []byte) logRecord {
	rbuf := recordBuffer{bytes: bytes}
	switch txType(rbuf.readInt()) {
	case CHECKPOINT:
		return checkpointLogRecord{}
	case START:
		return newStartLogRecord(rbuf)
	case COMMIT:
		return newCommitRecord(rbuf)
	case ROLLBACK:
		return newRollbackRecord(rbuf)
	case SETINT:
		return newSetIntRecord(rbuf)
	case SETSTRING:
		return newSetStringRecord(rbuf)
	}

	return nil
}
