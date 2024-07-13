package file

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// IntSize is the byte size of the system's int
const IntSize = int(unsafe.Sizeof(int(123)))

type Page struct {
	buf     []byte
	maxSize int
}

func NewPageWithSize(size int) *Page {
	return &Page{
		buf:     make([]byte, size),
		maxSize: size,
	}
}

func NewPageWithSlice(buf []byte) *Page {
	return &Page{
		buf:     buf,
		maxSize: len(buf),
	}
}

func (p *Page) assertSize(offset int, size int) {
	if offset+size > p.maxSize {
		panic(fmt.Sprintf("data out of page bounds. offset: %d length: %d. Max page size is %d", offset, size, p.maxSize))
	}
}

func (p *Page) contents() []byte {
	return p.buf
}

// SetBytes writes a byte slice at the provided offset.
// Bytes and strings are prepended by their length.
// The length itself is written as an integer.
// todo: this is currently wasteful and we can easily
// use a smaller representation for the length
func (p *Page) SetBytes(offset int, data []byte) {
	p.assertSize(offset, len(data))
	copy(p.buf[offset:], intToBytes(int64(len(data))))
	copy(p.buf[offset+IntSize:], data)
}

func (p *Page) Bytes(offset int) []byte {
	size := bytesToInt(p.buf[offset : offset+IntSize])
	from := offset + IntSize
	to := offset + IntSize + int(size)
	return p.buf[from:to]
}

func (p *Page) SetInt(offset int, val int) {
	p.assertSize(offset, IntSize)

	lb := intToBytes(int64(val))
	copy(p.buf[offset:], lb[:])
}

func (p *Page) Int(offset int) int {
	v := bytesToInt(p.buf[offset : offset+IntSize])
	return int(v)
}

func (p *Page) SetString(offset int, v string) {
	p.SetBytes(offset, []byte(v))
}

func (p *Page) String(offset int) string {
	buf := p.Bytes(offset)
	return string(buf)
}

func intToBytes(v int64) []byte {
	buf := make([]byte, IntSize)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	return buf
}

func bytesToInt(b []byte) int64 {
	v := binary.LittleEndian.Uint64(b)
	return int64(v)
}

// MaxLength returns the size of an encoded string
// this is currently equal to the length of the string (or byte slice) plus the int32 prefix
func MaxLength(strlen int) int {
	return strlen + IntSize
}
