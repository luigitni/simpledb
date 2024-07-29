package file

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

const PageSize = 1024 * 8

// IntSize is the byte size of the system's int
const IntSize = int(unsafe.Sizeof(int(123)))

type Page struct {
	buf [PageSize]byte
}

func NewPage() *Page {
	return &Page{}
}

func WrapPage(buf [PageSize]byte) *Page {
	return &Page{
		buf: buf,
	}
}

func (p *Page) assertSize(offset int, size int) {
	if offset+size > PageSize {
		panic(fmt.Sprintf("data out of page bounds. offset: %d length: %d. Max page size is %d", offset, size, PageSize))
	}
}

func (p *Page) contents() []byte {
	return p.buf[:]
}

// SetBytes writes a byte slice at the provided offset.
// Bytes and strings are prepended by their length.
// The length itself is written as an integer.
// todo: this is currently wasteful and we can easily
// use a smaller representation for the length
func (p *Page) SetBytes(offset int, data []byte) {
	p.assertSize(offset, len(data))
	binary.LittleEndian.PutUint64(p.buf[offset:], uint64(len(data)))
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
	binary.LittleEndian.PutUint64(p.buf[offset:], uint64(val))
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
