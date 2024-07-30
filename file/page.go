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

func (p *Page) Slice(from int, to int) []byte {
	return p.buf[from:to]
}

// copies raw bytes into the page
func (p *Page) UnsafeCopyRaw(offset int, data []byte) {
	p.assertSize(offset, len(data))
	copy(p.buf[offset:], data)
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
	dataStart := offset + IntSize
	size := bytesToInt(p.buf[offset:dataStart])
	dataEnd := dataStart + int(size)
	return p.buf[dataStart:dataEnd]
}

func (p *Page) SetInt(offset int, val int) {
	p.assertSize(offset, IntSize)
	binary.LittleEndian.PutUint64(p.buf[offset:], uint64(val))
}

func (p *Page) Int(offset int) int {
	v := bytesToInt(p.buf[offset : offset+IntSize])
	return int(v)
}

// RawInt returns the raw bytes of the integer
// at the offset without conversion
func (p *Page) RawInt(offset int) []byte {
	return p.buf[offset : offset+IntSize]

}

func (p *Page) SetString(offset int, v string) {
	p.SetBytes(offset, []byte(v))
}

func (p *Page) String(offset int) string {
	return string(p.RawString(offset))
}

func (p *Page) RawString(offset int) []byte {
	return p.Bytes(offset)
}

func bytesToInt(b []byte) int64 {
	v := binary.LittleEndian.Uint64(b)
	return int64(v)
}

// StrLength returns the size of an encoded string
// this is currently equal to the length of the string (or byte slice) plus the int32 prefix
func StrLength(strlen int) int {
	return strlen + IntSize
}
