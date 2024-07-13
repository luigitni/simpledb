package file

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// size in bytes of an int
const IntBytes = int(unsafe.Sizeof(int(123)))

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


func (p *Page) SetBytes(offset int, data []byte) {
	p.assertSize(offset, len(data))
	copy(p.buf[offset:], intToBytes(int64(len(data))))
	copy(p.buf[offset+IntBytes:], data)
}

func (p *Page) Bytes(offset int) []byte {
	size := bytesToInt(p.buf[offset : offset+IntBytes])
	from := offset + IntBytes
	to := offset + IntBytes + int(size)
	return p.buf[from:to]
}

// Int methods

func (p *Page) SetInt(offset int, val int) {
	p.assertSize(offset, IntBytes)

	lb := intToBytes(int64(val))
	copy(p.buf[offset:], lb[:])
}

func (p *Page) Int(offset int) int {
	v := bytesToInt(p.buf[offset : offset+IntBytes])
	return int(v)
}

// String methods

func (p *Page) SetString(offset int, v string) {
	p.SetBytes(offset, []byte(v))
}

func (p *Page) String(offset int) string {
	buf := p.Bytes(offset)
	return string(buf)
}

func intToBytes(v int64) []byte {
	buf := make([]byte, IntBytes)
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
	return strlen + IntBytes
}
