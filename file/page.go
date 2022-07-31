package file

import (
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

// The book class uses Java's ByteBuffer to represent the page.
// While writing out of range data to a ByteBuffer throws an exception, golang slices are allowed to grow
// To mimick the book spirit, we will panic if the data to insert exceeds a page's length
func (p *Page) SetBytes(offset int, data []byte) {
	p.assertSize(offset, len(data))
	// append the byte array with size, using the first 4 bytes
	l := len(data)
	lb := (*[IntBytes]byte)(unsafe.Pointer(&l))
	copy(p.buf[offset:], lb[:])
	// copy the payload
	copy(p.buf[offset+IntBytes:], data)
}

func (p *Page) GetBytes(offset int) []byte {

	// get the size of the contingous slice
	// todo: this supposes that records are prepended by their length
	// while this makes sense in java, I am currently unsure it does in go
	// because of slices.
	// todo: return on this once it is clear what this method is used for
	// at the moment, implement it by the book
	size := (*int32)(unsafe.Pointer(&p.buf[offset]))
	from := offset + IntBytes
	to := offset + IntBytes + int(*size)
	return p.buf[from:to]
}

// Int methods

func (p *Page) SetInt(offset int, val int) {
	p.assertSize(offset, int(unsafe.Sizeof(val)))

	lb := (*[IntBytes]byte)(unsafe.Pointer(&val))
	copy(p.buf[offset:], lb[:])
}

func (p *Page) GetInt(offset int) int {
	v := (*int32)(unsafe.Pointer(&p.buf[offset]))
	return int(*v)
}

// String methods

func (p *Page) SetString(offset int, v string) {
	p.SetBytes(offset, []byte(v))
}

func (p *Page) GetString(offset int) string {
	buf := p.GetBytes(offset)
	return string(buf)
}

// MaxLength returns the size of an encoded string
// this is currently equal to the length of the string (or byte slice) plus the int32 prefix
func MaxLength(strlen int) int {
	return strlen + IntBytes
}
