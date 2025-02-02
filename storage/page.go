package storage

import (
	"fmt"
	"unsafe"
)

const PageSize = 1024 * 8

// IntSize is the byte size of the system's int
const IntSize = int(unsafe.Sizeof(int(123)))

// Offset is the offset of a field within a page
// Because pages are fixed 8KB, we can use a uint16 to address any offset within a page
type Offset uint16

// Size is the byte size of a field
type Size uint16

const (
	SizeOfOffset Size = 2
	SizeOfSize   Size = 2

	SizeOfTinyInt  Size = 1
	SizeOfSmallInt Size = 2
	SizeOfInt      Size = 4
	SizeOfLong     Size = 8

	SizeOfTxID Size = 4

	// SizeOfGoInt is the size of a Go int
	// This is not necessarily the same as the size of the int in the database
	// It is mostly used for legacy code while all the code is migrated to the new types
	SizeOfGoInt = (Size)(unsafe.Sizeof(int(0)))
)

type (
	TinyInt  uint8
	SmallInt uint16
	Int      uint32
	Long     uint64
)

type Integer interface {
	TinyInt | SmallInt | Int | Long | TxID | Size | Offset
}

type TxID uint32

const (
	TxIDInvalid TxID = 0
	TxIDStart   TxID = 1
)

// varlen is a variable length value
type Varlen []byte

// Len returns the length of the variable length value
func (v Varlen) Len() Int {
	return UnsafeFixedToInteger[Int](FixedLen(v[:SizeOfInt]))
}

// Size returns the byte size of the Varlen struct
// It accounts for the size of the length field and the length of the data
func (v Varlen) Size() Int {
	return v.Len() + Int(SizeOfInt)
}

func (v Varlen) LenAsFixed() FixedLen {
	return FixedLen(v[:SizeOfInt])
}

func (v Varlen) Data() []byte {
	return v[SizeOfInt:]
}

// UnsafeNewVarlenFromGoString creates a Varlen type from a string
// The original string is copied into the Varlen's byte slice
// This could allocate a new byte slice and copies the string into it
// The allocation's decision depends on the append semantics of the Go runtime
func UnsafeNewVarlenFromGoString(s string) Varlen {
	b := append(UnsafeIntegerToFixed[Int](SizeOfInt, Int(len(s))), []byte(s)...)
	return Varlen(b)
}

// UnsafeNewVarlenFromBytes creates a Varlen type from a byte slice
// The byte slice is copied into the Varlen's byte slice
// And the length of the byte slice is prepended to the byte slice
// This could allocate a new byte slice.
// The allocation's decision depends on the append semantics of the Go runtime
func UnsafeNewVarlenFromBytes(b []byte) Varlen {
	buf := append(UnsafeIntegerToFixed[Int](SizeOfInt, Int(len(b))), b...)
	return Varlen(buf)
}

// UnsafeVarlenToGoString converts a Varlen to a string
// The string is not copied, but the underlying byte slice is reinterpret casted to a string
func UnsafeVarlenToGoString(v Varlen) string {
	if v.Len() == 0 {
		return ""
	}

	return unsafe.String(&v[SizeOfInt], v.Len())
}

// UnsafeBytesToVarlen creates a Varlen type from a byte slice
// The byte slice must be formatted as a Varlen.
func UnsafeBytesToVarlen(b []byte) Varlen {
	return Varlen(b)
}

// UnsafeVarlenToBytes converts a Varlen to a byte slice
// The first 4 bytes in the slice are the length of the Varlen
// The remaining bytes are the data of the varlen
func UnsafeVarlenToBytes(v Varlen) []byte {
	// size of the varlen len, plus the size of the slice header, plus the size of the data
	return v
}

// UnsafeWriteVarlenToBytes writes a Varlen to a byte slice
func UnsafeWriteVarlenToBytes(buf []byte, v Varlen) {
	// write the size of the varlen
	copy(buf, v)
}

// UnsafeSizeOfStringAsVarlen returns the size of a string as a Varlen
// The Size is constrained to the addressable size of the page
// The maximum size of a string is thus 2^16 - 4 = 65532 bytes
// This should only be used for strings that are know to fit in a page
func UnsafeSizeOfStringAsVarlen(s string) Int {
	return Int(len(s)) + Int(SizeOfInt)
}

// FixedLen is a fixed length value
// It is a byte slice of a fixed length
// It is used for fixed length values
type FixedLen []byte

func (f FixedLen) Size() Size {
	return Size(len(f))
}

func (f FixedLen) UnsafeAsOffset() Offset {
	return UnsafeFixedToInteger[Offset](f)
}

func (f FixedLen) UnsafeAsTxID() TxID {
	return UnsafeFixedToInteger[TxID](f)
}

func (f FixedLen) UnsafeAsTinyInt() TinyInt {
	return UnsafeFixedToInteger[TinyInt](f)
}

func (f FixedLen) UnsafeAsSmallInt() SmallInt {
	return UnsafeFixedToInteger[SmallInt](f)
}

func (f FixedLen) UnsafeAsInt() Int {
	return UnsafeFixedToInteger[Int](f)
}

func (f FixedLen) UnsafeAsLong() Long {
	return UnsafeFixedToInteger[Long](f)
}

// Page is a fixed size byte slice
// It is used to store data in fixed size blocks
// It is used for storage and caching
// The size of a page is 8KB
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

func runtimeAssert(exp bool, msg string, args ...interface{}) {
	if !exp {
		panic("runtime assert failed: " + fmt.Sprintf(msg, args...))
	}
}

func (p *Page) assertSize(offset int, size int) {
	if offset+size > PageSize {
		panic(fmt.Sprintf("data out of page bounds. offset: %d length: %d. Max page size is %d", offset, size, PageSize))
	}
}

func (p *Page) Contents() []byte {
	return p.buf[:]
}

func (p *Page) Slice(from int, to int) []byte {
	return p.buf[from:to]
}

func (p *Page) UnsafeSetFixedLen(offset Offset, size Size, val FixedLen) {
	from := offset
	to := offset + Offset(size)
	runtimeAssert(to <= PageSize, "SetFixedLen: out of bounds (from: %d to: %d)", from, to)
	copy(p.buf[from:to], val)
}

func (p *Page) UnsafeGetFixedLen(offset Offset, size Size) FixedLen {
	return p.buf[offset : offset+Offset(size)]
}

// UnsafeWriteRawVarlen writes a raw byte slice to a page as a Varlen
func (p *Page) UnsafeWriteRawVarlen(offset Offset, raw []byte) {
	size := Int(len(raw))
	from := offset
	to := offset + Offset(size) + Offset(SizeOfInt)
	runtimeAssert(to <= PageSize, "WriteRawVarlen: out of bounds (from: %d to: %d)", from, to)

	v := UnsafeIntegerToFixed[Int](SizeOfInt, size)
	p.UnsafeSetFixedLen(from, SizeOfInt, v)

	copy(p.buf[from+Offset(SizeOfInt):to], raw)
}

func (p *Page) UnsafeSetVarlen(offset Offset, val Varlen) {
	from := offset
	to := offset + Offset(val.Size())
	runtimeAssert(to <= PageSize, "SetVarlen: out of bounds (from: %d to: %d)", from, to)

	UnsafeWriteVarlenToBytes(p.buf[from:to], val)
}

func (p *Page) UnsafeGetVarlen(offset Offset) Varlen {
	l := *(*Int)(unsafe.Pointer(&p.buf[offset]))

	return Varlen(unsafe.Slice(&p.buf[offset], int(l)+int(SizeOfInt)))
}

func UnsafeFixedToInteger[V Integer](val FixedLen) V {
	n := *(*V)(unsafe.Pointer(&val[0]))
	return n
}

func UnsafeIntegerToFixed[V Integer](size Size, val V) FixedLen {
	return unsafe.Slice((*byte)(unsafe.Pointer(&val)), int(size))
}

func UnsafeByteSliceToFixed(val []byte) FixedLen {
	return FixedLen(val)
}

// SetBytes writes a byte slice at the provided offset.
// Bytes and strings are prepended by their length.
// The length itself is written as an integer.
// todo: this is currently wasteful and we can easily
// use a smaller representation for the length
/*func (p *Page) SetBytes(offset int, data []byte) {
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

func bytesToInt(b []byte) int64 {
	v := binary.LittleEndian.Uint64(b)
	return int64(v)
}

// StrLength returns the size of an encoded string
// this is currently equal to the length of the string (or byte slice) plus the int32 prefix
func StrLength(strlen int) int {
	return strlen + IntSize
}*/
