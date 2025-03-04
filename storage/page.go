package storage

import (
	"fmt"
	"math"
	"strings"
	"unsafe"
)

const PageSize = 1024 * 8

// Offset is the offset of a field within a page
// Because pages are fixed 8KB, we can use a uint16 to address any offset within a page
type Offset uint16

const (
	SizeOfOffset Offset = 2

	SizeOfTinyInt  Offset = 1
	SizeOfSmallInt Offset = 2
	SizeOfInt      Offset = 4
	SizeOfLong     Offset = 8

	SizeOfTxID Offset = 4

	SizeOfVarlen    Offset = Offset(math.MaxUint16)
	SizeOfVarlenLen Offset = Offset(SizeOfInt)
)

type (
	TinyInt  uint8
	SmallInt uint16
	Int      uint32
	Long     uint64
)

type Integer interface {
	TinyInt | SmallInt | Int | Long | TxID | Offset
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
	return FixedLenToInteger[Int](FixedLen(v[:SizeOfInt]))
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

func (v Varlen) AsGoString() string {
	return VarlenToGoString(v)
}

func (v Varlen) Bytes() []byte {
	return VarlenToBytes(v)
}

// String returns a string representation of the Varlen value
// It should only be used for debugging purposes
// The string is a new string created from the Varlen's data
// This could allocate a new string and copy the data into it
func (v Varlen) String() string {
	size := FixedLenToInteger[Int](FixedLen(v[:SizeOfInt]))
	return string(v[SizeOfInt : Int(SizeOfInt)+size])
}

// NewVarlenFromGoString creates a Varlen type from a string
// The original string is copied into the Varlen's byte slice
// This could allocate a new byte slice and copies the string into it
// The allocation's decision depends on the append semantics of the Go runtime
func NewVarlenFromGoString(s string) Varlen {
	b := append(IntegerToFixedLen[Int](SizeOfInt, Int(len(s))), []byte(s)...)
	return Varlen(b)
}

// VarlenToGoString converts a Varlen to a string
// The string is not copied, but the underlying byte slice is reinterpret casted to a string
func VarlenToGoString(v Varlen) string {
	if v.Len() == 0 {
		return ""
	}

	return unsafe.String(&v[SizeOfInt], v.Len())
}

// BytesToVarlen creates a Varlen type from a byte slice
// The byte slice must be formatted as a Varlen.
func BytesToVarlen(b []byte) Varlen {
	size := ByteSliceToFixedlen(b[:SizeOfInt]).AsInt()

	return Varlen(b[:Int(SizeOfInt)+size])
}

// VarlenToBytes converts a Varlen to a byte slice
// The first 4 bytes in the slice are the length of the Varlen
// The remaining bytes are the data of the varlen
func VarlenToBytes(v Varlen) []byte {
	// size of the varlen len, plus the size of the slice header, plus the size of the data
	return v
}

// WriteVarlenToBytes writes a Varlen to a byte slice
func WriteVarlenToBytes(buf []byte, v Varlen) {
	// write the size of the varlen
	copy(buf, v)
}

func WriteVarlenToBytesFromGoString(buf []byte, s string) {
	len := Int(len(s))
	copy(buf, IntegerToFixedLen[Int](SizeOfInt, len))
	copy(buf[SizeOfInt:], s)
}

// SizeOfStringAsVarlen returns the size of a string as a Varlen
// The Size is constrained to the addressable size of the page
// The maximum size of a string is thus 2^16 - 4 = 65532 bytes
// This should only be used for strings that are know to fit in a page
func SizeOfStringAsVarlen(s string) Int {
	return Int(len(s)) + Int(SizeOfInt)
}

// FixedLen is a fixed length value
// It is a byte slice of a fixed length
// It is used for fixed length values
type FixedLen []byte

func (f FixedLen) Size() Offset {
	return Offset(len(f))
}

func (f FixedLen) Bytes() []byte {
	return f
}

func (f FixedLen) AsOffset() Offset {
	return FixedLenToInteger[Offset](f)
}

func (f FixedLen) AsTxID() TxID {
	return FixedLenToInteger[TxID](f)
}

func (f FixedLen) AsTinyInt() TinyInt {
	return FixedLenToInteger[TinyInt](f)
}

func (f FixedLen) AsSmallInt() SmallInt {
	return FixedLenToInteger[SmallInt](f)
}

func (f FixedLen) AsInt() Int {
	return FixedLenToInteger[Int](f)
}

func (f FixedLen) AsLong() Long {
	return FixedLenToInteger[Long](f)
}

// String returns a string representation of the FixedLen value
// It should only be used for debugging purposes
// The type is inferred from the size of the FixedLen value
// and will always be interpreted as an integer type
func (f FixedLen) String() string {

	var builder strings.Builder
	builder.WriteByte('[')

	for i := range f {
		builder.WriteByte(f[i])
		if i != len(f)-1 {
			builder.WriteByte(',')
			builder.WriteByte(' ')
		}
	}

	builder.WriteByte(']')

	return builder.String()
}

func (f FixedLen) Format(state fmt.State, verb rune) {

	if verb == 'd' {
		switch Offset(len(f)) {
		case SizeOfTinyInt:
			state.Write([]byte(fmt.Sprintf("%d", f.AsTinyInt())))
		case SizeOfSmallInt:
			state.Write([]byte(fmt.Sprintf("%d", f.AsSmallInt())))
		case SizeOfInt:
			state.Write([]byte(fmt.Sprintf("%d", f.AsInt())))
		case SizeOfLong:
			state.Write([]byte(fmt.Sprintf("%d", f.AsLong())))
		}

		return
	}

	state.Write([]byte(f.String()))
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

func (p *Page) Contents() []byte {
	return p.buf[:]
}

func (p *Page) Slice(from Offset, to Offset) []byte {
	return p.buf[from:to]
}

func (p *Page) SetFixedlen(offset Offset, size Offset, val FixedLen) {
	from := offset
	to := offset + Offset(size)
	runtimeAssert(to <= PageSize, "SetFixedLen: out of bounds (from: %d to: %d)", from, to)
	copy(p.buf[from:to], val)
}

func (p *Page) GetFixedLen(offset Offset, size Offset) FixedLen {
	return p.buf[offset : offset+Offset(size)]
}

// WriteRawVarlen writes a raw byte slice to a page as a Varlen
func (p *Page) WriteRawVarlen(offset Offset, raw []byte) {
	size := Int(len(raw))
	from := offset
	to := offset + Offset(size) + Offset(SizeOfInt)
	runtimeAssert(to <= PageSize, "WriteRawVarlen: out of bounds (from: %d to: %d)", from, to)

	v := IntegerToFixedLen[Int](SizeOfInt, size)
	p.SetFixedlen(from, SizeOfInt, v)

	copy(p.buf[from+Offset(SizeOfInt):to], raw)
}

func (p *Page) SetVarlen(offset Offset, val Varlen) {
	from := offset
	to := offset + Offset(val.Size())
	runtimeAssert(to <= PageSize, "SetVarlen: out of bounds (from: %d to: %d)", from, to)

	WriteVarlenToBytes(p.buf[from:to], val)
}

func (p *Page) GetVarlen(offset Offset) Varlen {
	l := *(*Int)(unsafe.Pointer(&p.buf[offset]))

	return Varlen(unsafe.Slice(&p.buf[offset], int(l)+int(SizeOfInt)))
}

func (p *Page) Copy(src Offset, dst Offset, length Offset) {
	runtimeAssert(src+length <= PageSize && dst+length <= PageSize, "Copy: src out of bounds (from: %d to: %d)", src, src+length)

	copy(p.buf[dst:dst+length], p.buf[src:src+length])
}

func FixedLenToInteger[V Integer](val FixedLen) V {
	n := *(*V)(unsafe.Pointer(&val[0]))
	return n
}

func IntegerToFixedLen[V Integer](size Offset, val V) FixedLen {
	return unsafe.Slice((*byte)(unsafe.Pointer(&val)), int(size))
}

func ByteSliceToFixedlen(val []byte) FixedLen {
	return FixedLen(val)
}
