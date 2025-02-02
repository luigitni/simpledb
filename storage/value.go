package storage

import (
	"slices"
)

type FieldType int

const (
	INTEGER FieldType = iota
	STRING
)

// Value is a generic value
type Value []byte

func ValueFromFixedLen(i FixedLen) Value {
	return Value(i)
}

func ValueFromVarlen(v Varlen) Value {
	return UnsafeVarlenToBytes(v)
}

func ValueFromGoString(s string) Value {
	varlen := UnsafeNewVarlenFromGoString(s)
	return UnsafeVarlenToBytes(varlen)
}

func ValueFromInteger[V Integer](size Size, i V) Value {
	return Value(UnsafeIntegerToFixed(size, i))
}

func (v Value) AsFixedLen() FixedLen {
	return FixedLen(v)
}

func (v Value) AsVarlen() Varlen {
	return ValueAsVarlen(v)
}

func (v Value) AsGoString() string {
	return ValueAsGoString(v)
}

func ValueAsInteger[V Integer](v Value) V {
	return UnsafeFixedToInteger[V](v.AsFixedLen())
}

func ValueAsGoString(v Value) string {
	return UnsafeVarlenToGoString(UnsafeBytesToVarlen(v))
}

func ValueAsVarlen(v Value) Varlen {
	return UnsafeBytesToVarlen(v)
}

// todo: use specialised function to avoid branch prediction misses
func (c Value) Size(t FieldType) Offset {
	switch t {
	case INTEGER:
		return Offset(SizeOfInt)
	case STRING:
		v := c.AsVarlen()
		return Offset(v.Size())
	}

	panic("unsupported type")
}

func (c Value) Hash() int {
	return sbdmHash(c)
}

func sbdmHash(s Value) int {
	var hash int
	for _, r := range s {
		hash = int(r) + (hash << 6) + (hash << 16) - hash
	}

	return hash
}

func (v Value) Equals(other Value) bool {
	return slices.Equal(v, other)
}

// todo: implement type specific comparisons
func (v Value) Less(other Value) bool {
	panic("not implemented")
}

// todo: implement supported type specific comparisons
func (v Value) More(other Value) bool {
	panic("not implemented")
	return false
}

func (c Value) String() string {
	return "not implemented"
}
