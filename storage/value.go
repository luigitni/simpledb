package storage

import (
	"fmt"
	"slices"
)

// Value is a generic value
type Value []byte

func ValueFromFixedLen(i FixedLen) Value {
	return Value(i)
}

func ValueFromName(n Name) Value {
	return Value(n)
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

func (v Value) AsName() Name {
	return Name(v)
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
	return Offset(t.Size())
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
}

func (c Value) String(t FieldType) string {
	return stringFunc[t](c)
}

var stringFunc = [...]func(Value) string{
	TINYINT: func(v Value) string {
		return fmt.Sprintf("%d", ValueAsInteger[TinyInt](v))
	},
	SMALLINT: func(v Value) string {
		return fmt.Sprintf("%d", ValueAsInteger[SmallInt](v))
	},
	INT: func(v Value) string {
		return fmt.Sprintf("%d", ValueAsInteger[Int](v))
	},
	LONG: func(v Value) string {
		return fmt.Sprintf("%d", ValueAsInteger[Long](v))
	},
	NAME: func(v Value) string {
		return ValueAsGoString(v)
	},
	TEXT: func(v Value) string {
		return ValueAsGoString(v)
	},
}
