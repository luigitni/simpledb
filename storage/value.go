package storage

import (
	"fmt"
	"slices"
)

// Value is a generic value
type Value []byte

func Copy(v Value) Value {
	cpy := make([]byte, len(v))
	copy(cpy, v)

	return cpy
}

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
	return Value(UnsafeIntegerToFixedlen(size, i))
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

func ValueAsInteger[V Integer](v Value) V {
	return UnsafeFixedToInteger[V](v.AsFixedLen())
}

func ValueAsGoString(v Value) string {
	return UnsafeVarlenToGoString(UnsafeBytesToVarlen(v))
}

func ValueAsVarlen(v Value) Varlen {
	return UnsafeBytesToVarlen(v)
}

func (v Value) Size(t FieldType) Offset {
	if size := t.Size(); size != SizeOfVarlen {
		return Offset(size)
	}

	// todo: implement sizes when toasts are implemented
	return Offset(v.AsVarlen().Size())
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
func (v Value) Less(t FieldType, other Value) bool {
	return lessFunc[t](v, other)
}

var lessFunc = [...]func(Value, Value) bool{
	TINYINT: func(a, b Value) bool {
		return ValueAsInteger[TinyInt](a) < ValueAsInteger[TinyInt](b)
	},
	SMALLINT: func(a, b Value) bool {
		return ValueAsInteger[SmallInt](a) < ValueAsInteger[SmallInt](b)
	},
	INT: func(a, b Value) bool {
		return ValueAsInteger[Int](a) < ValueAsInteger[Int](b)
	},
	LONG: func(a, b Value) bool {
		return ValueAsInteger[Long](a) < ValueAsInteger[Long](b)
	},
	NAME: func(a, b Value) bool {
		return a.AsName().UnsafeAsGoString() < b.AsName().UnsafeAsGoString()
	},
	TEXT: func(a, b Value) bool {
		return ValueAsGoString(a) < ValueAsGoString(b)
	},
}

func (v Value) More(t FieldType, other Value) bool {
	return moreFunc[t](v, other)
}

var moreFunc = [...]func(Value, Value) bool{
	TINYINT: func(a, b Value) bool {
		return ValueAsInteger[TinyInt](a) > ValueAsInteger[TinyInt](b)
	},
	SMALLINT: func(a, b Value) bool {
		return ValueAsInteger[SmallInt](a) > ValueAsInteger[SmallInt](b)
	},
	INT: func(a, b Value) bool {
		return ValueAsInteger[Int](a) > ValueAsInteger[Int](b)
	},
	LONG: func(a, b Value) bool {
		return ValueAsInteger[Long](a) > ValueAsInteger[Long](b)
	},
	NAME: func(a, b Value) bool {
		return a.AsName().UnsafeAsGoString() > b.AsName().UnsafeAsGoString()
	},
	TEXT: func(a, b Value) bool {
		return ValueAsGoString(a) > ValueAsGoString(b)
	},
}

func MinValue(t FieldType) Value {
	return minValFunc[t]()
}

var minValFunc = [...]func() Value{
	TINYINT: func() Value {
		return ValueFromInteger[TinyInt](SizeOfTinyInt, 0)
	},
	SMALLINT: func() Value {
		return ValueFromInteger[SmallInt](SizeOfSmallInt, 0)
	},
	INT: func() Value {
		return ValueFromInteger[Int](SizeOfInt, 0)
	},
	LONG: func() Value {
		return ValueFromInteger[Long](SizeOfLong, 0)
	},
	NAME: func() Value {
		return ValueFromName(NewNameFromGoString(""))
	},
	TEXT: func() Value {
		return ValueFromGoString("")
	},
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
		return v.AsName().UnsafeAsGoString()
	},
	TEXT: func(v Value) string {
		return ValueAsGoString(v)
	},
}
