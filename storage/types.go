package storage

import (
	"errors"
	"unsafe"
)

type FieldType int16

var ErrInvalidFieldType = errors.New("invalid field type")

const (
	TINYINT FieldType = iota
	SMALLINT
	INT
	LONG
	NAME
	TEXT
)

var typeSizes = [...]Offset{
	TINYINT:  SizeOfTinyInt,
	SMALLINT: SizeOfSmallInt,
	INT:      SizeOfInt,
	LONG:     SizeOfLong,
	NAME:     SizeOfName,
	TEXT:     SizeOfVarlen,
}

var typeNames = [...]string{
	"TINYINT",
	"SMALLINT",
	"INT",
	"LONG",
	"NAME",
	"TEXT",
}

func (t FieldType) Size() Offset {
	return typeSizes[t]
}

func (t FieldType) String() string {
	return typeNames[t]
}

// Name is a fixed-length string of 64 bytes, only composed of ASCII characters.
// The first byte is the length of the string.
// If a name is longer than 63 bytes, it is truncated to 63 bytes.
// It is used for table and field names.
type Name FixedLen

const (
	SizeOfName Offset   = 64
	NameMaxLen SmallInt = 63
)

// NewName creates a new Name from a string.
// If the string is less than 64 bytes, it is padded with spaces.
// If the string is longer than 64 bytes, it is truncated to 64 bytes.
func NewNameFromGoString(s string) Name {
	n := make(Name, 64)
	n.WriteGoString(s)

	return n
}

func (n Name) WriteGoString(s string) {
	bound := SizeOfName
	size := byte(NameMaxLen)
	if len(s) < int(NameMaxLen) {
		bound = Offset(len(s))
		size = byte(len(s))
	}

	n[0] = size

	copy(n[1:], s[:bound])
}

func (f FixedLen) AsName() Name {
	return Name(f)
}

func (n Name) AsGoString() string {
	return unsafe.String(&n[1], int(n[0]))
}
