package file

import (
	"fmt"
	"strconv"
)

type FieldType int

const (
	INTEGER FieldType = iota
	STRING
)

// Value is a generic value
type Value struct {
	isInt  bool
	intVal int
	strVal string
}

func ValueFromInt(i int) Value {
	return Value{intVal: i, isInt: true}
}

func ValueFromString(s string) Value {
	return Value{strVal: s}
}

func (c Value) AsIntVal() int {
	return c.intVal
}

func (c Value) AsStringVal() string {
	return c.strVal
}

func (c Value) Size() int {
	if c.isInt {
		return IntSize
	}

	return StrLength(len(c.strVal))
}

func (c Value) Hash() int {
	if c.isInt {
		return c.intVal
	}

	return sbdmHash(c.strVal)
}

func sbdmHash(s string) int {
	var hash int
	for _, r := range s {
		hash = int(r) + (hash << 6) + (hash << 16) - hash
	}

	return hash
}

func (v Value) Equals(other Value) bool {
	if v.isInt != other.isInt {
		return false
	}

	if v.isInt {
		return v.intVal == other.intVal
	}

	return v.strVal == other.strVal
}

func (v Value) Less(other Value) bool {
	if v.isInt {
		return v.intVal < other.intVal
	}

	return v.strVal < other.strVal
}

func (v Value) More(other Value) bool {
	if v.isInt {
		return v.intVal > other.intVal
	}

	return v.strVal > other.strVal
}

func (c Value) String() string {
	if c.isInt {
		return strconv.FormatInt(int64(c.intVal), 10)
	}
	return fmt.Sprintf("'%s'", c.strVal)
}
