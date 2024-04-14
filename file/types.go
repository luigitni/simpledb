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

func (c Value) String() string {
	if c.isInt {
		return strconv.FormatInt(int64(c.intVal), 10)
	}
	return fmt.Sprintf("'%s'", c.strVal)
}