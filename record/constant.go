package record

import (
	"fmt"
	"strconv"
)

type Constant struct {
	isInt  bool
	intVal int
	strVal string
}

func ConstantFromInt(i int) Constant {
	return Constant{intVal: i, isInt: true}
}

func ConstantFromString(s string) Constant {
	return Constant{strVal: s}
}

func (c Constant) AsIntVal() int {
	return c.intVal
}

func (c Constant) AsStringVal() string {
	return c.strVal
}

func (c Constant) String() string {
	if c.isInt {
		return strconv.FormatInt(int64(c.intVal), 10)
	}
	return fmt.Sprintf("'%s'", c.strVal)
}
