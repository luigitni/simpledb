package sql

import "github.com/luigitni/simpledb/file"

type Scan interface {
	GetVal(fieldName string) (file.Value, error)
}

type Schema interface {
	HasField(fieldName string) bool
}

type Expression struct {
	val   file.Value
	fname string
}

func NewExpressionWithVal(v file.Value) Expression {
	return Expression{val: v}
}

func NewExpressionWithField(fname string) Expression {
	return Expression{fname: fname}
}

func (exp Expression) IsFieldName() bool {
	return exp.fname != ""
}

func (exp Expression) AsConstant() file.Value {
	return exp.val
}

func (exp Expression) AsFieldName() string {
	return exp.fname
}

func (exp Expression) Evaluate(scan Scan) (file.Value, error) {
	if empty := (file.Value{}); exp.val != empty {
		return exp.val, nil
	}

	return scan.GetVal(exp.fname)
}

func (exp Expression) AppliesTo(schema Schema) bool {
	if empty := (file.Value{}); exp.val != empty {
		return true
	}

	return schema.HasField(exp.fname)
}

func (exp Expression) String() string {
	if empty := (file.Value{}); exp.val != empty {
		return exp.val.String()
	}

	return exp.fname
}
