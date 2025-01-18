package sql

import "github.com/luigitni/simpledb/types"

type Scan interface {
	Val(fieldName string) (types.Value, error)
}

type Schema interface {
	HasField(fieldName string) bool
}

type Expression struct {
	val   types.Value
	fname string
}

func NewExpressionWithVal(v types.Value) Expression {
	return Expression{val: v}
}

func NewExpressionWithField(fname string) Expression {
	return Expression{fname: fname}
}

func (exp Expression) IsFieldName() bool {
	return exp.fname != ""
}

func (exp Expression) AsConstant() types.Value {
	return exp.val
}

func (exp Expression) AsFieldName() string {
	return exp.fname
}

func (exp Expression) Evaluate(scan Scan) (types.Value, error) {
	if exp.val != nil {
		return exp.val, nil
	}

	return scan.Val(exp.fname)
}

func (exp Expression) AppliesTo(schema Schema) bool {
	if exp.val != nil {
		return true
	}

	return schema.HasField(exp.fname)
}

func (exp Expression) String() string {
	if exp.val != nil {
		return exp.val.String()
	}

	return exp.fname
}
