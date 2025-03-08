package sql

import "github.com/luigitni/simpledb/storage"

type Scan interface {
	Val(fieldName string) (storage.Value, error)
}

type Schema interface {
	HasField(fieldName string) bool
}

type Expression struct {
	val   storage.Value
	fname string
}

func NewExpressionWithVal(v storage.Value) Expression {
	return Expression{val: v}
}

func NewExpressionWithField(fname string) Expression {
	return Expression{fname: fname}
}

func (exp Expression) IsFieldName() bool {
	return exp.fname != ""
}

func (exp Expression) AsConstant() storage.Value {
	return exp.val
}

func (exp Expression) AsFieldName() string {
	return exp.fname
}

func (exp Expression) Evaluate(scan Scan) (storage.Value, error) {
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

func (exp Expression) String(t storage.FieldType) string {
	if exp.val != nil {
		return exp.val.String(t)
	}

	return exp.fname
}
