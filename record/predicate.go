package record

import "fmt"

type Expression struct {
	val   interface{}
	fname string
}

func NewExpressionWithVal(v interface{}) Expression {
	return Expression{val: v}
}

func NewExpressionWithField(fname string) Expression {
	return Expression{fname: fname}
}

func (exp Expression) IsFieldName() bool {
	return exp.fname != ""
}

func (exp Expression) AsConstant() interface{} {
	return exp.val
}

func (exp Expression) AsFieldName() string {
	return exp.fname
}

func (exp Expression) Evaluate(scan Scan) (interface{}, error) {
	if exp.val != nil {
		return exp.val, nil
	}

	return scan.GetVal(exp.fname)
}

func (exp Expression) AppliesTo(schema Schema) bool {
	if exp.val != nil {
		return true
	}

	return schema.HasField(exp.fname)
}

func (exp Expression) String() string {
	if exp.val != nil {
		return fmt.Sprintf("%v", exp.val)
	}

	return exp.fname
}

type Predicate struct{}

func (p Predicate) IsSatisfied(s Scan) bool {
	return true
}
