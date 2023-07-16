package record

type Expression struct {
	val   Constant
	fname string
}

func NewExpressionWithVal(v Constant) Expression {
	return Expression{val: v}
}

func NewExpressionWithField(fname string) Expression {
	return Expression{fname: fname}
}

func (exp Expression) IsFieldName() bool {
	return exp.fname != ""
}

func (exp Expression) AsConstant() Constant {
	return exp.val
}

func (exp Expression) AsFieldName() string {
	return exp.fname
}

func (exp Expression) Evaluate(scan Scan) (Constant, error) {
	if empty := (Constant{}); exp.val != empty {
		return exp.val, nil
	}

	return scan.GetVal(exp.fname)
}

func (exp Expression) AppliesTo(schema Schema) bool {
	if empty := (Constant{}); exp.val != empty {
		return true
	}

	return schema.HasField(exp.fname)
}

func (exp Expression) String() string {
	if empty := (Constant{}); exp.val != empty {
		return exp.val.String()
	}

	return exp.fname
}
