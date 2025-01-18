package types

import "testing"

func TestValueFromFixed(t *testing.T) {
	const exp = 42

	fixed := UnsafeIntegerToFixed(SizeOfInt, Int(exp))
	val := ValueFromFixedLen(fixed)

	if v := ValueAsInteger[Int](val); v != exp {
		t.Fatalf("expected %d, got %d", exp, v)
	}
}

func TestValueFromVarlen(t *testing.T) {
	const exp = "hello"

	varlen := UnsafeNewVarlenFromGoString(exp)

	val := ValueFromVarlen(varlen)

	if v := ValueAsGoString(val); v != exp {
		t.Fatalf("expected %s, got %s", exp, v)
	}
}
