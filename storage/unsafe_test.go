package storage

import (
	"testing"
)

func TestUnsafeFixedToInt(t *testing.T) {
	var i Int = 8

	fixed := IntegerToFixedLen[Int](SizeOfInt, i)

	if v := FixedLenToInteger[Int](fixed); v != i {
		t.Fatalf("expected %d, got %d", i, v)
	}
}

func TestUnsafeVarlen(t *testing.T) {
	t.Run("Varlen to string", func(t *testing.T) {
		const s = "hello"
		size := Offset(len(s)) + SizeOfInt

		var v Varlen = NewVarlenFromGoString(s)

		if v.Size() != Int(size) {
			t.Fatalf("expected %d, got %d", size, v.Size())
		}

		if v.Len() != Int(len(s)) {
			t.Fatalf("expected %d, got %d", len(s), v.Len())
		}

		ss := VarlenToGoString(v)
		if ss != s {
			t.Fatalf("expected %s, got %s", s, ss)
		}
	})

	t.Run("Test serialization", func(t *testing.T) {
		const s = "hello this is a test"
		size := Offset(len(s)) + SizeOfInt
		buf := make([]byte, size)

		WriteVarlenToBytes(buf, NewVarlenFromGoString(s))

		v := BytesToVarlen(buf)
		if v.Size() != Int(size) {
			t.Fatalf("expected %d, got %d", size, v.Size())
		}

		if v.Len() != Int(len(s)) {
			t.Fatalf("expected %d, got %d", len(s), v.Len())
		}

		ss := VarlenToGoString(v)
		if ss != s {
			t.Fatalf("expected %s, got %s", s, ss)
		}
	})

}

func BenchmarkUnsafeFixedToInt(b *testing.B) {
	var i Int = 8

	fixed := IntegerToFixedLen[Int](SizeOfInt, i)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FixedLenToInteger[Int](fixed)
	}
}

func BenchmarkUnsafeVarlen(b *testing.B) {
	const s = "hello"

	var v Varlen = NewVarlenFromGoString(s)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VarlenToGoString(v)
	}
}
