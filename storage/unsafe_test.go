package storage

import (
	"testing"
)

func TestUnsafeFixedToInt(t *testing.T) {
	var i Int = 8

	fixed := UnsafeIntegerToFixed[Int](SizeOfInt, i)

	if v := UnsafeFixedToInteger[Int](fixed); v != i {
		t.Fatalf("expected %d, got %d", i, v)
	}
}

func TestUnsafeVarlen(t *testing.T) {
	t.Run("Varlen to string", func(t *testing.T) {
		const s = "hello"
		size := Size(len(s)) + SizeOfInt

		var v Varlen = UnsafeNewVarlenFromGoString(s)

		if v.Size() != Int(size) {
			t.Fatalf("expected %d, got %d", size, v.Size())
		}

		if v.Len() != Int(len(s)) {
			t.Fatalf("expected %d, got %d", len(s), v.Len())
		}

		ss := UnsafeVarlenToGoString(v)
		if ss != s {
			t.Fatalf("expected %s, got %s", s, ss)
		}
	})

	t.Run("Test serialization", func(t *testing.T) {
		const s = "hello this is a test"
		size := Size(len(s)) + SizeOfInt
		buf := make([]byte, size)

		UnsafeWriteVarlenToBytes(buf, UnsafeNewVarlenFromGoString(s))

		v := UnsafeBytesToVarlen(buf)
		if v.Size() != Int(size) {
			t.Fatalf("expected %d, got %d", size, v.Size())
		}

		if v.Len() != Int(len(s)) {
			t.Fatalf("expected %d, got %d", len(s), v.Len())
		}

		ss := UnsafeVarlenToGoString(v)
		if ss != s {
			t.Fatalf("expected %s, got %s", s, ss)
		}
	})

}

func BenchmarkUnsafeFixedToInt(b *testing.B) {
	var i Int = 8

	fixed := UnsafeIntegerToFixed[Int](SizeOfInt, i)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnsafeFixedToInteger[Int](fixed)
	}
}

func BenchmarkUnsafeVarlen(b *testing.B) {
	const s = "hello"

	var v Varlen = UnsafeNewVarlenFromGoString(s)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnsafeVarlenToGoString(v)
	}
}
