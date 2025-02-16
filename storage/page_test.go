package storage

import "testing"

func TestWriteFixedLen(t *testing.T) {
	page := NewPage()

	t.Run("write a single tinyint", func(t *testing.T) {
		const v TinyInt = 77

		page.UnsafeSetFixedlen(0, SizeOfTinyInt, UnsafeIntegerToFixedlen(SizeOfTinyInt, v))

		got := page.UnsafeGetFixedlen(0, SizeOfTinyInt)
		if got := UnsafeFixedToInteger[TinyInt](got); got != v {
			t.Fatalf("expected %d, got %d", v, got)
		}
	})

	t.Run("write a single smallint", func(t *testing.T) {
		const v SmallInt = 77

		page.UnsafeSetFixedlen(0, SizeOfSmallInt, UnsafeIntegerToFixedlen(SizeOfSmallInt, v))
		got := page.UnsafeGetFixedlen(0, SizeOfSmallInt)

		if got := UnsafeFixedToInteger[SmallInt](got); got != v {
			t.Fatalf("expected %d, got %d", v, got)
		}
	})

	t.Run("write a single Int", func(t *testing.T) {
		const v Int = 77

		page.UnsafeSetFixedlen(0, SizeOfInt, UnsafeIntegerToFixedlen(SizeOfInt, v))

		got := page.UnsafeGetFixedlen(0, SizeOfInt)

		if got := UnsafeFixedToInteger[Int](got); got != v {
			t.Fatalf("expected %d, got %d", v, got)
		}
	})

	t.Run("write a single Long", func(t *testing.T) {
		const v Long = 77

		page.UnsafeSetFixedlen(0, SizeOfLong, UnsafeIntegerToFixedlen(SizeOfLong, v))
		got := page.UnsafeGetFixedlen(0, SizeOfLong)

		if got := UnsafeFixedToInteger[Long](got); got != v {
			t.Fatalf("expected %d, got %d", v, got)
		}
	})

	t.Run("write multiple TinyInts", func(t *testing.T) {
		nums := []TinyInt{1, 2, 3, 4, 5, 6}

		var offset Offset = 0
		for _, v := range nums {
			page.UnsafeSetFixedlen(offset, SizeOfTinyInt, UnsafeIntegerToFixedlen(SizeOfTinyInt, v))
			offset += Offset(SizeOfTinyInt)
		}

		offset = 0
		for _, v := range nums {
			got := page.UnsafeGetFixedlen(offset, SizeOfTinyInt)

			if got := UnsafeFixedToInteger[TinyInt](got); got != v {
				t.Fatalf("expected %d, got %d", v, got)
			}

			offset += Offset(SizeOfTinyInt)
		}
	})

	t.Run("write multiple SmallInts", func(t *testing.T) {
		nums := []SmallInt{1, 2, 3, 4, 5, 6}

		var offset Offset = 0
		for _, v := range nums {
			page.UnsafeSetFixedlen(offset, SizeOfSmallInt, UnsafeIntegerToFixedlen(SizeOfSmallInt, v))
			offset += Offset(SizeOfSmallInt)
		}

		offset = 0
		for _, v := range nums {
			got := page.UnsafeGetFixedlen(offset, SizeOfSmallInt)

			if got := UnsafeFixedToInteger[SmallInt](got); got != v {
				t.Fatalf("expected %d, got %d", v, got)
			}

			offset += Offset(SizeOfSmallInt)
		}
	})

	t.Run("write multiple Ints", func(t *testing.T) {
		nums := []int{256, 123, 1, 0, 10000000, 16543}

		var offset Offset = 0
		for _, v := range nums {
			v := Int(v)

			page.UnsafeSetFixedlen(offset, SizeOfInt, UnsafeIntegerToFixedlen(SizeOfInt, v))
			offset += Offset(SizeOfInt)
		}

		offset = 0
		for _, v := range nums {
			v := Int(v)
			got := page.UnsafeGetFixedlen(offset, SizeOfInt)

			if got := UnsafeFixedToInteger[Int](got); got != v {
				t.Fatalf("expected %d, got %d", v, got)
			}

			offset += Offset(SizeOfInt)
		}
	})

	t.Run("write multiple Longs", func(t *testing.T) {
		nums := []Long{256, 123, 1, 0, 10000000, 16543}

		var offset Offset = 0
		for _, v := range nums {
			page.UnsafeSetFixedlen(offset, SizeOfLong, UnsafeIntegerToFixedlen(SizeOfLong, v))
			offset += Offset(SizeOfLong)
		}

		offset = 0
		for _, v := range nums {
			got := page.UnsafeGetFixedlen(offset, SizeOfLong)

			if got := UnsafeFixedToInteger[Long](got); got != v {
				t.Fatalf("expected %d, got %d", v, got)
			}

			offset += Offset(SizeOfLong)
		}
	})
}

func TestWriteVarlen(t *testing.T) {
	page := NewPage()

	t.Run("write a single string", func(t *testing.T) {
		const v = "this is a test"

		page.UnsafeSetVarlen(0, UnsafeNewVarlenFromGoString(v))

		got := page.UnsafeGetVarlen(0)
		if got := UnsafeVarlenToGoString(got); got != v {
			t.Fatalf("expected %q, got %q", v, got)
		}
	})

	t.Run("write a raw byte slice", func(t *testing.T) {
		const v = "this is a test"

		page.UnsafeWriteRawVarlen(0, []byte(v))

		got := page.UnsafeGetVarlen(0)

		if got := UnsafeVarlenToGoString(got); got != v {
			t.Fatalf("expected %q, got %q", v, got)
		}
	})

	t.Run("write multiple strings", func(t *testing.T) {
		strs := []string{"hello", "world", "this", "is", "a", "test"}
		var offset Offset = 0
		for _, v := range strs {
			page.UnsafeSetVarlen(offset, UnsafeNewVarlenFromGoString(v))
			offset += Offset(UnsafeSizeOfStringAsVarlen(v))
		}

		offset = 0
		for _, v := range strs {
			got := page.UnsafeGetVarlen(offset)
			if got := UnsafeVarlenToGoString(got); got != v {
				t.Fatalf("expected %q, got %q", v, got)
			}
			offset += Offset(UnsafeSizeOfStringAsVarlen(v))
		}
	})
}
