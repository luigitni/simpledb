package storage

import "testing"

func TestWriteFixedLen(t *testing.T) {
	page := NewPage()

	t.Run("write a single tinyint", func(t *testing.T) {
		const v TinyInt = 77

		page.SetFixedlen(0, SizeOfTinyInt, IntegerToFixedLen(SizeOfTinyInt, v))

		got := page.GetFixedLen(0, SizeOfTinyInt)
		if got := FixedLenToInteger[TinyInt](got); got != v {
			t.Fatalf("expected %d, got %d", v, got)
		}
	})

	t.Run("write a single smallint", func(t *testing.T) {
		const v SmallInt = 77

		page.SetFixedlen(0, SizeOfSmallInt, IntegerToFixedLen(SizeOfSmallInt, v))
		got := page.GetFixedLen(0, SizeOfSmallInt)

		if got := FixedLenToInteger[SmallInt](got); got != v {
			t.Fatalf("expected %d, got %d", v, got)
		}
	})

	t.Run("write a single Int", func(t *testing.T) {
		const v Int = 77

		page.SetFixedlen(0, SizeOfInt, IntegerToFixedLen(SizeOfInt, v))

		got := page.GetFixedLen(0, SizeOfInt)

		if got := FixedLenToInteger[Int](got); got != v {
			t.Fatalf("expected %d, got %d", v, got)
		}
	})

	t.Run("write a single Long", func(t *testing.T) {
		const v Long = 77

		page.SetFixedlen(0, SizeOfLong, IntegerToFixedLen(SizeOfLong, v))
		got := page.GetFixedLen(0, SizeOfLong)

		if got := FixedLenToInteger[Long](got); got != v {
			t.Fatalf("expected %d, got %d", v, got)
		}
	})

	t.Run("write multiple TinyInts", func(t *testing.T) {
		nums := []TinyInt{1, 2, 3, 4, 5, 6}

		var offset Offset = 0
		for _, v := range nums {
			page.SetFixedlen(offset, SizeOfTinyInt, IntegerToFixedLen(SizeOfTinyInt, v))
			offset += Offset(SizeOfTinyInt)
		}

		offset = 0
		for _, v := range nums {
			got := page.GetFixedLen(offset, SizeOfTinyInt)

			if got := FixedLenToInteger[TinyInt](got); got != v {
				t.Fatalf("expected %d, got %d", v, got)
			}

			offset += Offset(SizeOfTinyInt)
		}
	})

	t.Run("write multiple SmallInts", func(t *testing.T) {
		nums := []SmallInt{1, 2, 3, 4, 5, 6}

		var offset Offset = 0
		for _, v := range nums {
			page.SetFixedlen(offset, SizeOfSmallInt, IntegerToFixedLen(SizeOfSmallInt, v))
			offset += Offset(SizeOfSmallInt)
		}

		offset = 0
		for _, v := range nums {
			got := page.GetFixedLen(offset, SizeOfSmallInt)

			if got := FixedLenToInteger[SmallInt](got); got != v {
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

			page.SetFixedlen(offset, SizeOfInt, IntegerToFixedLen(SizeOfInt, v))
			offset += Offset(SizeOfInt)
		}

		offset = 0
		for _, v := range nums {
			v := Int(v)
			got := page.GetFixedLen(offset, SizeOfInt)

			if got := FixedLenToInteger[Int](got); got != v {
				t.Fatalf("expected %d, got %d", v, got)
			}

			offset += Offset(SizeOfInt)
		}
	})

	t.Run("write multiple Longs", func(t *testing.T) {
		nums := []Long{256, 123, 1, 0, 10000000, 16543}

		var offset Offset = 0
		for _, v := range nums {
			page.SetFixedlen(offset, SizeOfLong, IntegerToFixedLen(SizeOfLong, v))
			offset += Offset(SizeOfLong)
		}

		offset = 0
		for _, v := range nums {
			got := page.GetFixedLen(offset, SizeOfLong)

			if got := FixedLenToInteger[Long](got); got != v {
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

		page.SetVarlen(0, NewVarlenFromGoString(v))

		got := page.GetVarlen(0)
		if got := VarlenToGoString(got); got != v {
			t.Fatalf("expected %q, got %q", v, got)
		}
	})

	t.Run("write a raw byte slice", func(t *testing.T) {
		const v = "this is a test"

		page.WriteRawVarlen(0, []byte(v))

		got := page.GetVarlen(0)

		if got := VarlenToGoString(got); got != v {
			t.Fatalf("expected %q, got %q", v, got)
		}
	})

	t.Run("write multiple strings", func(t *testing.T) {
		strs := []string{"hello", "world", "this", "is", "a", "test"}
		var offset Offset = 0
		for _, v := range strs {
			page.SetVarlen(offset, NewVarlenFromGoString(v))
			offset += Offset(SizeOfStringAsVarlen(v))
		}

		offset = 0
		for _, v := range strs {
			got := page.GetVarlen(offset)
			if got := VarlenToGoString(got); got != v {
				t.Fatalf("expected %q, got %q", v, got)
			}
			offset += Offset(SizeOfStringAsVarlen(v))
		}
	})
}
