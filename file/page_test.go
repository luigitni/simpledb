package file

import "testing"

func TestWriteInt(t *testing.T) {

	page := NewPageWithSize(1024)

	const v = 77
	page.SetInt(0, 77)

	if got := page.Int(0); got != v {
		t.Fatalf("expected %d, got %d", v, got)
	}
}

func TestWriteIntLoop(t *testing.T) {

	page := NewPageWithSize(1024)

	nums := []int{256, 123, 1, 0, 10000000, 16543}

	j := 0
	for i := 0; i < len(nums)*IntSize; i += IntSize {
		page.SetInt(i, nums[j])
		j++
	}

	j = 0
	for i := 0; i < len(nums)*IntSize; i += IntSize {
		v := page.Int(i)
		if v != nums[j] {
			t.Fatalf("expected %d got %d", nums[j], v)
		}
		j++
	}
}

func TestWriteString(t *testing.T) {

	page := NewPageWithSize(1024)

	const v = "this is a test"
	page.SetString(0, v)

	if got := page.String(0); got != v {
		t.Fatalf("expected %q got %q", v, got)
	}
}

func TestWriteStringMultiple(t *testing.T) {

	page := NewPageWithSize(1024)

	const v = "this is a test"
	const v2 = "this is another test"

	page.SetString(0, v)

	off := MaxLength(len(v))
	page.SetString(off, v2)

	if got := page.String(0); got != v {
		t.Fatalf("expected %q got %q", v, got)
	}

	if got := page.String(off); got != v2 {
		t.Fatalf("expected %q got %q", v2, got)
	}
}
