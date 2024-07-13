package test

import (
	"testing"

	"github.com/luigitni/simpledb/file"
)

func AssertIntAtPos(t *testing.T, page *file.Page, pos int, exp int) {
	if v := page.Int(pos); v != exp {
		t.Fatalf("expected %d at pos %d. Got %d", exp, pos, v)
	}
}

func AssertStrAtPos(t *testing.T, page *file.Page, pos int, exp string) {
	if v := page.String(pos); v != exp {
		t.Fatalf("expected %q at pos %d. Got %q", exp, pos, v)
	}
}
