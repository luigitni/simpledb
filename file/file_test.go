package file_test

import (
	"testing"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
)

func TestFile(t *testing.T) {
	conf := test.DefaultConfig(t)
	fman := file.NewFileManager(conf.DbFolder, conf.BlockSize)

	block := storage.NewBlock(conf.BlockFile, 2)
	page := storage.NewPage()

	var offset storage.Offset = 88

	const val = "abcdefghilmno"
	const intv = 352

	varlen := storage.UnsafeNewVarlenFromGoString(val)

	page.UnsafeSetVarlen(offset, varlen)

	offset2 := offset + storage.Offset(varlen.Size())

	page.UnsafeSetFixedLen(offset2, storage.SizeOfInt, storage.UnsafeIntegerToFixed(storage.SizeOfInt, storage.Int(intv)))

	// write the page to the block
	fman.Write(block, page)

	// create a new page and read it back from the block
	p2 := storage.NewPage()
	fman.Read(block, p2)

	got := p2.UnsafeGetFixedLen(offset2, storage.SizeOfInt)
	if got := storage.UnsafeFixedToInteger[storage.Int](got); got != intv {
		t.Fatalf("expected %d at offset %d. Got %d", intv, offset2, got)
	}

	sgot := p2.UnsafeGetVarlen(offset)
	if got := storage.UnsafeVarlenToGoString(sgot); got != val {
		t.Fatalf("expected %q at offset %d. Got %q", val, offset, got)
	}
}
