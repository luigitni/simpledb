package file_test

import (
	"testing"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/types"
)

func TestFile(t *testing.T) {
	conf := test.DefaultConfig(t)
	fman := file.NewFileManager(conf.DbFolder, conf.BlockSize)

	block := types.NewBlock(conf.BlockFile, 2)
	page := types.NewPage()

	var offset types.Offset = 88

	const val = "abcdefghilmno"
	const intv = 352

	varlen := types.UnsafeNewVarlenFromGoString(val)

	page.UnsafeSetVarlen(offset, varlen)

	offset2 := offset + types.Offset(varlen.Size())

	page.UnsafeSetFixedLen(offset2, types.SizeOfInt, types.UnsafeIntegerToFixed(types.SizeOfInt, types.Int(intv)))

	// write the page to the block
	fman.Write(block, page)

	// create a new page and read it back from the block
	p2 := types.NewPage()
	fman.Read(block, p2)

	got := p2.UnsafeGetFixedLen(offset2, types.SizeOfInt)
	if got := types.UnsafeFixedToInteger[types.Int](got); got != intv {
		t.Fatalf("expected %d at offset %d. Got %d", intv, offset2, got)
	}

	sgot := p2.UnsafeGetVarlen(offset)
	if got := types.UnsafeVarlenToGoString(sgot); got != val {
		t.Fatalf("expected %q at offset %d. Got %q", val, offset, got)
	}
}
