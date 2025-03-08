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

	varlen := storage.NewVarlenFromGoString(val)

	page.SetVarlen(offset, varlen)

	offset2 := offset + storage.Offset(varlen.Size())

	page.SetFixedlen(offset2, storage.SizeOfInt, storage.IntegerToFixedLen(storage.SizeOfInt, storage.Int(intv)))

	// write the page to the block
	fman.Write(block, page)

	// create a new page and read it back from the block
	p2 := storage.NewPage()
	fman.Read(block, p2)

	got := p2.GetFixedLen(offset2, storage.SizeOfInt)
	if got := storage.FixedLenToInteger[storage.Int](got); got != intv {
		t.Fatalf("expected %d at offset %d. Got %d", intv, offset2, got)
	}

	sgot := p2.GetVarlen(offset)
	if got := storage.VarlenToGoString(sgot); got != val {
		t.Fatalf("expected %q at offset %d. Got %q", val, offset, got)
	}
}
