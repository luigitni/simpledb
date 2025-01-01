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

	pos := 88

	const val = "abcdefghilmno"
	const intv = 352
	page.SetString(pos, val)

	pos2 := pos + types.StrLength(len(val))

	page.SetInt(pos2, intv)

	fman.Write(block, page)

	p2 := types.NewPage()
	fman.Read(block, p2)

	if got := p2.Int(pos2); got != intv {
		t.Fatalf("expected %d at offset %d. Got %d", intv, pos2, got)
	}
	t.Logf("offset %d contains %d", pos2, p2.Int(pos2))

	if got := p2.String(pos); got != val {
		t.Fatalf("expected %q at offset %d. Got %q", val, pos, got)
	}

	t.Logf("offset %d contains %s", pos, p2.String(pos))
}
