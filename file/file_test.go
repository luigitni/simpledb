package file_test

import (
	"os"
	"path"
	"testing"

	"github.com/luigitni/simpledb/file"
)

func TestFile(t *testing.T) {

	const dbfolder = "../test_data"
	const blockfile = "testblock"
	const blockSize = 400

	t.Cleanup(func() {
		p := path.Join(dbfolder, blockfile)
		os.Remove(p)
	})
	fman := file.NewFileManager(dbfolder, blockSize)

	block := file.NewBlockID(blockfile, 2)
	page := file.NewPageWithSize(fman.BlockSize())

	pos := 88

	const val = "abcdefghilmno"
	const intv = 352
	page.SetString(pos, val)

	pos2 := pos + file.MaxLength(len(val))

	page.SetInt(pos2, intv)

	fman.Write(block, page)

	p2 := file.NewPageWithSize(fman.BlockSize())
	fman.Read(block, p2)

	if got := p2.GetInt(pos2); got != intv {
		t.Fatalf("expected %d at offset %d. Got %d", intv, pos2, got)
	}
	t.Logf("offset %d contains %d", pos2, p2.GetInt(pos2))

	if got := p2.GetString(pos); got != val {
		t.Fatalf("expected %q at offset %d. Got %q", val, pos, got)
	}

	t.Logf("offset %d contains %s", pos, p2.GetString(pos))
}
