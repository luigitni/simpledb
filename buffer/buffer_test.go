package buffer_test

import (
	"os"
	"path"
	"testing"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

func TestBuffer(t *testing.T) {
	const dbFolder = "../test_data"
	const logfile = "testlog"
	const blockfile = "testfile"
	const blockSize = 400
	const buffersAvaialble = 3

	t.Cleanup(func() {
		p := path.Join(dbFolder, blockfile)
		os.Remove(p)
		p = path.Join(dbFolder, logfile)
		os.Remove(p)
	})

	fm := file.NewFileManager(dbFolder, blockSize)
	lm := log.NewLogManager(fm, logfile)

	bm := buffer.NewBufferManager(fm, lm, buffersAvaialble)

	buff1, err := bm.Pin(file.NewBlockID(blockfile, 1))
	if err != nil {
		t.Fatal(err)
	}

	page := buff1.Contents()

	n := page.GetInt(80)

	// update the value in the page and flag the buffer as dirty
	page.SetInt(80, n+1)

	buff1.SetModified(1, 0)

	t.Logf("the new value is %d", n+1)

	bm.Unpin(buff1)

	// One of these pins will flush buffer1 to disk:
	// buff1 has been modified, and we have 3 buffers available
	// When Pin is requested, the buffer manager will see that
	// buff1 is unpinned and will swap the assigned block
	// Since buff1 has been modified, Pin will flush the old block to disk

	buff2, err := bm.Pin(file.NewBlockID(blockfile, 2))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := bm.Pin(file.NewBlockID(blockfile, 3)); err != nil {
		t.Fatal(err)
	}

	if _, err := bm.Pin(file.NewBlockID(blockfile, 4)); err != nil {
		t.Fatal(err)
	}

	// test that buff1 has been reassigned to a different block
	if !buff1.IsPinned() {
		t.Fatal("expected buff 1 to be pinned")
	}

	bm.Unpin(buff2)

	// buff2 will not be written to disk, as no other block needs to be associated with a buffer
	buff2, err = bm.Pin(file.NewBlockID(blockfile, 1))
	if err != nil {
		t.Fatal(err)
	}

	page2 := buff2.Contents()
	page2.SetInt(80, 9999)
	buff2.SetModified(1, 0)
	bm.Unpin(buff2)

	// test that at position 80, block1 DOES NOT contain 9999

	blankPage := file.NewPageWithSize(fm.BlockSize())
	fm.Read(file.NewBlockID(blockfile, 1), blankPage)

	v := blankPage.GetInt(80)

	if v == 9999 {
		t.Fatalf("expected contents of buff2 to not be written. Contents found in block 1")
	}

	if v != n+1 {
		t.Fatalf("expected contents of block1 to be %d. Found %d", n+1, v)
	}
}
