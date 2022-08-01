package buffer_test

import (
	"os"
	"path"
	"testing"

	"github.com/luigitni/simpledb/buffer"
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

const dbFolder = "../test_data"
const logfile = "testlog"
const blockfile = "testfile"
const blockSize = 400
const buffersAvaialble = 3

var clearTestFolder = func() {
	p := path.Join(dbFolder, blockfile)
	os.Remove(p)
	p = path.Join(dbFolder, logfile)
	os.Remove(p)
}

func makeManagers() (*file.Manager, *log.Manager, *buffer.Manager) {
	fm := file.NewFileManager(dbFolder, blockSize)
	lm := log.NewLogManager(fm, logfile)

	bm := buffer.NewBufferManager(fm, lm, buffersAvaialble)

	return fm, lm, bm
}

func TestBuffer(t *testing.T) {

	t.Cleanup(func() {
		clearTestFolder()
	})

	fm, _, bm := makeManagers()

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

func TestBufferManager(t *testing.T) {

	t.Cleanup(func() {
		clearTestFolder()
	})

	_, _, bm := makeManagers()

	buffers := make([]*buffer.Buffer, 6)

	var err error
	for i := 0; i < 3; i++ {
		// assign all buffers to blocks
		buffers[i], err = bm.Pin(file.NewBlockID(blockfile, i))
		if err != nil {
			t.Fatal(err)
		}
	}

	bm.Unpin(buffers[1])
	buffers[1] = nil

	if buffers[3], err = bm.Pin(file.NewBlockID(blockfile, 0)); err != nil {
		t.Fatal(err)
	}

	if buffers[4], err = bm.Pin(file.NewBlockID(blockfile, 1)); err != nil {
		t.Fatal(err)
	}

	// expect this buffer to timeout
	buffers[5], err = bm.Pin(file.NewBlockID(blockfile, 3))
	if err != buffer.ErrClientTimeout {
		t.Fatalf("expected pin on buffer 5 to timeount. Got %v", err)
	} else {
		t.Log("buffer 5 has timed out")
	}

	bm.Unpin(buffers[2])
	buffers[2] = nil
	if buffers[5], err = bm.Pin(file.NewBlockID(blockfile, 3)); err != nil {
		t.Fatalf("expected client not to time out. Got %v", err)
	}

	for i, buf := range buffers {
		if buf != nil {
			t.Logf("buffer %d at %p pinned to block %s", i, buffers[i], buf.BlockID())
		}
	}
}
