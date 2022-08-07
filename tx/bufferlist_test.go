package tx

import (
	"testing"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/test"
)

func TestBufferlistPin(t *testing.T) {
	t.Cleanup(test.ClearTestFolder)

	_, _, bm := test.MakeManagers()

	buflist := MakeBufferList(bm)

	testBlock := file.NewBlockID(test.DefaultConfig.BlockFile, 1)

	buflist.Pin(testBlock)

	// test that available buffers in the manager are all minus one
	if av := bm.Available(); av != test.DefaultConfig.BuffersAvailable-1 {
		t.Fatalf("expected %d buffers available, got %d", test.DefaultConfig.BuffersAvailable-1, av)
	}

	// test that the pinned buffer is correctly accounted for in the list
	if v := buflist.pins[testBlock.String()]; v != 1 {
		t.Fatalf("expected only one pin for block %s, got %d", testBlock.String(), v)
	}

	buflist.Unpin(testBlock)

	// test that internal counters are set to 0
	if _, ok := buflist.buffers[testBlock.String()]; ok {
		t.Fatalf("expected buffer for block %s to not be listed as pinned.", testBlock)
	}

	if _, ok := buflist.pins[testBlock.String()]; ok {
		t.Fatalf("expected buffer for block %s to be counted as pinned.", testBlock)
	}
}

func TestBufferlistUnpinAll(t *testing.T) {
	t.Cleanup(test.ClearTestFolder)

	_, _, bm := test.MakeManagers()

	buflist := MakeBufferList(bm)

	for i := 0; i < 3; i++ {
		block := file.NewBlockID(test.DefaultConfig.BlockFile, i)
		buflist.Pin(block)
	}

	buflist.UnpinAll()

	if l := len(buflist.buffers); l != 0 {
		t.Fatalf("expected pinned buffers list to be empty, got %d buffers", l)
	}

	if l := len(buflist.pins); l != 0 {
		t.Fatalf("expected pinned buffers count to be empty, got %d buffers", l)
	}
}
