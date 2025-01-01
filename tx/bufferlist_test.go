package tx

import (
	"testing"

	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/types"
)

func TestBufferlistPin(t *testing.T) {
	conf := test.DefaultConfig(t)

	_, _, bm := test.MakeManagers(t)

	buflist := makeBufferList(bm)

	testBlock := types.NewBlock(conf.BlockFile, 1)

	buflist.pin(testBlock)

	// test that available buffers in the manager are all minus one
	if av := bm.Available(); av != conf.BuffersAvailable-1 {
		t.Fatalf("expected %d buffers available, got %d", conf.BuffersAvailable-1, av)
	}

	// test that the pinned buffer is correctly accounted for in the list
	if v := buflist.pins[testBlock.ID()]; v != 1 {
		t.Fatalf("expected only one pin for block %s, got %d", testBlock.String(), v)
	}

	buflist.unpin(testBlock)

	// test that internal counters are set to 0
	if _, ok := buflist.buffers[testBlock.ID()]; ok {
		t.Fatalf("expected buffer for block %s to not be listed as pinned.", testBlock)
	}

	if _, ok := buflist.pins[testBlock.ID()]; ok {
		t.Fatalf("expected buffer for block %s to be counted as pinned.", testBlock)
	}
}

func TestBufferlistUnpinAll(t *testing.T) {
	conf := test.DefaultConfig(t)

	_, _, bm := test.MakeManagers(t)

	buflist := makeBufferList(bm)

	for i := 0; i < 3; i++ {
		block := types.NewBlock(conf.BlockFile, i)
		buflist.pin(block)
	}

	buflist.unpinAll()

	if l := len(buflist.buffers); l != 0 {
		t.Fatalf("expected pinned buffers list to be empty, got %d buffers", l)
	}

	if l := len(buflist.pins); l != 0 {
		t.Fatalf("expected pinned buffers count to be empty, got %d buffers", l)
	}
}
