package buffer

import (
	"context"
	"testing"

	"github.com/luigitni/simpledb/types"
)

type mockFileManager struct {
	writeCalls    int
	readCalls     int
	writtenBlocks []types.BlockID
}

func (fm *mockFileManager) Write(block types.Block, page *types.Page) {
	fm.writeCalls++
	fm.writtenBlocks = append(fm.writtenBlocks, block.ID())
}

func (fm *mockFileManager) Read(block types.Block, page *types.Page) {
	fm.readCalls++
}

func (fm *mockFileManager) BlockSize() int {
	return 512
}

type mockLogManager struct {
	flushCalls int
}

func (lm *mockLogManager) Flush(lsn int) {
	lm.flushCalls++
}

func TestBuffer(t *testing.T) {
	t.Parallel()

	t.Run("dirty buffers are flushed to disks and log is flushed", func(t *testing.T) {
		t.Parallel()
		const (
			txNum = 123
			lsn   = 1
		)
		fm, lm := &mockFileManager{}, &mockLogManager{}

		buf := newBuffer(fm, lm)
		buf.SetModified(txNum, lsn)
		buf.flush()

		if lm.flushCalls != 1 {
			t.Fatalf("expected one WAL flush call, got %d", lm.flushCalls)
		}

		if fm.writeCalls != 1 {
			t.Fatalf("expected block to be written to disk once, got %d", fm.writeCalls)
		}
	})

	t.Run("unmodified buffers are not flushed to disk and WAL", func(t *testing.T) {
		t.Parallel()
		fm, lm := &mockFileManager{}, &mockLogManager{}

		buf := newBuffer(fm, lm)
		buf.flush()

		if lm.flushCalls > 0 {
			t.Fatalf("expected buffer to not be flushed to WAL. Flush called %d times", lm.flushCalls)
		}

		if fm.writeCalls > 0 {
			t.Fatalf("expected buffer to not be writte to file. Write called %d times", fm.writeCalls)
		}
	})
}

func TestBufferManager(t *testing.T) {
	t.Parallel()

	t.Run("buffers are available", func(t *testing.T) {
		t.Parallel()
		fm, lm := &mockFileManager{}, &mockLogManager{}
		const size = 10

		bufMan := NewBufferManager(fm, lm, size)
		for i := 0; i < size-1; i++ {
			block := types.NewBlock("test", i)
			bufMan.Pin(block)
		}

		if bufMan.Available() != 1 {
			t.Fatal("expected one buffer be available")
		}
	})

	t.Run("all buffers are pinned", func(t *testing.T) {
		t.Parallel()
		fm, lm := &mockFileManager{}, &mockLogManager{}
		const size = 10

		bufMan := NewBufferManager(fm, lm, size)
		for i := 0; i < size; i++ {
			block := types.NewBlock("test", i)
			bufMan.Pin(block)
		}

		if bufMan.Available() > 0 {
			t.Fatal("expected all buffers to be unavailable")
		}
	})

	t.Run("block is pinned and the same block will reuse the same buffer", func(t *testing.T) {
		t.Parallel()
		fm, lm := &mockFileManager{}, &mockLogManager{}
		const size = 10

		bufMan := NewBufferManager(fm, lm, size)
		block := types.NewBlock("test", 1)
		bufMan.Pin(block)
		bufMan.Pin(block)

		if n := bufMan.Available(); n != 9 {
			t.Fatalf("expected the same buffer to be pinned for the same block. %d buffers were pinned", n)
		}
	})

	t.Run("the least pinned buffer will be flushed", func(t *testing.T) {
		t.Parallel()
		fm, lm := &mockFileManager{}, &mockLogManager{}
		const size = 10

		bufMan := NewBufferManager(fm, lm, size)
		for i := 0; i < size; i++ {
			block := types.NewBlock("test", i)
			buf, err := bufMan.Pin(block)
			if err != nil {
				t.Fatal(err)
			}
			buf.SetModified(1, 1)
		}

		toBeEvicted := types.NewBlock("test", 3).ID()
		for i := 0; i < size; i++ {
			block := types.NewBlock("test", i)
			if block.ID() == toBeEvicted {
				continue
			}
			if _, err := bufMan.Pin(block); err != nil {
				t.Fatal(err)
			}
		}

		block := types.NewBlock("anothertable", 1)
		bufMan.Pin(block)

		if n := bufMan.Available(); n > 0 {
			t.Fatalf("expected no buffers to be available, got %d", n)
		}

		if lm.flushCalls != 1 {
			t.Fatalf("expected only one block to have been flushed, got %d", lm.flushCalls)
		}

		if fm.writeCalls != 1 {
			t.Fatalf("expected only one block to have been flushed, got %d", fm.writeCalls)
		}

		if evicted := fm.writtenBlocks[0]; evicted != toBeEvicted {
			t.Fatalf("expected block to be written to be %q, got %q", toBeEvicted, evicted)
		}
	})

	t.Run("pinning will time out if high contention", func(t *testing.T) {
		t.Parallel()
		const (
			size  = 3
			fname = "test"
		)

		fm, lm := &mockFileManager{}, &mockLogManager{}
		bufMan := NewBufferManager(fm, lm, 3)

		ctx, canc := context.WithCancel(context.Background())
		defer canc()

		started := make(chan struct{})

		go func(ctx context.Context) {
			blocks := []types.Block{
				types.NewBlock(fname, 1),
				types.NewBlock(fname, 2),
				types.NewBlock(fname, 3),
			}

			done := false

			for {
				select {
				case <-ctx.Done():
					return
				default:
					for i := 0; i < size; i++ {
						bufMan.Pin(blocks[i])
					}

					if !done {
						close(started)
						done = true
					}
				}
			}
		}(ctx)

		<-started

		_, err := bufMan.Pin(types.NewBlock("anotherfile", 0))
		if err != ErrClientTimeout {
			t.Fatalf("expected ErrClientTimeout, got %s", err)
		}

		if fc := lm.flushCalls; fc > 0 {
			t.Fatalf("expected blocks to not cause a WAL flush. WAL has been flushed %d times", fc)
		}

		if wc := fm.writeCalls; wc > 0 {
			t.Fatalf("expected blocks to not be written. They have been written %d times", wc)
		}
	})
}
