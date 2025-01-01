package tx_test

import (
	"os"
	"testing"

	"github.com/luigitni/simpledb/tx"
	"github.com/luigitni/simpledb/types"
)

var lockTable = tx.GetLockTable()

func TestMain(m *testing.M) {
	defer lockTable.Close()

	os.Exit(m.Run())
}

func TestLockTable(t *testing.T) {

	block := types.NewBlock("test", 1)

	// test that no wait happens when all clients request Slocks
	for i := 0; i < 100; i++ {
		lockTable.SLock(block)
	}

	t.Log("will unlock")
	for i := 0; i < 100; i++ {
		lockTable.Unlock(block)
	}

	t.Log("will acquire xlock")
	// acquire an x lock
	lockTable.XLock(block)
	t.Log("Xlock has been successfully acquired")

	out := make(chan error)

	for i := 0; i < 100; i++ {
		go func(block types.Block) {
			out <- lockTable.SLock(block)
		}(block)
	}

	for i := 0; i < 100; i++ {
		if e := <-out; e != tx.ErrLockAcquisitionTimeout {
			t.Fatalf("expected timeout on Slock acquisition when block is Xlocked. got nil")
		}
	}

	// unlock the Xlock
	lockTable.Unlock(block)
	t.Log("xlock has been unlocked")

	for i := 0; i < 100; i++ {
		go func(block types.Block) {
			out <- lockTable.SLock(block)
		}(block)
	}

	for i := 0; i < 100; i++ {
		if e := <-out; e != nil {
			t.Fatalf("expected shared lock to be successfully acquired")
		}
		lockTable.Unlock(block)
	}

	if err := lockTable.XLock(block); err != nil {
		t.Fatal("expected xlock to be successfully acquired after unlocks")
	}

}

func TestAcquireXLock(t *testing.T) {

	const fname = "tesblock"
	const howMany = 1000

	blocks := make([]types.Block, howMany)

	for i := 0; i < howMany; i++ {
		block := types.NewBlock(fname, i)
		if e := lockTable.XLock(block); e != nil {
			t.Fatalf("expected x lock to be acquired on block %d. Got error %s", i, e.Error())
		}

		blocks[i] = block
	}
}
