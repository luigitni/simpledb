package tx_test

import (
	"testing"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestSerialTx(t *testing.T) {
	const blockname = "thisisablock"

	fm, lm, bm := test.MakeManagers(t)

	tx1 := tx.NewTx(fm, lm, bm)

	block := storage.NewBlock(blockname, 1)
	tx1.Pin(block)

	const (
		expInt1 = 42
		expStr1 = "one"
	)

	intVal1 := storage.UnsafeIntegerToFixedlen[storage.Int](storage.SizeOfInt, expInt1)
	strVal1 := storage.UnsafeNewVarlenFromGoString(expStr1)
	// the block initially contains unknown bytes
	// so do not log the values yet
	if err := tx1.SetFixedlen(block, 80, storage.SizeOfInt, intVal1, false); err != nil {
		t.Fatal(err)
	}

	if err := tx1.SetVarlen(block, 40, strVal1, false); err != nil {
		t.Fatal(err)
	}

	tx1.Commit()

	// after commit, expect values on the block to be correct
	tx2 := tx.NewTx(fm, lm, bm)
	tx2.Pin(block)

	ival, err := tx2.Fixedlen(block, 80, storage.SizeOfInt)
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.UnsafeFixedToInteger[storage.Int](ival); v != expInt1 {
		t.Fatalf("expected intval to be %d, got %d", expInt1, v)
	}

	sval, err := tx2.Varlen(block, 40)
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.UnsafeVarlenToGoString(sval); v != expStr1 {
		t.Fatalf("expected strval to be %s, got %s", expStr1, v)
	}

	// test overriding the same offset location
	const (
		expInt2 = 45
		expStr2 = "two"
	)
	intVal2 := storage.UnsafeIntegerToFixedlen[storage.Int](storage.SizeOfInt, expInt2)
	strVal2 := storage.UnsafeNewVarlenFromGoString(expStr2)

	if err := tx2.SetFixedlen(block, 80, storage.SizeOfInt, intVal2, true); err != nil {
		t.Fatal(err)
	}

	if err := tx2.SetVarlen(block, 40, strVal2, true); err != nil {
		t.Fatal(err)
	}

	tx2.Commit()

	// read the values again in tx3
	tx3 := tx.NewTx(fm, lm, bm)
	tx3.Pin(block)

	ival, err = tx3.Fixedlen(block, 80, storage.SizeOfInt)
	if err != nil {
		t.Fatal(err)
	}

	if v := storage.UnsafeFixedToInteger[storage.Int](ival); v != expInt2 {
		t.Fatalf("expected intval to be %d, got %d", expInt2, v)
	}

	sval, err = tx3.Varlen(block, 40)
	if err != nil {
		t.Fatal(err)
	}

	if s := storage.UnsafeVarlenToGoString(sval); s != expStr2 {
		t.Fatalf("expected strval to be %s, got %s", expStr2, s)
	}

	// test rollback

	const expInt3 = 9999

	intVal3 := storage.UnsafeIntegerToFixedlen[storage.Int](storage.SizeOfInt, expInt3)
	tx3.SetFixedlen(block, 80, storage.SizeOfInt, intVal3, true)

	ival, err = tx3.Fixedlen(block, 80, storage.SizeOfInt)
	if err != nil {
		t.Fatal(err)
	}

	// expect pre-rollback value to be exactly like the value that has been written by tx3
	if v := storage.UnsafeFixedToInteger[storage.Int](ival); v != expInt3 {
		t.Fatalf("expected intval to be %d, got %d", expInt3, v)
	}

	tx3.Rollback()

	tx4 := tx.NewTx(fm, lm, bm)
	tx4.Pin(block)

	ival, err = tx4.Fixedlen(block, 80, storage.SizeOfInt)
	if err != nil {
		t.Fatal(err)
	}

	// test that after rollback of tx3, intval has the value set by tx2
	if v := storage.UnsafeFixedToInteger[storage.Int](ival); v != expInt2 {
		t.Fatalf("expected intval to be %d, got %d", expInt2, v)
	}

	tx4.Commit()
}
