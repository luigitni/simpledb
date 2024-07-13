package tx_test

import (
	"testing"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/tx"
)

func TestSerialTx(t *testing.T) {
	const blockname = "testfile"

	fm, lm, bm := test.MakeManagers(t)

	tx1 := tx.NewTx(fm, lm, bm)

	block := file.NewBlock(blockname, 1)
	tx1.Pin(block)

	const intVal1 = 1
	const strVal1 = "one"
	// the block initially contains unknown bytes
	// so do not log the values yet
	if err := tx1.SetInt(block, 80, intVal1, false); err != nil {
		t.Fatal(err)
	}

	if err := tx1.SetString(block, 40, strVal1, false); err != nil {
		t.Fatal(err)
	}

	tx1.Commit()

	// after commit, expect values on the block to be correct
	tx2 := tx.NewTx(fm, lm, bm)
	tx2.Pin(block)

	ival, err := tx2.Int(block, 80)
	if err != nil {
		t.Fatal(err)
	}

	if ival != intVal1 {
		t.Fatalf("expected intval to be %d, got %d", intVal1, ival)
	}

	sval, err := tx2.String(block, 40)
	if err != nil {
		t.Fatal(err)
	}

	if sval != strVal1 {
		t.Fatalf("expected strval to be %s, got %s", strVal1, sval)
	}

	const intVal2 = intVal1 + 1
	const strVal2 = strVal1 + "!"

	if err := tx2.SetInt(block, 80, intVal2, true); err != nil {
		t.Fatal(err)
	}

	if err := tx2.SetString(block, 40, strVal2, true); err != nil {
		t.Fatal(err)
	}

	tx2.Commit()

	// transaction 3
	tx3 := tx.NewTx(fm, lm, bm)
	tx3.Pin(block)

	ival, err = tx3.Int(block, 80)
	if err != nil {
		t.Fatal(err)
	}

	if ival != intVal2 {
		t.Fatalf("expected intval to be %d, got %d", intVal2, ival)
	}

	sval, err = tx3.String(block, 40)
	if err != nil {
		t.Fatal(err)
	}

	if sval != strVal2 {
		t.Fatalf("expected strval to be %s, got %s", strVal2, sval)
	}

	const intVal3 = 9999
	tx3.SetInt(block, 80, intVal3, true)

	v, err := tx3.Int(block, 80)
	if err != nil {
		t.Fatal(err)
	}

	// expect pre-rollback value to be exactly like the value that has been written by tx3
	if v != intVal3 {
		t.Fatalf("expected intval to be %d, got %d", intVal3, ival)
	}

	tx3.Rollback()

	tx4 := tx.NewTx(fm, lm, bm)
	tx4.Pin(block)

	v, err = tx4.Int(block, 80)
	if err != nil {
		t.Fatal(err)
	}

	// test that after rollback of tx3, intval has the value set by tx2
	if v != intVal2 {
		t.Fatalf("expected intval to be %d, got %d", intVal2, ival)
	}

	tx4.Commit()
}
