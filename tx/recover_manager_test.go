package tx

import (
	"bytes"
	"testing"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
	"github.com/luigitni/simpledb/wal"
)

type mockLogManager struct {
	buf                 bytes.Buffer
	flushCalledTimes    int
	iteratorCalledTimes int
}

func (m *mockLogManager) Append(rec []byte) int {
	n, _ := m.buf.Write(rec)
	return n
}

func (m *mockLogManager) Flush(lsn int) {
	m.flushCalledTimes++
}

func (m *mockLogManager) Iterator() *wal.WalIterator {
	m.iteratorCalledTimes++
	return &wal.WalIterator{}
}

func TestRecoveryManagerLogs(t *testing.T) {
	fm, _, bm := test.MakeManagers(t)

	lm := &mockLogManager{}

	// prepare the buffer with mock initial values
	block := storage.NewBlock("blockname", 7)
	buffer, err := bm.Pin(block)
	if err != nil {
		t.Fatal(err)
	}

	oldVals := [...]any{
		12: storage.Int(123),
		40: storage.NewVarlenFromGoString("this is the old val"),
		80: storage.SmallInt(6),
	}

	for k, v := range oldVals {
		k := storage.Offset(k)
		switch v := v.(type) {
		case storage.Int:
			buffer.Contents().SetFixedlen(k, storage.SizeOfInt, storage.IntegerToFixedLen(storage.SizeOfInt, v))
		case storage.Varlen:
			buffer.Contents().SetVarlen(k, v)
		case storage.SmallInt:
			buffer.Contents().SetFixedlen(k, storage.SizeOfSmallInt, storage.IntegerToFixedLen(storage.SizeOfSmallInt, v))
		}
	}

	t.Run("logs are written in sequence and tightly packed", func(t *testing.T) {
		x := NewTx(fm, lm, bm).(transactionImpl)
		man := x.recoverMan

		for k, v := range oldVals {
			k := storage.Offset(k)
			switch v.(type) {
			case storage.SmallInt:
				man.setFixedLen(buffer, k, storage.SizeOfSmallInt, nil)
			case storage.Int:
				man.setFixedLen(buffer, k, storage.SizeOfInt, nil)
			case storage.Varlen:
				man.setVarLen(buffer, k, nil)
			}
		}

		man.commit()

		written := lm.buf.Bytes()

		rb := &recordBuffer{bytes: written}
		if start := newStartLogRecord(rb); start.String() != "<START 2>" {
			t.Fatalf("expected <START 2>, got %s", start)
		}

		exp := "<SETFIXED:4 2 f:blocknameb:7 12 123>"
		if fixed := newSetFixedLenRecord(rb); fixed.String() != exp {
			t.Fatalf("expected %s, got %s", exp, fixed)
		}

		exp = "<SETVARLEN 2 f:blocknameb:7 40 this is the old val>"
		if varlen := newSetVarLenRecord(rb); varlen.String() != exp {
			t.Fatalf("expected %s, got %s", exp, varlen)
		}

		exp = "<SETFIXED:2 2 f:blocknameb:7 80 6>"
		if fixed := newSetFixedLenRecord(rb); fixed.String() != exp {
			t.Fatalf("expected %s, got %s", exp, fixed)
		}

		exp = "<COMMIT 2>"
		if commit := newCommitRecord(rb); commit.String() != exp {
			t.Fatalf("expected %s, got %s", exp, commit)
		}

		if lm.flushCalledTimes != 1 {
			t.Fatalf("expected flush to be called once, got %d", lm.flushCalledTimes)
		}
	})
}

func TestDoRollback(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	block := storage.NewBlock(test.DefaultTestBlockfile, 1)
	x := NewTx(fm, lm, bm)

	x.Pin(block)

	oldVals := [...]any{
		12: storage.Int(123),
		40: "this is the old val",
		80: storage.SmallInt(6),
	}

	newVals := [...]any{
		12:  storage.Int(456),
		40:  "this is the NEW val",
		80:  storage.SmallInt(7),
		812: storage.Int(999),
	}

	for k, v := range oldVals {
		k := storage.Offset(k)
		switch v := v.(type) {
		case storage.SmallInt:
			x.SetFixedlen(block, k, storage.SizeOfSmallInt, storage.IntegerToFixedLen(storage.SizeOfSmallInt, v), true)
		case storage.Int:
			x.SetFixedlen(block, k, storage.SizeOfInt, storage.IntegerToFixedLen(storage.SizeOfInt, v), true)
		case string:
			vl := storage.NewVarlenFromGoString(v)
			x.SetVarlen(block, k, vl, true)
		}
	}

	x.Commit()

	t.Run("rollback restores the old values", func(t *testing.T) {
		x := NewTx(fm, lm, bm)
		x.Pin(block)

		for k, v := range newVals {
			k := storage.Offset(k)
			switch v := v.(type) {
			case storage.SmallInt:
				x.SetFixedlen(block, k, storage.SizeOfSmallInt, storage.IntegerToFixedLen(storage.SizeOfSmallInt, v), true)
			case storage.Int:
				x.SetFixedlen(block, k, storage.SizeOfInt, storage.IntegerToFixedLen(storage.SizeOfInt, v), true)
			case string:
				vl := storage.NewVarlenFromGoString(v)
				x.SetVarlen(block, k, vl, true)
			}
		}

		x.Rollback()

		for k, v := range oldVals {
			k := storage.Offset(k)
			switch v := v.(type) {
			case storage.SmallInt:
				val, _ := x.Fixedlen(block, k, storage.SizeOfSmallInt)
				if got := storage.FixedLenToInteger[storage.SmallInt](val); got != v {
					t.Fatalf("expected %d, got %d", v, got)
				}
			case storage.Int:
				val, _ := x.Fixedlen(block, k, storage.SizeOfInt)
				if got := storage.FixedLenToInteger[storage.Int](val); got != v {
					t.Fatalf("expected %d, got %d", v, got)
				}
			case string:
				val, _ := x.Varlen(block, k)
				if got := storage.VarlenToGoString(val); got != v {
					t.Fatalf("expected %s, got %s", v, got)
				}
			}
		}

		// test the newly inserted value is not in the buffer
		v, _ := x.Fixedlen(block, 812, storage.SizeOfInt)
		if got := storage.FixedLenToInteger[storage.Int](v); got != 0 {
			t.Fatalf("expected 0, got %d", got)
		}
	})

	t.Run("recover restores the buffer to the last committed state", func(t *testing.T) {
		setLastTxNum(17)

		x := NewTx(fm, lm, bm).(transactionImpl)
		x.Pin(block)

		for k, v := range newVals {
			k := storage.Offset(k)
			switch v := v.(type) {
			case storage.SmallInt:
				x.SetFixedlen(block, k, storage.SizeOfSmallInt, storage.IntegerToFixedLen(storage.SizeOfSmallInt, v), true)
			case storage.Int:
				x.SetFixedlen(block, k, storage.SizeOfInt, storage.IntegerToFixedLen(storage.SizeOfInt, v), true)
			case string:
				vl := storage.NewVarlenFromGoString(v)
				x.SetVarlen(block, k, vl, true)
			}
		}

		// check the new values are in the buffer
		for k, v := range newVals {
			k := storage.Offset(k)
			switch v := v.(type) {
			case storage.SmallInt:
				val, _ := x.Fixedlen(block, k, storage.SizeOfSmallInt)
				if got := storage.FixedLenToInteger[storage.SmallInt](val); got != v {
					t.Fatalf("expected %d, got %d", v, got)
				}
			case storage.Int:
				val, _ := x.Fixedlen(block, k, storage.SizeOfInt)
				if got := storage.FixedLenToInteger[storage.Int](val); got != v {
					t.Fatalf("expected %d, got %d", v, got)
				}
			case string:
				val, _ := x.Varlen(block, k)
				if got := storage.VarlenToGoString(val); got != v {
					t.Fatalf("expected %s, got %s", v, got)
				}
			}
		}

		// do not commit, just recover
		x.Unpin(block)
		x.release()

		x = NewTx(fm, lm, bm).(transactionImpl)
		x.Pin(block)

		x.Recover()
		defer x.Commit()

		// check the quiescent checkpoint is in the wal
		it := lm.Iterator()
		log := it.Next()
		_ = newCheckpointRecord(&recordBuffer{bytes: log})
		it.Close()

		// check the old values are in the buffer
		for k, v := range oldVals {
			k := storage.Offset(k)
			switch v := v.(type) {
			case storage.SmallInt:
				val, _ := x.Fixedlen(block, k, storage.SizeOfSmallInt)
				if got := storage.FixedLenToInteger[storage.SmallInt](val); got != v {
					t.Fatalf("expected %d, got %d", v, got)
				}
			case storage.Int:
				val, _ := x.Fixedlen(block, k, storage.SizeOfInt)
				if got := storage.FixedLenToInteger[storage.Int](val); got != v {
					t.Fatalf("expected %d, got %d", v, got)
				}
			case string:
				val, _ := x.Varlen(block, k)
				if got := storage.VarlenToGoString(val); got != v {
					t.Fatalf("expected %s, got %s", v, got)
				}
			}
		}
	})
}

func TestRecoverCopy(t *testing.T) {
	fm, lm, bm := test.MakeManagers(t)

	block := storage.NewBlock(test.DefaultTestBlockfile, 1)
	x := NewTx(fm, lm, bm)

	x.Pin(block)

	oldVals := [...]any{
		12:  storage.Int(123),
		40:  "this is the old val",
		80:  storage.SmallInt(6),
		812: storage.Int(999),
	}

	for k, v := range oldVals {
		k := storage.Offset(k)
		switch v := v.(type) {
		case storage.SmallInt:
			x.SetFixedlen(block, k, storage.SizeOfSmallInt, storage.IntegerToFixedLen(storage.SizeOfSmallInt, v), true)
		case storage.Int:
			x.SetFixedlen(block, k, storage.SizeOfInt, storage.IntegerToFixedLen(storage.SizeOfInt, v), true)
		case string:
			vl := storage.NewVarlenFromGoString(v)
			x.SetVarlen(block, k, vl, true)
		}
	}

	x.Commit()

	t.Run("recover restores the buffer to the last committed state after copy", func(t *testing.T) {
		setLastTxNum(17)

		x := NewTx(fm, lm, bm).(transactionImpl)
		x.Pin(block)

		x.Copy(block, 12, 812, storage.Offset(storage.SizeOfInt), true)

		// expect the buffer to contain the new value at offset 812
		val, err := x.Fixedlen(block, 812, storage.SizeOfInt)
		if err != nil {
			t.Fatal(err)
		}

		if got := storage.FixedLenToInteger[storage.Int](val); got != 123 {
			t.Fatalf("expected 123, got %d", got)
		}

		// do not commit, just recover
		x.Unpin(block)
		x.release()

		x = NewTx(fm, lm, bm).(transactionImpl)
		x.Pin(block)

		x.Recover()

		// check the old value is at the location where the copy was made
		val, err = x.Fixedlen(block, 812, storage.SizeOfInt)
		if err != nil {
			t.Fatal(err)
		}

		if got := storage.FixedLenToInteger[storage.Int](val); got != 999 {
			t.Fatalf("expected 999, got %d", got)
		}
	})
}
