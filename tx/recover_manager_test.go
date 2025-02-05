package tx

import (
	"bytes"
	"testing"

	"github.com/luigitni/simpledb/log"
	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/test"
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

func (m *mockLogManager) Iterator() *log.WalIterator {
	m.iteratorCalledTimes++
	return &log.WalIterator{}
}

func TestRecoveryManager(t *testing.T) {
	fm, _, bm := test.MakeManagers(t)

	lm := &mockLogManager{}
	x := NewTx(fm, lm, bm).(transactionImpl)

	block := storage.NewBlock("blockname", 7)
	buffer, err := bm.Pin(block)
	if err != nil {
		t.Fatal(err)
	}

	oldVals := [...]any{
		12: storage.Int(123),
		40: storage.UnsafeNewVarlenFromGoString("this is the old val"),
		80: storage.SmallInt(6),
	}

	for k, v := range oldVals {
		k := storage.Offset(k)
		switch v := v.(type) {
		case storage.Int:
			buffer.Contents().UnsafeSetFixedLen(k, storage.SizeOfInt, storage.UnsafeIntegerToFixed(storage.SizeOfInt, v))
		case storage.Varlen:
			buffer.Contents().UnsafeSetVarlen(k, v)
		case storage.SmallInt:
			buffer.Contents().UnsafeSetFixedLen(k, storage.SizeOfSmallInt, storage.UnsafeIntegerToFixed(storage.SizeOfSmallInt, v))
		}
	}

	t.Run("test setFixedLen and setVarLen", func(t *testing.T) {

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
	})

}
