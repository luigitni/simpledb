package tx

import (
	"slices"
	"testing"

	"github.com/luigitni/simpledb/storage"
)

func TestWriteBlock(t *testing.T) {
	buf := make([]byte, 100)
	rec := recordBuffer{bytes: buf}

	const fname = "testfile"
	const bid = 1

	block := storage.NewBlock(fname, bid)

	rec.writeBlock(block)

	out := recordBuffer{bytes: buf}

	if b := out.readBlock(); b != block {
		t.Fatalf("expected block %v. Got %v", block, b)
	}
}

func assertIntegerAtOffset[V storage.Integer](t *testing.T, data []byte, pos storage.Offset, size storage.Offset, exp V) {
	t.Helper()
	rec := recordBuffer{bytes: data, offset: pos}

	if v := rec.readFixedLen(size); storage.FixedLenToInteger[V](v) != exp {
		t.Fatalf("expected %v at pos %d. Got %v", exp, pos, v)
	}
}

func assertVarlenAtPos(t *testing.T, data []byte, pos storage.Offset, exp string) {
	t.Helper()
	rec := recordBuffer{bytes: data, offset: pos}

	if v := rec.readVarlen(); storage.VarlenToGoString(v) != exp {
		t.Fatalf("expected %q at pos %d. Got %q", exp, pos, v)
	}
}

func TestLogCheckpointRecord(t *testing.T) {
	buf := make([]byte, sizeOfCheckpointRecord)
	writeCheckpoint(buf)

	// test that the first entry is CHECKPOINT
	assertIntegerAtOffset(t, buf, 0, storage.SizeOfTinyInt, storage.TinyInt(CHECKPOINT))
}

func TestLogStartRecord(t *testing.T) {
	const txNum storage.TxID = 123

	p := make([]byte, sizeOfStartRecord)
	writeStart(p, txNum)

	var offset storage.Offset

	assertIntegerAtOffset(t, p, offset, storage.SizeOfTinyInt, storage.TinyInt(START))
	offset += storage.SizeOfTinyInt

	assertIntegerAtOffset(t, p, offset, storage.SizeOfTxID, txNum)
}

func TestLogRollbackRecord(t *testing.T) {
	const txNum storage.TxID = 123

	p := make([]byte, sizeOfRollbackRecord)
	writeRollback(p, txNum)

	// test that the first entry is ROLLBACK
	var offset storage.Offset

	assertIntegerAtOffset(t, p, offset, storage.SizeOfTinyInt, storage.TinyInt(ROLLBACK))
	offset += storage.SizeOfTinyInt

	assertIntegerAtOffset(t, p, offset, storage.SizeOfTxID, txNum)
}

func TestLogCommitRecord(t *testing.T) {
	const txNum storage.TxID = 123

	p := make([]byte, sizeOfCommitRecord)
	writeCommit(p, txNum)

	var offset storage.Offset

	assertIntegerAtOffset(t, p, offset, storage.SizeOfTinyInt, storage.TinyInt(COMMIT))
	offset += storage.SizeOfTinyInt

	assertIntegerAtOffset(t, p, offset, storage.SizeOfTxID, txNum)
}

func TestLogFixedlenRecord(t *testing.T) {
	const txNum storage.TxID = 123
	const val storage.Int = 476
	const offsetVal storage.Offset = 57

	const fname = "testblock"
	const bid storage.Long = 1

	block := storage.NewBlock(fname, bid)

	p := make([]byte, sizeOfFixedLenRecord+int(storage.SizeOfInt))

	writeFixedLen(p, txNum, block, offsetVal, storage.SizeOfInt, storage.IntegerToFixedLen[storage.Int](storage.SizeOfInt, val))

	var offset storage.Offset
	// test that the first entry is SETFIXED
	assertIntegerAtOffset(t, p, offset, storage.SizeOfTinyInt, storage.TinyInt(SETFIXEDLEN))
	offset += storage.SizeOfTinyInt

	// tx number
	assertIntegerAtOffset(t, p, offset, storage.SizeOfTxID, txNum)
	offset += storage.SizeOfTxID

	// block name
	assertVarlenAtPos(t, p, offset, fname)
	offset += storage.Offset(storage.SizeOfStringAsVarlen(fname))

	// block number
	assertIntegerAtOffset(t, p, offset, storage.SizeOfLong, bid)
	offset += storage.SizeOfLong

	// offset of the record
	assertIntegerAtOffset(t, p, offset, storage.SizeOfOffset, offsetVal)
	offset += storage.SizeOfOffset

	// size of the record
	assertIntegerAtOffset(t, p, offset, storage.SizeOfOffset, storage.SizeOfInt)
	offset += storage.SizeOfOffset

	// value of the record
	assertIntegerAtOffset(t, p, offset, storage.SizeOfInt, val)

	newSetFixedLenRecord(&recordBuffer{bytes: p})
}

func TestLogSetVarlenRecord(t *testing.T) {
	const txNum storage.TxID = 123
	const val = "testvalue"
	const offsetVal storage.Offset = 57

	const fname = "testblock"
	const bid storage.Long = 1

	block := storage.NewBlock(fname, bid)

	p := make([]byte, sizeOfVarlenRecord+len(val))

	writeVarlen(p, txNum, block, offsetVal, storage.NewVarlenFromGoString(val))

	var offset storage.Offset

	// test that the first entry is SETSTRING
	assertIntegerAtOffset(t, p, offset, storage.SizeOfTinyInt, storage.TinyInt(SETVARLEN))
	offset += storage.SizeOfTinyInt

	// tx number
	assertIntegerAtOffset(t, p, offset, storage.SizeOfTxID, txNum)
	offset += storage.SizeOfTxID

	// block name
	assertVarlenAtPos(t, p, offset, fname)
	offset += storage.Offset(storage.SizeOfStringAsVarlen(fname))

	// block id number
	assertIntegerAtOffset(t, p, offset, storage.SizeOfLong, bid)
	offset += storage.SizeOfLong

	// offset of the record
	assertIntegerAtOffset(t, p, offset, storage.SizeOfOffset, offsetVal)
	offset += storage.SizeOfOffset

	// value of the record
	assertVarlenAtPos(t, p, offset, val)
	offset += storage.Offset(storage.SizeOfStringAsVarlen(val))

	newSetVarLenRecord(&recordBuffer{bytes: p})
}

func TestLogCopy(t *testing.T) {
	const txNum storage.TxID = 123
	const val = "testvalue"
	const offsetVal storage.Offset = 57

	const fname = "testblock"
	const bid storage.Long = 1

	block := storage.NewBlock(fname, bid)

	p := make([]byte, sizeOfCopyRecord+len(val))

	writeCopy(p, txNum, block, offsetVal, []byte(val))

	var offset storage.Offset

	// test that the first entry is COPY
	assertIntegerAtOffset(t, p, offset, storage.SizeOfTinyInt, storage.TinyInt(COPY))
	offset += storage.SizeOfTinyInt

	// tx number
	assertIntegerAtOffset(t, p, offset, storage.SizeOfTxID, txNum)
	offset += storage.SizeOfTxID

	// block name
	assertVarlenAtPos(t, p, offset, fname)
	offset += storage.Offset(storage.SizeOfStringAsVarlen(fname))

	// block id number
	assertIntegerAtOffset(t, p, offset, storage.SizeOfLong, bid)
	offset += storage.SizeOfLong

	// offset of the record
	assertIntegerAtOffset(t, p, offset, storage.SizeOfOffset, offsetVal)
	offset += storage.SizeOfOffset

	// length of the data
	assertIntegerAtOffset(t, p, offset, storage.SizeOfOffset, storage.Offset(len(val)))
	offset += storage.SizeOfOffset

	got := p[offset : offset+storage.Offset(len(val))]

	if !slices.Equal(got, []byte(val)) {
		t.Fatalf("expected %q at pos %d. Got %q", val, offset, got)
	}

	newCopyRecord(&recordBuffer{bytes: p})
}
