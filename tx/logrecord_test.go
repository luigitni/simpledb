package tx

import (
	"testing"

	"github.com/luigitni/simpledb/types"
)

func TestWriteBlock(t *testing.T) {
	buf := make([]byte, 100)
	rec := recordBuffer{bytes: buf}

	const fname = "testfile"
	const bid = 1

	block := types.NewBlock(fname, bid)

	rec.writeBlock(block)

	out := recordBuffer{bytes: buf}

	if b := out.readBlock(); b != block {
		t.Fatalf("expected block %v. Got %v", block, b)
	}
}

func assertIntegerAtOffset[V types.Integer](t *testing.T, data []byte, pos types.Offset, size types.Size, exp V) {
	t.Helper()
	rec := recordBuffer{bytes: data, offset: pos}

	if v := rec.readFixedLen(size); types.UnsafeFixedToInteger[V](v) != exp {
		t.Fatalf("expected %v at pos %d. Got %v", exp, pos, v)
	}
}

func assertVarlenAtPos(t *testing.T, data []byte, pos types.Offset, exp string) {
	t.Helper()
	rec := recordBuffer{bytes: data, offset: pos}

	if v := rec.readVarlen(); types.UnsafeVarlenToGoString(v) != exp {
		t.Fatalf("expected %q at pos %d. Got %q", exp, pos, v)
	}
}

func TestLogCheckpointRecord(t *testing.T) {
	buf := make([]byte, types.SizeOfTinyInt)
	writeCheckpoint(&buf)

	// test that the first entry is CHECKPOINT
	assertIntegerAtOffset(t, buf, 0, types.SizeOfTinyInt, types.TinyInt(CHECKPOINT))
}

func TestLogStartRecord(t *testing.T) {
	const txNum types.TxID = 123

	p := make([]byte, 16)
	writeStart(&p, txNum)

	var offset types.Offset

	assertIntegerAtOffset(t, p, offset, types.SizeOfTinyInt, types.TinyInt(START))
	offset += types.Offset(types.SizeOfTinyInt)

	assertIntegerAtOffset(t, p, offset, types.SizeOfTxID, txNum)
}

func TestLogRollbackRecord(t *testing.T) {
	const txNum types.TxID = 123

	p := make([]byte, 16)
	writeRollback(&p, txNum)

	// test that the first entry is ROLLBACK
	var offset types.Offset

	assertIntegerAtOffset(t, p, offset, types.SizeOfTinyInt, types.TinyInt(ROLLBACK))
	offset += types.Offset(types.SizeOfTinyInt)

	assertIntegerAtOffset(t, p, offset, types.SizeOfTxID, txNum)
}

func TestLogCommitRecord(t *testing.T) {
	const txNum types.TxID = 123

	p := make([]byte, 16)
	writeCommit(&p, txNum)

	var offset types.Offset

	assertIntegerAtOffset(t, p, offset, types.SizeOfTinyInt, types.TinyInt(COMMIT))
	offset += types.Offset(types.SizeOfTinyInt)

	assertIntegerAtOffset(t, p, offset, types.SizeOfTxID, txNum)
}

func TestLogFixedLenRecord(t *testing.T) {
	const txNum types.TxID = 123
	const val types.Int = 476
	const offsetVal types.Offset = 57

	const fname = "testblock"
	const bid types.Long = 1

	block := types.NewBlock(fname, bid)

	p := make([]byte, logSetIntSize)

	writeFixedLen(&p, txNum, block, offsetVal, types.SizeOfInt, types.UnsafeIntegerToFixed[types.Int](types.SizeOfInt, val))

	var offset types.Offset
	// test that the first entry is SETFIXED
	assertIntegerAtOffset(t, p, offset, types.SizeOfTinyInt, types.TinyInt(SETFIXED))
	offset += types.Offset(types.SizeOfTinyInt)

	// tx number
	assertIntegerAtOffset(t, p, offset, types.SizeOfTxID, txNum)
	offset += types.Offset(types.SizeOfTxID)

	// block name
	assertVarlenAtPos(t, p, offset, fname)
	offset += types.Offset(types.UnsafeSizeOfStringAsVarlen(fname))

	// block number
	assertIntegerAtOffset(t, p, offset, types.SizeOfLong, bid)
	offset += types.Offset(types.SizeOfLong)

	// offset of the record
	assertIntegerAtOffset(t, p, offset, types.SizeOfOffset, offsetVal)
	offset += types.Offset(types.SizeOfOffset)

	// size of the record
	assertIntegerAtOffset(t, p, offset, types.SizeOfSize, types.SizeOfInt)
	offset += types.Offset(types.SizeOfSize)

	// value of the record
	assertIntegerAtOffset(t, p, offset, types.SizeOfInt, val)

	newSetFixedLenRecord(recordBuffer{bytes: p})
}

func TestLogSetStrRecord(t *testing.T) {
	const txNum types.TxID = 123
	const val = "testvalue"
	const offsetVal types.Offset = 57

	const fname = "testblock"
	const bid types.Long = 1

	block := types.NewBlock(fname, bid)

	p := make([]byte, logSetIntSize+len(val))

	writeVarlen(&p, txNum, block, offsetVal, types.UnsafeNewVarlenFromGoString(val))

	var offset types.Offset

	// test that the first entry is SETSTRING
	assertIntegerAtOffset(t, p, offset, types.SizeOfTinyInt, types.TinyInt(SETSTRING))
	offset += types.Offset(types.SizeOfTinyInt)

	// tx number
	assertIntegerAtOffset(t, p, offset, types.SizeOfTxID, txNum)
	offset += types.Offset(types.SizeOfTxID)

	// block name
	assertVarlenAtPos(t, p, offset, fname)
	offset += types.Offset(types.UnsafeSizeOfStringAsVarlen(fname))

	// block id number
	assertIntegerAtOffset(t, p, offset, types.SizeOfLong, bid)
	offset += types.Offset(types.SizeOfLong)

	// offset of the record
	assertIntegerAtOffset(t, p, offset, types.SizeOfOffset, offsetVal)
	offset += types.Offset(types.SizeOfOffset)

	// value of the record
	assertVarlenAtPos(t, p, offset, val)
	offset += types.Offset(types.UnsafeSizeOfStringAsVarlen(val))

	newSetVarLenRecord(recordBuffer{bytes: p})
}
