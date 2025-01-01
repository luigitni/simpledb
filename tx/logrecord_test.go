package tx

import (
	"testing"

	"github.com/luigitni/simpledb/types"
)

func assertIntAtPos(t *testing.T, data []byte, pos int, exp int) {
	t.Helper()
	rec := recordBuffer{bytes: data, offset: pos}
	if v := rec.readInt(); v != exp {
		t.Fatalf("expected %d at pos %d. Got %d", exp, pos, v)
	}
}

func assertStringAtPos(t *testing.T, data []byte, pos int, exp string) {
	t.Helper()
	rec := recordBuffer{bytes: data, offset: pos}
	if v := rec.readString(); v != exp {
		t.Fatalf("expected %q at pos %d. Got %q", exp, pos, v)
	}
}

func TestLogCheckpointRecord(t *testing.T) {
	buf := make([]byte, 8)
	writeCheckpoint(&buf)

	// test that the first entry is CHECKPOINT
	assertIntAtPos(t, buf, 0, int(CHECKPOINT))
}

func TestLogStartRecord(t *testing.T) {
	const txNum = 123

	p := make([]byte, 16)
	writeStart(&p, txNum)

	assertIntAtPos(t, p, 0, int(START))
	assertIntAtPos(t, p, types.IntSize, txNum)

	newStartLogRecord(recordBuffer{bytes: p})
}

func TestLogRollbackRecord(t *testing.T) {
	const txNum = 123

	p := make([]byte, 16)
	writeRollback(&p, txNum)

	// test that the first entry is ROLLBACK
	assertIntAtPos(t, p, 0, int(ROLLBACK))
	assertIntAtPos(t, p, types.IntSize, txNum)

	newRollbackRecord(recordBuffer{bytes: p})
}

func TestLogCommitRecord(t *testing.T) {
	const txNum = 123

	p := make([]byte, 16)
	writeCommit(&p, txNum)

	assertIntAtPos(t, p, 0, int(COMMIT))
	assertIntAtPos(t, p, types.IntSize, txNum)

	newCommitRecord(recordBuffer{bytes: p})
}

func TestLogSetIntRecord(t *testing.T) {
	const txNum = 123
	const val = 476
	const offset = 57
	const fname = "testblock"
	const bid = 1

	block := types.NewBlock(fname, bid)

	p := make([]byte, logSetIntSize)
	writeInt(&p, txNum, block, offset, val)

	const tpos = types.IntSize
	// filename
	const fpos = tpos + types.IntSize
	// block id number
	bpos := fpos + types.StrLength(len(block.FileName()))
	// offset
	opos := bpos + types.IntSize
	// value
	vpos := opos + types.IntSize

	assertIntAtPos(t, p, 0, int(SETINT))
	assertIntAtPos(t, p, tpos, txNum)
	assertStringAtPos(t, p, fpos, fname)
	assertIntAtPos(t, p, opos, offset)
	assertIntAtPos(t, p, vpos, val)

	newSetIntRecord(recordBuffer{bytes: p})
}

func TestLogSetStrRecord(t *testing.T) {
	const txNum = 123
	const val = "testvalue"
	const offset = 57
	const fname = "testblock"
	const bid = 1

	block := types.NewBlock(fname, bid)

	p := make([]byte, logSetIntSize+len(val))
	writeString(&p, txNum, block, offset, val)

	const tpos = types.IntSize
	// filename
	const fpos = tpos + types.IntSize
	// block id number
	bpos := fpos + types.StrLength(len(block.FileName()))
	// offset
	opos := bpos + types.IntSize
	// value
	vpos := opos + types.IntSize

	assertIntAtPos(t, p, 0, int(SETSTRING))
	assertIntAtPos(t, p, tpos, txNum)
	assertStringAtPos(t, p, fpos, fname)
	assertIntAtPos(t, p, opos, offset)
	assertStringAtPos(t, p, vpos, val)

	newSetStringRecord(recordBuffer{bytes: p})
}
