package tx

import (
	"testing"

	"github.com/luigitni/simpledb/file"
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
	logCheckpoint(&buf)

	// test that the first entry is CHECKPOINT
	assertIntAtPos(t, buf, 0, int(CHECKPOINT))
}

func TestLogStartRecord(t *testing.T) {
	const txNum = 123

	p := make([]byte, 16)
	logStart(&p, txNum)

	assertIntAtPos(t, p, 0, int(START))
	assertIntAtPos(t, p, file.IntSize, txNum)
}

func TestLogRollbackRecord(t *testing.T) {
	const txNum = 123

	p := make([]byte, 16)
	logRollback(&p, txNum)

	// test that the first entry is ROLLBACK
	assertIntAtPos(t, p, 0, int(ROLLBACK))
	assertIntAtPos(t, p, file.IntSize, txNum)
}

func TestLogCommitRecord(t *testing.T) {
	const txNum = 123

	p := make([]byte, 16)
	logCommit(&p, txNum)

	assertIntAtPos(t, p, 0, int(COMMIT))
	assertIntAtPos(t, p, file.IntSize, txNum)
}

func TestLogSetIntRecord(t *testing.T) {
	const txNum = 123
	const val = 476
	const offset = 57
	const fname = "testblock"
	const bid = 1

	block := file.NewBlock(fname, bid)

	p := make([]byte, logSetIntSize)
	logSetInt(&p, txNum, block, offset, val)

	const tpos = file.IntSize
	// filename
	const fpos = tpos + file.IntSize
	// block id number
	bpos := fpos + file.StrLength(len(block.FileName()))
	// offset
	opos := bpos + file.IntSize
	// value
	vpos := opos + file.IntSize

	assertIntAtPos(t, p, 0, int(SETINT))
	assertIntAtPos(t, p, tpos, txNum)
	assertStringAtPos(t, p, fpos, fname)
	assertIntAtPos(t, p, opos, offset)
	assertIntAtPos(t, p, vpos, val)
}

func TestLogSetStrRecord(t *testing.T) {
	const txNum = 123
	const val = "testvalue"
	const offset = 57
	const fname = "testblock"
	const bid = 1

	block := file.NewBlock(fname, bid)

	p := make([]byte, logSetIntSize+len(val))
	logSetString(&p, txNum, block, offset, val)

	const tpos = file.IntSize
	// filename
	const fpos = tpos + file.IntSize
	// block id number
	bpos := fpos + file.StrLength(len(block.FileName()))
	// offset
	opos := bpos + file.IntSize
	// value
	vpos := opos + file.IntSize

	assertIntAtPos(t, p, 0, int(SETSTRING))
	assertIntAtPos(t, p, tpos, txNum)
	assertStringAtPos(t, p, fpos, fname)
	assertIntAtPos(t, p, opos, offset)
	assertStringAtPos(t, p, vpos, val)
}
