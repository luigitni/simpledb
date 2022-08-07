package tx

import (
	"testing"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/test"
)

func TestLogCheckpointRecord(t *testing.T) {
	record := logCheckpoint()

	p := file.NewPageWithSlice(record)
	// test that the first entry is CHECKPOINT
	test.AssertIntAtPos(t, p, 0, int(CHECKPOINT))
}

func TestLogStartRecord(t *testing.T) {
	const txNum = 123

	record := logStart(txNum)
	// test that the record is properly formatted
	p := file.NewPageWithSlice(record)

	test.AssertIntAtPos(t, p, 0, int(START))
	test.AssertIntAtPos(t, p, file.IntBytes, txNum)
}

func TestLogRollbackRecord(t *testing.T) {
	const txNum = 123

	record := logRollback(txNum)
	// test that the record is properly formatted
	p := file.NewPageWithSlice(record)

	// test that the first entry is ROLLBACK
	test.AssertIntAtPos(t, p, 0, int(ROLLBACK))
	test.AssertIntAtPos(t, p, file.IntBytes, txNum)
}

func TestLogCommitRecord(t *testing.T) {
	const txNum = 123

	record := logCommit(txNum)
	// test that the record is properly formatted
	p := file.NewPageWithSlice(record)

	test.AssertIntAtPos(t, p, 0, int(COMMIT))
	test.AssertIntAtPos(t, p, file.IntBytes, txNum)
}

func TestLogSetIntRecord(t *testing.T) {
	const txNum = 123
	const val = 476
	const offset = 57
	const fname = "testblock"
	const bid = 1

	block := file.NewBlockID(fname, bid)

	record := logSetInt(txNum, block, offset, val)

	p := file.NewPageWithSlice(record)

	const tpos = file.IntBytes
	// filename
	const fpos = tpos + file.IntBytes
	// block id number
	bpos := fpos + file.MaxLength(len(block.Filename()))
	// offset
	opos := bpos + file.IntBytes
	// value
	vpos := opos + file.IntBytes

	test.AssertIntAtPos(t, p, 0, int(SETINT))
	test.AssertIntAtPos(t, p, tpos, txNum)
	test.AssertStrAtPos(t, p, fpos, fname)
	test.AssertIntAtPos(t, p, opos, offset)
	test.AssertIntAtPos(t, p, vpos, val)
}

func TestLogSetStrRecord(t *testing.T) {
	const txNum = 123
	const val = "testvalue"
	const offset = 57
	const fname = "testblock"
	const bid = 1

	block := file.NewBlockID(fname, bid)

	record := logSetString(txNum, block, offset, val)

	p := file.NewPageWithSlice(record)

	const tpos = file.IntBytes
	// filename
	const fpos = tpos + file.IntBytes
	// block id number
	bpos := fpos + file.MaxLength(len(block.Filename()))
	// offset
	opos := bpos + file.IntBytes
	// value
	vpos := opos + file.IntBytes

	test.AssertIntAtPos(t, p, 0, int(SETSTRING))
	test.AssertIntAtPos(t, p, tpos, txNum)
	test.AssertStrAtPos(t, p, fpos, fname)
	test.AssertIntAtPos(t, p, opos, offset)
	test.AssertStrAtPos(t, p, vpos, val)
}
