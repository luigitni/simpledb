package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type SetIntLogRecord struct {
	txnum  int
	offset int
	block  file.Block
	val    int
}

func NewSetIntRecord(p *file.Page) SetIntLogRecord {
	const tpos = file.IntSize
	const fpos = tpos + file.IntSize

	si := SetIntLogRecord{}
	si.txnum = p.Int(tpos)
	fname := p.String(fpos)

	bpos := fpos + file.MaxLength(len(fname))
	blockId := p.Int(bpos)

	si.block = file.NewBlock(fname, blockId)

	opos := bpos + file.IntSize
	si.offset = p.Int(opos)

	vpos := opos + file.IntSize
	si.val = p.Int(vpos)

	return si
}

func (si SetIntLogRecord) Op() txType {
	return SETINT
}

func (si SetIntLogRecord) TxNumber() int {
	return si.txnum
}

func (si SetIntLogRecord) String() string {
	return fmt.Sprintf("<SETINT %d %s %d %d>", si.txnum, si.block, si.offset, si.val)
}

func (si SetIntLogRecord) Undo(tx Transaction) {
	tx.Pin(si.block)
	tx.SetInt(si.block, si.offset, si.val, false)
	tx.Unpin(si.block)
}

// LogSetInt appends a string records to the log file, by calling log.Manager.Append
// An int log entry has the following layout:
// | log type | tx number | filename | block number | offset | value |
func LogSetInt(lm *log.LogManager, txnum int, block file.Block, offset int, val int) int {
	r := logSetInt(txnum, block, offset, val)
	return lm.Append(r)
}

func logSetInt(txnum int, block file.Block, offset int, val int) []byte {
	// precompute all the record offsets
	// tx number
	const tpos = file.IntSize
	// filename
	const fpos = tpos + file.IntSize
	// block id number
	bpos := fpos + file.MaxLength(len(block.FileName()))
	// offset
	opos := bpos + file.IntSize
	// value
	vpos := opos + file.IntSize
	vlen := vpos + file.IntSize

	record := make([]byte, vlen)

	page := file.NewPageWithSlice(record)
	// populate the page at all the offsets
	page.SetInt(0, int(SETINT))
	page.SetInt(tpos, txnum)
	page.SetString(fpos, block.FileName())
	page.SetInt(bpos, block.Number())
	page.SetInt(opos, offset)
	page.SetInt(vpos, val)
	return record
}
