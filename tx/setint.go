package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type SetIntLogRecord struct {
	txnum  int
	offset int
	block  file.BlockID
	val    int
}

func NewSetIntRecord(p *file.Page) SetIntLogRecord {
	const tpos = file.IntBytes
	const fpos = tpos + file.IntBytes

	si := SetIntLogRecord{}
	si.txnum = p.GetInt(tpos)
	fname := p.GetString(fpos)

	bpos := fpos + file.MaxLength(len(fname))
	blockId := p.GetInt(bpos)

	si.block = file.NewBlockID(fname, blockId)

	opos := bpos + file.IntBytes
	si.offset = p.GetInt(opos)

	vpos := opos + file.IntBytes
	si.val = p.GetInt(vpos)

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
func LogSetInt(lm *log.Manager, txnum int, block file.BlockID, offset int, val int) int {
	r := logSetInt(txnum, block, offset, val)
	return lm.Append(r)
}

func logSetInt(txnum int, block file.BlockID, offset int, val int) []byte {
	// precompute all the record offsets
	// tx number
	const tpos = file.IntBytes
	// filename
	const fpos = tpos + file.IntBytes
	// block id number
	bpos := fpos + file.MaxLength(len(block.Filename()))
	// offset
	opos := bpos + file.IntBytes
	// value
	vpos := opos + file.IntBytes
	vlen := vpos + file.IntBytes

	record := make([]byte, vlen)

	page := file.NewPageWithSlice(record)
	// populate the page at all the offsets
	page.SetInt(0, int(SETINT))
	page.SetInt(tpos, txnum)
	page.SetString(fpos, block.Filename())
	page.SetInt(bpos, block.BlockNumber())
	page.SetInt(opos, offset)
	page.SetInt(vpos, val)
	return record
}
