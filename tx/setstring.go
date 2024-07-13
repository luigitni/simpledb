package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type SetStringLogRecord struct {
	txnum  int
	offset int
	block  file.Block
	val    string
}

// NewSetStringRecord constructs a SetStringLogRecord
// by reading from the given page.
// The layout of a string log record is populated according to WriteStringToLog
func NewSetStringRecord(p *file.Page) SetStringLogRecord {
	const tpos = file.IntSize
	const fpos = tpos + file.IntSize

	ss := SetStringLogRecord{}

	ss.txnum = p.Int(tpos)

	fname := p.String(fpos)

	bpos := fpos + file.MaxLength(len(fname))

	blocknum := p.Int(bpos)
	ss.block = file.NewBlock(fname, blocknum)

	opos := bpos + file.IntSize
	ss.offset = p.Int(opos)

	vpos := opos + file.IntSize
	ss.val = p.String(vpos)

	return ss
}

func (ss SetStringLogRecord) Op() txType {
	return SETSTRING
}

func (ss SetStringLogRecord) TxNumber() int {
	return ss.txnum
}

func (ss SetStringLogRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %s %d %s>", ss.txnum, ss.block, ss.offset, ss.val)
}

// Undo replaces the specified data value with the value saved in the log record.
// The method pins a buffer to the specified block, calls tx.SetString to restore the saved value,
// and unpins the buffer
func (ss SetStringLogRecord) Undo(tx Transaction) {
	tx.Pin(ss.block)
	tx.SetString(ss.block, ss.offset, ss.val, false)
	tx.Unpin(ss.block)
}

// LogSetString appends a string records to the log file, by calling log.Manager.Append
// A string log entry has the following layout:
// | log type | tx number | filename | block number | offset | value |
func LogSetString(lm *log.LogManager, txnum int, block file.Block, offset int, val string) int {
	// precompute all the record offsets
	// tx number
	r := logSetString(txnum, block, offset, val)
	return lm.Append(r)
}

func logSetString(txnum int, block file.Block, offset int, val string) []byte {
	const tpos = file.IntSize
	// filename
	const fpos = tpos + file.IntSize
	// block id number
	bpos := fpos + file.MaxLength(len(block.FileName()))
	// offset
	opos := bpos + file.IntSize
	// value
	vpos := opos + file.IntSize
	vlen := vpos + file.MaxLength(len(val))

	record := make([]byte, vlen)

	page := file.NewPageWithSlice(record)
	// populate the page at all the offsets
	page.SetInt(0, int(SETSTRING))
	page.SetInt(tpos, txnum)
	page.SetString(fpos, block.FileName())
	page.SetInt(bpos, block.Number())
	page.SetInt(opos, offset)
	page.SetString(vpos, val)

	return record
}
