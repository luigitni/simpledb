package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type SetStringLogRecord struct {
	txnum  int
	offset int
	block  file.BlockID
	val    string
}

// NewSetStringRecord constructs a SetStringLogRecord
// by reading from the given page.
// The layout of a string log record is populated according to WriteStringToLog
func NewSetStringRecord(p *file.Page) SetStringLogRecord {
	const tpos = file.IntBytes
	const fpos = tpos + file.IntBytes

	ss := SetStringLogRecord{}

	ss.txnum = p.GetInt(tpos)

	fname := p.GetString(fpos)

	bpos := fpos + file.MaxLength(len(fname))

	blocknum := p.GetInt(bpos)
	ss.block = file.NewBlockID(fname, blocknum)

	opos := bpos + file.IntBytes
	ss.offset = p.GetInt(opos)

	vpos := opos + file.IntBytes
	ss.val = p.GetString(vpos)

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
func LogSetString(lm *log.Manager, txnum int, block file.BlockID, offset int, val string) int {
	// precompute all the record offsets
	// tx number
	r := logSetString(txnum, block, offset, val)
	return lm.Append(r)
}

func logSetString(txnum int, block file.BlockID, offset int, val string) []byte {
	const tpos = file.IntBytes
	// filename
	const fpos = tpos + file.IntBytes
	// block id number
	bpos := fpos + file.MaxLength(len(block.Filename()))
	// offset
	opos := bpos + file.IntBytes
	// value
	vpos := opos + file.IntBytes
	vlen := vpos + file.MaxLength(len(val))

	record := make([]byte, vlen)

	page := file.NewPageWithSlice(record)
	// populate the page at all the offsets
	page.SetInt(0, int(SETSTRING))
	page.SetInt(tpos, txnum)
	page.SetString(fpos, block.Filename())
	page.SetInt(bpos, block.BlockNumber())
	page.SetInt(opos, offset)
	page.SetString(vpos, val)

	return record
}
