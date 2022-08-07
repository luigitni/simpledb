package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type StartLogRecord struct {
	txnum int
}

func NewStartLogRecord(p *file.Page) StartLogRecord {
	const tpos = file.IntBytes
	return StartLogRecord{
		txnum: p.GetInt(tpos),
	}
}

func (record StartLogRecord) Op() txType {
	return START
}

func (record StartLogRecord) TxNumber() int {
	return record.txnum
}

func (record StartLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record StartLogRecord) String() string {
	return fmt.Sprintf("<START %d>", record.txnum)
}

func LogStart(lm *log.Manager, txnum int) int {
	r := logStart(txnum)
	return lm.Append(r)
}

func logStart(txnum int) []byte {
	record := make([]byte, 2*file.IntBytes)
	p := file.NewPageWithSlice(record)
	p.SetInt(0, int(START))
	p.SetInt(file.IntBytes, txnum)
	return record
}
