package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type CommitLogRecord struct {
	txnum int
}

func NewCommitRecord(p *file.Page) CommitLogRecord {
	const tpos = file.IntBytes
	return CommitLogRecord{
		txnum: p.Int(tpos),
	}
}

func (record CommitLogRecord) Op() txType {
	return COMMIT
}

func (record CommitLogRecord) TxNumber() int {
	return record.txnum
}

func (record CommitLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record CommitLogRecord) String() string {
	return fmt.Sprintf("<COMMIT %d>", record.txnum)
}

func LogCommit(lm *log.LogManager, txnum int) int {
	record := logCommit(txnum)
	return lm.Append(record)
}

func logCommit(txnum int) []byte {
	record := make([]byte, 2*file.IntBytes)
	p := file.NewPageWithSlice(record)
	p.SetInt(0, int(COMMIT))
	p.SetInt(file.IntBytes, txnum)
	return record
}
