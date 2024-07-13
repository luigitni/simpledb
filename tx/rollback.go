package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type RollbackLogRecord struct {
	txNum int
}

func NewRollbackRecord(p *file.Page) RollbackLogRecord {
	const tpos = file.IntSize
	return RollbackLogRecord{
		txNum: p.Int(tpos),
	}
}

func (record RollbackLogRecord) Op() txType {
	return ROLLBACK
}

func (record RollbackLogRecord) TxNumber() int {
	return record.txNum
}

func (record RollbackLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record RollbackLogRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", record.txNum)
}

func LogRollback(lm *log.LogManager, txnum int) int {
	r := logRollback(txnum)
	return lm.Append(r)
}

func logRollback(txnum int) []byte {
	record := make([]byte, 2*file.IntSize)
	p := file.NewPageWithSlice(record)
	p.SetInt(0, int(ROLLBACK))
	p.SetInt(file.IntSize, txnum)
	return record
}
