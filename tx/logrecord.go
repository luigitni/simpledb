package tx

import (
	"github.com/luigitni/simpledb/file"
)

type logRecord interface {

	// op returns the log record's type
	Op() txType

	// txNumber returns the tx id stored with the log record
	TxNumber() int

	// undo undoes the operation encoded by this log recod.
	Undo(tx Transaction)
}

type txType int

const (
	CHECKPOINT txType = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

func CreateLogRecord(bytes []byte) logRecord {
	p := file.NewPageWithSlice(bytes)
	switch txType(p.Int(0)) {
	case CHECKPOINT:
		return CheckpointLogRecord{}
	case START:
		return NewStartLogRecord(p)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollbackRecord(p)
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	}

	return nil
}
