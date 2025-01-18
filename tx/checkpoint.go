package tx

import "github.com/luigitni/simpledb/types"

type checkpointLogRecord struct{}

func newCheckpointRecord(record recordBuffer) checkpointLogRecord {
	_ = record.readFixedLen(types.SizeOfTinyInt)

	return checkpointLogRecord{}
}

func (record checkpointLogRecord) Op() txType {
	return CHECKPOINT
}

func (record checkpointLogRecord) TxNumber() types.TxID {
	return 0
}

func (record checkpointLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record checkpointLogRecord) String() string {
	return "<CHECKPOINT>"
}

func logCheckpoint(lm logManager) int {
	p := logPools.tiny1int.Get().(*[]byte)
	defer logPools.tiny1int.Put(p)
	writeCheckpoint(p)
	return lm.Append(*p)
}

func writeCheckpoint(dst *[]byte) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeFixedLen(
		types.SizeOfTinyInt,
		types.UnsafeIntegerToFixed[types.TinyInt](types.SizeOfTinyInt, types.TinyInt(CHECKPOINT)),
	)
}
