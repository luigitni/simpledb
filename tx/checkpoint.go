package tx

import "github.com/luigitni/simpledb/storage"

type checkpointLogRecord struct{}

func newCheckpointRecord(record recordBuffer) checkpointLogRecord {
	_ = record.readFixedLen(storage.SizeOfTinyInt)

	return checkpointLogRecord{}
}

func (record checkpointLogRecord) Op() txType {
	return CHECKPOINT
}

func (record checkpointLogRecord) TxNumber() storage.TxID {
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
		storage.SizeOfTinyInt,
		storage.UnsafeIntegerToFixed[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(CHECKPOINT)),
	)
}
