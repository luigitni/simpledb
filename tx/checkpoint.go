package tx

import (
	"fmt"

	"github.com/luigitni/simpledb/storage"
)

type checkpointLogRecord struct{}

const sizeOfCheckpointRecord = storage.SizeOfTinyInt

func newCheckpointRecord(record *recordBuffer) checkpointLogRecord {
	f := record.readFixedLen(storage.SizeOfTinyInt)
	if v := txTypeFromFixedLen(f); v != CHECKPOINT {
		panic(fmt.Sprintf("bad %s record: %s", COMMIT, v))
	}

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
	buf := make([]byte, sizeOfCheckpointRecord)
	writeCheckpoint(buf)
	return lm.Append(buf)
}

func writeCheckpoint(dst []byte) {
	rbuf := recordBuffer{bytes: dst}
	rbuf.writeFixedLen(
		storage.SizeOfTinyInt,
		storage.IntegerToFixedLen[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(CHECKPOINT)),
	)
}
