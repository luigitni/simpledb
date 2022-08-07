package tx

import (
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/log"
)

type CheckpointLogRecord struct{}

func (record CheckpointLogRecord) Op() txType {
	return CHECKPOINT
}

func (record CheckpointLogRecord) TxNumber() int {
	return -1
}

func (record CheckpointLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record CheckpointLogRecord) String() string {
	return "<CHECKPOINT>"
}

func LogCheckpoint(lm *log.Manager) int {
	rec := logCheckpoint()
	return lm.Append(rec)
}

func logCheckpoint() []byte {
	rec := make([]byte, file.IntBytes)
	p := file.NewPageWithSlice(rec)
	p.SetInt(0, int(CHECKPOINT))
	return rec
}
