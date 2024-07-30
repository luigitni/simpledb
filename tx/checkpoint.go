package tx

type checkpointLogRecord struct{}

func (record checkpointLogRecord) Op() txType {
	return CHECKPOINT
}

func (record checkpointLogRecord) TxNumber() int {
	return -1
}

func (record checkpointLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record checkpointLogRecord) String() string {
	return "<CHECKPOINT>"
}

func LogCheckpoint(lm logManager) int {
	p := logPools.tiny1int.Get().(*[]byte)
	defer logPools.tiny1int.Put(p)
	logCheckpoint(p)
	return lm.Append(*p)
}

func logCheckpoint(dst *[]byte) {
	rbuf := recordBuffer{bytes: *dst}
	rbuf.writeInt(int(CHECKPOINT))
}
