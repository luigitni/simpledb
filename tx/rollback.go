package tx

import (
	"fmt"
	"unsafe"

	"github.com/luigitni/simpledb/storage"
)

type rollbackLogRecord struct {
	txnum storage.TxID
}

const sizeOfRollbackRecord = int(unsafe.Sizeof(rollbackLogRecord{})) + int(storage.SizeOfTinyInt)

func newRollbackRecord(record *recordBuffer) rollbackLogRecord {
	f := record.readFixedLen(storage.SizeOfTinyInt)
	if v := txTypeFromFixedLen(f); v != ROLLBACK {
		panic(fmt.Sprintf("bad %s record: %s", ROLLBACK, v))
	}

	return rollbackLogRecord{
		txnum: storage.UnsafeFixedToInteger[storage.TxID](record.readFixedLen(storage.SizeOfTxID)),
	}
}

func (record rollbackLogRecord) Op() txType {
	return ROLLBACK
}

func (record rollbackLogRecord) TxNumber() storage.TxID {
	return record.txnum
}

func (record rollbackLogRecord) Undo(tx Transaction) {
	// do nothing
}

func (record rollbackLogRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d>", record.txnum)
}

func logRollback(lm logManager, txnum storage.TxID) int {
	buf := make([]byte, sizeOfRollbackRecord)

	writeRollback(buf, txnum)

	return lm.Append(buf)
}

func writeRollback(dst []byte, txnum storage.TxID) {
	rbuf := recordBuffer{bytes: dst}
	rbuf.writeFixedLen(
		storage.SizeOfTinyInt,
		storage.UnsafeIntegerToFixedlen[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(ROLLBACK)),
	)
	rbuf.writeFixedLen(
		storage.SizeOfTxID,
		storage.UnsafeIntegerToFixedlen[storage.TxID](storage.SizeOfTxID, txnum),
	)
}
