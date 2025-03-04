package tx

import (
	"fmt"
	"unsafe"

	"github.com/luigitni/simpledb/storage"
)

type copyRecord struct {
	txnum  storage.TxID
	offset storage.Offset
	block  storage.Block
	size   storage.Offset
	data   []byte
}

const sizeOfCopyRecord = int(unsafe.Sizeof(copyRecord{})) + int(storage.SizeOfTinyInt)

func newCopyRecord(record *recordBuffer) copyRecord {
	rec := copyRecord{}

	f := record.readFixedLen(storage.SizeOfTinyInt)
	if v := txTypeFromFixedLen(f); v != COPY {
		panic(fmt.Sprintf("bad %s record: %s", COPY, v))
	}

	// read the transaction number
	rec.txnum = storage.FixedLenToInteger[storage.TxID](record.readFixedLen(storage.SizeOfTxID))
	// read the block name
	rec.block = record.readBlock()
	// read the block offset
	rec.offset = storage.FixedLenToInteger[storage.Offset](record.readFixedLen(storage.SizeOfOffset))
	// read the size of the data
	rec.size = storage.FixedLenToInteger[storage.Offset](record.readFixedLen(storage.SizeOfOffset))
	// read the data
	rec.data = record.readFixedLen(rec.size)

	return rec
}

func (cr copyRecord) Op() txType {
	return COPY
}

func (cr copyRecord) TxNumber() storage.TxID {
	return cr.txnum
}

func (cr copyRecord) String() string {
	// COPY txnum block offset data
	return fmt.Sprintf("<COPY[%d:%d] LEN:%d TX:%d B:%s DATA:%v>", cr.offset, cr.size + cr.offset, cr.size, cr.txnum, cr.block.ID(), cr.data)
}

func (cr copyRecord) Undo(tx Transaction) {
	tx.Pin(cr.block)
	tx.SetFixedlen(cr.block, cr.offset, cr.size, storage.ByteSliceToFixedlen(cr.data), false)
	tx.Unpin(cr.block)
}

func logCopy(lm logManager, txnum storage.TxID, block storage.Block, offset storage.Offset, data []byte) int {
	blocknameSize := storage.SizeOfStringAsVarlen(block.FileName())

	l := sizeOfCopyRecord + len(data) + int(blocknameSize)
	buf := make([]byte, l)
	written := writeCopy(buf, txnum, block, offset, data)

	return lm.Append(buf[:written])
}

func writeCopy(dst []byte, txnum storage.TxID, block storage.Block, offset storage.Offset, data []byte) storage.Offset {
	record := &recordBuffer{bytes: dst}
	record.writeFixedLen(storage.SizeOfTinyInt, storage.IntegerToFixedLen[storage.TinyInt](storage.SizeOfTinyInt, storage.TinyInt(COPY)))
	record.writeFixedLen(storage.SizeOfTxID, storage.IntegerToFixedLen[storage.TxID](storage.SizeOfTxID, txnum))
	record.writeBlock(block)

	// original offset of the data
	record.writeFixedLen(storage.SizeOfOffset, storage.IntegerToFixedLen[storage.Offset](storage.SizeOfOffset, offset))

	// write the size of the data
	record.writeFixedLen(storage.SizeOfOffset, storage.IntegerToFixedLen[storage.Offset](storage.SizeOfOffset, storage.Offset((len(data)))))
	// write the raw data
	record.writeRaw(data)

	return record.offset
}
