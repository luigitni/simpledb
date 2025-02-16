package engine

import (
	"io"

	"github.com/luigitni/simpledb/pages"
	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

var _ UpdateScan = &tableScan{}

// tableScan store arbitrarily many records in multiple blocks of a file.
// The tableScan manages the records in a table: it hides the block structure
// from its clients, which will not know or care which block is currently being accessed.
// Each table scan in a query holds its current record page, which holds a buffer, which pins a page.
// When records in that page have been read, its buffer is unpinned
// and a record page for the next block in the file takes its place.
// Thus, a single pass through the table scan will access each block exactly once.
//
// The cost of a table scan is thus:
// The number of blocks in the underlying table +
// The number of records in the underlying table +
// The number of distinct values in the underlying table
type tableScan struct {
	x           tx.Transaction
	layout      Layout
	recordPage  *pages.SlottedPage
	fileName    string
	currentSlot storage.SmallInt
}

func newTableScan(tx tx.Transaction, tablename string, layout Layout) *tableScan {
	fname := tablename + ".tbl"

	ts := &tableScan{
		x:        tx,
		layout:   layout,
		fileName: fname,
	}

	size, err := tx.Size(fname)
	if err != nil {
		panic(err)
	}

	if size == 0 {
		ts.moveToNewBlock()
	} else {
		ts.moveToBlock(0)
	}

	return ts
}

func (ts *tableScan) BeforeFirst() error {
	ts.moveToBlock(0)
	return nil
}

// Close unpins the underlying buffer over the record page.
func (ts *tableScan) Close() {
	if ts.recordPage != nil {
		ts.recordPage.Close()
	}
}

// Next moves to the next record in the current record page.
// If there are no more records in that page, then moves to the next block of the file
// and gets its next record.
// It then continues until either a next record is found or the end of the file is encountered, in which case returns false
func (ts *tableScan) Next() error {
	for {
		slot, err := ts.recordPage.NextAfter(ts.currentSlot)
		if err == nil {
			ts.currentSlot = slot
			break
		}

		if err != pages.ErrNoFreeSlot {
			return err
		}

		atLastBlock, err := ts.isAtLastBlock()
		if err != nil {
			return err
		}

		if atLastBlock {
			return io.EOF
		}

		// move to block next to the one the record page is pointing to
		nextBlock := ts.recordPage.Block().Number() + 1
		ts.moveToBlock(nextBlock)
	}

	return nil
}

func (ts *tableScan) FixedLen(fieldname string) (storage.FixedLen, error) {
	return ts.recordPage.FixedLen(ts.currentSlot, fieldname)
}

func (ts *tableScan) Varlen(fieldname string) (storage.Varlen, error) {
	return ts.recordPage.VarLen(ts.currentSlot, fieldname)
}

func (ts *tableScan) Val(fieldname string) (storage.Value, error) {
	if size := ts.layout.schema.ftype(fieldname).Size(); size != storage.SizeOfVarlen {
		v, err := ts.FixedLen(fieldname)
		if err != nil {
			return storage.Value{}, err
		}

		return storage.ValueFromFixedLen(v), nil
	}

	v, err := ts.Varlen(fieldname)
	if err != nil {
		return storage.Value{}, err
	}

	return storage.ValueFromVarlen(v), nil
}

func (ts *tableScan) HasField(fieldname string) bool {
	return ts.layout.schema.HasField(fieldname)
}

func (ts *tableScan) SetVal(fieldname string, val storage.Value) error {
	if size := ts.layout.schema.ftype(fieldname).Size(); size != storage.SizeOfVarlen {
		return ts.recordPage.SetFixedLen(ts.currentSlot, fieldname, val.AsFixedLen())
	}

	return ts.recordPage.SetVarLen(ts.currentSlot, fieldname, val.AsVarlen())
}

// Insert looks for an empty slot to flag as used.
// It starts scanning the current block until such a slot is found.
// If the current block does not contain free slots, it attempts to move to the next block
// If the next block is at the end of the file, appends a new block and starts scanning from there.
func (ts *tableScan) Insert(recordSize storage.Offset) error {
	rid, err := ts.insert(recordSize, false)
	if err != nil {
		return err
	}

	ts.MoveToRID(rid)

	return nil
}

func (ts *tableScan) insert(recordSize storage.Offset, update bool) (RID, error) {
	for {
		slot, err := ts.recordPage.InsertAfter(ts.currentSlot, recordSize, update)
		if err == nil {
			return NewRID(ts.recordPage.Block().Number(), slot), nil
		}

		if err != pages.ErrNoFreeSlot {
			return RID{}, err
		}

		atLastBlock, err := ts.isAtLastBlock()
		if err != nil {
			return RID{}, err
		}

		if atLastBlock {
			if err := ts.moveToNewBlock(); err != nil {
				return RID{}, err
			}
		} else {
			ts.moveToBlock(ts.recordPage.Block().Number() + 1)
		}
	}
}

func (ts *tableScan) Update(recordSize storage.Offset) error {
	if err := ts.Delete(); err != nil {
		return err
	}

	rid, err := ts.insert(recordSize, true)
	if err != nil {
		return err
	}

	ts.MoveToRID(rid)

	return nil
}

func (ts *tableScan) Delete() error {
	return ts.recordPage.Delete(ts.currentSlot)
}

func (ts *tableScan) MoveToRID(rid RID) {
	ts.Close()
	block := storage.NewBlock(ts.fileName, rid.Blocknum)
	ts.recordPage = pages.NewSlottedPage(ts.x, block, ts.layout)
	ts.currentSlot = rid.Slot
}

func (ts *tableScan) GetRID() RID {
	return NewRID(ts.recordPage.Block().Number(), ts.currentSlot)
}

// moveToBlock closes the current page record page and opens a new one for the specified block.
// After the page has been changed, the TableScan positions itself before the first slot of the new block
func (ts *tableScan) moveToBlock(block storage.Long) {
	ts.Close()
	b := storage.NewBlock(ts.fileName, block)
	ts.recordPage = pages.NewSlottedPage(ts.x, b, ts.layout)
	ts.currentSlot = pages.BeforeFirstSlot
}

// moveToNewBlock attempts to append a new block to the file
// If the operation succeeds, it associates the new block with a new record page
// formats the page according to the layout and sets the current slot pointer to -1
func (ts *tableScan) moveToNewBlock() error {
	ts.Close()
	block, err := ts.x.Append(ts.fileName)
	if err != nil {
		return err
	}
	ts.recordPage = pages.NewSlottedPage(ts.x, block, ts.layout)
	ts.recordPage.Format(0)
	ts.currentSlot = pages.BeforeFirstSlot
	return nil
}

// isAtLastBlock returns true if the block the underlying record page is pointing to
// is the last block of the file.
// Returns an error if the transaction fails to acquire a read lock on the final block
func (ts *tableScan) isAtLastBlock() (bool, error) {
	// get the number of blocks in the associated file
	size, err := ts.x.Size(ts.fileName)
	if err != nil {
		return false, err
	}

	return ts.recordPage.Block().Number() == storage.Long(size-1), nil
}
