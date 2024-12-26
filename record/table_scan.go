package record

import (
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/pages"
	"github.com/luigitni/simpledb/tx"
)

var _ Scan = &tableScan{}

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
	recordPage  *pages.SlottedRecordPage
	fileName    string
	currentSlot int
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

func (ts *tableScan) Int(fieldname string) (int, error) {
	return ts.recordPage.Int(ts.currentSlot, fieldname)
}

func (ts *tableScan) String(fieldname string) (string, error) {
	return ts.recordPage.String(ts.currentSlot, fieldname)
}

func (ts *tableScan) Val(fieldname string) (file.Value, error) {
	switch ts.layout.schema.ftype(fieldname) {
	case file.INTEGER:
		v, err := ts.Int(fieldname)
		if err != nil {
			return file.Value{}, err
		}
		return file.ValueFromInt(v), nil
	case file.STRING:
		v, err := ts.String(fieldname)
		if err != nil {
			return file.Value{}, err
		}
		return file.ValueFromString(v), nil
	}

	pm := "invalid type for field " + fieldname
	panic(pm)
}

func (ts *tableScan) HasField(fieldname string) bool {
	return ts.layout.schema.HasField(fieldname)
}

// write methods

func (ts *tableScan) SetInt(fieldname string, val int) error {
	return ts.recordPage.SetInt(ts.currentSlot, fieldname, val)
}

func (ts *tableScan) SetString(fieldname string, val string) error {
	return ts.recordPage.SetString(ts.currentSlot, fieldname, val)
}

func (ts *tableScan) SetVal(fieldname string, val file.Value) error {
	switch ts.layout.schema.ftype(fieldname) {
	case file.INTEGER:
		return ts.SetInt(fieldname, val.AsIntVal())
	case file.STRING:
		return ts.SetString(fieldname, val.AsStringVal())
	}

	pm := "invalid type for field " + fieldname
	panic(pm)
}

// Insert looks for an empty slot to flag as used.
// It starts scanning the current block until such a slot is found.
// If the current block does not contain free slots, it attempts to move to the next block
// If the next block is at the end of the file, appends a new block and starts scanning from there.
func (ts *tableScan) Insert(recordSize int) error {
	for {
		slot, err := ts.recordPage.InsertAfter(ts.currentSlot, recordSize)
		if err == nil {
			ts.currentSlot = slot
			return nil
		}

		if err != pages.ErrNoFreeSlot {
			return err
		}

		atLastBlock, err := ts.isAtLastBlock()
		if err != nil {
			return err
		}

		if atLastBlock {
			if err := ts.moveToNewBlock(); err != nil {
				return err
			}
		} else {
			ts.moveToBlock(ts.recordPage.Block().Number() + 1)
		}
	}
}

func (ts *tableScan) Delete() error {
	return ts.recordPage.Delete(ts.currentSlot)
}

func (ts *tableScan) MoveToRID(rid RID) {
	ts.Close()
	block := file.NewBlock(ts.fileName, rid.Blocknum)
	ts.recordPage = pages.NewSlottedRecordPage(ts.x, block, ts.layout)
	ts.currentSlot = rid.Slot
}

func (ts *tableScan) GetRID() RID {
	return NewRID(ts.recordPage.Block().Number(), ts.currentSlot)
}

// moveToBlock closes the current page record page and opens a new one for the specified block.
// After the page has been changed, the TableScan positions itself before the first slot of the new block
func (ts *tableScan) moveToBlock(block int) {
	ts.Close()
	b := file.NewBlock(ts.fileName, block)
	ts.recordPage = pages.NewSlottedRecordPage(ts.x, b, ts.layout)
	ts.currentSlot = -1
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
	ts.recordPage = pages.NewSlottedRecordPage(ts.x, block, ts.layout)
	ts.recordPage.Format()
	ts.currentSlot = -1
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

	return ts.recordPage.Block().Number() == size-1, nil
}
