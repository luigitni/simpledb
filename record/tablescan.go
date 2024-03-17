package record

import (
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

// TableScan store arbitrarily many records in multiple blocks of a file.
// The TableScan manages the records in a table: it hides the block structure
// from its clients, which will not know or care which block is currently being accessed.
type TableScan struct {
	tx          tx.Transaction
	layout      Layout
	rp          *RecordPage
	fname       string
	currentSlot int
}

func NewTableScan(tx tx.Transaction, tablename string, layout Layout) *TableScan {
	fname := tablename + ".tbl"

	ts := &TableScan{
		tx:     tx,
		layout: layout,
		fname:  fname,
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

func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.Block())
	}
}

// Next moves to the next record in the current record page.
// If there are no more records in that page, then moves to the next block of the file
// and gets its next record.
// It then continues until either a next record is found or the end of the file is encountered, in which case returns false
func (ts *TableScan) Next() error {
	slot, err := ts.rp.NextAfter(ts.currentSlot)
	if err != nil {
		return err
	}

	ts.currentSlot = slot

	for {
		if ts.currentSlot >= 0 {
			break
		}

		// return false if the record page is pointing to the last block
		lb, err := ts.isAtLastBlock()
		if err != nil {
			return err
		}

		if lb {
			return io.EOF
		}

		// move to block next to the one the record page is pointing to
		ts.moveToBlock(ts.rp.Block().BlockNumber() + 1)
		slot, err := ts.rp.NextAfter(ts.currentSlot)
		if err != nil {
			return err
		}

		ts.currentSlot = slot
	}

	return nil
}

func (ts *TableScan) GetInt(fieldname string) (int, error) {
	return ts.rp.GetInt(ts.currentSlot, fieldname)
}

func (ts *TableScan) GetString(fieldname string) (string, error) {
	return ts.rp.GetString(ts.currentSlot, fieldname)
}

func (ts *TableScan) GetVal(fieldname string) (Constant, error) {
	switch ts.layout.schema.Type(fieldname) {
	case INTEGER:
		v, err := ts.GetInt(fieldname)
		if err != nil {
			return Constant{}, err
		}
		return ConstantFromInt(v), nil
	case STRING:
		v, err := ts.GetString(fieldname)
		if err != nil {
			return Constant{}, err
		}
		return ConstantFromString(v), nil
	}

	pm := "invalid type for field " + fieldname
	panic(pm)
}

func (ts *TableScan) HasField(fieldname string) bool {
	return ts.layout.schema.HasField(fieldname)
}

// write methods

func (ts *TableScan) SetInt(fieldname string, val int) error {
	return ts.rp.SetInt(ts.currentSlot, fieldname, val)
}

func (ts *TableScan) SetString(fieldname string, val string) error {
	return ts.rp.SetString(ts.currentSlot, fieldname, val)
}

func (ts *TableScan) SetVal(fieldname string, val interface{}) error {
	switch ts.layout.schema.Type(fieldname) {
	case INTEGER:
		return ts.SetInt(fieldname, val.(int))
	case STRING:
		return ts.SetString(fieldname, val.(string))
	}

	pm := "invalid type for field " + fieldname
	panic(pm)
}

// Insert looks for an empty slot to flag as used
// It starts scanning the current block until such a slot is found.
// If the current block does not contain free slots, it attempts to move to the next block
// If the next block is at the end of the file, appends a new block and start scanning from there.
func (ts *TableScan) Insert() error {
	slot, err := ts.rp.InsertAfter(ts.currentSlot)
	if err != nil {
		return err
	}

	ts.currentSlot = slot
	for {
		if ts.currentSlot >= 0 {
			break
		}

		last, err := ts.isAtLastBlock()
		if err != nil {
			return err
		}

		// if we reached the end of the file, append a new file
		// otherwise move to the next block
		// and claim the slot
		if last {
			if err := ts.moveToNewBlock(); err != nil {
				return err
			}
		} else {
			ts.moveToBlock(ts.rp.block.BlockNumber() + 1)
		}

		slot, err := ts.rp.InsertAfter(ts.currentSlot)
		if err != nil {
			return err
		}

		ts.currentSlot = slot
	}

	return nil
}

func (ts *TableScan) Delete() error {
	return ts.rp.Delete(ts.currentSlot)
}

func (ts *TableScan) MoveToRID(rid RID) {
	ts.Close()
	block := file.NewBlockID(ts.fname, rid.Blocknum)
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	ts.currentSlot = rid.Slot
}

func (ts *TableScan) GetRID() RID {
	return NewRID(ts.rp.block.BlockNumber(), ts.currentSlot)
}

// moveToBlock closes the current page record page and opens a new one for the specified block.
// After the page has been changed, the TableScan positions itself before the first slot of the new block
func (ts *TableScan) moveToBlock(block int) {
	ts.Close()
	b := file.NewBlockID(ts.fname, block)
	ts.rp = NewRecordPage(ts.tx, b, ts.layout)
	ts.currentSlot = -1
}

// moveToNewBlock attempts to append a new block to the file
// If the operation succeeds, it associates the new block with a new record page
// formats the page according to the layout and sets the current slot pointer to -1
func (ts *TableScan) moveToNewBlock() error {
	ts.Close()
	block, err := ts.tx.Append(ts.fname)
	if err != nil {
		return err
	}
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	ts.rp.Format()
	ts.currentSlot = -1
	return nil
}

// isAtLastBlock returns true if the block the underlying record page is pointing to
// is the last block of the file.
// Returns an error if the transaction fails to acquire a read lock on the final block
func (ts *TableScan) isAtLastBlock() (bool, error) {
	// get the number of blocks in the associated file
	size, err := ts.tx.Size(ts.fname)
	if err != nil {
		return false, err
	}

	return ts.rp.Block().BlockNumber() == size-1, nil
}
