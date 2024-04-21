package record

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

const (
	// bTreePageFlagOffset is the byte offset of the page flag.
	// The flag is a value of type INT
	bTreePageFlagOffset = 0
	// bTreePageNumRecordsOffset is the byte offset of the records number.
	// The number of records is a value of type INT.
	bTreePageNumRecordsOffset = file.IntBytes
	// bTreePageContentOffset is the byte offset from the left of the page
	// where records start
	bTreePageContentOffset = bTreePageNumRecordsOffset + file.IntBytes
)

// bTreePage represents a page used by B-Tree blocks.
// Differently from logs and record pages, bTreePage must support the following:
// 1. Records need to be in sorted order
// 2. Records don't have a permanent ID - they can be moved around within the page
// 3. The page needs to be able to split its records with another page
// bTreePage is used both for directory blocks and for leaf.
// Directory blocks will use a flag to hold its level, while leaf blocks will
// use the flag to point to an overflow block for duplicate records.
//
// When a new record is inserted, we determine its position in the page and the
// records that follow are shifted to the right by one place.
// When a record is deleted, the records that follow are shifted to the left to
// "fill the hole".
// The page also maintains an integer that stores the number of records.
type bTreePage struct {
	x       tx.Transaction
	blockID file.BlockID
	layout  Layout
}

func newBTreePage(x tx.Transaction, currentBlock file.BlockID, layout Layout) bTreePage {
	x.Pin(currentBlock)
	return bTreePage{
		x:       x,
		blockID: currentBlock,
		layout:  layout,
	}
}

func (page bTreePage) slotPosition(slot int) int {
	size := page.layout.slotsize
	return bTreePageContentOffset + slot*size
}

// fieldPosition returns the position of the field in the page.
func (page bTreePage) fieldPosition(slot int, fieldName string) int {
	offset := page.layout.Offset(fieldName)
	return page.slotPosition(slot) + offset
}

func (page bTreePage) getInt(slot int, fieldName string) (int, error) {
	pos := page.fieldPosition(slot, fieldName)
	return page.x.GetInt(page.blockID, pos)
}

func (page bTreePage) setInt(slot int, fieldName string, val int) error {
	pos := page.fieldPosition(slot, fieldName)
	return page.x.SetInt(page.blockID, pos, val, true)
}

func (page bTreePage) setString(slot int, fieldName string, val string) error {
	pos := page.fieldPosition(slot, fieldName)
	return page.x.SetString(page.blockID, pos, val, true)
}

func (page bTreePage) getString(slot int, fieldName string) (string, error) {
	pos := page.fieldPosition(slot, fieldName)
	return page.x.GetString(page.blockID, pos)
}

func (page bTreePage) setVal(slot int, fieldName string, val file.Value) error {
	if t := page.layout.schema.Type(fieldName); t == file.INTEGER {
		return page.setInt(slot, fieldName, val.AsIntVal())
	}

	return page.setString(slot, fieldName, val.AsStringVal())
}

func (page bTreePage) getVal(slot int, fieldName string) (file.Value, error) {
	t := page.layout.schema.Type(fieldName)
	if t == file.INTEGER {
		v, err := page.getInt(slot, fieldName)
		return file.ValueFromInt(v), err
	}

	v, err := page.getString(slot, fieldName)
	return file.ValueFromString(v), err
}

func (page bTreePage) getDataVal(slot int) (file.Value, error) {
	return page.getVal(slot, "dataval")
}

func (page bTreePage) getFlag() (int, error) {
	return page.x.GetInt(page.blockID, 0)
}

func (page bTreePage) setFlag(v int) error {
	return page.x.SetInt(page.blockID, bTreePageFlagOffset, v, true)
}

func (page bTreePage) getNumRecords() (int, error) {
	return page.x.GetInt(page.blockID, bTreePageNumRecordsOffset)
}

func (page bTreePage) setNumRecords(n int) error {
	return page.x.SetInt(page.blockID, bTreePageNumRecordsOffset, n, true)
}

func (page bTreePage) Close() {
	page.x.Unpin(page.blockID)
}

func (page bTreePage) appendNew(flag int) (file.BlockID, error) {
	block, err := page.x.Append(page.blockID.Filename())
	if err != nil {
		return file.BlockID{}, fmt.Errorf("error appending at blockID: %w", err)
	}

	page.x.Pin(block)
	page.format(block, flag)
	return block, nil
}

// format formats the page and initialises it with the flag and the number or records.
// The operations that occurs when the page is being formatted are NOT logged to
// the WAL.
func (page bTreePage) format(block file.BlockID, flag int) error {
	if err := page.x.SetInt(block, 0, flag, false); err != nil {
		return err
	}

	if err := page.x.SetInt(block, file.IntBytes, 0, false); err != nil {
		return err
	}

	recordSize := page.layout.slotsize

	for pos := bTreePageContentOffset; pos+recordSize <= page.x.BlockSize(); pos += recordSize {
		page.makeDefaultRecord(block, pos)
	}

	return nil
}

func (page bTreePage) makeDefaultRecord(block file.BlockID, pos int) error {
	for _, f := range page.layout.schema.fields {
		offset := page.layout.Offset(f)
		if page.layout.schema.Type(f) == file.INTEGER {
			if err := page.x.SetInt(block, pos+offset, 0, false); err != nil {
				return fmt.Errorf("error creating default int record: %w", err)
			}
		} else if page.layout.schema.Type(f) == file.STRING {
			if err := page.x.SetString(block, pos+offset, "", false); err != nil {
				return fmt.Errorf("error creating default string record: %w", err)
			}
		}
	}

	return nil
}

// findSlotBefore looks for the smallest slot such that its dataval is
// the highest for which dataval(slot) <= key still holds.
func (page bTreePage) findSlotBefore(key file.Value) (int, error) {
	slot := 0
	for {
		rec, err := page.getNumRecords()
		if err != nil {
			return slot, fmt.Errorf("error retrieving number of records: %w", err)
		}

		data, err := page.getDataVal(slot)
		if err != nil {
			return slot, fmt.Errorf("error reading dataval: %w", err)
		}

		if slot < rec && data.Less(key) {
			slot++
		} else {
			return slot - 1, nil
		}
	}
}

func (page bTreePage) isFull() (bool, error) {
	totalRecords, err := page.getNumRecords()
	if err != nil {
		return false, err
	}

	return page.slotPosition(totalRecords+1) > page.x.BlockSize(), nil
}

func (page bTreePage) split(splitpos int, flag int) (file.BlockID, error) {

	block, err := page.appendNew(flag)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("error in split when appending new block: %w", err)
	}

	newPage := newBTreePage(page.x, block, page.layout)

	if err := page.transferRecords(splitpos, newPage); err != nil {
		return file.BlockID{}, fmt.Errorf("error in split when transferring records: %w", err)
	}

	newPage.setFlag(flag)
	newPage.Close()

	return block, nil
}

// insert assumes that the current slot of the page has already been set
// (that is, findSlotBefore has been already called).
// Starting at the end of the block, it shifts all records to the right,
// until it reaches the current slot.
func (page bTreePage) insert(slot int) error {
	for i, err := page.getNumRecords(); i > slot; i-- {
		if err != nil {
			return err
		}

		if err := page.copyRecord(i-1, i); err != nil {
			return err
		}
	}

	n, err := page.getNumRecords()
	if err != nil {
		return err
	}

	if err := page.setNumRecords(n); err != nil {
		return err
	}

	return nil
}

// delete assumes that the current slot of the page has already been set
// (that is, findSlotBefore has been already called),
// and shifts left all records to the right of the given slot by one place.
func (page bTreePage) delete(slot int) error {
	for i := slot + 1; ; i++ {
		recs, err := page.getNumRecords()
		if err != nil {
			return err
		}

		if recs < i {
			break
		}

		if err := page.copyRecord(i, i-1); err != nil {
			return err
		}
	}

	recs, err := page.getNumRecords()
	if err != nil {
		return err
	}

	if err := page.setNumRecords(recs - 1); err != nil {
		return err
	}

	return nil
}

func (page bTreePage) copyRecord(fromSlot int, toSlot int) error {
	for _, f := range page.layout.schema.Fields() {
		v, err := page.getVal(fromSlot, f)
		if err != nil {
			return err
		}

		if err := page.setVal(toSlot, f, v); err != nil {
			return err
		}
	}

	return nil
}

func (page bTreePage) transferRecords(slot int, dst bTreePage) error {
	dstSlot := 0

	for {
		records, err := page.getNumRecords()
		if err != nil {
			return err
		}

		if records >= slot {
			break
		}

		if err := dst.insert(dstSlot); err != nil {
			return err
		}

		for _, f := range page.layout.schema.Fields() {
			v, err := page.getVal(slot, f)
			if err != nil {
				return err
			}

			if err := dst.setVal(dstSlot, f, v); err != nil {
				return err
			}
		}

		if page.delete(slot); err != nil {
			return err
		}

		dstSlot++
	}

	return nil
}

func (page bTreePage) getChildNum(slot int) (int, error) {
	return page.getInt(slot, "block")
}

// insertDirectory insert a directory value into the page
func (page bTreePage) insertDirectory(slot int, val file.Value, blockNumber int) error {
	if err := page.insert(slot); err != nil {
		return err
	}

	if err := page.setVal(slot, "dataval", val); err != nil {
		return err
	}

	if err := page.setInt(slot, "block", blockNumber); err != nil {
		return err
	}

	return nil
}

func (page bTreePage) getDataRID(slot int) (RID, error) {
	block, err := page.getInt(slot, "block")
	if err != nil {
		return RID{}, err
	}

	id, err := page.getInt(slot, "id")
	if err != nil {
		return RID{}, err
	}

	return NewRID(block, id), nil
}

// insertLeaf inserts a leaf value into the page
func (page bTreePage) insertLeaf(slot int, val file.Value, rid RID) error {
	if err := page.insert(slot); err != nil {
		return err
	}

	if err := page.setVal(slot, "dataval", val); err != nil {
		return err
	}

	if err := page.setInt(slot, "block", rid.Blocknum); err != nil {
		return err
	}

	if err := page.setInt(slot, "id", rid.Slot); err != nil {
		return err
	}

	return nil
}
