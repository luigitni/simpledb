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

func (page bTreePage) int(slot int, fieldName string) (int, error) {
	pos := page.fieldPosition(slot, fieldName)
	return page.x.Int(page.blockID, pos)
}

func (page bTreePage) setInt(slot int, fieldName string, val int) error {
	pos := page.fieldPosition(slot, fieldName)
	return page.x.SetInt(page.blockID, pos, val, true)
}

func (page bTreePage) setString(slot int, fieldName string, val string) error {
	pos := page.fieldPosition(slot, fieldName)
	return page.x.SetString(page.blockID, pos, val, true)
}

func (page bTreePage) string(slot int, fieldName string) (string, error) {
	pos := page.fieldPosition(slot, fieldName)
	return page.x.String(page.blockID, pos)
}

func (page bTreePage) setVal(slot int, fieldName string, val file.Value) error {
	if t := page.layout.schema.ftype(fieldName); t == file.INTEGER {
		return page.setInt(slot, fieldName, val.AsIntVal())
	}

	return page.setString(slot, fieldName, val.AsStringVal())
}

func (page bTreePage) mustGetVal(slot int, fieldName string) file.Value {
	v, err := page.val(slot, fieldName)
	if err != nil {
		panic(err)
	}

	return v
}

func (page bTreePage) val(slot int, fieldName string) (file.Value, error) {
	t := page.layout.schema.ftype(fieldName)
	if t == file.INTEGER {
		v, err := page.int(slot, fieldName)
		return file.ValueFromInt(v), err
	}

	v, err := page.string(slot, fieldName)
	return file.ValueFromString(v), err
}

func (page bTreePage) mustGetDataVal(slot int) file.Value {
	v, err := page.getDataVal(slot)
	if err != nil {
		panic(err)
	}

	return v
}

func (page bTreePage) getDataVal(slot int) (file.Value, error) {
	return page.val(slot, "dataval")
}

func (page bTreePage) mustGetDataRID(slot int) RID {
	rid, err := page.getDataRID(slot)
	if err != nil {
		panic(err)
	}

	return rid
}

func (page bTreePage) getDataRID(slot int) (RID, error) {
	block, err := page.int(slot, "block")
	if err != nil {
		return RID{}, err
	}

	id, err := page.int(slot, "id")
	if err != nil {
		return RID{}, err
	}

	return NewRID(block, id), nil
}

func (page bTreePage) mustGetFlag() int {
	f, err := page.getFlag()
	if err != nil {
		panic(err)
	}

	return f
}

func (page bTreePage) getFlag() (int, error) {
	return page.x.Int(page.blockID, 0)
}

func (page bTreePage) mustSetFlag(v int) {
	if err := page.setFlag(v); err != nil {
		panic(err)
	}
}

func (page bTreePage) setFlag(v int) error {
	return page.x.SetInt(page.blockID, bTreePageFlagOffset, v, true)
}

func (page bTreePage) mustGetNumRecords() int {
	n, err := page.getNumRecords()
	if err != nil {
		panic(err)
	}

	return n
}

func (page bTreePage) getNumRecords() (int, error) {
	return page.x.Int(page.blockID, bTreePageNumRecordsOffset)
}

func (page bTreePage) setNumRecords(n int) error {
	return page.x.SetInt(page.blockID, bTreePageNumRecordsOffset, n, true)
}

func (page bTreePage) Close() {
	page.x.Unpin(page.blockID)
}

// appendNew creates a new block and appends at the end of the file.
// The block is then formatted as a bTreePage.
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
		if page.layout.schema.ftype(f) == file.INTEGER {
			if err := page.x.SetInt(block, pos+offset, 0, false); err != nil {
				return fmt.Errorf("error creating default int record: %w", err)
			}
		} else if page.layout.schema.ftype(f) == file.STRING {
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

// split splits the block into two.
// It appends a new bTreePage to the underlying index file and copies there the records
// starting from splitpos position.
// Once records have been moved, it sets the flag to the new page and closes it.
func (page bTreePage) split(splitpos int, flag int) (file.BlockID, error) {

	block, err := page.appendNew(flag)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("error in split when appending new block: %w", err)
	}

	newPage := newBTreePage(page.x, block, page.layout)

	if err := page.transferRecords(splitpos, newPage); err != nil {
		return file.BlockID{}, fmt.Errorf("error in split when transferring records: %w", err)
	}

	if err := newPage.setFlag(flag); err != nil {
		return file.BlockID{}, err
	}

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
	for _, f := range page.layout.schema.fields {
		v, err := page.val(fromSlot, f)
		if err != nil {
			return err
		}

		if err := page.setVal(toSlot, f, v); err != nil {
			return err
		}
	}

	return nil
}

// transferRecords copies all records from the provided slot from the
// current page to dst.
// It deletes the record from the current page once it's successfully copied over.
// Because the transfer happens from slot to the right, and records are in increasing
// key order, the records that are moved are those with the highest key value.
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

		for _, f := range page.layout.schema.fields {
			v, err := page.val(slot, f)
			if err != nil {
				return err
			}

			if err := dst.setVal(dstSlot, f, v); err != nil {
				return err
			}
		}

		if err := page.delete(slot); err != nil {
			return err
		}

		dstSlot++
	}

	return nil
}

func (page bTreePage) getChildNum(slot int) (int, error) {
	return page.int(slot, "block")
}

// insertDirectory insert a directory value into the page
func (page bTreePage) insertDirectoryRecord(slot int, val file.Value, blockNumber int) error {
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

// insertLeaf inserts a leaf value into the page
func (page bTreePage) insertLeafRecord(slot int, val file.Value, rid RID) error {
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

// bTreeLeaf represents the leaf block of a B+-Tree.
// It embeds a bTreePage that contains ordered tuples of (key -> RID).
// Remember that a RID is composed of:
// - A block number to identify the block the record belongs to
// - A slot number to identify where the record is located within the block
// The page content is ordered by key.
type bTreeLeaf struct {
	x           tx.Transaction
	layout      Layout
	key         file.Value
	contents    bTreePage
	currentSlot int
	fileName    string
}

// newBTreeLeaf creates a new bTreePage for the specified block, and then positions the slot
// pointer to the slot immediately before the first record containing the search key.
func newBTreeLeaf(x tx.Transaction, blk file.BlockID, layout Layout, key file.Value) (*bTreeLeaf, error) {
	contents := newBTreePage(x, blk, layout)
	currentSlot, err := contents.findSlotBefore(key)
	if err != nil {
		return nil, err
	}

	return &bTreeLeaf{
		x:           x,
		layout:      layout,
		key:         key,
		contents:    contents,
		currentSlot: currentSlot,
		fileName:    blk.Filename(),
	}, nil
}

func (leaf *bTreeLeaf) Close() {
	leaf.contents.Close()
}

// next moves to the next record and returns true if the search key is found.
// if leaf.key is not found in the block, check in the overflow block.
func (leaf *bTreeLeaf) next() (bool, error) {
	leaf.currentSlot++

	recs, err := leaf.contents.getNumRecords()
	if err != nil {
		return false, err
	}

	if leaf.currentSlot >= recs {
		return leaf.tryOverflow()
	}

	dataval, err := leaf.contents.getDataVal(leaf.currentSlot)
	if err != nil {
		return false, err
	}

	if dataval.Equals(leaf.key) {
		return true, nil
	}

	return leaf.tryOverflow()
}

// tryOverflow looks into an overflow block.
// If the block starts with a different key, returns.
// Otherwise, use an overflow block.
func (leaf *bTreeLeaf) tryOverflow() (bool, error) {
	firstKey, err := leaf.contents.getDataVal(0)
	if err != nil {
		return false, err
	}

	flag, err := leaf.contents.getFlag()
	if err != nil {
		return false, err
	}

	if !leaf.key.Equals(firstKey) || flag < 0 {
		return false, nil
	}

	leaf.Close()

	nextBlock := file.NewBlockID(leaf.fileName, flag)
	leaf.contents = newBTreePage(leaf.x, nextBlock, leaf.layout)
	leaf.currentSlot = 0

	return true, nil
}

func (leaf *bTreeLeaf) getDataRID() (RID, error) {
	return leaf.contents.getDataRID(leaf.currentSlot)
}

// delete deteletes a record.
// It assumes that the slot pointer is set to the beginning of the page.
// Iterates from left to right looking for the record with the given rid.
// If found, deletes it.
func (leaf *bTreeLeaf) delete(rid RID) error {
	for {
		ok, err := leaf.next()
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		other, err := leaf.getDataRID()
		if err != nil {
			return err
		}

		if other == rid {
			return leaf.contents.delete(leaf.currentSlot)
		}
	}
}

type dirEntry struct {
	value    file.Value
	blockNum int
}

var emptyDirEntry = dirEntry{}

// insert inserts a new record into the bTreeLeaf.
// It assumes that findSlotBefore has already been called, and positions
// the record pointer at the first record greater than or equal to the search key,
// and inserts the value there.
// If the page already contains records having that search key, then the new record
// will be inserted at the front of the list.
// The method returns a dirEntry, which is empty if the insertion does not cause the
// block to split.
// Otherwise, the dirEntry contains the (dataval, blocknumber) pair corresponding to the
// new index block.
func (leaf *bTreeLeaf) insert(rid RID) (entry dirEntry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	flag := leaf.contents.mustGetFlag()
	firstVal := leaf.contents.mustGetDataVal(0)

	if flag > 0 && firstVal.More(leaf.key) {
		// the first value in the page is greater than the key.
		// Because the block is in ascending order, this means that the current
		// RID needs to be included at the beginning of the block.
		// We have to split the block and update the directory pages, otherwise they will point
		// to the wrong block.
		newBlock, err := leaf.contents.split(0, flag)
		if err != nil {
			return dirEntry{}, err
		}

		leaf.currentSlot = 0
		leaf.contents.setFlag(-1)

		if err := leaf.contents.insertLeafRecord(leaf.currentSlot, leaf.key, rid); err != nil {
			return dirEntry{}, err
		}

		return dirEntry{
			value:    firstVal,
			blockNum: newBlock.BlockNumber(),
		}, nil
	}

	// otherwise, insert the record at the current slot.
	// Remember that insert assumes that findSlotBefore has already been called, so current slot
	// points to the correct slot for the key.
	leaf.currentSlot++
	if err := leaf.contents.insertLeafRecord(leaf.currentSlot, leaf.key, rid); err != nil {
		return dirEntry{}, err
	}

	full, err := leaf.contents.isFull()
	if err != nil {
		return dirEntry{}, err
	}

	// the block still has space, do nothing.
	if !full {
		return dirEntry{}, nil
	}

	// else, page is full, split it
	firstKey := leaf.contents.mustGetDataVal(0)
	lastKey := leaf.contents.mustGetDataVal(leaf.contents.mustGetNumRecords() - 1)

	// the block contains the same record, create an overflow block to contain
	// all but the first record
	if lastKey.Equals(firstKey) {
		nb, err := leaf.contents.split(1, leaf.contents.mustGetFlag())
		if err != nil {
			return dirEntry{}, err
		}

		leaf.contents.mustSetFlag(nb.BlockNumber())

		return dirEntry{}, nil
	}

	// the block contains distinct values
	// split it in half.
	splitPos := leaf.contents.mustGetNumRecords() / 2
	splitKey := leaf.contents.mustGetDataVal(splitPos)

	// check on the right for duplicate records, as they must be kept in the same block
	if splitKey.Equals(firstKey) {
		for leaf.contents.mustGetDataVal(splitPos).Equals(splitKey) {
			splitPos++
		}

		splitKey = leaf.contents.mustGetDataVal(splitPos)
	} else {
		// check on the left as well
		for leaf.contents.mustGetDataVal(splitPos - 1).Equals(splitKey) {
			splitPos--
		}
	}

	// finally, split the block.
	nb, err := leaf.contents.split(splitPos, -1)
	if err != nil {
		return dirEntry{}, err
	}

	return dirEntry{splitKey, nb.BlockNumber()}, nil
}

type bTreeDir struct {
	x        tx.Transaction
	layout   Layout
	contents bTreePage
	fileName string
}

func newBTreeDir(x tx.Transaction, block file.BlockID, layout Layout) bTreeDir {
	return bTreeDir{
		x:        x,
		layout:   layout,
		contents: newBTreePage(x, block, layout),
		fileName: block.Filename(),
	}
}

func (dir *bTreeDir) Close() {
	dir.contents.Close()
}

func (dir *bTreeDir) makeNewRoot(entry dirEntry) error {
	firstVal, err := dir.contents.getDataVal(0)
	if err != nil {
		return err
	}

	level, err := dir.contents.getFlag()
	if err != nil {
		return err
	}

	block, err := dir.contents.split(0, level)
	if err != nil {
		return err
	}

	oldRoot := dirEntry{firstVal, block.BlockNumber()}
	if _, err := dir.insertEntry(oldRoot); err != nil {
		return err
	}

	if _, err := dir.insertEntry(entry); err != nil {
		return err
	}

	return dir.contents.setFlag(level + 1)
}

// search looks down the directory tree for the searched value and returns the block
// containing the record, if found.
// The method starts at the root of the tree and moves down the B+ tree levels.
// Once the level 0 is found, it searches that page and returns the block number
// of the leaf containing the search key.
func (dir *bTreeDir) search(key file.Value) (int, error) {
	child, err := dir.findChildBlock(key)
	if err != nil {
		return 0, err
	}

	n := dir.contents.getFlag

	for flag, err := n(); flag > 0; flag, err = n() {
		if err != nil {
			return 0, err
		}

		dir.Close()
		dir.contents = newBTreePage(dir.x, child, dir.layout)
		child, err = dir.findChildBlock(key)
		if err != nil {
			return 0, err
		}
	}

	return child.BlockNumber(), nil
}

func (dir *bTreeDir) findChildBlock(key file.Value) (file.BlockID, error) {
	slot, err := dir.contents.findSlotBefore(key)
	if err != nil {
		return file.BlockID{}, err
	}

	val, err := dir.contents.getDataVal(slot + 1)
	if err != nil {
		return file.BlockID{}, err
	}

	if val.Equals(key) {
		slot++
	}

	blockNum, err := dir.contents.getChildNum(slot)
	if err != nil {
		return file.BlockID{}, err
	}

	return file.NewBlockID(dir.fileName, blockNum), nil
}

// insert recursively traverses the tree, starting from the root, and
// inserts a new directory record.
// If the dirEntry value is not empty, the insertion has caused the page to split.
func (dir *bTreeDir) insert(entry dirEntry) (dirEntry, error) {
	flag, err := dir.contents.getFlag()
	if err != nil {
		return dirEntry{}, err
	}

	if flag == 0 {
		return dir.insertEntry(entry)
	}

	childBlock, err := dir.findChildBlock(entry.value)
	if err != nil {
		return dirEntry{}, err
	}

	child := newBTreeDir(dir.x, childBlock, dir.layout)
	ce, err := child.insert(entry)
	if err != nil {
		return dirEntry{}, err
	}

	child.Close()

	if ce == emptyDirEntry {
		return dirEntry{}, nil
	}

	return dir.insertEntry(ce)
}

func (dir *bTreeDir) insertEntry(entry dirEntry) (out dirEntry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	slot, err := dir.contents.findSlotBefore(entry.value)
	if err != nil {
		return
	}

	slot++

	err = dir.contents.insertDirectoryRecord(slot, entry.value, entry.blockNum)
	if err != nil {
		return
	}

	full, err := dir.contents.isFull()
	if err != nil {
		return dirEntry{}, err
	}

	if !full {
		return dirEntry{}, nil
	}

	// page is full, split it
	splitPos := dir.contents.mustGetNumRecords() / 2

	splitVal := dir.contents.mustGetDataVal(splitPos)

	level := dir.contents.mustGetFlag()

	newblock, err := dir.contents.split(splitPos, level)

	return dirEntry{
		value:    splitVal,
		blockNum: newblock.BlockNumber(),
	}, nil
}
