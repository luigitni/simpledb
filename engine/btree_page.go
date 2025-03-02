package engine

import (
	"fmt"
	"math"
	"strings"

	"github.com/luigitni/simpledb/pages"
	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

const (
	// bTreePageFlagOffset is the byte offset of the page flag.
	// The flag is a value of type types.Long, as it must contain the block number of overflow blocks.
	// For leaf pages, the flag is the overflow block number.
	// For directory pages, the flag is the level of the page, with 0 indicating the
	// directory just above the leaf pages.
	bTreePageFlagOffset storage.Offset = 0
	// bTreePageNumRecordsOffset is the byte offset of the records size.
	// The size is a value of type SmallInt.
	bTreePageNumRecordsOffset storage.Offset = bTreePageFlagOffset + storage.SizeOfLong

	bTreeSpecialBlockSize storage.Offset = storage.SizeOfLong + storage.SizeOfSmallInt

	bTreeMaxSizeOfKey storage.Offset = 512
)

const (
	flagBTreeRoot = storage.Long(0)
	flagUnset     = storage.Long(math.MaxUint64)
)

const (
	indexFieldDataVal     = "dataval"
	indexFieldBlockNumber = "block"
	indexFieldRecordID    = "id"
)

// dirEntry is the value of a record for the btree directory pages.
// It contains the indexed value and the block number of the child block,
type dirEntry struct {
	value    storage.Value
	blockNum storage.Long
}

func (e dirEntry) empty() bool {
	return e.value == nil
}

// we will store the btrees using the slotted record page.
// We will use the special space in the page to store the needed information.
// The page will have the following layout:
// 1. a flag that indicates the level of the page. A flag of 0 indicates a leaf page.

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
	x           tx.Transaction
	block       storage.Block
	layout      Layout
	dataValType storage.FieldType
	slottedPage *pages.SlottedPage
}

func newBTreePage(x tx.Transaction, block storage.Block, layout Layout) bTreePage {
	// the block is pinned by the underlying slotted page
	return bTreePage{
		x:           x,
		block:       block,
		layout:      layout,
		dataValType: layout.schema.ftype(indexFieldDataVal),
		slottedPage: pages.NewSlottedPage(x, block, layout),
	}
}

func (p bTreePage) setVal(slot storage.SmallInt, fieldName string, val storage.Value) error {
	if s := p.layout.schema.ftype(fieldName).Size(); s != storage.SizeOfVarlen {
		return p.slottedPage.SetFixedLen(slot, fieldName, val.AsFixedLen())
	}

	return p.slottedPage.SetVarLen(slot, fieldName, val.AsVarlen())
}

func (p bTreePage) val(slot storage.SmallInt, fieldName string) (storage.Value, error) {
	if size := p.layout.schema.ftype(fieldName).Size(); size != storage.SizeOfVarlen {
		v, err := p.slottedPage.FixedLen(slot, fieldName)
		if err != nil {
			return storage.Value{}, err
		}

		return storage.ValueFromFixedLen(v), nil
	}

	v, err := p.slottedPage.VarLen(slot, fieldName)
	if err != nil {
		return storage.Value{}, err
	}

	return storage.ValueFromVarlen(v), nil
}

func (p bTreePage) dataVal(slot storage.SmallInt) (storage.Value, error) {
	return p.val(slot, indexFieldDataVal)
}

func (p bTreePage) dataRID(slot storage.SmallInt) (RID, error) {
	block, err := p.slottedPage.FixedLen(slot, indexFieldBlockNumber)
	if err != nil {
		return RID{}, err
	}

	id, err := p.slottedPage.FixedLen(slot, indexFieldRecordID)
	if err != nil {
		return RID{}, err
	}

	return NewRID(
		block.AsLong(),
		id.AsSmallInt(),
	), nil
}

func (p bTreePage) flag() (storage.Long, error) {
	v, err := p.slottedPage.FixedLenAtSpecial(bTreePageFlagOffset, storage.SizeOfLong)
	if err != nil {
		return 0, err
	}

	return storage.FixedLenToInteger[storage.Long](v), nil
}

func (p bTreePage) setFlag(v storage.Long) error {
	return p.slottedPage.SetFixedLenAtSpecial(
		bTreePageFlagOffset,
		storage.SizeOfLong,
		storage.IntegerToFixedLen[storage.Long](storage.SizeOfLong, v),
	)
}

func (p bTreePage) numRecords() (storage.SmallInt, error) {
	v, err := p.slottedPage.FixedLenAtSpecial(bTreePageNumRecordsOffset, storage.SizeOfSmallInt)
	if err != nil {
		return 0, err
	}

	return storage.FixedLenToInteger[storage.SmallInt](v), nil
}

func (p bTreePage) setNumRecords(s storage.SmallInt) error {
	return p.slottedPage.SetFixedLenAtSpecial(
		bTreePageNumRecordsOffset,
		storage.SizeOfSmallInt,
		storage.IntegerToFixedLen[storage.SmallInt](storage.SizeOfSmallInt, s),
	)
}

func (p bTreePage) format(flag storage.Long) error {
	if err := p.slottedPage.Format(bTreeSpecialBlockSize); err != nil {
		return err
	}

	if err := p.setFlag(flag); err != nil {
		return err
	}

	return nil
}

func (p bTreePage) Close() {
	p.slottedPage.Close()
}

func (p bTreePage) isFull() (bool, error) {
	fits, err := p.slottedPage.RecordsFit(bTreeMaxSizeOfKey)
	if err != nil {
		return false, err
	}

	return !fits, nil
}

// findSlotBefore looks for the rank of the key in the page and returns the slot
// of the predecessor of the key within the page.
// It uses a binary search to find the slot that contains the key or the slot
// immediately before the key.
func (page bTreePage) findSlotBefore(key storage.Value) (storage.SmallInt, error) {
	records, err := page.numRecords()
	if err != nil {
		return pages.InvalidSlot, err
	}

	var left storage.SmallInt
	right := records

	for left < right {
		mid := left + (right-left)/2

		val, err := page.dataVal(mid)
		if err != nil {
			return pages.InvalidSlot, err
		}

		if val.Less(page.dataValType, key) {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left - 1, nil
}

// split splits the block into two.
// It appends a new bTreePage to the underlying index file and copies there the records
// starting from splitpos position.
// Once records have been moved, it sets the flag to the new page and closes it.
func (p bTreePage) split(splitpos storage.SmallInt, flag storage.Long) (storage.Block, error) {
	block, err := p.x.Append(p.block.FileName())
	if err != nil {
		return storage.Block{}, fmt.Errorf("error appending at blockID: %w", err)
	}

	newPage := newBTreePage(p.x, block, p.layout)
	newPage.format(flag)

	defer newPage.Close()

	if err := p.transferRecords(splitpos, newPage); err != nil {
		return storage.Block{}, fmt.Errorf("error in split when transferring records: %w", err)
	}

	return block, nil
}

func (page bTreePage) insert(slot storage.SmallInt, val storage.Value, size storage.Offset) (storage.SmallInt, error) {
	if err := page.slottedPage.InsertAt(slot, size); err != nil {
		return pages.InvalidSlot, fmt.Errorf("BTreePage: insert ShiftSlotsRight: %w", err)
	}

	if err := page.setVal(slot, indexFieldDataVal, val); err != nil {
		return pages.InvalidSlot, err
	}

	recs, err := page.numRecords()
	if err != nil {
		return pages.InvalidSlot, err
	}

	if err := page.setNumRecords(recs + 1); err != nil {
		return pages.InvalidSlot, err
	}

	return slot, nil
}

// delete assumes that the current slot of the page has already been set
// (that is, findSlotBefore has been already called),
// and shifts left all records to the right of the given slot by one place.
// Within the slotted page, this effectively overwrites the header entry of the record.
// When the page is compacted, all the existing records will be shifted at the end of the free space,
// and the overwritten record will be ignored.
func (page bTreePage) delete(slot storage.SmallInt) error {
	if err := page.slottedPage.ShiftSlotsLeft(slot); err != nil {
		return fmt.Errorf("delete: slottedPage.ShiftSlotsLeft: %w", err)
	}

	recs, err := page.numRecords()
	if err != nil {
		return err
	}

	if err := page.setNumRecords(recs - 1); err != nil {
		return err
	}

	return nil
}

// transferRecords copies all records from the provided slot in the
// current page to the dst page.
// It deletes the record from the current page once it's successfully copied over.
// Because the transfer happens from slot to the right, and records are in increasing
// key order, the records that are moved are those with the highest key value.
func (page bTreePage) transferRecords(srcSlot storage.SmallInt, dst bTreePage) error {
	var dstSlot storage.SmallInt

	records, err := page.numRecords()
	if err != nil {

		return fmt.Errorf("transferRecords: getNumRecords: %w", err)
	}

	for slot := srcSlot; slot < records; slot++ {
		deleted, err := page.slottedPage.IsDeleted(slot)
		if err != nil {
			return fmt.Errorf("transferRecords: slottedPage.IsDeleted: %w", err)
		}

		if deleted {
			continue
		}

		var recordSize storage.Offset

		vals := make([]storage.Value, 0, len(page.layout.schema.fields))
		for _, f := range page.layout.schema.fields {
			v, err := page.val(slot, f)
			if err != nil {
				return fmt.Errorf("transferRecords: page.val: %w", err)
			}

			recordSize += v.Size(page.layout.schema.ftype(f))
			vals = append(vals, v)
		}

		if err := dst.slottedPage.InsertAt(dstSlot, recordSize); err != nil {
			return fmt.Errorf(
				"transferRecords: dst.InsertAfter: %w: dst.slot: %d, recordSize %d, block %d",
				err,
				dstSlot,
				recordSize,
				dst.block.Number(),
			)
		}

		for i, f := range page.layout.schema.fields {
			v := vals[i]

			if err := dst.setVal(dstSlot, f, v); err != nil {
				return fmt.Errorf("transferRecords: dst.setVal: %w", err)
			}
		}

		dstSlot++
		dst.setNumRecords(dstSlot)
	}

	if err := page.slottedPage.Truncate(srcSlot); err != nil {
		return fmt.Errorf("transferRecords: slottedPage.Truncate: %w", err)
	}

	page.setNumRecords(srcSlot)

	return nil
}

func (page bTreePage) getBlockNumber(slot storage.SmallInt) (storage.Long, error) {
	v, err := page.slottedPage.FixedLen(slot, indexFieldBlockNumber)
	if err != nil {
		return 0, err
	}

	return storage.FixedLenToInteger[storage.Long](v), nil
}

// insertDirectory insert a directory value into the page
func (page bTreePage) insertDirectoryRecord(slot storage.SmallInt, val storage.Value, blockNumber storage.Long) error {
	recordSize := val.Size(page.dataValType) + storage.SizeOfLong

	slot, err := page.insert(slot, val, recordSize)
	if err != nil {
		return err
	}

	if err := page.slottedPage.SetFixedLen(
		slot,
		indexFieldBlockNumber,
		storage.IntegerToFixedLen[storage.Long](storage.SizeOfLong, blockNumber),
	); err != nil {
		return err
	}

	return nil
}

// insertLeaf inserts a leaf value into the page
func (page bTreePage) insertLeafRecord(slot storage.SmallInt, val storage.Value, rid RID) error {
	recordSize := val.Size(page.dataValType) + SizeOfRID

	slot, err := page.insert(slot, val, recordSize)
	if err != nil {
		return err
	}

	if err := page.slottedPage.SetFixedLen(
		slot,
		indexFieldBlockNumber,
		storage.IntegerToFixedLen[storage.Long](storage.SizeOfLong, rid.Blocknum),
	); err != nil {
		return err
	}

	if err := page.slottedPage.SetFixedLen(
		slot,
		indexFieldRecordID,
		storage.IntegerToFixedLen[storage.SmallInt](storage.SizeOfSmallInt, rid.Slot),
	); err != nil {
		return err
	}

	return nil
}

type Dump struct {
	Blocknum   storage.Long
	Flag       storage.Long
	NumRecords storage.SmallInt
	ValType    storage.FieldType
	Datavals   []storage.Value
	Blocknums  []storage.Long
}

func (page bTreePage) dump() (Dump, error) {
	flag, err := page.flag()
	if err != nil {
		return Dump{}, err
	}

	numRecords, err := page.numRecords()
	if err != nil {
		return Dump{}, err
	}

	dump := Dump{
		Blocknum:   page.block.Number(),
		Flag:       flag,
		NumRecords: numRecords,
		ValType:    page.layout.schema.ftype(indexFieldDataVal),
		Datavals:   make([]storage.Value, 0, numRecords),
		Blocknums:  make([]storage.Long, 0, numRecords),
	}

	for i := storage.SmallInt(0); i < numRecords; i++ {
		val, err := page.dataVal(i)
		if err != nil {
			return Dump{}, err
		}

		dump.Datavals = append(dump.Datavals, val)

		blocknum, err := page.getBlockNumber(i)
		if err != nil {
			return Dump{}, err
		}
		dump.Blocknums = append(dump.Blocknums, blocknum)
	}

	return dump, nil
}

func (dump Dump) String() string {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf(
		"Blocknum: %d, Flag: %d, NumRecords: %d, ValType: %s\n",
		dump.Blocknum,
		dump.Flag,
		dump.NumRecords,
		dump.ValType,
	))

	for i := 0; i < len(dump.Datavals); i++ {
		builder.WriteString(fmt.Sprintf(
			"slot %d: ( %s ) -> (blocknum: %d)\n",
			i,
			dump.Datavals[i].String(dump.ValType),
			dump.Blocknums[i],
		))
	}

	return builder.String()
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
	key         storage.Value
	contents    bTreePage
	currentSlot storage.SmallInt
	fileName    string
}

// newBTreeLeaf creates a new bTreePage for the specified block, and then positions the slot
// pointer to the slot immediately before the first record containing the search key.
func newBTreeLeaf(x tx.Transaction, blk storage.Block, layout Layout, key storage.Value) (*bTreeLeaf, error) {
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
		fileName:    blk.FileName(),
	}, nil
}

func (leaf *bTreeLeaf) Close() {
	leaf.contents.Close()
}

// next moves to the next record and returns true if the search key is found.
// if leaf.key is not found in the block, check in the overflow block.
func (leaf *bTreeLeaf) next() (bool, error) {
	leaf.currentSlot++

	recs, err := leaf.contents.numRecords()
	if err != nil {
		return false, err
	}

	if leaf.currentSlot >= recs {
		return leaf.tryOverflow()
	}

	dataval, err := leaf.contents.dataVal(leaf.currentSlot)
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
	firstKey, err := leaf.contents.dataVal(0)
	if err != nil {
		return false, err
	}

	flag, err := leaf.contents.flag()
	if err != nil {
		return false, err
	}

	if !leaf.key.Equals(firstKey) || flag == flagUnset {
		return false, nil
	}

	leaf.Close()

	nextBlock := storage.NewBlock(leaf.fileName, leaf.contents.slottedPage.Block().Number()+1)

	leaf.contents = newBTreePage(leaf.x, nextBlock, leaf.layout)
	leaf.currentSlot = 0

	return true, nil
}

func (leaf *bTreeLeaf) dataRID() (RID, error) {
	return leaf.contents.dataRID(leaf.currentSlot)
}

// delete deletes a record.
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

		other, err := leaf.dataRID()
		if err != nil {
			return err
		}

		if other == rid {
			return leaf.contents.delete(leaf.currentSlot)
		}
	}
}

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
func (leaf *bTreeLeaf) insert(rid RID) (dirEntry, error) {
	flag, err := leaf.contents.flag()
	if err != nil {
		return dirEntry{}, err
	}

	firstVal, err := leaf.contents.dataVal(0)
	if err != nil {
		return dirEntry{}, err
	}

	t := leaf.contents.layout.schema.ftype(indexFieldDataVal)

	// If the page has an overflow page and the first value in the page is greater than the key,
	// move the current page to a new block.
	if flag != flagUnset && firstVal.More(t, leaf.key) {

		// split the block and transfer all records of the current to the new block.
		newBlock, err := leaf.contents.split(0, flag)
		if err != nil {
			return dirEntry{}, err
		}

		// go back to the beginning of the block and insert the record.
		leaf.currentSlot = 0
		if err := leaf.contents.setFlag(flagUnset); err != nil {
			return dirEntry{}, fmt.Errorf("btree.leaf: insert: set flag: %w", err)
		}

		if err := leaf.contents.insertLeafRecord(leaf.currentSlot, leaf.key, rid); err != nil {
			return dirEntry{}, err
		}

		// return the new block number and the first value of the block.
		// so that the directory can be updated.
		return dirEntry{
			value:    firstVal,
			blockNum: newBlock.Number(),
		}, nil
	}

	// Insert the record after the current slot.
	// if the record does not fit, split the block.
	leaf.currentSlot++
	if err = leaf.contents.insertLeafRecord(leaf.currentSlot, leaf.key, rid); err != nil {
		return dirEntry{}, fmt.Errorf("inserting leaf entry: %w", err)
	}

	// if if the page is full and can't contain the record, we need to make room and split the block.
	isFull, err := leaf.contents.isFull()
	if err != nil {
		return dirEntry{}, err
	}

	if !isFull {
		return dirEntry{}, nil
	}

	records, err := leaf.contents.numRecords()
	if err != nil {
		return dirEntry{}, err
	}

	lastKey, err := leaf.contents.dataVal(records - 1)
	if err != nil {
		return dirEntry{}, err
	}

	// the block contains only values for the same record value.
	// create an overflow block to contain
	// all but the first record
	if lastKey.Equals(firstVal) {
		flag, err := leaf.contents.flag()
		if err != nil {
			return dirEntry{}, err
		}

		nb, err := leaf.contents.split(1, flag)
		if err != nil {
			return dirEntry{}, err
		}

		// record the overflow block number in the current block.
		if err := leaf.contents.setFlag(nb.Number()); err != nil {
			return dirEntry{}, err
		}

		return dirEntry{}, nil
	}

	// the block contains distinct values
	// split it in half.
	// todo: determine the half by size of the records
	splitPos := records / 2
	splitKey, err := leaf.contents.dataVal(splitPos)
	if err != nil {
		return dirEntry{}, err
	}

	// duplicate records can be to the right or left of the split key.
	// if the split key is the first key, then the duplicate records are to the right.
	// if the split key is not the first key, then with split the leftmost duplicate records.
	if splitKey.Equals(firstVal) {
		for {
			v, err := leaf.contents.dataVal(splitPos)
			if err != nil {
				return dirEntry{}, err
			}

			if v.Equals(splitKey) {
				splitPos++
				continue
			}

			break
		}

		sk, err := leaf.contents.dataVal(splitPos)
		if err != nil {
			return dirEntry{}, err
		}

		splitKey = sk
	} else {
		// check on the left as well
		for {
			v, err := leaf.contents.dataVal(splitPos - 1)
			if err != nil {
				return dirEntry{}, err
			}

			if v.Equals(splitKey) {
				splitPos--
				continue
			}

			break
		}
	}

	// copy the split key, as the split operation will change the underlying data buffer.
	key := storage.Copy(splitKey)

	// finally, split the block.
	nb, err := leaf.contents.split(splitPos, flagUnset)
	if err != nil {
		return dirEntry{}, err
	}

	return dirEntry{key, nb.Number()}, nil
}

type bTreeDir struct {
	x        tx.Transaction
	layout   Layout
	contents bTreePage
	fileName string
}

func newBTreeDir(x tx.Transaction, block storage.Block, layout Layout) bTreeDir {
	return bTreeDir{
		x:        x,
		layout:   layout,
		contents: newBTreePage(x, block, layout),
		fileName: block.FileName(),
	}
}

func (dir *bTreeDir) Close() {
	dir.contents.Close()
}

func (dir *bTreeDir) makeNewRoot(entry dirEntry) error {
	firstVal, err := dir.contents.dataVal(0)
	if err != nil {
		return err
	}

	level, err := dir.contents.flag()
	if err != nil {
		return err
	}

	block, err := dir.contents.split(0, level)
	if err != nil {
		return err
	}

	oldRoot := dirEntry{firstVal, block.Number()}
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
func (dir *bTreeDir) search(key storage.Value) (storage.Long, error) {
	child, err := dir.findChildBlock(key)
	if err != nil {
		return 0, err
	}

	level := dir.contents.flag

	// traverse the directory tree until we reach the leaf level.
	// if the flag is 0, we are at the leaf level.
	for flag, err := level(); flag != flagUnset && flag > 0; flag, err = level() {
		if err != nil {
			return 0, err
		}

		dir.contents.Close()

		dir.contents = newBTreePage(dir.x, child, dir.layout)
		child, err = dir.findChildBlock(key)
		if err != nil {
			return 0, err
		}
	}

	return child.Number(), nil
}

func (dir *bTreeDir) findChildBlock(key storage.Value) (storage.Block, error) {
	slot, err := dir.contents.findSlotBefore(key)
	if err != nil {
		return storage.Block{}, err
	}

	val, err := dir.contents.dataVal(slot + 1)
	if err != nil {
		return storage.Block{}, fmt.Errorf("block not found for key %s: %s", key, err)
	}

	if val.Equals(key) {
		slot++
	}

	blockNum, err := dir.contents.getBlockNumber(slot)
	if err != nil {
		return storage.Block{}, err
	}

	return storage.NewBlock(dir.fileName, storage.Long(blockNum)), nil
}

// insert recursively traverses the tree, starting from the root, and
// inserts a new directory record.
// If the returned dirEntry value is not empty, the insertion has caused the page to split.
func (dir *bTreeDir) insert(entry dirEntry) (dirEntry, error) {
	flag, err := dir.contents.flag()
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
		child.Close()

		return dirEntry{}, err
	}

	child.Close()

	if ce.empty() {
		return dirEntry{}, nil
	}

	return dir.insertEntry(ce)
}

func (dir *bTreeDir) insertEntry(entry dirEntry) (dirEntry, error) {
	slot, err := dir.contents.findSlotBefore(entry.value)
	if err != nil {
		return dirEntry{}, err
	}

	slot++

	if err := dir.contents.insertDirectoryRecord(slot, entry.value, entry.blockNum); err != nil {
		return dirEntry{}, err
	}

	isFull, err := dir.contents.isFull()
	if err != nil {
		return dirEntry{}, err
	}

	if !isFull {
		return dirEntry{}, nil
	}

	records, err := dir.contents.numRecords()
	if err != nil {
		return dirEntry{}, err
	}

	splitPos := records / 2

	splitVal, err := dir.contents.dataVal(splitPos)
	if err != nil {
		return dirEntry{}, err
	}

	level, err := dir.contents.flag()
	if err != nil {
		return dirEntry{}, err
	}

	v := storage.Copy(splitVal)

	newblock, err := dir.contents.split(splitPos, level)
	if err != nil {
		return dirEntry{}, err
	}

	return dirEntry{
		value:    v,
		blockNum: newblock.Number(),
	}, nil
}
