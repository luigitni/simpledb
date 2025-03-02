package pages

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/luigitni/simpledb/storage"
	"github.com/luigitni/simpledb/tx"
)

var (
	errNoFreeSpaceAvailable = errors.New("no free space available on page to insert record")
	ErrNoFreeSlot           = errors.New("no free slot available")
)

type flag storage.Int

const (
	flagEmptyRecord flag = 1 << iota
	flagInUseRecord
	flagDeletedRecord
)

const sizeOfHeaderEntry = storage.SizeOfLong

// slottedPageHeaderEntry represents an entry in the page header.
// Each entry is an 8-byte value
// It is bitmasked to store:
// - the first 2 bytes store the offset of the record within the page
// - the next 2 bytes store the length of the record within the page
// - byte 5 to 8 store record flags (whether empty/deleted etc)
type slottedPageHeaderEntry [8]byte

const sizeOfPageHeaderEntry = storage.Size(len(slottedPageHeaderEntry{}))

func (e slottedPageHeaderEntry) recordOffset() storage.Offset {
	return storage.UnsafeFixedToInteger[storage.Offset](e[:2])
}

func (e slottedPageHeaderEntry) recordLength() storage.Offset {
	return storage.UnsafeFixedToInteger[storage.Offset](e[2:4])
}

// todo: this is not really a bit mask, more of a status flag.
// We can handle this better
func (e slottedPageHeaderEntry) flags() flag {
	return flag(storage.UnsafeFixedToInteger[storage.Int](e[4:6]))
}

func (e slottedPageHeaderEntry) setOffset(offset storage.Offset) slottedPageHeaderEntry {
	fixed := storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, offset)
	copy(e[:2], fixed)
	return e
}

func (e slottedPageHeaderEntry) setLength(length storage.Offset) slottedPageHeaderEntry {
	fixed := storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, length)
	copy(e[2:4], fixed)
	return e
}

func (e slottedPageHeaderEntry) setFlag(f flag) slottedPageHeaderEntry {
	fixed := storage.UnsafeIntegerToFixedlen[storage.Int](storage.SizeOfInt, storage.Int(f))
	copy(e[4:], fixed)
	return e
}

const (
	MaxSlot         storage.SmallInt = (1 << (storage.SizeOfSmallInt * 8)) - 3
	InvalidSlot                      = MaxSlot + 1
	BeforeFirstSlot                  = InvalidSlot + 1
)

// slottedPageHeader represents the header of a slotted record page
// It holds the metadata about the page and the slot array for the records.
type slottedPageHeader struct {
	x     tx.Transaction
	block *storage.Block
}

const defaultFreeSpaceEnd = storage.PageSize

const (
	// blockNumber is a storage.Long that stores the block number of the page
	blockNumberOffset storage.Offset = 0
	// numSlots is a storage.SmallInt that stores the number of slots in the page
	numSlotsOffset storage.Offset = blockNumberOffset + storage.Offset(storage.SizeOfLong)
	// freeSpaceEnd is a storage.SmallInt that stores the end of the free space in the page
	freeSpaceEndOffset storage.Offset = numSlotsOffset + storage.Offset(storage.SizeOfSmallInt)
	// specialSpaceStart is a storage.Offset that stores the start of the special space in the page
	specialSpaceStartOffset storage.Offset = freeSpaceEndOffset + storage.Offset(storage.SizeOfSmallInt)
	// entriesOffset is the offset of the first slot entry in the page
	entriesOffset storage.Offset = specialSpaceStartOffset + storage.Offset(storage.SizeOfSmallInt)
)

func (h slottedPageHeader) freeSpaceStart() storage.Offset {
	return h.lastSlotOffset()
}

func (h slottedPageHeader) lastSlotOffset() storage.Offset {
	return entriesOffset + storage.Offset(h.mustNumSlots())*storage.Offset(sizeOfHeaderEntry)
}

func (h slottedPageHeader) freeSpaceAvailable() storage.Offset {
	return h.mustFreeSpaceEnd() - h.freeSpaceStart()
}

func (h slottedPageHeader) setBlockNumber(block storage.Long) error {
	return h.x.SetFixedlen(
		*h.block,
		blockNumberOffset,
		storage.SizeOfLong,
		storage.UnsafeIntegerToFixedlen[storage.Long](storage.SizeOfLong, block),
		true,
	)
}

func (h slottedPageHeader) blockNumber() (storage.Long, error) {
	v, err := h.x.Fixedlen(*h.block, blockNumberOffset, storage.SizeOfLong)

	if err != nil {
		return 0, err
	}

	return storage.UnsafeFixedToInteger[storage.Long](v), nil
}

func (h slottedPageHeader) mustBlockNumber() storage.Long {
	v, err := h.blockNumber()
	if err != nil {
		panic(err)
	}

	return v
}

func (h slottedPageHeader) setNumSlots(numSlots storage.SmallInt) error {
	return h.x.SetFixedlen(
		*h.block,
		numSlotsOffset,
		storage.SizeOfSmallInt,
		storage.UnsafeIntegerToFixedlen[storage.SmallInt](storage.SizeOfSmallInt, numSlots),
		true,
	)
}

func (h slottedPageHeader) numSlots() (storage.SmallInt, error) {
	v, err := h.x.Fixedlen(*h.block, numSlotsOffset, storage.SizeOfSmallInt)

	if err != nil {
		return 0, err
	}

	return storage.UnsafeFixedToInteger[storage.SmallInt](v), nil
}

func (p slottedPageHeader) mustNumSlots() storage.SmallInt {
	v, err := p.numSlots()
	if err != nil {
		panic(err)
	}

	return v
}

func (h slottedPageHeader) setFreeSpaceEnd(offset storage.Offset) error {
	return h.x.SetFixedlen(
		*h.block,
		freeSpaceEndOffset,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, offset),
		true,
	)
}

func (h slottedPageHeader) freeSpaceEnd() (storage.Offset, error) {
	v, err := h.x.Fixedlen(*h.block, freeSpaceEndOffset, storage.SizeOfOffset)

	if err != nil {
		return 0, err
	}

	return storage.UnsafeFixedToInteger[storage.Offset](v), nil
}

func (h slottedPageHeader) mustFreeSpaceEnd() storage.Offset {
	v, err := h.freeSpaceEnd()
	if err != nil {
		panic(err)
	}

	return v
}

func (h slottedPageHeader) setSpecialSpaceStart(offset storage.Offset) error {
	return h.x.SetFixedlen(
		*h.block,
		specialSpaceStartOffset,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, offset),
		true,
	)
}

func (h slottedPageHeader) specialSpaceStart() (storage.Offset, error) {
	v, err := h.x.Fixedlen(*h.block, specialSpaceStartOffset, storage.SizeOfOffset)

	if err != nil {
		return 0, err
	}

	return storage.UnsafeFixedToInteger[storage.Offset](v), nil
}

func (h slottedPageHeader) mustSpecialSpaceStart() storage.Offset {
	v, err := h.specialSpaceStart()
	if err != nil {
		panic(err)
	}

	return v
}

// appendRecordSlot appends a new record slot to the page header
// It takes in the actual size of the record to be inserted
func (header *slottedPageHeader) appendRecordSlot(actualRecordSize storage.Offset) error {
	available := header.freeSpaceAvailable()

	totalSize := actualRecordSize + storage.Offset(sizeOfHeaderEntry)

	if totalSize > available {
		return errNoFreeSpaceAvailable
	}

	freeSpaceEnd := header.mustFreeSpaceEnd()

	freeSpaceEnd -= actualRecordSize
	entry := slottedPageHeaderEntry{}.
		setOffset(freeSpaceEnd).
		setLength(actualRecordSize).
		setFlag(flagInUseRecord)

	return header.appendEntry(entry)
}

func (header *slottedPageHeader) entry(slot storage.SmallInt) (slottedPageHeaderEntry, error) {
	offset := entriesOffset + storage.Offset(sizeOfHeaderEntry)*storage.Offset(slot)
	v, err := header.x.Fixedlen(*header.block, offset, sizeOfHeaderEntry)
	if err != nil {
		return slottedPageHeaderEntry{}, err
	}

	return slottedPageHeaderEntry(v), nil
}

func (header *slottedPageHeader) setEntry(slot storage.SmallInt, entry slottedPageHeaderEntry) error {
	offset := entriesOffset + storage.Offset(sizeOfHeaderEntry)*storage.Offset(slot)

	if err := header.x.SetFixedlen(
		*header.block,
		offset,
		sizeOfHeaderEntry, storage.UnsafeByteSliceToFixedlen(entry[:]),
		true,
	); err != nil {
		return fmt.Errorf("set entry: %w", err)
	}

	return nil
}

func (header *slottedPageHeader) appendEntry(entry slottedPageHeaderEntry) error {
	if err := header.setFreeSpaceEnd(entry.recordOffset()); err != nil {
		return fmt.Errorf("set freeSpaceEnd: %w", err)
	}

	// update the number of slots in the page
	currentSlots, err := header.x.Fixedlen(*header.block, numSlotsOffset, storage.SizeOfSmallInt)
	if err != nil {
		return fmt.Errorf("get numSlots: %w", err)
	}

	numSlots := storage.UnsafeFixedToInteger[storage.SmallInt](currentSlots)
	numSlots++

	if err := header.setNumSlots(storage.SmallInt(numSlots)); err != nil {
		return fmt.Errorf("set numSlots: %w", err)
	}

	if err := header.setEntry(storage.SmallInt(numSlots-1), entry); err != nil {

		return fmt.Errorf("set entry: %w", err)
	}

	return nil
}

// SlottedPage implements an efficient record management system within a database page.
// This structure utilizes a slotted page approach, where the page is divided into a header
// section and a data section. The header contains metadata about the page and an array of
// slots, each representing a record. The data section stores the actual record data, growing
// from the end of the page towards the beginning.
//
// The page header includes information such as the block number, number of slots, and a
// pointer to the end of the free space. Each slot in the header is a compact structure
// that stores the record's offset, length, and status flags. This design allows for quick
// access to records and efficient space management.
//
// When inserting a record, the system finds an available slot (either an empty one or by
// creating a new one) and writes the record data at the end of the free space. The slot
// is then updated with the record's location and metadata. Deleting a record is as simple
// as flagging its slot as empty, which allows the space to be reused later. Updates can
// be performed in-place if the new data fits in the existing space, or by marking the old
// slot as empty and inserting the updated record as a new entry.
//
// This approach offers several advantages, including efficient space utilization, fast
// record retrieval, and flexible management of variable-length records. The system also
// includes methods for searching available slots and managing the page's free space,
// ensuring optimal use of the available storage.
//
// The SlottedPage struct encapsulates the necessary components to interact with
// the page, including the transaction, block, and layout information. It provides methods
// for all basic record operations, as well as utilities for page formatting and maintenance.
type SlottedPage struct {
	x      tx.Transaction
	block  storage.Block
	layout Layout
}

// recordHeader represents the header stored before each record on disk
// It stores the offsets of the ends of each field to allow direct access
// todo: this can be represented by an array of 16 bit ints to save space
// and possibly speed up access, once support for smaller ints is added
type recordHeader struct {
	txinfo recordHeaderTxInfo
}

type recordHeaderTxInfo struct {
	// xmin stores the transaction id that created the record
	xmin storage.TxID
	// xmax stores the transaction id that deleted the record
	xmax storage.TxID
	// txop stores the operation number of the transaction that created or deleted the record
	txop storage.SmallInt
	// flags stores additional flags for the record
	flags recordHeaderFlag
}

type recordHeaderFlag storage.SmallInt

const (
	flagUpdated recordHeaderFlag = 1 << iota
)

func (h recordHeader) setFlag(flag recordHeaderFlag) recordHeader {
	h.txinfo.flags |= flag
	return h
}

func (h recordHeader) hasFlag(flag recordHeaderFlag) bool {
	return h.txinfo.flags&flag != 0
}

// recordHeaderSize is the size of the recordHeaderTxInfo struct
const recordHeaderSize storage.Offset = storage.Offset(2*storage.SizeOfTxID + 2*storage.SizeOfSmallInt)

// NewSlottedPage creates a new SlottedRecordPage struct
func NewSlottedPage(tx tx.Transaction, block storage.Block, layout Layout) *SlottedPage {
	tx.Pin(block)
	return &SlottedPage{
		x:      tx,
		block:  block,
		layout: layout,
	}
}

// Close closes the page and unpins it from the transaction
func (p *SlottedPage) Close() {
	emptyBlock := storage.Block{}
	if p.block != emptyBlock {
		p.x.Unpin(p.block)
	}

	p.block = emptyBlock
}

func (p *SlottedPage) Block() storage.Block {
	return p.block
}

// recordSizeIncludingRecordHeader calculates the size of a record on disk including header
func (p *SlottedPage) recordSizeIncludingRecordHeader(recordSize storage.Offset) storage.Offset {
	return storage.Offset(recordHeaderSize) + recordSize
}

func (p *SlottedPage) Header() (slottedPageHeader, error) {

	return slottedPageHeader{
		block: &p.block,
		x:     p.x,
	}, nil
}

func (p *SlottedPage) writeRecordHeader(offset storage.Offset, recordHeader recordHeader) error {
	if err := p.x.SetFixedlen(
		p.block,
		offset,
		storage.SizeOfTxID,
		storage.UnsafeIntegerToFixedlen[storage.TxID](storage.SizeOfTxID, recordHeader.txinfo.xmin),
		true,
	); err != nil {
		return err
	}

	offset += storage.Offset(storage.SizeOfTxID)

	if err := p.x.SetFixedlen(
		p.block,
		offset,
		storage.SizeOfTxID,
		storage.UnsafeIntegerToFixedlen[storage.TxID](storage.SizeOfTxID, recordHeader.txinfo.xmax),
		true,
	); err != nil {
		return err
	}

	offset += storage.Offset(storage.SizeOfTxID)

	if err := p.x.SetFixedlen(
		p.block,
		offset,
		storage.SizeOfInt,
		storage.UnsafeIntegerToFixedlen[storage.SmallInt](storage.SizeOfSmallInt, recordHeader.txinfo.txop),
		true,
	); err != nil {
		return err
	}

	offset += storage.Offset(storage.SizeOfSmallInt)

	if err := p.x.SetFixedlen(
		p.block,
		offset,
		storage.SizeOfSmallInt,
		storage.UnsafeIntegerToFixedlen[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(recordHeader.txinfo.flags)),
		true,
	); err != nil {
		return err
	}

	return nil
}

func (p *SlottedPage) readRecordHeader(offset storage.Offset) (recordHeader, error) {
	xmin, err := p.x.Fixedlen(p.block, offset, storage.SizeOfTxID)
	if err != nil {
		return recordHeader{}, err
	}

	offset += storage.Offset(storage.SizeOfTxID)

	xmax, err := p.x.Fixedlen(p.block, offset, storage.SizeOfTxID)
	if err != nil {
		return recordHeader{}, err
	}

	offset += storage.Offset(storage.SizeOfTxID)

	txop, err := p.x.Fixedlen(p.block, offset, storage.SizeOfSmallInt)
	if err != nil {
		return recordHeader{}, err
	}

	offset += storage.Offset(storage.SizeOfSmallInt)

	flags, err := p.x.Fixedlen(p.block, offset, storage.SizeOfSmallInt)
	if err != nil {
		return recordHeader{}, err
	}

	return recordHeader{
		txinfo: recordHeaderTxInfo{
			xmin:  storage.UnsafeFixedToInteger[storage.TxID](xmin),
			xmax:  storage.UnsafeFixedToInteger[storage.TxID](xmax),
			txop:  storage.UnsafeFixedToInteger[storage.SmallInt](txop),
			flags: recordHeaderFlag(storage.UnsafeFixedToInteger[storage.SmallInt](flags)),
		},
	}, nil
}

// entry returns the entry of the record pointed to by the given slot
func (p *SlottedPage) entry(slot storage.SmallInt) (slottedPageHeaderEntry, error) {
	offset := entriesOffset + storage.Offset(sizeOfHeaderEntry)*storage.Offset(slot)
	v, err := p.x.Fixedlen(p.block, offset, sizeOfHeaderEntry)
	if err != nil {
		return slottedPageHeaderEntry{}, err
	}

	return slottedPageHeaderEntry(v), nil
}

func (p *SlottedPage) writeEntry(slot storage.SmallInt, entry slottedPageHeaderEntry) error {
	offset := entriesOffset + storage.Offset(sizeOfHeaderEntry)*storage.Offset(slot)
	return p.x.SetFixedlen(p.block, offset, sizeOfHeaderEntry, storage.FixedLen(entry[:]), true)
}

func (p *SlottedPage) RecordOffset(slot storage.SmallInt) (storage.Offset, error) {
	entry, err := p.entry(slot)
	if err != nil {
		return 0, err
	}

	return entry.recordOffset(), nil
}

// FieldOffset returns the offset of the field for the record pointed by the given slot
// the value is stored in the record header, in the ends array of offsets
func (p *SlottedPage) FieldOffset(slot storage.SmallInt, fieldname string) (storage.Offset, error) {
	entry, err := p.entry(slot)
	if err != nil {
		return 0, err
	}

	offset := entry.recordOffset()
	idx := p.layout.FieldIndex(fieldname)

	for i := range idx {
		size := p.layout.FieldSizeByIndex(i)
		if size == storage.SizeOfVarlen {
			// read the varlen size at the offset
			v, err := p.x.Fixedlen(p.block, offset, storage.SizeOfInt)
			if err != nil {
				return 0, err
			}

			offset += storage.UnsafeFixedToInteger[storage.Offset](v)
			offset += storage.Offset(storage.SizeOfInt)
		} else {
			offset += storage.Offset(size)
		}
	}

	return offset, nil
}

// Int returns the value of an integer field for the record pointed by the given slot
func (p *SlottedPage) FixedLen(slot storage.SmallInt, fieldname string) (storage.FixedLen, error) {
	offset, err := p.FieldOffset(slot, fieldname)
	if err != nil {
		return nil, err
	}

	size := p.layout.FieldSize(fieldname)

	return p.x.Fixedlen(p.block, offset, size)
}

// String returns the value of a string field for the record pointed by the given slot
func (p *SlottedPage) VarLen(slot storage.SmallInt, fieldname string) (storage.Varlen, error) {
	offset, err := p.FieldOffset(slot, fieldname)
	if err != nil {
		return storage.Varlen{}, err
	}

	return p.x.Varlen(p.block, offset)
}

// SetFixedLen sets the value of an integer field for the record pointed by the given slot
func (p *SlottedPage) SetFixedLen(slot storage.SmallInt, fieldname string, val storage.FixedLen) error {
	// get the offset of the field for the record at slot
	offset, err := p.FieldOffset(slot, fieldname)
	if err != nil {
		return err
	}

	// get the size of the field from the layout schema in catalog
	size := p.layout.FieldSize(fieldname)

	// write the new field value to the page
	if err := p.x.SetFixedlen(p.block, offset, size, val, true); err != nil {
		return err
	}

	return nil
}

// SetString sets the value of a string field for the record pointed by the given slot
func (p *SlottedPage) SetVarLen(slot storage.SmallInt, fieldname string, val storage.Varlen) error {
	offset, err := p.FieldOffset(slot, fieldname)
	if err != nil {
		return err
	}

	if err := p.x.SetVarlen(p.block, offset, val, true); err != nil {
		return err
	}

	return nil
}

// SetFixedLenAtSpecial sets the value of a fixed len field in the special space.
// The offset is relative to the start of the special space.
func (p *SlottedPage) SetFixedLenAtSpecial(offset storage.Offset, size storage.Size, val storage.FixedLen) error {
	header, err := p.Header()
	if err != nil {

		return err
	}

	offset = header.mustSpecialSpaceStart() + offset

	if err := p.x.SetFixedlen(p.block, offset, size, val, true); err != nil {

		return err
	}

	return nil
}

// SetVarLenAtSpecial sets the value of a var len field in the special space.
// The offset is relative to the start of the special space.
func (p *SlottedPage) SetVarLenAtSpecial(offset storage.Offset, val storage.Varlen) error {
	header, err := p.Header()
	if err != nil {

		return err
	}

	offset = header.mustSpecialSpaceStart() + offset

	if err := p.x.SetVarlen(p.block, offset, val, true); err != nil {

		return err
	}

	return nil
}

func (p *SlottedPage) VarLenAtSpecial(offset storage.Offset) (storage.Varlen, error) {
	header, err := p.Header()
	if err != nil {

		return nil, err
	}

	return p.x.Varlen(p.block, header.mustSpecialSpaceStart()+offset)
}

func (p *SlottedPage) FixedLenAtSpecial(offset storage.Offset, size storage.Size) (storage.FixedLen, error) {
	header, err := p.Header()
	if err != nil {

		return nil, err
	}

	return p.x.Fixedlen(p.block, header.mustSpecialSpaceStart()+offset, size)
}

// Delete flags the record's slot as empty by setting its flag to deleted.
// The record is not actually removed from the page and its slot is marked as dead.
// Delete also updates the record header to set the xmax field to the transaction id of the current transaction.
// This is used to track the transaction that deleted the record.
func (p *SlottedPage) Delete(slot storage.SmallInt) error {
	entry, err := p.entry(slot)
	if err != nil {
		return err
	}

	entry = entry.setFlag(flagDeletedRecord)
	if err := p.writeEntry(slot, entry); err != nil {
		return err
	}

	recordHeader, err := p.readRecordHeader(entry.recordOffset())
	if err != nil {
		return err
	}

	recordHeader.txinfo.xmax = p.x.Id()

	if err := p.writeRecordHeader(entry.recordOffset(), recordHeader); err != nil {
		return err
	}

	return nil
}

func (p *SlottedPage) IsDeleted(slot storage.SmallInt) (bool, error) {
	entry, err := p.entry(slot)
	if err != nil {
		return false, err
	}

	return entry.flags() == flagDeletedRecord, nil
}

// Format formats the page by writing a default header
func (p *SlottedPage) Format(specialSpaceSize storage.Offset) error {

	blockNumber := storage.Long(p.block.Number())
	freeSpaceEnd := defaultFreeSpaceEnd - specialSpaceSize
	specialSpaceStart := defaultFreeSpaceEnd - specialSpaceSize

	header := slottedPageHeader{
		x:     p.x,
		block: &p.block,
	}

	if err := header.setBlockNumber(blockNumber); err != nil {
		return err
	}

	if err := header.setNumSlots(0); err != nil {
		return err
	}

	if err := header.setFreeSpaceEnd(freeSpaceEnd); err != nil {
		return err
	}

	if err := header.setSpecialSpaceStart(specialSpaceStart); err != nil {
		return err
	}

	return nil
}

func (p *SlottedPage) AvailableSpace() (storage.Offset, error) {
	header, err := p.Header()
	if err != nil {
		return 0, err
	}

	return header.freeSpaceAvailable(), nil
}

func (p *SlottedPage) RecordsFit(size ...storage.Offset) (bool, error) {
	header, err := p.Header()
	if err != nil {
		return false, err
	}

	var tot storage.Offset
	for _, s := range size {
		tot += p.recordSizeIncludingRecordHeader(s) +
			storage.Offset(sizeOfHeaderEntry)
	}

	available := header.freeSpaceAvailable()

	return available >= tot, nil
}

// NextFrom returns the next used slot after the given one
// Returns ErrNoFreeSlot if such slot cannot be found within the transaction's block
func (p *SlottedPage) NextAfter(slot storage.SmallInt) (storage.SmallInt, error) {
	return p.searchAfter(slot, flagInUseRecord, 0)
}

// InsertFrom returns the next empty slot starting at the given one such that
// it can hold the provided record size.
func (p *SlottedPage) InsertAfter(slot storage.SmallInt, recordSize storage.Offset, update bool) (storage.SmallInt, error) {
	nextSlot, err := p.searchAfter(slot, flagEmptyRecord, recordSize)
	// no empty slot found, try to append to the end
	if err == ErrNoFreeSlot {
		header, err := p.Header()
		if err != nil {
			return InvalidSlot, err
		}

		// calculate the actual size of the record including the record header
		actualRecordSize := p.recordSizeIncludingRecordHeader(recordSize)

		// append a new slot to the page header for the record
		if err := header.appendRecordSlot(actualRecordSize); err == errNoFreeSpaceAvailable {
			// todo: return a more specific error to signal that the page is full
			return InvalidSlot, ErrNoFreeSlot
		}

		recordHeader := recordHeader{
			txinfo: recordHeaderTxInfo{
				xmin: p.x.Id(),
				xmax: 0,
				txop: 0,
			},
		}

		if update {
			recordHeader = recordHeader.setFlag(flagUpdated)
		}

		// write the record header at the end of the free space
		if err := p.writeRecordHeader(header.mustFreeSpaceEnd(), recordHeader); err != nil {
			return InvalidSlot, err
		}

		return header.mustNumSlots() - 1, nil
	}

	if err != nil {
		return InvalidSlot, err
	}

	return nextSlot, nil
}

// searchFrom searches for the next empty slot starting from the given one with the provided flag such that the record fits.
// A slot can be empty because the record was deleted and the space reclaimed, or because it was never used.
// If such a slot cannot be found within the block, it returns an ErrNoFreeSlot error.
// Otherwise it returns the slot index
// todo: we can optimise this by looking for the best slot available for the record of size, for example by picking
// the smallest empty slot that can fit the record rather than just the first one
func (p *SlottedPage) searchAfter(slot storage.SmallInt, flag flag, recordSize storage.Offset) (storage.SmallInt, error) {
	header, err := p.Header()
	if err != nil {
		return InvalidSlot, err
	}

	numSlots, err := header.numSlots()
	if err != nil {
		return InvalidSlot, err
	}

	for i := slot + 1; i < numSlots; i++ {
		entry, err := p.entry(i)
		if err != nil {
			return InvalidSlot, err
		}

		recordHeader, err := p.readRecordHeader(entry.recordOffset())
		if err != nil {
			return InvalidSlot, err
		}

		if recordHeader.txinfo.xmin == p.x.Id() && recordHeader.hasFlag(flagUpdated) {
			continue
		}

		if entry.flags() == flag && entry.recordLength() >= recordSize {
			return i, nil
		}
	}

	return InvalidSlot, ErrNoFreeSlot
}

// InsertAt shifts to the right all the records after the given slot and inserts the record at the slot
// If the slot is at the end of the slot array, it just appends the slot
func (page *SlottedPage) InsertAt(slot storage.SmallInt, recordSize storage.Offset) error {
	header, err := page.Header()
	if err != nil {
		return fmt.Errorf("ShiftSlotsRight: header %w", err)
	}

	numSlots, err := header.numSlots()
	if err != nil {
		return fmt.Errorf("ShiftSlotsRight: numSlots %w", err)
	}

	if slot > numSlots {
		return fmt.Errorf("InsertAt: slot %d out of bounds", slot)
	}

	freeSpaceEnd, err := header.freeSpaceEnd()
	if err != nil {
		return fmt.Errorf("InsertAt: freeSpaceEnd %w", err)
	}

	totalSize := recordSize + storage.Offset(sizeOfHeaderEntry)

	// check if there is enough space to shift the slots
	if header.freeSpaceStart()+totalSize > freeSpaceEnd {
		return fmt.Errorf("ShiftSlotsRight: %w", errNoFreeSpaceAvailable)
	}

	if err := header.appendRecordSlot(recordSize); err != nil {
		return fmt.Errorf("ShiftSlotsRight: appendRecordSlot: %w", err)
	}

	if numSlots != 0 {
		if err := header.x.Copy(
			*header.block,
			entriesOffset+storage.Offset(sizeOfPageHeaderEntry)*storage.Offset(slot),
			entriesOffset+storage.Offset(sizeOfPageHeaderEntry)*storage.Offset(slot+1),
			storage.Offset(numSlots-slot)*storage.Offset(sizeOfPageHeaderEntry),
			true,
		); err != nil {
			return fmt.Errorf("ShiftSlotsRight: copy: %w", err)
		}
	}

	entry, err := header.entry(slot)
	if err != nil {
		return fmt.Errorf("ShiftSlotsRight: entry: %w", err)
	}

	freeSpaceEnd = header.mustFreeSpaceEnd()

	entry = entry.
		setFlag(flagInUseRecord).
		setLength(recordSize).
		setOffset(freeSpaceEnd)

	if err := header.setEntry(slot, entry); err != nil {
		return fmt.Errorf("ShiftSlotsRight: set entry: %w", err)
	}

	return nil
}

// ShiftSlotsLeft shifts the record slots one position to the left starting from the given slot
func (page *SlottedPage) ShiftSlotsLeft(slot storage.SmallInt) error {
	if slot == 0 {
		return nil
	}

	header, err := page.Header()
	if err != nil {
		return fmt.Errorf("ShiftSlotsLeft: header %w", err)
	}

	numSlots := header.mustNumSlots()

	if err := header.x.Copy(
		*header.block,
		entriesOffset+storage.Offset(sizeOfPageHeaderEntry)*storage.Offset(slot),
		entriesOffset+storage.Offset(sizeOfPageHeaderEntry)*storage.Offset(slot-1),
		storage.Offset(numSlots-slot)*storage.Offset(sizeOfPageHeaderEntry),
		true,
	); err != nil {

		return fmt.Errorf("ShiftSlotsLeft: copy: %w", err)
	}

	if err := header.setNumSlots(numSlots - 1); err != nil {
		return fmt.Errorf("ShiftSlotsLeft: set numSlots: %w", err)
	}

	return nil
}

// Truncate truncates the page by removing all slots after the given one
// It compacts the page by moving all records pointed to by slots in use to the end of the free space.
func (p *SlottedPage) Truncate(slot storage.SmallInt) error {
	header, err := p.Header()
	if err != nil {
		return err
	}

	if slot >= header.mustNumSlots() {
		return fmt.Errorf("slot %d out of bounds", slot)
	}

	header.setNumSlots(slot)

	if err := p.Compact(); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	return nil
}

// Compact compacts the page by moving all records pointed to by slots in use
// to the end of the free space. This operation is useful to reclaim space.
func (p *SlottedPage) Compact() error {
	header, err := p.Header()
	if err != nil {

		return err
	}

	// the page is empty, nothing to compact
	if header.mustFreeSpaceEnd() == header.mustSpecialSpaceStart() {
		return nil
	}

	numSlots := header.mustNumSlots()
	// sort the slots by record offset so that the highest offset is processed first
	// and shift all records to the right to fill the gaps
	// then update the slot entries.
	cpy := make([]storage.SmallInt, numSlots)
	for i := range numSlots {
		cpy[i] = storage.SmallInt(i)
	}

	slices.SortFunc(cpy, func(i, j storage.SmallInt) int {

		f, e := p.entry(i)
		if e != nil {
			err = e

			return 0
		}

		s, e := p.entry(j)
		if e != nil {
			err = e

			return 0
		}

		diff := int(s.recordOffset()) - int(f.recordOffset())

		return diff
	})

	if err != nil {

		return err
	}

	freeSpaceEnd := header.mustSpecialSpaceStart()
	for _, i := range cpy {
		entry, err := p.entry(i)
		if err != nil {

			return err
		}

		if entry.flags() != flagInUseRecord {
			continue
		}

		len := entry.recordLength()
		freeSpaceEnd -= len
		// copy the record to the new offset
		src := entry.recordOffset()
		if err := p.x.Copy(p.block, src, freeSpaceEnd, len, true); err != nil {
			return err
		}

		// update the real slot entry
		entry = entry.setOffset(freeSpaceEnd)
		if err := p.writeEntry(i, entry); err != nil {

			return err
		}
	}

	if err := header.setFreeSpaceEnd(freeSpaceEnd); err != nil {

		return err
	}

	return nil
}

type DumpSlot struct {
	Offset storage.Offset
	Length storage.Offset
	Flags  flag
}

type Dump struct {
	BlockNumber       storage.Long
	NumSlots          storage.SmallInt
	FreeSpaceStart    storage.Offset
	FreeSpaceEnd      storage.Offset
	SpecialSpaceStart storage.Offset
	Slots             []DumpSlot
}

func (page SlottedPage) Dump() (Dump, error) {
	header, err := page.Header()
	if err != nil {
		return Dump{}, err
	}

	numSlots, err := header.numSlots()
	if err != nil {
		return Dump{}, err
	}

	freeSpaceStart := header.freeSpaceStart()

	freeSpaceEnd, err := header.freeSpaceEnd()
	if err != nil {
		return Dump{}, err
	}

	specialSpaceStart, err := header.specialSpaceStart()
	if err != nil {
		return Dump{}, err
	}

	dump := Dump{
		BlockNumber:       page.block.Number(),
		NumSlots:          numSlots,
		FreeSpaceStart:    freeSpaceStart,
		FreeSpaceEnd:      freeSpaceEnd,
		SpecialSpaceStart: specialSpaceStart,
		Slots:             make([]DumpSlot, numSlots),
	}

	for i := storage.SmallInt(0); i < numSlots; i++ {
		entry, err := page.entry(i)
		if err != nil {
			return Dump{}, err
		}

		dump.Slots[i] = DumpSlot{
			Offset: entry.recordOffset(),
			Length: entry.recordLength(),
			Flags:  entry.flags(),
		}
	}

	return dump, nil
}

func (dump Dump) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("BlockNumber: %d\n", dump.BlockNumber))
	b.WriteString(fmt.Sprintf("NumSlots: %d\n", dump.NumSlots))
	b.WriteString(fmt.Sprintf("FreeSpaceStart: %d\n", dump.FreeSpaceStart))
	b.WriteString(fmt.Sprintf("FreeSpaceEnd: %d\n", dump.FreeSpaceEnd))
	b.WriteString(fmt.Sprintf("SpecialSpaceStart: %d\n", dump.SpecialSpaceStart))

	for i, slot := range dump.Slots {
		b.WriteString(fmt.Sprintf("Slot %d: Offset: %d, Length: %d, Flags: %d\n", i, slot.Offset, slot.Length, slot.Flags))
	}

	return b.String()
}
