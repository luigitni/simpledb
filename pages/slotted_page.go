package pages

import (
	"errors"
	"fmt"
	"slices"

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
	blockNumberOffset       storage.Offset = 0
	numSlotsOffset          storage.Offset = blockNumberOffset + storage.Offset(storage.SizeOfLong)
	freeSpaceEndOffset      storage.Offset = numSlotsOffset + storage.Offset(storage.SizeOfSmallInt)
	specialSpaceStartOffset storage.Offset = freeSpaceEndOffset + storage.Offset(storage.SizeOfSmallInt)
	entriesOffset           storage.Offset = specialSpaceStartOffset + storage.Offset(storage.SizeOfOffset)
)

func (h slottedPageHeader) freeSpaceStart() storage.Offset {
	return h.lastSlotOffset() + storage.Offset(sizeOfHeaderEntry)
}

func (h slottedPageHeader) lastSlotOffset() storage.Offset {
	return entriesOffset + storage.Offset(h.mustNumSlots())*storage.Offset(sizeOfHeaderEntry)
}

func (h slottedPageHeader) freeSpaceAvailable() storage.Offset {
	return h.mustFreeSpaceEnd() - h.freeSpaceStart()
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
	if actualRecordSize > header.freeSpaceAvailable() {
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

func (header *slottedPageHeader) appendEntry(entry slottedPageHeaderEntry) error {
	if err := header.x.SetFixedlen(
		*header.block,
		freeSpaceEndOffset,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, entry.recordOffset()),
		true,
	); err != nil {
		return err
	}

	currentSlots, err := header.x.Fixedlen(*header.block, numSlotsOffset, storage.SizeOfSmallInt)
	if err != nil {

		return err
	}

	numSlots := storage.UnsafeFixedToInteger[storage.SmallInt](currentSlots)
	numSlots++

	if err := header.x.SetFixedlen(
		*header.block,
		numSlotsOffset,
		storage.SizeOfSmallInt,
		storage.UnsafeIntegerToFixedlen[storage.SmallInt](storage.SizeOfSmallInt, numSlots),
		true,
	); err != nil {

		return err
	}

	return header.x.SetFixedlen(
		*header.block,
		entriesOffset+storage.Offset(numSlots-1)*storage.Offset(sizeOfPageHeaderEntry),
		sizeOfPageHeaderEntry,
		storage.UnsafeByteSliceToFixedlen(entry[:]),
		true,
	)
}

func (header *slottedPageHeader) CopyRecordSlot(src storage.SmallInt, dest storage.SmallInt) error {
	if got := header.mustNumSlots(); got < dest {
		return fmt.Errorf("invalid destination slot %d against %d slots available", dest, got)
	}

	v, err := header.entry(src)
	if err != nil {

		return err
	}

	if err := header.x.SetFixedlen(
		*header.block,
		entriesOffset+storage.Offset(dest)*storage.Offset(sizeOfPageHeaderEntry),
		sizeOfPageHeaderEntry,
		storage.UnsafeByteSliceToFixedlen(v[:]),
		true,
	); err != nil {
		return err
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
	// ends stores the offsets of the ends of each field in the record
	ends []storage.Offset

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

// recordHeaderTxInfoSize is the size of the recordHeaderTxInfo struct
const recordHeaderTxInfoSize storage.Size = 2*storage.SizeOfTxID + 2*storage.SizeOfSmallInt

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
	p.x.Unpin(p.block)
}

func (p *SlottedPage) Block() storage.Block {
	return p.block
}

// recordHeaderSize returns the size of the record header
// which includes the transaction info and the ends of the fields
func (p *SlottedPage) recordHeaderSize() storage.Offset {
	return storage.Offset(recordHeaderTxInfoSize) +
		storage.Offset(p.layout.FieldsCount())*storage.Offset(storage.SizeOfSmallInt)
}

// recordSizeIncludingRecordHeader calculates the size of a record on disk including header
func (p *SlottedPage) recordSizeIncludingRecordHeader(recordSize storage.Offset) storage.Offset {
	return p.recordHeaderSize() + recordSize
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

	offset += storage.Offset(storage.SizeOfSmallInt)

	for _, field := range recordHeader.ends {
		offset += storage.Offset(storage.SizeOfOffset)

		if err := p.x.SetFixedlen(
			p.block,
			offset,
			storage.SizeOfOffset,
			storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, field),
			true,
		); err != nil {
			return err
		}
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

	offset += storage.Offset(storage.SizeOfSmallInt)

	ends := make([]storage.Offset, 0, p.layout.FieldsCount())

	for range p.layout.FieldsCount() {
		offset += storage.Offset(storage.SizeOfOffset)

		end, err := p.x.Fixedlen(p.block, offset, storage.SizeOfOffset)
		if err != nil {
			return recordHeader{}, err
		}

		v := storage.UnsafeFixedToInteger[storage.Offset](end)

		ends = append(ends, v)
	}

	return recordHeader{
		ends: ends,
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

	// read the record header to find the requested field
	fieldIndex := p.layout.FieldIndex(fieldname)
	if fieldIndex == -1 {
		return 0, fmt.Errorf("invalid field %q for record", fieldname)
	}

	recordOffset := entry.recordOffset()
	// the first field starts right after the record header
	firstFieldOffset := recordOffset + p.recordHeaderSize()

	if fieldIndex == 0 {
		return firstFieldOffset, nil
	}

	prevIndex := fieldIndex - 1
	// read the end of the previous field to find the start of this one
	fieldOffset := recordOffset +
		storage.Offset(recordHeaderTxInfoSize) +
		storage.Offset(prevIndex)*storage.Offset(storage.SizeOfSmallInt)

	fo, err := p.x.Fixedlen(p.block, fieldOffset, storage.SizeOfOffset)
	if err != nil {
		return 0, err
	}

	return storage.UnsafeFixedToInteger[storage.Offset](fo), nil
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

func (p *SlottedPage) updateFieldEnd(slot storage.SmallInt, fieldname string, fieldEnd storage.Offset) error {
	fieldIndex := p.layout.FieldIndex(fieldname)
	if fieldIndex == -1 {
		return fmt.Errorf("invalid field %q for record", fieldname)
	}

	entry, err := p.entry(slot)
	if err != nil {
		return err
	}

	fieldOffsetEntry := storage.Offset(recordHeaderTxInfoSize) +
		entry.recordOffset() +
		storage.Offset(fieldIndex)*storage.Offset(storage.SizeOfOffset)

	return p.x.SetFixedlen(
		p.block,
		fieldOffsetEntry,
		storage.SizeOfSmallInt,
		storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, fieldEnd),
		true,
	)
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

	// update the end of this field in the record header
	fieldEnd := offset + storage.Offset(size)

	return p.updateFieldEnd(slot, fieldname, fieldEnd)
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

	fieldEnd := offset + storage.Offset(val.Size())

	return p.updateFieldEnd(slot, fieldname, fieldEnd)
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

	if err := p.x.SetFixedlen(
		p.block,
		blockNumberOffset,
		storage.SizeOfLong,
		storage.UnsafeIntegerToFixedlen[storage.Long](storage.SizeOfLong, blockNumber),
		true,
	); err != nil {
		return err
	}

	if err := p.x.SetFixedlen(
		p.block,
		numSlotsOffset,
		storage.SizeOfSmallInt,
		storage.UnsafeIntegerToFixedlen[storage.SmallInt](storage.SizeOfSmallInt, 0),
		true,
	); err != nil {
		return err
	}

	if err := p.x.SetFixedlen(
		p.block,
		freeSpaceEndOffset,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, freeSpaceEnd),
		true,
	); err != nil {
		return err
	}

	if err := p.x.SetFixedlen(
		p.block,
		specialSpaceStartOffset,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, specialSpaceStart),
		true,
	); err != nil {
		return err
	}

	return nil
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
			ends: make([]storage.Offset, p.layout.FieldsCount()),
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

	for i := slot + 1; i < header.mustNumSlots(); i++ {
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
	// sort the slots by record offset
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

		return int(s.recordOffset() - f.recordOffset())
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
		if err := p.x.Copy(p.block, entry.recordOffset(), freeSpaceEnd, len, true); err != nil {
			return err
		}

		// update the real slot entry
		entry = entry.setOffset(freeSpaceEnd)
		if err := p.writeEntry(i, entry); err != nil {

			return err
		}
	}

	if err := p.x.SetFixedlen(
		p.block,
		freeSpaceEndOffset,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixedlen[storage.Offset](storage.SizeOfOffset, freeSpaceEnd),
		true,
	); err != nil {

		return err
	}

	return nil
}
