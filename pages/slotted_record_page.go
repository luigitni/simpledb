package pages

import (
	"errors"
	"fmt"

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

// slottedRecordPageHeaderEntry represents an entry in the page header.
// Each entry is an 8-byte value
// It is bitmasked to store:
// - the first 2 bytes store the offset of the record within the page
// - the next 2 bytes store the length of the record within the page
// - byte 5 to 8 store record flags (whether empty/deleted etc)
type slottedRecordPageHeaderEntry [8]byte

const sizeOfRecordPageHeaderEntry = storage.Size(len(slottedRecordPageHeaderEntry{}))

func (e slottedRecordPageHeaderEntry) recordOffset() storage.Offset {
	return storage.UnsafeFixedToInteger[storage.Offset](e[:2])
}

func (e slottedRecordPageHeaderEntry) recordLength() storage.Offset {
	return storage.UnsafeFixedToInteger[storage.Offset](e[2:4])
}

func (e slottedRecordPageHeaderEntry) flags() flag {
	return flag(storage.UnsafeFixedToInteger[storage.Int](e[4:6]))
}

func (e slottedRecordPageHeaderEntry) setOffset(offset storage.Offset) slottedRecordPageHeaderEntry {
	fixed := storage.UnsafeIntegerToFixed[storage.Offset](storage.SizeOfOffset, offset)
	copy(e[:2], fixed)
	return e
}

func (e slottedRecordPageHeaderEntry) setLength(length storage.Offset) slottedRecordPageHeaderEntry {
	fixed := storage.UnsafeIntegerToFixed[storage.Offset](storage.SizeOfOffset, length)
	copy(e[2:4], fixed)
	return e
}

func (e slottedRecordPageHeaderEntry) setFlag(f flag) slottedRecordPageHeaderEntry {
	fixed := storage.UnsafeIntegerToFixed[storage.Int](storage.SizeOfInt, storage.Int(f))
	copy(e[4:], fixed)
	return e
}

const (
	MaxSlot         storage.SmallInt = (1 << (storage.SizeOfSmallInt * 8)) - 3
	InvalidSlot                      = MaxSlot + 1
	BeforeFirstSlot                  = InvalidSlot + 1
)

// slottedRecordPageHeader represents the header of a slotted record page
// It holds the metadata about the page and the slot array for the records.
type slottedRecordPageHeader struct {
	blockNumber  storage.Long
	numSlots     storage.SmallInt
	freeSpaceEnd storage.Offset
	entries      []slottedRecordPageHeaderEntry
}

const defaultFreeSpaceEnd = storage.PageSize

const (
	blockNumberOffset  storage.Offset = 0
	numSlotsOffset     storage.Offset = blockNumberOffset + storage.Offset(storage.SizeOfLong)
	freeSpaceEndOffset storage.Offset = numSlotsOffset + storage.Offset(storage.SizeOfSmallInt)
	entriesOffset      storage.Offset = freeSpaceEndOffset + storage.Offset(storage.SizeOfOffset)
)

func (h slottedRecordPageHeader) freeSpaceStart() storage.Offset {
	return h.lastSlotOffset() + storage.Offset(sizeOfHeaderEntry)
}

func (h slottedRecordPageHeader) lastSlotOffset() storage.Offset {
	return entriesOffset + storage.Offset(h.numSlots)*storage.Offset(sizeOfHeaderEntry)
}

func (h slottedRecordPageHeader) freeSpaceAvailable() storage.Offset {
	return h.freeSpaceEnd - h.freeSpaceStart()
}

// appendRecordSlot appends a new record slot to the page header
// It takes in the actual size of the record to be inserted
func (header *slottedRecordPageHeader) appendRecordSlot(actualRecordSize storage.Offset) error {
	if actualRecordSize > header.freeSpaceAvailable() {
		return errNoFreeSpaceAvailable
	}

	header.freeSpaceEnd -= actualRecordSize
	entry := slottedRecordPageHeaderEntry{}.
		setOffset(header.freeSpaceEnd).
		setLength(actualRecordSize).
		setFlag(flagInUseRecord)
	header.entries = append(header.entries, entry)
	header.numSlots++

	return nil
}

func (header *slottedRecordPageHeader) popRecordSlot() slottedRecordPageHeaderEntry {
	entry := header.entries[header.numSlots-1]
	header.entries = header.entries[:header.numSlots-1]
	header.numSlots--

	if header.numSlots > 0 {
		header.freeSpaceEnd += header.entries[header.numSlots].recordLength()
	} else {
		header.freeSpaceEnd = defaultFreeSpaceEnd
	}

	return entry
}

// SlottedRecordPage implements an efficient record management system within a database page.
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
// The SlottedRecordPage struct encapsulates the necessary components to interact with
// the page, including the transaction, block, and layout information. It provides methods
// for all basic record operations, as well as utilities for page formatting and maintenance.
type SlottedRecordPage struct {
	tx     tx.Transaction
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

// NewSlottedRecordPage creates a new SlottedRecordPage struct
func NewSlottedRecordPage(tx tx.Transaction, block storage.Block, layout Layout) *SlottedRecordPage {
	tx.Pin(block)
	return &SlottedRecordPage{
		tx:     tx,
		block:  block,
		layout: layout,
	}
}

// Close closes the page and unpins it from the transaction
func (p *SlottedRecordPage) Close() {
	p.tx.Unpin(p.block)
}

func (p *SlottedRecordPage) Block() storage.Block {
	return p.block
}

// recordHeaderSize returns the size of the record header
// which includes the transaction info and the ends of the fields
func (p *SlottedRecordPage) recordHeaderSize() storage.Offset {
	return storage.Offset(recordHeaderTxInfoSize) +
		storage.Offset(p.layout.FieldsCount())*storage.Offset(storage.SizeOfSmallInt)
}

// recordSizeIncludingRecordHeader calculates the size of a record on disk including header
func (p *SlottedRecordPage) recordSizeIncludingRecordHeader(recordSize storage.Offset) storage.Offset {
	return p.recordHeaderSize() + recordSize
}

func (p *SlottedRecordPage) writeHeader(header slottedRecordPageHeader) error {
	if err := p.tx.SetFixedLen(
		p.block,
		blockNumberOffset,
		storage.SizeOfLong,
		storage.UnsafeIntegerToFixed[storage.Long](storage.SizeOfLong, header.blockNumber),
		true,
	); err != nil {
		return err
	}

	if err := p.tx.SetFixedLen(
		p.block,
		numSlotsOffset,
		storage.SizeOfSmallInt,
		storage.UnsafeIntegerToFixed[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(len(header.entries))),
		true,
	); err != nil {
		return err
	}

	if err := p.tx.SetFixedLen(
		p.block,
		freeSpaceEndOffset,
		storage.SizeOfOffset,
		storage.UnsafeIntegerToFixed[storage.Offset](storage.SizeOfOffset, header.freeSpaceEnd),
		true,
	); err != nil {
		return err
	}

	offset := entriesOffset

	for _, entry := range header.entries {

		if err := p.tx.SetFixedLen(
			p.block,
			offset,
			sizeOfRecordPageHeaderEntry,
			storage.FixedLen(entry[:]),
			true,
		); err != nil {
			return err
		}

		offset += storage.Offset(sizeOfRecordPageHeaderEntry)
	}

	return nil
}

func (p *SlottedRecordPage) readHeader() (slottedRecordPageHeader, error) {
	blockNum, err := p.tx.FixedLen(p.block, blockNumberOffset, storage.SizeOfLong)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	numSlots, err := p.tx.FixedLen(p.block, numSlotsOffset, storage.SizeOfSmallInt)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	freeSpaceEnd, err := p.tx.FixedLen(p.block, freeSpaceEndOffset, storage.SizeOfOffset)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	numSlotsVal := storage.UnsafeFixedToInteger[storage.SmallInt](numSlots)
	entries := make([]slottedRecordPageHeaderEntry, numSlotsVal)

	offset := entriesOffset
	for i := range len(entries) {
		slot, err := p.tx.FixedLen(p.block, offset, sizeOfRecordPageHeaderEntry)
		if err != nil {
			return slottedRecordPageHeader{}, err
		}

		copy(entries[i][:], slot)

		offset += storage.Offset(sizeOfRecordPageHeaderEntry)
	}

	return slottedRecordPageHeader{
		blockNumber:  storage.UnsafeFixedToInteger[storage.Long](blockNum),
		numSlots:     storage.UnsafeFixedToInteger[storage.SmallInt](numSlots),
		freeSpaceEnd: storage.UnsafeFixedToInteger[storage.Offset](freeSpaceEnd),
		entries:      entries,
	}, nil
}

func (p *SlottedRecordPage) writeRecordHeader(offset storage.Offset, recordHeader recordHeader) error {
	if err := p.tx.SetFixedLen(
		p.block,
		offset,
		storage.SizeOfTxID,
		storage.UnsafeIntegerToFixed[storage.TxID](storage.SizeOfTxID, recordHeader.txinfo.xmin),
		true,
	); err != nil {
		return err
	}

	offset += storage.Offset(storage.SizeOfTxID)

	if err := p.tx.SetFixedLen(
		p.block,
		offset,
		storage.SizeOfTxID,
		storage.UnsafeIntegerToFixed[storage.TxID](storage.SizeOfTxID, recordHeader.txinfo.xmax),
		true,
	); err != nil {
		return err
	}

	offset += storage.Offset(storage.SizeOfTxID)

	if err := p.tx.SetFixedLen(
		p.block,
		offset,
		storage.SizeOfInt,
		storage.UnsafeIntegerToFixed[storage.SmallInt](storage.SizeOfSmallInt, recordHeader.txinfo.txop),
		true,
	); err != nil {
		return err
	}

	offset += storage.Offset(storage.SizeOfSmallInt)

	if err := p.tx.SetFixedLen(
		p.block,
		offset,
		storage.SizeOfSmallInt,
		storage.UnsafeIntegerToFixed[storage.SmallInt](storage.SizeOfSmallInt, storage.SmallInt(recordHeader.txinfo.flags)),
		true,
	); err != nil {
		return err
	}

	offset += storage.Offset(storage.SizeOfSmallInt)

	for _, field := range recordHeader.ends {
		offset += storage.Offset(storage.SizeOfOffset)

		if err := p.tx.SetFixedLen(
			p.block,
			offset,
			storage.SizeOfOffset,
			storage.UnsafeIntegerToFixed[storage.Offset](storage.SizeOfOffset, field),
			true,
		); err != nil {
			return err
		}
	}

	return nil
}

func (p *SlottedRecordPage) readRecordHeader(offset storage.Offset) (recordHeader, error) {
	xmin, err := p.tx.FixedLen(p.block, offset, storage.SizeOfTxID)
	if err != nil {
		return recordHeader{}, err
	}

	offset += storage.Offset(storage.SizeOfTxID)

	xmax, err := p.tx.FixedLen(p.block, offset, storage.SizeOfTxID)
	if err != nil {
		return recordHeader{}, err
	}

	offset += storage.Offset(storage.SizeOfTxID)

	txop, err := p.tx.FixedLen(p.block, offset, storage.SizeOfSmallInt)
	if err != nil {
		return recordHeader{}, err
	}

	offset += storage.Offset(storage.SizeOfSmallInt)

	flags, err := p.tx.FixedLen(p.block, offset, storage.SizeOfSmallInt)
	if err != nil {
		return recordHeader{}, err
	}

	offset += storage.Offset(storage.SizeOfSmallInt)

	ends := make([]storage.Offset, 0, p.layout.FieldsCount())

	for range p.layout.FieldsCount() {
		offset += storage.Offset(storage.SizeOfOffset)

		end, err := p.tx.FixedLen(p.block, offset, storage.SizeOfOffset)
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
func (p *SlottedRecordPage) entry(slot storage.SmallInt) (slottedRecordPageHeaderEntry, error) {
	offset := entriesOffset + storage.Offset(sizeOfHeaderEntry)*storage.Offset(slot)
	v, err := p.tx.FixedLen(p.block, offset, sizeOfHeaderEntry)
	if err != nil {
		return slottedRecordPageHeaderEntry{}, err
	}

	return slottedRecordPageHeaderEntry(v), nil
}

func (p *SlottedRecordPage) writeEntry(slot storage.SmallInt, entry slottedRecordPageHeaderEntry) error {
	offset := entriesOffset + storage.Offset(sizeOfHeaderEntry)*storage.Offset(slot)
	return p.tx.SetFixedLen(p.block, offset, sizeOfHeaderEntry, storage.FixedLen(entry[:]), true)
}

func (p *SlottedRecordPage) setFlag(slot storage.SmallInt, flag flag) error {
	entry, err := p.entry(slot)
	if err != nil {
		return err
	}

	entry.setFlag(flag)

	return nil
}

// fieldOffset returns the offset of the field for the record pointed by the given slot
// the value is stored in the record header, in the ends array of offsets
func (p *SlottedRecordPage) fieldOffset(slot storage.SmallInt, fieldname string) (storage.Offset, error) {
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

	fo, err := p.tx.FixedLen(p.block, fieldOffset, storage.SizeOfOffset)
	if err != nil {
		return 0, err
	}

	return storage.UnsafeFixedToInteger[storage.Offset](fo), nil
}

// Int returns the value of an integer field for the record pointed by the given slot
func (p *SlottedRecordPage) FixedLen(slot storage.SmallInt, fieldname string) (storage.FixedLen, error) {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return nil, err
	}

	size := p.layout.FieldSize(fieldname)

	return p.tx.FixedLen(p.block, offset, size)
}

// String returns the value of a string field for the record pointed by the given slot
func (p *SlottedRecordPage) VarLen(slot storage.SmallInt, fieldname string) (storage.Varlen, error) {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return storage.Varlen{}, err
	}

	return p.tx.VarLen(p.block, offset)
}

func (p *SlottedRecordPage) updateFieldEnd(slot storage.SmallInt, fieldname string, fieldEnd storage.Offset) error {
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

	return p.tx.SetFixedLen(
		p.block,
		fieldOffsetEntry,
		storage.SizeOfSmallInt,
		storage.UnsafeIntegerToFixed[storage.Offset](storage.SizeOfOffset, fieldEnd),
		true,
	)
}

// SetFixedLen sets the value of an integer field for the record pointed by the given slot
func (p *SlottedRecordPage) SetFixedLen(slot storage.SmallInt, fieldname string, val storage.FixedLen) error {
	// get the offset of the field for the record at slot
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return err
	}

	// get the size of the field from the layout schema in catalog
	size := p.layout.FieldSize(fieldname)

	// write the new field value to the page
	if err := p.tx.SetFixedLen(p.block, offset, size, val, true); err != nil {
		return err
	}

	// update the end of this field in the record header
	fieldEnd := offset + storage.Offset(size)

	return p.updateFieldEnd(slot, fieldname, fieldEnd)
}

// SetString sets the value of a string field for the record pointed by the given slot
func (p *SlottedRecordPage) SetVarLen(slot storage.SmallInt, fieldname string, val storage.Varlen) error {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return err
	}

	if err := p.tx.SetVarLen(p.block, offset, val, true); err != nil {
		return err
	}

	fieldEnd := offset + storage.Offset(val.Size())

	return p.updateFieldEnd(slot, fieldname, fieldEnd)
}

// Delete flags the record's slot as empty by setting its flag
func (p *SlottedRecordPage) Delete(slot storage.SmallInt) error {
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

	recordHeader.txinfo.xmax = p.tx.Id()

	if err := p.writeRecordHeader(entry.recordOffset(), recordHeader); err != nil {
		return err
	}

	return nil
}

// Format formats the page by writing a default header
func (p *SlottedRecordPage) Format() error {
	header := slottedRecordPageHeader{
		blockNumber:  storage.Long(p.block.Number()),
		numSlots:     0,
		freeSpaceEnd: defaultFreeSpaceEnd,
	}

	return p.writeHeader(header)
}

// NextFrom returns the next used slot after the given one
// Returns ErrNoFreeSlot if such slot cannot be found within the transaction's block
func (p *SlottedRecordPage) NextAfter(slot storage.SmallInt) (storage.SmallInt, error) {
	return p.searchAfter(slot, flagInUseRecord, 0)
}

// InsertFrom returns the next empty slot starting at the given one such that
// it can hold the provided record size.
func (p *SlottedRecordPage) InsertAfter(slot storage.SmallInt, recordSize storage.Offset, update bool) (storage.SmallInt, error) {
	nextSlot, err := p.searchAfter(slot, flagEmptyRecord, recordSize)
	// no empty slot found, try to append to the end
	if err == ErrNoFreeSlot {
		header, err := p.readHeader()
		if err != nil {
			return InvalidSlot, err
		}

		// calculate the actual size of the record including the record header
		actualRecordSize := p.recordSizeIncludingRecordHeader(recordSize)

		// append a new slot to the page header for the record
		if err := header.appendRecordSlot(actualRecordSize); err == errNoFreeSpaceAvailable {
			return InvalidSlot, ErrNoFreeSlot
		}

		if err := p.writeHeader(header); err != nil {
			return InvalidSlot, err
		}

		recordHeader := recordHeader{
			ends: make([]storage.Offset, p.layout.FieldsCount()),
			txinfo: recordHeaderTxInfo{
				xmin: p.tx.Id(),
				xmax: 0,
				txop: 0,
			},
		}

		if update {
			recordHeader = recordHeader.setFlag(flagUpdated)
		}

		// write the record header at the end of the free space
		if err := p.writeRecordHeader(header.freeSpaceEnd, recordHeader); err != nil {
			return InvalidSlot, err
		}

		return header.numSlots - 1, nil
	}

	if err != nil {
		return InvalidSlot, err
	}

	return nextSlot, nil
}

// searchFrom searches for the next empty slot starting from the given one with the provided flag such that the record fits
// If such a slot cannot be found within the block, it returns an ErrNoFreeSlot error.
// Otherwise it returns the slot index
// todo: we can optimise this by looking for the best slot available for the record of size, for example by picking
// the smallest empty slot that can fit the record rather than just the first one
func (p *SlottedRecordPage) searchAfter(slot storage.SmallInt, flag flag, recordSize storage.Offset) (storage.SmallInt, error) {
	header, err := p.readHeader()
	if err != nil {
		return InvalidSlot, err
	}

	for i := slot + 1; i < header.numSlots; i++ {
		entry, err := p.entry(i)
		if err != nil {
			return InvalidSlot, err
		}

		recordHeader, err := p.readRecordHeader(entry.recordOffset())
		if err != nil {
			return InvalidSlot, err
		}

		if recordHeader.txinfo.xmin == p.tx.Id() && recordHeader.hasFlag(flagUpdated) {
			continue
		}

		if entry.flags() == flag && entry.recordLength() >= recordSize {
			return i, nil
		}
	}

	return InvalidSlot, ErrNoFreeSlot
}
