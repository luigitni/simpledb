package pages

import (
	"errors"
	"fmt"

	"github.com/luigitni/simpledb/tx"
	"github.com/luigitni/simpledb/types"
)

var (
	errNoFreeSpaceAvailable = errors.New("no free space available on page to insert record")
	ErrNoFreeSlot           = errors.New("no free slot available")
)

type flag uint16

const (
	flagEmptyRecord flag = 1 << iota
	flagInUseRecord
	flagDeletedRecord
)

const headerEntrySize = types.SizeOfLong

// slottedRecordPageHeaderEntry represents an entry in the page header.
// Each entry is an 8-byte value
// It is bitmasked to store:
// - the first 2 bytes store the offset of the record within the page
// - the next 2 bytes store the length of the record within the page
// - byte 5 to 8 store record flags (whether empty/deleted etc)
type slottedRecordPageHeaderEntry [8]byte

const sizeOfRecordPageHeaderEntry = types.Size(len(slottedRecordPageHeaderEntry{}))

func (e slottedRecordPageHeaderEntry) recordOffset() types.Offset {
	return types.UnsafeFixedToInteger[types.Offset](e[:2])
}

func (e slottedRecordPageHeaderEntry) recordLength() types.Offset {
	return types.UnsafeFixedToInteger[types.Offset](e[2:4])
}

func (e slottedRecordPageHeaderEntry) flags() flag {
	return flag(types.UnsafeFixedToInteger[types.Int](e[4:6]))
}

func (e slottedRecordPageHeaderEntry) setOffset(offset types.Offset) slottedRecordPageHeaderEntry {
	fixed := types.UnsafeIntegerToFixed[types.Offset](types.SizeOfOffset, offset)
	copy(e[:2], fixed)
	return e
}

func (e slottedRecordPageHeaderEntry) setLength(length types.Offset) slottedRecordPageHeaderEntry {
	fixed := types.UnsafeIntegerToFixed[types.Offset](types.SizeOfOffset, length)
	copy(e[2:4], fixed)
	return e
}

func (e slottedRecordPageHeaderEntry) setFlag(f flag) slottedRecordPageHeaderEntry {
	fixed := types.UnsafeIntegerToFixed[types.Int](types.SizeOfInt, types.Int(f))
	copy(e[4:], fixed)
	return e
}

// slottedRecordPageHeader represents the header of a slotted record page
// It holds the metadata about the page and the slot array for the records.
type slottedRecordPageHeader struct {
	blockNumber  types.Long
	numSlots     types.SmallInt
	freeSpaceEnd types.Offset
	entries      []slottedRecordPageHeaderEntry
}

const defaultFreeSpaceEnd = types.PageSize

const (
	blockNumberOffset  types.Offset = 0
	numSlotsOffset     types.Offset = blockNumberOffset + types.Offset(types.SizeOfLong)
	freeSpaceEndOffset types.Offset = numSlotsOffset + types.Offset(types.SizeOfSmallInt)
	entriesOffset      types.Offset = freeSpaceEndOffset + types.Offset(types.SizeOfOffset)
)

func (h slottedRecordPageHeader) freeSpaceStart() types.Offset {
	return h.lastSlotOffset() + types.Offset(headerEntrySize)
}

func (h slottedRecordPageHeader) lastSlotOffset() types.Offset {
	return entriesOffset + types.Offset(h.numSlots)*types.Offset(headerEntrySize)
}

func (h slottedRecordPageHeader) freeSpaceAvailable() types.Offset {
	return h.freeSpaceEnd - h.freeSpaceStart()
}

// appendRecordSlot appends a new record slot to the page header
// It takes in the actual size of the record to be inserted
func (header *slottedRecordPageHeader) appendRecordSlot(actualRecordSize types.Offset) error {
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
	block  types.Block
	layout Layout
}

// recordHeader represents the header stored before each record on disk
// It stores the offsets of the ends of each field to allow direct access
// todo: this can be represented by an array of 16 bit ints to save space
// and possibly speed up access, once support for smaller ints is added
type recordHeader struct {
	// ends stores the offsets of the ends of each field in the record
	ends []types.Offset

	txinfo recordHeaderTxInfo
}

type recordHeaderTxInfo struct {
	// xmin stores the transaction id that created the record
	xmin types.TxID
	// xmax stores the transaction id that deleted the record
	xmax types.TxID
	// txop stores the operation number of the transaction that created or deleted the record
	txop types.SmallInt
	// flags stores additional flags for the record
	flags recordHeaderFlag
}

type recordHeaderFlag types.SmallInt

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
const recordHeaderTxInfoSize types.Size = 2*types.SizeOfTxID + 2*types.SizeOfSmallInt

// NewSlottedRecordPage creates a new SlottedRecordPage struct
func NewSlottedRecordPage(tx tx.Transaction, block types.Block, layout Layout) *SlottedRecordPage {
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

func (p *SlottedRecordPage) Block() types.Block {
	return p.block
}

// recordHeaderSize returns the size of the record header
// which includes the transaction info and the ends of the fields
func (p *SlottedRecordPage) recordHeaderSize() types.Offset {
	return types.Offset(recordHeaderTxInfoSize) +
		types.Offset(p.layout.FieldsCount())*types.Offset(types.SizeOfSmallInt)
}

// recordSizeIncludingRecordHeader calculates the size of a record on disk including header
func (p *SlottedRecordPage) recordSizeIncludingRecordHeader(recordSize types.Offset) types.Offset {
	return p.recordHeaderSize() + recordSize
}

func (p *SlottedRecordPage) writeHeader(header slottedRecordPageHeader) error {
	if err := p.tx.SetFixedLen(
		p.block,
		blockNumberOffset,
		types.SizeOfLong,
		types.UnsafeIntegerToFixed[types.Long](types.SizeOfLong, header.blockNumber),
		true,
	); err != nil {
		return err
	}

	if err := p.tx.SetFixedLen(
		p.block,
		numSlotsOffset,
		types.SizeOfSmallInt,
		types.UnsafeIntegerToFixed[types.SmallInt](types.SizeOfSmallInt, types.SmallInt(len(header.entries))),
		true,
	); err != nil {
		return err
	}

	if err := p.tx.SetFixedLen(
		p.block,
		freeSpaceEndOffset,
		types.SizeOfOffset,
		types.UnsafeIntegerToFixed[types.Offset](types.SizeOfOffset, header.freeSpaceEnd),
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
			types.FixedLen(entry[:]),
			true,
		); err != nil {
			return err
		}

		offset += types.Offset(sizeOfRecordPageHeaderEntry)
	}

	return nil
}

func (p *SlottedRecordPage) readHeader() (slottedRecordPageHeader, error) {
	blockNum, err := p.tx.FixedLen(p.block, blockNumberOffset, types.SizeOfLong)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	numSlots, err := p.tx.FixedLen(p.block, numSlotsOffset, types.SizeOfSmallInt)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	freeSpaceEnd, err := p.tx.FixedLen(p.block, freeSpaceEndOffset, types.SizeOfOffset)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	numSlotsVal := types.UnsafeFixedToInteger[types.SmallInt](numSlots)
	entries := make([]slottedRecordPageHeaderEntry, numSlotsVal)

	offset := entriesOffset
	for i := range len(entries) {
		slot, err := p.tx.FixedLen(p.block, offset, sizeOfRecordPageHeaderEntry)
		if err != nil {
			return slottedRecordPageHeader{}, err
		}

		copy(entries[i][:], slot)

		offset += types.Offset(sizeOfRecordPageHeaderEntry)
	}

	return slottedRecordPageHeader{
		blockNumber:  types.UnsafeFixedToInteger[types.Long](blockNum),
		numSlots:     types.UnsafeFixedToInteger[types.SmallInt](numSlots),
		freeSpaceEnd: types.UnsafeFixedToInteger[types.Offset](freeSpaceEnd),
		entries:      entries,
	}, nil
}

func (p *SlottedRecordPage) writeRecordHeader(offset types.Offset, recordHeader recordHeader) error {
	if err := p.tx.SetFixedLen(
		p.block,
		offset,
		types.SizeOfTxID,
		types.UnsafeIntegerToFixed[types.TxID](types.SizeOfTxID, recordHeader.txinfo.xmin),
		true,
	); err != nil {
		return err
	}

	offset += types.Offset(types.SizeOfTxID)

	if err := p.tx.SetFixedLen(
		p.block,
		offset,
		types.SizeOfTxID,
		types.UnsafeIntegerToFixed[types.TxID](types.SizeOfTxID, recordHeader.txinfo.xmax),
		true,
	); err != nil {
		return err
	}

	offset += types.Offset(types.SizeOfTxID)

	if err := p.tx.SetFixedLen(
		p.block,
		offset,
		types.SizeOfInt,
		types.UnsafeIntegerToFixed[types.SmallInt](types.SizeOfSmallInt, recordHeader.txinfo.txop),
		true,
	); err != nil {
		return err
	}

	offset += types.Offset(types.SizeOfSmallInt)

	if err := p.tx.SetFixedLen(
		p.block,
		offset,
		types.SizeOfSmallInt,
		types.UnsafeIntegerToFixed[types.SmallInt](types.SizeOfSmallInt, types.SmallInt(recordHeader.txinfo.flags)),
		true,
	); err != nil {
		return err
	}

	offset += types.Offset(types.SizeOfSmallInt)

	for _, field := range recordHeader.ends {
		offset += types.Offset(types.SizeOfOffset)

		if err := p.tx.SetFixedLen(
			p.block,
			offset,
			types.SizeOfOffset,
			types.UnsafeIntegerToFixed[types.Offset](types.SizeOfOffset, field),
			true,
		); err != nil {
			return err
		}
	}

	return nil
}

func (p *SlottedRecordPage) readRecordHeader(offset types.Offset) (recordHeader, error) {
	xmin, err := p.tx.FixedLen(p.block, offset, types.SizeOfTxID)
	if err != nil {
		return recordHeader{}, err
	}

	offset += types.Offset(types.SizeOfTxID)

	xmax, err := p.tx.FixedLen(p.block, offset, types.SizeOfTxID)
	if err != nil {
		return recordHeader{}, err
	}

	offset += types.Offset(types.SizeOfTxID)

	txop, err := p.tx.FixedLen(p.block, offset, types.SizeOfSmallInt)
	if err != nil {
		return recordHeader{}, err
	}

	offset += types.Offset(types.SizeOfSmallInt)

	flags, err := p.tx.FixedLen(p.block, offset, types.SizeOfSmallInt)
	if err != nil {
		return recordHeader{}, err
	}

	offset += types.Offset(types.SizeOfSmallInt)

	ends := make([]types.Offset, 0, p.layout.FieldsCount())

	for range p.layout.FieldsCount() {
		offset += types.Offset(types.SizeOfOffset)

		end, err := p.tx.FixedLen(p.block, offset, types.SizeOfOffset)
		if err != nil {
			return recordHeader{}, err
		}

		v := types.UnsafeFixedToInteger[types.Offset](end)

		ends = append(ends, v)
	}

	return recordHeader{
		ends: ends,
		txinfo: recordHeaderTxInfo{
			xmin:  types.UnsafeFixedToInteger[types.TxID](xmin),
			xmax:  types.UnsafeFixedToInteger[types.TxID](xmax),
			txop:  types.UnsafeFixedToInteger[types.SmallInt](txop),
			flags: recordHeaderFlag(types.UnsafeFixedToInteger[types.SmallInt](flags)),
		},
	}, nil
}

// entry returns the entry of the record pointed to by the given slot
func (p *SlottedRecordPage) entry(slot types.SmallInt) (slottedRecordPageHeaderEntry, error) {
	offset := entriesOffset + types.Offset(headerEntrySize)*types.Offset(slot)
	v, err := p.tx.FixedLen(p.block, offset, headerEntrySize)
	if err != nil {
		return slottedRecordPageHeaderEntry{}, err
	}

	return slottedRecordPageHeaderEntry(v), nil
}

func (p *SlottedRecordPage) setFlag(slot types.SmallInt, flag flag) error {
	entry, err := p.entry(slot)
	if err != nil {
		return err
	}

	entry.setFlag(flag)

	return nil
}

// fieldOffset returns the offset of the field for the record pointed by the given slot
// the value is stored in the record header, in the ends array of offsets
func (p *SlottedRecordPage) fieldOffset(slot types.SmallInt, fieldname string) (types.Offset, error) {
	entry, err := p.entry(slot)
	if err != nil {
		return 0, err
	}

	// read the record header to find the requested field
	fieldIndex := p.layout.FieldIndex(fieldname)
	if fieldIndex == -1 {
		return 0, fmt.Errorf("invalid field %s for record", fieldname)
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
		types.Offset(recordHeaderTxInfoSize) +
		types.Offset(prevIndex)*types.Offset(types.SizeOfSmallInt)

	fo, err := p.tx.FixedLen(p.block, fieldOffset, types.SizeOfOffset)
	if err != nil {
		return 0, err
	}

	return types.UnsafeFixedToInteger[types.Offset](fo), nil
}

// Int returns the value of an integer field for the record pointed by the given slot
func (p *SlottedRecordPage) FixedLen(slot types.SmallInt, fieldname string) (types.FixedLen, error) {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return nil, err
	}

	size := p.layout.FieldSize(fieldname)

	return p.tx.FixedLen(p.block, offset, size)
}

// String returns the value of a string field for the record pointed by the given slot
func (p *SlottedRecordPage) VarLen(slot types.SmallInt, fieldname string) (types.Varlen, error) {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return types.Varlen{}, err
	}

	return p.tx.VarLen(p.block, offset)
}

func (p *SlottedRecordPage) updateFieldEnd(slot types.SmallInt, fieldname string, fieldEnd types.Offset) error {
	fieldIndex := p.layout.FieldIndex(fieldname)
	if fieldIndex == -1 {
		return fmt.Errorf("invalid field %s for record", fieldname)
	}

	entry, err := p.entry(slot)
	if err != nil {
		return err
	}

	fieldOffsetEntry := types.Offset(recordHeaderTxInfoSize) +
		entry.recordOffset() +
		types.Offset(fieldIndex)*types.Offset(types.SizeOfOffset)

	return p.tx.SetFixedLen(
		p.block,
		fieldOffsetEntry,
		types.SizeOfSmallInt,
		types.UnsafeIntegerToFixed[types.Offset](types.SizeOfOffset, fieldEnd),
		true,
	)
}

// SetFixedLen sets the value of an integer field for the record pointed by the given slot
func (p *SlottedRecordPage) SetFixedLen(slot types.SmallInt, fieldname string, val types.FixedLen) error {
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
	fieldEnd := offset + types.Offset(size)

	return p.updateFieldEnd(slot, fieldname, fieldEnd)
}

// SetString sets the value of a string field for the record pointed by the given slot
func (p *SlottedRecordPage) SetVarLen(slot types.SmallInt, fieldname string, val types.Varlen) error {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return err
	}

	if err := p.tx.SetVarLen(p.block, offset, val, true); err != nil {
		return err
	}

	fieldEnd := offset + types.Offset(val.Size())

	return p.updateFieldEnd(slot, fieldname, fieldEnd)
}

// Delete flags the record's slot as empty by setting its flag
func (p *SlottedRecordPage) Delete(slot types.SmallInt) error {
	if err := p.setFlag(slot, flagDeletedRecord); err != nil {
		return err
	}

	entry, err := p.entry(slot)
	if err != nil {
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
		blockNumber:  types.Long(p.block.Number()),
		numSlots:     0,
		freeSpaceEnd: defaultFreeSpaceEnd,
	}

	return p.writeHeader(header)
}

// NextFrom returns the next used slot after the given one
// Returns ErrNoFreeSlot if such slot cannot be found within the transaction's block
func (p *SlottedRecordPage) NextFrom(slot types.SmallInt) (types.SmallInt, error) {
	return p.searchFrom(slot, flagInUseRecord, 0)
}

// InsertFrom returns the next empty slot starting at the given one such that
// it can hold the provided record size.
func (p *SlottedRecordPage) InsertFrom(slot types.SmallInt, recordSize types.Offset, update bool) (types.SmallInt, error) {
	nextSlot, err := p.searchFrom(slot, flagEmptyRecord, recordSize)
	// no empty slot found, try to append to the end
	if err == ErrNoFreeSlot {
		header, err := p.readHeader()
		if err != nil {
			return 0, err
		}

		// calculate the actual size of the record including the record header
		actualRecordSize := p.recordSizeIncludingRecordHeader(recordSize)

		// append a new slot to the page header for the record
		if err := header.appendRecordSlot(actualRecordSize); err == errNoFreeSpaceAvailable {
			return 0, ErrNoFreeSlot
		}

		if err := p.writeHeader(header); err != nil {
			return 0, err
		}

		recordHeader := recordHeader{
			ends: make([]types.Offset, p.layout.FieldsCount()),
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
			return 0, err
		}

		return header.numSlots - 1, nil
	}

	if err != nil {
		return 0, err
	}

	return nextSlot, nil
}

// searchFrom searches for the next empty slot starting from the given one with the provided flag such that the record fits
// If such a slot cannot be found within the block, it returns an ErrNoFreeSlot error.
// Otherwise it returns the slot index
// todo: we can optimise this by looking for the best slot available for the record of size, for example by picking
// the smallest empty slot that can fit the record rather than just the first one
func (p *SlottedRecordPage) searchFrom(slot types.SmallInt, flag flag, recordSize types.Offset) (types.SmallInt, error) {
	header, err := p.readHeader()
	if err != nil {
		return 0, err
	}

	for i := slot; i < header.numSlots; i++ {
		entry, err := p.entry(i)
		if err != nil {
			return 0, err
		}

		recordHeader, err := p.readRecordHeader(entry.recordOffset())
		if err != nil {
			return 0, err
		}

		if recordHeader.txinfo.xmin == p.tx.Id() && recordHeader.hasFlag(flagUpdated) {
			continue
		}

		if entry.flags() == flag && entry.recordLength() >= recordSize {
			return i, nil
		}
	}

	return 0, ErrNoFreeSlot
}
