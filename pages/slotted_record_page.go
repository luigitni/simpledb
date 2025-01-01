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

const headerEntrySize = types.IntSize

// slottedRecordPageHeaderEntry represents an entry in the page header.
// It is bitmasked to store:
// - the first 2 bytes store the offset of the record within the page
// - the next 2 bytes store the length of the record within the page
// - byte 5 and 6 store record flags (whether empty/deleted etc)
// - byte 7 and 8th byte are reserved
type slottedRecordPageHeaderEntry int

func (e slottedRecordPageHeaderEntry) recordOffset() int {
	bytes := types.IntToBytes(int(e))

	return types.BytesToInt(bytes[:2])
}

func (e slottedRecordPageHeaderEntry) recordLength() int {
	bytes := types.IntToBytes(int(e))
	return types.BytesToInt(bytes[2:4])
}

func (e slottedRecordPageHeaderEntry) flags() flag {
	bytes := types.IntToBytes(int(e))
	return flag(types.BytesToInt(bytes[4:6]))
}

func (e slottedRecordPageHeaderEntry) setOffset(offset int) slottedRecordPageHeaderEntry {
	bytes := types.IntToBytes(int(e))
	ob := types.IntToBytes(int(offset))
	copy(bytes[:2], ob[:2])
	return slottedRecordPageHeaderEntry(types.BytesToInt(bytes))
}

func (e slottedRecordPageHeaderEntry) setLength(length int) slottedRecordPageHeaderEntry {
	bytes := types.IntToBytes(int(e))
	lb := types.IntToBytes(int(length))
	copy(bytes[2:4], lb[:2])
	return slottedRecordPageHeaderEntry(types.BytesToInt(bytes))
}

func (e slottedRecordPageHeaderEntry) setFlag(f flag) slottedRecordPageHeaderEntry {
	bytes := types.IntToBytes(int(e))
	fb := types.IntToBytes(int(f))
	copy(bytes[4:6], fb[:2])
	return slottedRecordPageHeaderEntry(types.BytesToInt(bytes))
}

// slottedRecordPageHeader represents the header of a slotted record page
// It holds the metadata about the page and the slot array for the records.
type slottedRecordPageHeader struct {
	blockNumber  int
	numSlots     int
	freeSpaceEnd int
	entries      []slottedRecordPageHeaderEntry
}

const defaultFreeSpaceEnd = types.PageSize

const (
	blockNumberOffset  = 0
	numSlotsOffset     = blockNumberOffset + types.IntSize
	freeSpaceEndOffset = numSlotsOffset + types.IntSize
	entriesOffset      = freeSpaceEndOffset + types.IntSize
)

func (h slottedRecordPageHeader) freeSpaceStart() int {
	return h.lastSlotOffset() + headerEntrySize
}

func (h slottedRecordPageHeader) lastSlotOffset() int {
	return entriesOffset + h.numSlots*headerEntrySize
}

func (h slottedRecordPageHeader) freeSpaceAvailable() int {
	return h.freeSpaceEnd - h.freeSpaceStart()
}

func (header *slottedRecordPageHeader) appendRecordSlot(actualRecordSize int) error {
	if actualRecordSize > header.freeSpaceAvailable() {
		return errNoFreeSpaceAvailable
	}

	header.freeSpaceEnd -= actualRecordSize
	entry := slottedRecordPageHeaderEntry(0).
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
	ends []int

	txinfo recordHeaderTxInfo
}

type recordHeaderTxInfo struct {
	// xmin stores the transaction id that created the record
	xmin int
	// xmax stores the transaction id that deleted the record
	xmax int
	// txop stores the operation number of the transaction that created or deleted the record
	txop int
	// flags stores additional flags for the record
	flags recordHeaderFlag
}

type recordHeaderFlag int

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

const recordHeaderTxInfoSize = 4 * types.IntSize

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

func (p *SlottedRecordPage) recordHeaderSize() int {
	return recordHeaderTxInfoSize + p.layout.FieldsCount()*types.IntSize
}

// recordSizeIncludingRecordHeader calculates the size of a record on disk including header
func (p *SlottedRecordPage) recordSizeIncludingRecordHeader(originalSize int) int {
	return p.recordHeaderSize() + originalSize
}

func (p *SlottedRecordPage) writeHeader(header slottedRecordPageHeader) error {
	if err := p.tx.SetInt(p.block, blockNumberOffset, header.blockNumber, false); err != nil {
		return err
	}

	if err := p.tx.SetInt(p.block, numSlotsOffset, len(header.entries), false); err != nil {
		return err
	}

	if err := p.tx.SetInt(p.block, freeSpaceEndOffset, header.freeSpaceEnd, false); err != nil {
		return err
	}

	for i, entry := range header.entries {
		offset := entriesOffset + headerEntrySize*i
		if err := p.tx.SetInt(p.block, offset, int(entry), false); err != nil {
			return err
		}
	}

	return nil
}

func (p *SlottedRecordPage) readHeader() (slottedRecordPageHeader, error) {
	blockNum, err := p.tx.Int(p.block, blockNumberOffset)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	numSlots, err := p.tx.Int(p.block, numSlotsOffset)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	freeSpaceEnd, err := p.tx.Int(p.block, freeSpaceEndOffset)
	if err != nil {
		return slottedRecordPageHeader{}, err
	}

	entries := make([]slottedRecordPageHeaderEntry, numSlots)
	for i := 0; i < numSlots; i++ {
		offset := entriesOffset + headerEntrySize*i

		v, err := p.tx.Int(p.block, offset)
		if err != nil {
			return slottedRecordPageHeader{}, err
		}

		entries[i] = slottedRecordPageHeaderEntry(v)
	}

	return slottedRecordPageHeader{
		blockNumber:  blockNum,
		numSlots:     numSlots,
		freeSpaceEnd: freeSpaceEnd,
		entries:      entries,
	}, nil
}

func (p *SlottedRecordPage) writeRecordHeader(offset int, recordHeader recordHeader) error {
	if err := p.tx.SetInt(p.block, offset, recordHeader.txinfo.xmin, false); err != nil {
		return err
	}

	offset += types.IntSize

	if err := p.tx.SetInt(p.block, offset, recordHeader.txinfo.xmax, false); err != nil {
		return err
	}

	offset += types.IntSize

	if err := p.tx.SetInt(p.block, offset, recordHeader.txinfo.txop, false); err != nil {
		return err
	}

	offset += types.IntSize

	if err := p.tx.SetInt(p.block, offset, int(recordHeader.txinfo.flags), false); err != nil {
		return err
	}

	offset += types.IntSize

	for i, end := range recordHeader.ends {
		if err := p.tx.SetInt(p.block, offset+i*types.IntSize, end, false); err != nil {
			return err
		}
	}

	return nil
}

func (p *SlottedRecordPage) readRecordHeader(offset int) (recordHeader, error) {
	xmin, err := p.tx.Int(p.block, offset)
	if err != nil {
		return recordHeader{}, err
	}

	offset += types.IntSize

	xmax, err := p.tx.Int(p.block, offset)
	if err != nil {
		return recordHeader{}, err
	}

	offset += types.IntSize

	txop, err := p.tx.Int(p.block, offset)
	if err != nil {
		return recordHeader{}, err
	}

	offset += types.IntSize

	flags, err := p.tx.Int(p.block, offset)
	if err != nil {
		return recordHeader{}, err
	}

	ends := make([]int, 0, p.layout.FieldsCount())

	for i := 0; i < p.layout.FieldsCount(); i++ {
		offset += types.IntSize
		end, err := p.tx.Int(p.block, offset)
		if err != nil {
			return recordHeader{}, err
		}

		ends = append(ends, end)
	}

	return recordHeader{
		ends: ends,
		txinfo: recordHeaderTxInfo{
			xmin:  xmin,
			xmax:  xmax,
			txop:  txop,
			flags: recordHeaderFlag(flags),
		},
	}, nil
}

// entry returns the entry of the record pointed to by the given slot
func (p *SlottedRecordPage) entry(slot int) (slottedRecordPageHeaderEntry, error) {
	offset := entriesOffset + headerEntrySize*slot
	v, err := p.tx.Int(p.block, offset)
	if err != nil {
		return 0, err
	}

	return slottedRecordPageHeaderEntry(v), nil
}

func (p *SlottedRecordPage) setFlag(slot int, flag flag) error {
	entry, err := p.entry(slot)
	if err != nil {
		return err
	}
	entryOffset := entriesOffset + headerEntrySize*slot

	entry = entry.setFlag(flag)

	return p.tx.SetInt(p.block, entryOffset, int(entry), false)
}

// fieldOffset returns the offset of the field for the record pointed by the given slot
func (p *SlottedRecordPage) fieldOffset(slot int, fieldname string) (int, error) {
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
	fieldOffset := recordOffset + recordHeaderTxInfoSize + prevIndex*types.IntSize

	return p.tx.Int(p.block, fieldOffset)
}

// Int returns the value of an integer field for the record pointed by the given slot
func (p *SlottedRecordPage) Int(slot int, fieldname string) (int, error) {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return 0, err
	}

	return p.tx.Int(p.block, offset)
}

// String returns the value of a string field for the record pointed by the given slot
func (p *SlottedRecordPage) String(slot int, fieldname string) (string, error) {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return "", err
	}

	return p.tx.String(p.block, offset)
}

func (p *SlottedRecordPage) updateFieldEnd(slot int, fieldname string, fieldEnd int) error {
	fieldIndex := p.layout.FieldIndex(fieldname)
	if fieldIndex == -1 {
		return fmt.Errorf("invalid field %s for record", fieldname)
	}

	entry, err := p.entry(slot)
	if err != nil {
		return err
	}

	fieldOffsetEntry := recordHeaderTxInfoSize + entry.recordOffset() + fieldIndex*types.IntSize

	return p.tx.SetInt(p.block, fieldOffsetEntry, fieldEnd, false)
}

// SetInt sets the value of an integer field for the record pointed by the given slot
func (p *SlottedRecordPage) SetInt(slot int, fieldname string, val int) error {
	// get the offest of the field
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return err
	}

	// write the actual value at the offset
	if err := p.tx.SetInt(p.block, offset, val, true); err != nil {
		return err
	}

	// update the end of this field in the record header
	fieldEnd := offset + types.IntSize

	return p.updateFieldEnd(slot, fieldname, fieldEnd)
}

// SetString sets the value of a string field for the record pointed by the given slot
func (p *SlottedRecordPage) SetString(slot int, fieldname string, val string) error {
	offset, err := p.fieldOffset(slot, fieldname)
	if err != nil {
		return err
	}

	if err := p.tx.SetString(p.block, offset, val, true); err != nil {
		return err
	}

	fieldEnd := offset + types.StrLength(len(val))

	return p.updateFieldEnd(slot, fieldname, fieldEnd)
}

// Delete flags the record's slot as empty by setting its flag
func (p *SlottedRecordPage) Delete(slot int) error {
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
		blockNumber:  p.block.Number(),
		numSlots:     0,
		freeSpaceEnd: defaultFreeSpaceEnd,
	}

	return p.writeHeader(header)
}

// NextAfter returns the next used slot after the given one
// Returns ErrNoFreeSlot if such slot cannot be found within the transaction's block
func (p *SlottedRecordPage) NextAfter(slot int) (int, error) {
	return p.searchAfter(slot, flagInUseRecord, 0)
}

// InsertAfter returns the next empty slot after the given one such that
// it can hold the provided record size.
func (p *SlottedRecordPage) InsertAfter(slot int, recordSize int, update bool) (int, error) {
	nextSlot, err := p.searchAfter(slot, flagEmptyRecord, recordSize)
	// no empty slot found, try to append to the end
	if err == ErrNoFreeSlot {
		header, err := p.readHeader()
		if err != nil {
			return -1, err
		}

		// calculate the actual size of the record including the record header
		actualRecordSize := p.recordSizeIncludingRecordHeader(recordSize)

		// append a new slot to the page header for the record
		if err := header.appendRecordSlot(actualRecordSize); err == errNoFreeSpaceAvailable {
			return -1, ErrNoFreeSlot
		}

		if err := p.writeHeader(header); err != nil {
			return -1, err
		}

		recordHeader := recordHeader{
			ends: make([]int, p.layout.FieldsCount()),
			txinfo: recordHeaderTxInfo{
				xmin: p.tx.Id(),
				xmax: 0,
				txop: p.tx.Id(),
			},
		}

		if update {
			recordHeader = recordHeader.setFlag(flagUpdated)
		}

		// write the record header at the end of the free space
		if err := p.writeRecordHeader(header.freeSpaceEnd, recordHeader); err != nil {
			return -1, err
		}

		return header.numSlots - 1, nil
	}

	if err != nil {
		return -1, err
	}

	return nextSlot, nil
}

// searchAfter searches for the next empty slot after the given one with the provided flag such that the record fits
// If such a slot cannot be found within the block, it returns an ErrNoFreeSlot error.
// Otherwise it returns the slot index
// todo: we can optimise this by looking for the best slot available for the record of size, for example by picking
// the smallest empty slot that can fit the record rather than just the first one
func (p *SlottedRecordPage) searchAfter(slot int, flag flag, recordSize int) (int, error) {
	header, err := p.readHeader()
	if err != nil {
		return -1, err
	}

	for i := slot + 1; i < header.numSlots; i++ {
		entry, err := p.entry(i)
		if err != nil {
			return -1, err
		}

		recordHeader, err := p.readRecordHeader(entry.recordOffset())
		if err != nil {
			return -1, err
		}

		if recordHeader.txinfo.xmin == p.tx.Id() && recordHeader.hasFlag(flagUpdated) {
			continue
		}

		if entry.flags() == flag && entry.recordLength() >= recordSize {
			return i, nil
		}
	}

	return -1, ErrNoFreeSlot
}
