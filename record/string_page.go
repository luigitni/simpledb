package record

import (
	"errors"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

var ErrNoFreeSlot = errors.New("no free slot available")

const maxChunkSize = 2048

const (
	numRecordsOffset   = 0
	freeSpaceEndOffset = file.IntSize
	entriesOffset      = freeSpaceEndOffset + file.IntSize
)

// Strings of variable length are stored in stringPages.
// stringPages are slotted pages:
// Each page contains a header that holds the following information:
// - number of record entries in the header
// - end of free space in the block
// Each header entry contains the offset of the record, its length and a continuation RID
// The latter is used to store strings longer than a single page can hold and points to the
// next page where the remainder of the string continues.
// If the string is contained in a single block, the RID is set to a termination value.
// The actual records are allocated contiguously in the block, starting from the end.
// If a record is inserted, space is allocated for it at the end of the free space
// and a header entry is appended.
// If a record is deleted, it's header entry is tombstoned (record length set to -1)
type stringPageHeader struct {
	numRecords   int
	freeSpaceEnd int
	entries      []headerEntry
}

func (header stringPageHeader) freeSpaceStart() int {
	return entriesOffset + header.numRecords*headerEntrySize
}

const (
	headerEntrySize = file.IntSize + ridSize
)

var terminationRID = RID{Blocknum: -1, Slot: -1}

type headerEntry struct {
	offset    int
	nextChunk RID
}

type stringPage struct {
	header stringPageHeader
	tx     tx.Transaction
	block  file.Block
}

func newStringPage(tx tx.Transaction, block file.Block) (*stringPage, error) {
	tx.Pin(block)

	p := &stringPage{
		tx:    tx,
		block: block,
	}

	header, err := p.readHeader()
	if err != nil {
		return nil, err
	}

	p.header = header
	return p, nil
}

func (p *stringPage) appendHeaderEntry(entry headerEntry) error {
	offset := p.header.freeSpaceStart()

	if err := p.tx.SetInt(p.block, offset, entry.offset, false); err != nil {
		return err
	}

	offset += file.IntSize
	if err := p.tx.SetInt(p.block, offset, entry.nextChunk.Blocknum, false); err != nil {
		return err
	}

	offset += file.IntSize
	if err := p.tx.SetInt(p.block, offset, entry.nextChunk.Slot, false); err != nil {
		return err
	}

	p.header.entries = append(p.header.entries, entry)

	p.header.numRecords++

	if err := p.tx.SetInt(p.block, 0, p.header.numRecords, false); err != nil {
		return err
	}

	return nil
}

func (p *stringPage) readHeader() (stringPageHeader, error) {
	numRecords, err := p.tx.Int(p.block, numRecordsOffset)
	if err != nil {
		return stringPageHeader{}, err
	}

	freeSpaceEnd, err := p.tx.Int(p.block, freeSpaceEndOffset)
	if err != nil {
		return stringPageHeader{}, err
	}

	entries := make([]headerEntry, numRecords)
	for i := 0; i < numRecords; i++ {
		offset := entriesOffset + headerEntrySize*i

		o, err := p.tx.Int(p.block, offset)
		if err != nil {
			return stringPageHeader{}, err
		}

		offset += file.IntSize

		blocknum, err := p.tx.Int(p.block, offset)
		if err != nil {
			return stringPageHeader{}, err
		}

		offset += file.IntSize

		slot, err := p.tx.Int(p.block, offset)
		if err != nil {
			return stringPageHeader{}, err
		}

		entries[i] = headerEntry{
			offset: o,
			nextChunk: RID{
				Blocknum: blocknum,
				Slot:     slot,
			},
		}
	}

	return stringPageHeader{
		numRecords:   numRecords,
		freeSpaceEnd: freeSpaceEnd,
		entries:      entries,
	}, nil
}

// stringFits checks if a string fits into the free space remaining on this page
func (p *stringPage) stringFits(v string) bool {
	return p.header.freeSpaceEnd-p.header.freeSpaceStart() >= file.StrLength(len(v))
}

// stringChunk retrieves a string chunk stored in the page at the specified slot
func (p *stringPage) stringChunk(slot int) (RID, string, error) {
	record := p.header.entries[slot]

	v, err := p.tx.String(p.block, record.offset)
	return record.nextChunk, v, err
}

// setStringChunk stores a string chunk at the end of the free space
// and updates the header and free space pointers.
// The chunk is stored at the end of the free space
// Chunks are stored in reverse order: the last chunk first, then then second last etc.
// This allows clients to efficient concatenate chunks into the final string
// by traversing the linked list of chunks from last to first.
// A pointer to the next chunk is stored in the header entry to quickly locate the next chunk
// when the string is being reassembled from chunks.
// The last chunk will have nextChunk set to a dummy terminationRID to signal the end of the string.
func (p *stringPage) setStringChunk(prevChunk RID, v string, lastChunk bool) (int, error) {

	offset := p.header.freeSpaceEnd - file.StrLength(len(v))

	if err := p.tx.SetString(p.block, offset+ridSize, v, false); err != nil {
		return -1, err
	}

	if err := p.tx.SetInt(p.block, file.IntSize, offset, false); err != nil {
		return -1, err
	}

	item := headerEntry{
		offset:    offset,
		nextChunk: prevChunk,
	}

	if lastChunk {
		item.nextChunk = terminationRID
	}

	if err := p.appendHeaderEntry(item); err != nil {
		return -1, err
	}

	return p.header.numRecords, nil
}
