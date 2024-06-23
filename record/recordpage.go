package record

import (
	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

const (
	EMPTY = 0
	USED  = 1
)

// RecordPage stores a record at a given location in a block.
// In SimpleDB pages records have the following fundamental properties:
// - unspanned: a whole record is fully contained within a single fixed-sized block
// - homogeneous: a block contains only one type of record
// - fixed-length: variable length values are truncated to the maximum allowed size.
// From the homogenous and fixed-length property, it follows that we can allocate
// the same amount of space for each record within a block.
// Because records are unspanned, they both have a maximum hard-limit length, which needs to be
// smaller than the block size.
// Furthermore, some space within a block might be wasted, as no record could fit in.
// Each slot begins with file.IntBytes bytes flags. Such flags indicate if the slot can be considered
// empty or is in use.
// Flag bytes are followed by the actual record.
// | flag 0 | Record 0 | flag 1 | Record 1 | ..... | flag N | Record N |
// ^-------------------^-------------------^
//
//	slot 0              slot 1
type RecordPage struct {
	tx     tx.Transaction
	block  file.BlockID
	layout Layout
}

func newRecordPage(tx tx.Transaction, block file.BlockID, layout Layout) *RecordPage {
	tx.Pin(block)
	return &RecordPage{
		tx:     tx,
		block:  block,
		layout: layout,
	}
}

func (p RecordPage) offset(slot int) int {
	return slot * p.layout.SlotSize()
}

// GetInt returns the integer value stored for the specified field of a specified slot
func (p RecordPage) GetInt(slot int, fieldname string) (int, error) {
	// compute the field offset within the page
	fieldpos := p.offset(slot) + p.layout.Offset(fieldname)
	return p.tx.GetInt(p.block, fieldpos)
}

// GetString returns the string value stored for the specified field of a specified slot
func (p RecordPage) GetString(slot int, fieldname string) (string, error) {
	fieldpos := p.offset(slot) + p.layout.Offset(fieldname)
	return p.tx.GetString(p.block, fieldpos)
}

// SetInt stores an integer value val for the specified field at the given slot
func (p RecordPage) SetInt(slot int, fieldname string, val int) error {
	fieldpos := p.offset(slot) + p.layout.Offset(fieldname)
	return p.tx.SetInt(p.block, fieldpos, val, true)
}

// SetString stores a string value val for the specified field at the given slot
func (p RecordPage) SetString(slot int, fieldname string, val string) error {
	fieldpos := p.offset(slot) + p.layout.Offset(fieldname)
	return p.tx.SetString(p.block, fieldpos, val, true)
}

// Delete flags the given slot as empty.
func (p RecordPage) Delete(slot int) error {
	return p.setFlag(slot, EMPTY)
}

// Format formats the block with default values for each field, as described by the schema.
func (p RecordPage) Format() error {
	slot := 0
	for {
		if !p.isValidSlot(slot) {
			break
		}
		p.tx.SetInt(p.block, p.offset(slot), EMPTY, false)
		schema := p.layout.Schema()
		for _, f := range schema.fields {
			fpos := p.offset(slot) + p.layout.Offset(f)
			switch schema.ftype(f) {
			case file.INTEGER:
				if err := p.tx.SetInt(p.block, fpos, 0, false); err != nil {
					return err
				}
			case file.STRING:
				if err := p.tx.SetString(p.block, fpos, "", false); err != nil {
					return err
				}
			}
		}
		slot++
	}
	return nil
}

// setFlag sets the record's empty/in use flag, at the beginning of the slot
func (p RecordPage) setFlag(slot int, flag int) error {
	return p.tx.SetInt(p.block, p.offset(slot), flag, true)
}

// NextAfter returns the next used slot after the provided one.
// Returns ErrNoFreeSlot if such slot cannot be found within the transaction's block
func (p RecordPage) NextAfter(slot int) (int, error) {
	return p.searchAfter(slot, USED)
}

// InsertAfter returns the next empty slot after the provided one.
// If such a slot is found, it flags it as USED, otherwise an ErrNoFreeSlot is returned
func (p RecordPage) InsertAfter(slot int) (int, error) {
	newslot, err := p.searchAfter(slot, EMPTY)
	if err != nil {
		return 0, err
	}

	if newslot >= 0 {
		p.setFlag(newslot, USED)
	}

	return newslot, nil
}

func (p RecordPage) Block() file.BlockID {
	return p.block
}

// searchAfter runs a linear search over the transaction block to find a slot
// with the provided flag.
// If such a slot cannot be found (within the block), it returns a ErrNoFreeSlot error.
// returns the slot index otherwise.
func (p RecordPage) searchAfter(slot int, flag int) (int, error) {
	slot++
	for p.isValidSlot(slot) {
		v, err := p.tx.GetInt(p.block, p.offset(slot))
		if err != nil {
			return 0, err
		}

		if v == flag {
			return slot, nil
		}

		slot++
	}
	return -1, nil
}

func (p RecordPage) isValidSlot(slot int) bool {
	return p.offset(slot+1) <= p.tx.BlockSize()
}
