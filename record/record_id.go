package record

import (
	"fmt"

	"github.com/luigitni/simpledb/file"
)

const ridSize = 2 * file.IntSize

// RID is an identifier of the record within a file.
// A RID consists of the block number in the file,
// and the location of the record in that block.
type RID struct {
	Blocknum int
	Slot     int
}

func NewRID(blocknum int, slot int) RID {
	return RID{
		Blocknum: blocknum,
		Slot:     slot,
	}
}

func (r RID) String() string {
	return fmt.Sprintf("n:%ds:%d", r.Blocknum, r.Slot)
}
