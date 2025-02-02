package engine

import (
	"fmt"

	"github.com/luigitni/simpledb/storage"
)

// RID is an identifier of the record within a file.
// A RID consists of the block number in the file,
// and the location of the record in that block.
type RID struct {
	Blocknum storage.Long
	Slot     storage.SmallInt
}

func NewRID(blocknum storage.Long, slot storage.SmallInt) RID {
	return RID{
		Blocknum: blocknum,
		Slot:     slot,
	}
}

func (r RID) String() string {
	return fmt.Sprintf("n:%ds:%d", r.Blocknum, r.Slot)
}
