package record

import (
	"fmt"

	"github.com/luigitni/simpledb/types"
)

const ridSize = 2 * types.IntSize

// RID is an identifier of the record within a file.
// A RID consists of the block number in the file,
// and the location of the record in that block.
type RID struct {
	Blocknum types.Long
	Slot     types.SmallInt
}

func NewRID(blocknum types.Long, slot types.SmallInt) RID {
	return RID{
		Blocknum: blocknum,
		Slot:     slot,
	}
}

func (r RID) String() string {
	return fmt.Sprintf("n:%ds:%d", r.Blocknum, r.Slot)
}
