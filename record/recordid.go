package record

import "fmt"

// RID is an identifier of the record within a file.
// A RID consists of the block number in the file,
// and the location of the record in that block.
type RID struct {
	blocknum int
	slot     int
}

func NewRID(blocknum int, slot int) RID {
	return RID{
		blocknum: blocknum,
		slot:     slot,
	}
}

func (r RID) String() string {
	return fmt.Sprintf("n:%ds:%d", r.blocknum, r.slot)
}
