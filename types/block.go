package types

import (
	"fmt"
	"math"
)

const EOF = Long(math.MaxUint64)

type BlockID string

type Block struct {
	id       BlockID
	filename string
	number   Long
}

func NewBlock(filename string, number Long) Block {
	return Block{
		filename: filename,
		number:   number,
		id:       BlockID(fmt.Sprintf("f:%sb:%d", filename, number)),
	}
}

func (bid Block) ID() BlockID {
	return bid.id
}

func (bid Block) FileName() string {
	return bid.filename
}

func (bid Block) Number() Long {
	return bid.number
}

func (bid Block) Equals(other Block) bool {
	return bid.filename == other.filename && bid.number == other.number
}

func (bid Block) String() string {
	return fmt.Sprintf("file %q block %d", bid.filename, bid.number)
}
