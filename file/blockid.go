package file

import "fmt"

type BlockID struct {
	filename    string
	blockNumber int
}

func NewBlockID(filename string, blockNumber int) BlockID {
	return BlockID{filename: filename, blockNumber: blockNumber}
}

func (bid BlockID) Filename() string {
	return bid.filename
}

func (bid BlockID) BlockNumber() int {
	return bid.blockNumber
}

func (bid BlockID) Equals(other BlockID) bool {
	return bid.filename == other.filename && bid.blockNumber == other.blockNumber
}

func (bid BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", bid.filename, bid.blockNumber)
}
