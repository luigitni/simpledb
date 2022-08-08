package file

import "fmt"

const EOF = -1

type BlockID struct {
	filename    string
	blockNumber int
	stringId    string
}

func NewBlockID(filename string, blockNumber int) BlockID {
	return BlockID{
		filename:    filename,
		blockNumber: blockNumber,
		stringId:    fmt.Sprintf("f:%sb:%d", filename, blockNumber),
	}
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
	return bid.stringId
}
