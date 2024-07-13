package file

import "fmt"

const EOF = -1

type BlockID string

type Block struct {
	id       BlockID
	filename string
	number   int
}

func NewBlock(filename string, number int) Block {
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

func (bid Block) Number() int {
	return bid.number
}

func (bid Block) Equals(other Block) bool {
	return bid.filename == other.filename && bid.number == other.number
}

func (bid Block) String() string {
	return fmt.Sprintf("file %q block %d", bid.filename, bid.number)
}
