package record

import (
	"errors"
	"fmt"
	"io"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

// bucketsNum is the static number of buckets supported by the index.
const bucketsNum = 100

// StaticHashIndex is an index that uses a fixed number bucketsNum of buckets.
// Each bucket is identified by a number from 0 to (bucketsNum - 1)
// which is concatenated with the index name to identify a set of disk blocks
// that contain the underlying indexed data.
// Buckets can be seen as tables and each bucket contains multiple blocks.
// Blocks are are linearly scanned using an underlying TableScan.
// As usual, blocks are accessed from disk and read into memory.
// The cost of finding a record depends on how many blocks are contained in a bucket.
// The index stores a record by putting it into the bucked assigned by an hash function.
// To retrieve a record, the index hashes the search key and looks in that bucket.
// To delete a record, the search key is hashed and the record is deleted from the bucket.
type StaticHashIndex struct {
	x         tx.Transaction
	name      string
	layout    Layout
	searchKey file.Value
	scan      *tableScan
}

func StaticHashIndexSearchCost(numBlocks int, recordsPerBucket int) int {
	return numBlocks / bucketsNum
}

func NewStaticHashIndex(x tx.Transaction, name string, layout Layout) *StaticHashIndex {
	return &StaticHashIndex{
		x:      x,
		name:   name,
		layout: layout,
	}
}

func (idx *StaticHashIndex) BeforeFirst(searchKey file.Value) error {
	idx.Close()
	idx.searchKey = searchKey
	bucket := searchKey.Hash() % bucketsNum
	tableName := fmt.Sprintf("%s%d", idx.name, bucket)
	idx.scan = newTableScan(idx.x, tableName, idx.layout)

	return nil
}

func (idx *StaticHashIndex) Next() error {
	if idx.scan == nil {
		return errors.New("index has no underlying table scan")
	}
	for {
		err := idx.scan.Next()
		if err == io.EOF {
			return io.EOF
		}

		if err != nil {
			return fmt.Errorf("index error scanning table: %w", err)
		}

		v, err := idx.scan.GetVal("dataval")
		if err != nil {
			return fmt.Errorf("invalid dataval for index: %w", err)
		}

		if v.Equals(idx.searchKey) {
			return nil
		}
	}
}

func (idx *StaticHashIndex) DataRID() (RID, error) {
	block, err := idx.scan.GetInt("block")
	if err != nil {
		return RID{}, err
	}

	id, err := idx.scan.GetInt("id")
	if err != nil {
		return RID{}, err
	}

	return NewRID(block, id), nil
}

func (idx *StaticHashIndex) Insert(v file.Value, rid RID) error {
	idx.BeforeFirst(v)
	if err := idx.scan.Insert(); err != nil {
		return fmt.Errorf("error inserting into tablescan: %w", err)
	}

	if err := idx.scan.SetInt("block", rid.Blocknum); err != nil {
		return fmt.Errorf("error setting block into tablescan: %w", err)
	}

	if err := idx.scan.SetInt("id", rid.Slot); err != nil {
		return fmt.Errorf("error setting id into tablescan: %w", err)
	}

	if err := idx.scan.SetVal("dataval", v); err != nil {
		return fmt.Errorf("error setting dataval into tablescan: %w", err)
	}

	return nil
}

func (idx *StaticHashIndex) Delete(v file.Value, rid RID) error {
	idx.BeforeFirst(v)
	for {
		err := idx.Next()
		if err != nil {
			return fmt.Errorf("error scanning for delete: %w", err)
		}

		datarid, err := idx.DataRID()
		if err != nil {
			return fmt.Errorf("error retrieving datarid: %w", err)
		}

		if datarid == rid {
			return idx.scan.Delete()
		}
	}
}

func (idx *StaticHashIndex) Close() {
	if idx.scan != nil {
		idx.scan.Close()
	}
}
