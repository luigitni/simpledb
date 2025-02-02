package engine

import (
	"errors"
	"fmt"
	"io"

	"github.com/luigitni/simpledb/storage"
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
	searchKey storage.Value
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

func (idx *StaticHashIndex) BeforeFirst(searchKey storage.Value) error {
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

		v, err := idx.scan.Val("dataval")
		if err != nil {
			return fmt.Errorf("invalid dataval for index: %w", err)
		}

		if v.Equals(idx.searchKey) {
			return nil
		}
	}
}

func (idx *StaticHashIndex) DataRID() (RID, error) {
	block, err := idx.scan.Val("block")
	if err != nil {
		return RID{}, err
	}

	id, err := idx.scan.Val("id")
	if err != nil {
		return RID{}, err
	}

	return NewRID(
		storage.ValueAsInteger[storage.Long](block),
		storage.ValueAsInteger[storage.SmallInt](id),
	), nil
}

func (idx *StaticHashIndex) Insert(v storage.Value, rid RID) error {
	idx.BeforeFirst(v)
	var size storage.Offset = storage.Offset(storage.SizeOfInt * 2)

	t := idx.scan.layout.schema.ftype("dataval")
	size += v.Size(t)

	if err := idx.scan.Insert(size); err != nil {
		return fmt.Errorf("error inserting into tablescan: %w", err)
	}

	if err := idx.scan.SetVal("block", storage.ValueFromInteger[storage.Long](storage.SizeOfLong, rid.Blocknum)); err != nil {
		return fmt.Errorf("error setting block into tablescan: %w", err)
	}

	if err := idx.scan.SetVal("id", storage.ValueFromInteger[storage.SmallInt](storage.SizeOfSmallInt, rid.Slot)); err != nil {
		return fmt.Errorf("error setting id into tablescan: %w", err)
	}

	if err := idx.scan.SetVal("dataval", v); err != nil {
		return fmt.Errorf("error setting dataval into tablescan: %w", err)
	}

	return nil
}

func (idx *StaticHashIndex) Delete(v storage.Value, rid RID) error {
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
