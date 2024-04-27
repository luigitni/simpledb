package record

import (
	"io"
	"math"

	"github.com/luigitni/simpledb/file"
	"github.com/luigitni/simpledb/tx"
)

var _ Index = &BTreeIndex{}

type BTreeIndex struct {
	x          tx.Transaction
	dirLayout  Layout
	leafLayout Layout
	leafTable  string
	leaf       *bTreeLeaf
	rootBlock  file.BlockID
}

func BTreeIndexSearchCost(numblocks int, recordsPerBucket int) int {
	return 1 + int(math.Log(float64(numblocks))/math.Log(float64(recordsPerBucket)))
}

func NewBTreeIndex(x tx.Transaction, idxName string, leafLayout Layout) (*BTreeIndex, error) {
	leafTable := idxName + "_leaf"

	size, err := x.Size(leafTable)
	if err != nil {
		return nil, err
	}

	if size == 0 {
		block, err := x.Append(leafTable)
		if err != nil {
			return nil, err
		}
		node := newBTreePage(x, block, leafLayout)
		defer node.Close()

		if err := node.format(block, -1); err != nil {
			return nil, err
		}
	}

	dirSchema := NewSchema()
	dirSchema.Add("block", leafLayout.schema)
	dirSchema.Add("dataval", leafLayout.schema)

	dirTable := idxName + "_dir"

	dirLayout := NewLayout(dirSchema)
	rootBlock := file.NewBlockID(dirTable, 0)

	dirSize, err := x.Size(dirTable)
	if err != nil {
		return nil, err
	}

	if dirSize == 0 {
		_, err := x.Append(dirTable)
		if err != nil {
			return nil, err
		}

		node := newBTreePage(x, rootBlock, dirLayout)
		defer node.Close()
		if err := node.format(rootBlock, 0); err != nil {
			return nil, err
		}

		fldType := dirSchema.Type("dataval")

		var minVal file.Value
		switch fldType {
		case file.INTEGER:
			minVal = file.ValueFromInt(math.MinInt)
		case file.STRING:
			minVal = file.ValueFromString("")
		}

		if err := node.insertDirectoryRecord(0, minVal, 0); err != nil {
			return nil, err
		}
	}

	return &BTreeIndex{
		x:          x,
		dirLayout:  dirLayout,
		leafLayout: leafLayout,
		leafTable:  leafTable,
		leaf:       nil,
		rootBlock:  rootBlock,
	}, nil
}

func (idx *BTreeIndex) Close() {
	if idx.leaf != nil {
		idx.leaf.Close()
	}
}

func (idx *BTreeIndex) BeforeFirst(key file.Value) error {
	idx.Close()

	root := newBTreeDir(idx.x, idx.rootBlock, idx.dirLayout)
	defer root.Close()

	blockNum, err := root.search(key)
	if err != nil {
		return err
	}

	leafBlock := file.NewBlockID(idx.leafTable, blockNum)
	leaf, err := newBTreeLeaf(idx.x, leafBlock, idx.leafLayout, key)
	if err != nil {
		return err
	}

	idx.leaf = leaf

	return nil
}

func (idx *BTreeIndex) Next() error {
	found, err := idx.leaf.next()
	if err != nil {
		return err
	}

	if !found {
		return io.EOF
	}

	return nil
}

func (idx *BTreeIndex) DataRID() (RID, error) {
	return idx.leaf.getDataRID()
}

func (idx *BTreeIndex) Insert(v file.Value, rid RID) error {
	if err := idx.BeforeFirst(v); err != nil {
		return err
	}

	e, err := idx.leaf.insert(rid)
	if err != nil {
		return err
	}

	if e == emptyDirEntry {
		return nil
	}

	root := newBTreeDir(idx.x, idx.rootBlock, idx.dirLayout)

	e, err = root.insert(e)
	if err != nil {
		return err
	}

	if err := root.makeNewRoot(e); err != nil {
		return err
	}

	root.Close()
	return nil
}

func (idx *BTreeIndex) Delete(v file.Value, rid RID) error {
	if err := idx.BeforeFirst(v); err != nil {
		return err
	}

	if err := idx.leaf.delete(rid); err != nil {
		return err
	}

	idx.leaf.Close()

	return nil
}
